import { randomUUID } from "node:crypto";
import type { Dirent } from "node:fs";
import { mkdir, open, readdir, readFile, rename, rm } from "node:fs/promises";
import { dirname, resolve, sep } from "node:path";
import { OpendataError } from "./util.ts";

/** Infrastructure only. Writes must atomically replace a key; reads must see complete values. */
export interface CacheAdapter {
	read(key: string): Promise<Uint8Array | undefined>;
	write(key: string, data: Uint8Array): Promise<void>;
	delete(key: string): Promise<void>;
	keys(prefix: string): AsyncIterable<string>;
}

export function validateKey(key: string): void {
	if (
		!/^[a-zA-Z0-9._/-]+$/.test(key) ||
		key.startsWith("/") ||
		key.split("/").some((p) => p === "." || p === ".." || !p)
	)
		throw new OpendataError("INVALID_CACHE_KEY", "Invalid cache key");
}

export class FileCache implements CacheAdapter {
	readonly directory: string;

	constructor(options: { directory: string }) {
		this.directory = resolve(options.directory);
	}

	private path(key: string): string {
		validateKey(key);
		return resolve(this.directory, key);
	}

	public async read(key: string): Promise<Uint8Array | undefined> {
		try {
			return await readFile(this.path(key));
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
			throw error;
		}
	}

	public async write(key: string, data: Uint8Array): Promise<void> {
		const path = this.path(key);
		await mkdir(dirname(path), { recursive: true });
		const temp = `${path}.${randomUUID()}.tmp`;
		const file = await open(temp, "wx");
		try {
			try {
				await file.writeFile(data);
				await file.sync();
			} finally {
				await file.close();
			}
			await rename(temp, path);
			const dir = await open(dirname(path), "r");
			try {
				await dir.sync();
			} finally {
				await dir.close();
			}
		} finally {
			await rm(temp, { force: true });
		}
	}

	public async delete(key: string): Promise<void> {
		await rm(this.path(key), { force: true });
	}

	public async *keys(prefix: string): AsyncGenerator<string> {
		validateKey(prefix);
		const walk = async function* (directory: string): AsyncGenerator<string> {
			let entries: Dirent[];
			try {
				entries = await readdir(directory, { withFileTypes: true });
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
				throw error;
			}
			for (const entry of entries) {
				const path = resolve(directory, entry.name);
				if (entry.isDirectory()) yield* walk(path);
				else if (entry.isFile() && !entry.name.endsWith(".tmp")) yield path;
			}
		};
		for await (const path of walk(this.path(prefix)))
			yield path
				.slice(this.directory.length + 1)
				.split(sep)
				.join("/");
	}
}

export async function readJson<T>(
	cache: CacheAdapter,
	key: string,
): Promise<T | undefined> {
	const bytes = await cache.read(key);
	if (!bytes) {
		return undefined;
	}
	try {
		return JSON.parse(Buffer.from(bytes).toString("utf8")) as T;
	} catch (error) {
		throw new OpendataError(
			"CACHE_CORRUPT",
			`Invalid cache document: ${key}`,
			{},
			error,
		);
	}
}

export async function writeJson(
	cache: CacheAdapter,
	key: string,
	data: unknown,
): Promise<void> {
	await cache.write(key, Buffer.from(JSON.stringify(data)));
}
