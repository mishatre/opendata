import { randomUUID } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import { type CacheAdapter, readJson, writeJson } from "./cache.ts";
import type {
	Metadata,
	Progress,
	SourceReference,
	Structure,
} from "./types.ts";
import {
	type HttpOptions,
	hash,
	OpendataError,
	request,
	responseBytes,
} from "./util.ts";

export interface Validators {
	etag?: string;
	lastModified?: string;
}

export interface Discovery {
	metadata: Metadata;
	source: SourceReference;
	structure: Structure;
	signature: string;
	checkedAt: number;
	fingerprint: string;
	validators: Validators;
}

interface Chunk {
	key: string;
	size: number;
	hash: string;
}

export interface Snapshot {
	schema: 1;
	id: string;
	dataset: string;
	discovery: Discovery;
	chunkBytes: number;
	chunks: Chunk[];
	bytes: number;
	complete: boolean;
	validatedProfiles: string[];
	validators: Validators;
	totalBytes?: number;
	encoded: boolean;
	touchedAt: number;
}

export interface CacheState {
	discovery?: Discovery;
	current?: string;
	generations: { id: string; touchedAt: number }[];
}

export function validators(response: Response): Validators {
	const etag = response.headers.get("etag");
	const lastModified = response.headers.get("last-modified");
	return {
		...(etag ? { etag } : {}),
		...(lastModified ? { lastModified } : {}),
	};
}

export function validatorsChanged(a: Validators, b: Validators): boolean {
	return (
		(a.etag !== undefined && b.etag !== undefined && a.etag !== b.etag) ||
		(a.lastModified !== undefined &&
			b.lastModified !== undefined &&
			a.lastModified !== b.lastModified)
	);
}

function strongEtag(value?: string): value is string {
	return value !== undefined && /^"[^"\r\n]*"$/.test(value);
}

function contentLength(response: Response): number | undefined {
	const value = response.headers.get("content-length");
	if (value === null) {
		return undefined;
	}
	const length = Number(value);
	if (!Number.isSafeInteger(length) || length < 0) {
		throw new OpendataError("INVALID_RESPONSE", "Invalid Content-Length");
	}
	return length;
}

export class SnapshotStore {
	readonly prefix: string;
	readonly cache: CacheAdapter;
	readonly dataset: string;

	constructor(cache: CacheAdapter, dataset: string) {
		this.cache = cache;
		this.dataset = dataset;
		this.prefix = `datasets/${dataset}`;
	}

	async state(): Promise<CacheState> {
		return (
			(await readJson<CacheState>(this.cache, `${this.prefix}/state.json`)) ?? {
				generations: [],
			}
		);
	}

	async saveState(state: CacheState): Promise<void> {
		await writeJson(this.cache, `${this.prefix}/state.json`, state);
	}

	async load(id: string): Promise<Snapshot | undefined> {
		if (!/^[a-f0-9-]{36}$/.test(id)) {
			throw new OpendataError("INVALID_CURSOR", "Invalid snapshot identity");
		}
		const snapshot = await readJson<Snapshot>(
			this.cache,
			`${this.prefix}/${id}/manifest.json`,
		);
		if (
			snapshot &&
			(snapshot.schema !== 1 ||
				snapshot.id !== id ||
				snapshot.dataset !== this.dataset ||
				!Array.isArray(snapshot.chunks) ||
				snapshot.bytes !== snapshot.chunks.reduce((n, c) => n + c.size, 0))
		) {
			throw new OpendataError(
				"CACHE_CORRUPT",
				"Inconsistent snapshot manifest",
			);
		}
		return snapshot;
	}

	async save(snapshot: Snapshot): Promise<void> {
		snapshot.touchedAt = Date.now();
		await writeJson(
			this.cache,
			`${this.prefix}/${snapshot.id}/manifest.json`,
			snapshot,
		);
	}

	async create(discovery: Discovery, chunkBytes: number): Promise<Snapshot> {
		const snapshot: Snapshot = {
			schema: 1,
			id: randomUUID(),
			dataset: this.dataset,
			discovery,
			chunkBytes,
			chunks: [],
			bytes: 0,
			complete: false,
			validatedProfiles: [],
			validators: {},
			encoded: false,
			touchedAt: Date.now(),
		};
		await this.save(snapshot);
		return snapshot;
	}

	async prune(
		state: CacheState,
		retentionMs: number,
		active?: string,
	): Promise<void> {
		const keep: CacheState["generations"] = [];
		for (const generation of state.generations) {
			if (
				generation.id === active ||
				generation.id === state.current ||
				Date.now() - generation.touchedAt < retentionMs
			) {
				keep.push(generation);
				continue;
			}
			const snapshot = await this.load(generation.id);
			if (snapshot && Date.now() - snapshot.touchedAt < retentionMs) {
				keep.push({ id: generation.id, touchedAt: snapshot.touchedAt });
				continue;
			}
			for await (const key of this.cache.keys(
				`${this.prefix}/${generation.id}`,
			)) {
				await this.cache.delete(key);
			}
		}
		state.generations = keep;
	}

	private async chunk(snapshot: Snapshot, bytes: Uint8Array): Promise<void> {
		const key = `${this.prefix}/${snapshot.id}/${snapshot.chunks.length}.bin`;
		await this.cache.write(key, bytes);
		snapshot.chunks.push({ key, size: bytes.length, hash: hash(bytes) });
		snapshot.bytes += bytes.length;
		await this.save(snapshot);
	}

	async *bytes(
		snapshot: Snapshot,
		options: HttpOptions,
		signal: AbortSignal,
		progress?: (progress: Progress) => void,
	): AsyncGenerator<Uint8Array> {
		// Replaying original bytes reconstructs parser and arbitrary deterministic repair-hook state.
		for (const chunk of snapshot.chunks) {
			signal.throwIfAborted();
			const bytes = await this.cache.read(chunk.key);
			if (!bytes || bytes.length !== chunk.size || hash(bytes) !== chunk.hash) {
				throw new OpendataError(
					"CACHE_CORRUPT",
					"Missing or corrupt cached chunk",
					{
						source: snapshot.discovery.source.url,
					},
				);
			}
			yield bytes;
		}
		if (snapshot.complete) {
			return;
		}
		for (let attempt = 0; ; attempt++) {
			try {
				yield* this.download(snapshot, options, signal, progress);
				return;
			} catch (error) {
				if (
					signal.aborted ||
					!(error instanceof OpendataError) ||
					error.code !== "DOWNLOAD_INTERRUPTED" ||
					attempt >= options.retries
				) {
					throw error;
				}
				await setTimeout(options.retryDelayMs * 2 ** attempt, undefined, {
					signal,
				});
			}
		}
	}

	private async *download(
		snapshot: Snapshot,
		options: HttpOptions,
		signal: AbortSignal,
		progress?: (progress: Progress) => void,
	): AsyncGenerator<Uint8Array> {
		const url = snapshot.discovery.source.url;
		const offset = snapshot.bytes;
		const range =
			offset > 0 && !snapshot.encoded && strongEtag(snapshot.validators.etag);
		let response = await request(
			url,
			{
				headers: {
					"Accept-Encoding": "identity",
					...(range
						? {
								Range: `bytes=${offset}-`,
								"If-Range": snapshot.validators.etag!,
							}
						: {}),
				},
			},
			options,
			signal,
		);
		if (response.status === 416) {
			await response.body?.cancel();
			response = await request(
				url,
				{ headers: { "Accept-Encoding": "identity" } },
				options,
				signal,
			);
		}
		const remote = validators(response);
		if (offset && validatorsChanged(snapshot.validators, remote)) {
			await response.body?.cancel();
			throw new OpendataError(
				"SOURCE_CHANGED",
				"Source validators changed during recovery; start a new import",
				{ source: url },
			);
		}
		let start = 0;
		let expected: number | undefined;
		let total: number | undefined;
		const encoded =
			!!response.headers.get("content-encoding") &&
			response.headers.get("content-encoding") !== "identity";
		if (response.status === 206) {
			const match = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(
				response.headers.get("content-range") ?? "",
			);
			if (
				!range ||
				encoded ||
				!match ||
				Number(match[1]) !== offset ||
				Number(match[2]) !== Number(match[3]) - 1 ||
				remote.etag !== snapshot.validators.etag
			) {
				await response.body?.cancel();
				throw new OpendataError(
					"INVALID_RANGE",
					"Server returned an unsafe or inconsistent range response",
					{ source: url },
				);
			}
			start = offset;
			total = Number(match[3]);
			expected = total - start;
			if (
				expected < 0 ||
				(contentLength(response) !== undefined &&
					contentLength(response) !== expected)
			) {
				await response.body?.cancel();
				throw new OpendataError(
					"INVALID_RANGE",
					"Range length does not match response length",
					{
						source: url,
					},
				);
			}
		} else if (response.status === 200) {
			expected = encoded ? undefined : contentLength(response);
			total = expected;
		} else {
			await response.body?.cancel();
			throw new OpendataError(
				"HTTP_ERROR",
				`HTTP ${response.status} fetching dataset`,
				{
					source: url,
				},
			);
		}
		if (
			offset &&
			snapshot.totalBytes !== undefined &&
			total !== undefined &&
			snapshot.totalBytes !== total
		) {
			await response.body?.cancel();
			throw new OpendataError(
				"SOURCE_CHANGED",
				"Source size changed during recovery",
				{
					source: url,
				},
			);
		}
		if (!offset) {
			snapshot.validators = remote;
			snapshot.encoded = encoded;
		}
		if (total !== undefined) {
			snapshot.totalBytes = total;
		}
		await this.save(snapshot);
		const existing = [...snapshot.chunks];
		let verifyIndex = 0;
		let received = 0;
		let position = start;
		let buffer = Buffer.allocUnsafe(snapshot.chunkBytes);
		let used = 0;
		const iterator = responseBytes(response)[Symbol.asyncIterator]();
		try {
			while (true) {
				signal.throwIfAborted();
				let item: IteratorResult<Uint8Array>;
				try {
					item = await iterator.next();
				} catch (error) {
					throw new OpendataError(
						"DOWNLOAD_INTERRUPTED",
						"Dataset transfer interrupted",
						{ source: url },
						error,
					);
				}
				if (item.done) {
					break;
				}
				received += item.value.length;
				if (expected !== undefined && received > expected) {
					throw new OpendataError(
						"INVALID_RESPONSE",
						"Dataset exceeds declared response length",
						{
							source: url,
						},
					);
				}
				let partOffset = 0;
				while (partOffset < item.value.length) {
					const verifying = position < offset;
					const target = verifying
						? existing[verifyIndex]!.size
						: snapshot.chunkBytes;
					const take = Math.min(target - used, item.value.length - partOffset);
					buffer.set(item.value.subarray(partOffset, partOffset + take), used);
					used += take;
					partOffset += take;
					if (used === target) {
						const bytes = buffer.subarray(0, used);
						if (verifying) {
							if (hash(bytes) !== existing[verifyIndex]!.hash) {
								throw new OpendataError(
									"SOURCE_CHANGED",
									"Downloaded prefix differs from cached bytes",
									{ source: url },
								);
							}
							verifyIndex++;
						} else {
							await this.chunk(snapshot, bytes);
							progress?.({
								phase: "download",
								snapshot: snapshot.id,
								bytes: snapshot.bytes,
								...(total !== undefined ? { totalBytes: total } : {}),
							});
							yield bytes;
						}
						position += used;
						buffer = Buffer.allocUnsafe(snapshot.chunkBytes);
						used = 0;
					}
				}
			}
			if (expected !== undefined && received !== expected) {
				throw new OpendataError(
					"DOWNLOAD_INTERRUPTED",
					"Dataset ended before its declared length",
					{ source: url },
				);
			}
			if (position < offset) {
				throw new OpendataError(
					"SOURCE_CHANGED",
					"Source ended before the cached prefix",
					{
						source: url,
					},
				);
			}
			const tail = used ? buffer.subarray(0, used) : undefined;
			if (tail) {
				await this.chunk(snapshot, tail);
			}
			// EOF and length checks are complete even if the parser rejects the final chunk.
			snapshot.complete = true;
			await this.save(snapshot);
			progress?.({
				phase: "download",
				snapshot: snapshot.id,
				bytes: snapshot.bytes,
				...(total !== undefined ? { totalBytes: total } : {}),
			});
			if (tail) {
				yield tail;
			}
		} finally {
			await iterator.return?.(undefined);
		}
	}
}
