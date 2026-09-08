import { createHash } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import type { Diagnostic } from "./types.ts";

export class OpendataError extends Error {
	readonly code: string;
	readonly diagnostic: Diagnostic;
	constructor(
		code: string,
		message: string,
		details: Omit<Diagnostic, "code" | "message"> = {},
		cause?: unknown,
	) {
		super(message, { cause });
		this.name = "OpendataError";
		this.code = code;
		this.diagnostic = { code, message, ...details };
	}
}

export function hash(value: string | Uint8Array): string {
	return createHash("sha256").update(value).digest("hex");
}

export function stable(value: unknown): string {
	if (Array.isArray(value)) return `[${value.map(stable).join(",")}]`;
	if (value && typeof value === "object") {
		const fields = Object.entries(value)
			.filter(([, v]) => v !== undefined)
			.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
		return (
			"{" +
			fields.map(([k, v]) => `${JSON.stringify(k)}:${stable(v)}`).join(",") +
			"}"
		);
	}
	return JSON.stringify(value);
}

export function positive(value: number, name: string): number {
	if (!Number.isSafeInteger(value) || value <= 0)
		throw new OpendataError(
			"INVALID_OPTION",
			`${name} must be a positive safe integer`,
		);
	return value;
}

/** Do not let JSON serialization silently coerce a hook's non-JSON values. */
export function assertJson(
	value: unknown,
	seen: Set<object> = new Set(),
	depth = 0,
): void {
	if (
		value === null ||
		typeof value === "string" ||
		typeof value === "boolean" ||
		(typeof value === "number" && Number.isFinite(value))
	)
		return;
	if (depth > 100 || typeof value !== "object" || seen.has(value))
		throw new OpendataError(
			"INVALID_TRANSFORM",
			"Record hook returned a non-JSON value, cycle, or excessive nesting",
		);
	if (
		!Array.isArray(value) &&
		Object.getPrototypeOf(value) !== Object.prototype &&
		Object.getPrototypeOf(value) !== null
	)
		throw new OpendataError(
			"INVALID_TRANSFORM",
			"Record hook must return plain JSON objects",
		);
	seen.add(value);
	for (const v of Array.isArray(value) ? value : Object.values(value))
		assertJson(v, seen, depth + 1);
	seen.delete(value);
}

export function httpUrl(value: string, base?: string): string {
	const url = new URL(value, base);
	if (
		!["http:", "https:"].includes(url.protocol) ||
		url.username ||
		url.password
	)
		throw new OpendataError(
			"INVALID_URL",
			"Expected an HTTP(S) URL without embedded credentials",
		);
	url.hash = "";
	return url.href;
}

export function freeze<T>(value: T): T {
	if (value && typeof value === "object" && !Object.isFrozen(value)) {
		Object.freeze(value);
		for (const v of Object.values(value)) freeze(v);
	}
	return value;
}

export interface HttpOptions {
	fetch: typeof globalThis.fetch;
	requestTimeoutMs: number;
	retries: number;
	retryDelayMs: number;
}

export async function request(
	url: string,
	init: RequestInit,
	options: HttpOptions,
	signal: AbortSignal,
): Promise<Response> {
	for (let attempt = 0; ; attempt++) {
		signal.throwIfAborted();
		try {
			const response = await options.fetch(url, {
				...init,
				signal: AbortSignal.any([
					signal,
					AbortSignal.timeout(options.requestTimeoutMs),
				]),
			});
			if (
				(response.status === 429 || response.status >= 500) &&
				attempt < options.retries
			) {
				await response.body?.cancel();
			} else return response;
		} catch (error) {
			if (signal.aborted || attempt >= options.retries) throw error;
		}
		await setTimeout(options.retryDelayMs * 2 ** attempt, undefined, {
			signal,
		});
	}
}

export async function* responseBytes(
	response: Response,
): AsyncGenerator<Uint8Array> {
	if (!response.body)
		throw new OpendataError("EMPTY_RESPONSE", "Response has no body");
	const reader = response.body.getReader();
	try {
		while (true) {
			const item = await reader.read();
			if (item.done) break;
			yield item.value;
		}
	} finally {
		await reader.cancel().catch(() => {});
		reader.releaseLock();
	}
}

export async function smallResponse(
	response: Response,
	limit: number,
): Promise<Buffer> {
	if (!response.ok) {
		await response.body?.cancel();
		throw new OpendataError("HTTP_ERROR", `HTTP ${response.status}`, {
			source: response.url,
		});
	}
	const parts: Uint8Array[] = [];
	let size = 0;
	for await (const part of responseBytes(response)) {
		size += part.length;
		if (size > limit)
			throw new OpendataError(
				"RESPONSE_TOO_LARGE",
				`Metadata/structure exceeds ${limit} bytes`,
			);
		parts.push(part);
	}
	return Buffer.concat(parts);
}
