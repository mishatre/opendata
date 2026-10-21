import { randomUUID } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import { type CacheAdapter, readJson, writeJson } from "./cache.ts";
import type { Metadata, Progress, SourceReference, Structure } from "./types.ts";
import { type HttpOptions, hash, OpendataError, request, responseBytes } from "./util.ts";

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

function differs(a?: string, b?: string): boolean {
  return a !== undefined && b !== undefined && a !== b;
}

/** Only known values on both sides prove a change; unknown validators prove nothing. */
export function validatorsChanged(a: Validators, b: Validators): boolean {
  return differs(a.etag, b.etag) || differs(a.lastModified, b.lastModified);
}

/** A strong ETag is required for `If-Range`; weak or absent ETags cannot pin content. */
function strongEtag(value?: string): value is string {
  return value !== undefined && /^"[^"\r\n]*"$/.test(value);
}

function encoded(response: Response): boolean {
  const value = response.headers.get("content-encoding");
  return !!value && value !== "identity";
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

/** Fetches with an unencoded body so cached byte offsets stay comparable. */
function get(
  url: string,
  headers: Record<string, string>,
  options: HttpOptions,
  signal: AbortSignal,
): Promise<Response> {
  return request(url, { headers: { "Accept-Encoding": "identity", ...headers } }, options, signal);
}

/** Discards an unusable body before the caller throws. */
async function discard(response: Response): Promise<void> {
  await response.body?.cancel();
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
    const snapshot = await readJson<Snapshot>(this.cache, `${this.prefix}/${id}/manifest.json`);
    const inconsistent =
      snapshot &&
      (snapshot.schema !== 1 ||
        snapshot.id !== id ||
        snapshot.dataset !== this.dataset ||
        !Array.isArray(snapshot.chunks) ||
        snapshot.bytes !== snapshot.chunks.reduce((n, c) => n + c.size, 0));
    if (inconsistent) {
      throw new OpendataError("CACHE_CORRUPT", "Inconsistent snapshot manifest");
    }
    return snapshot;
  }

  async save(snapshot: Snapshot): Promise<void> {
    snapshot.touchedAt = Date.now();
    await writeJson(this.cache, `${this.prefix}/${snapshot.id}/manifest.json`, snapshot);
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

  async prune(state: CacheState, retentionMs: number, active?: string): Promise<void> {
    const keep: CacheState["generations"] = [];
    for (const generation of state.generations) {
      const retained =
        generation.id === active ||
        generation.id === state.current ||
        Date.now() - generation.touchedAt < retentionMs;
      if (retained) {
        keep.push(generation);
        continue;
      }
      const snapshot = await this.load(generation.id);
      if (snapshot && Date.now() - snapshot.touchedAt < retentionMs) {
        keep.push({ id: generation.id, touchedAt: snapshot.touchedAt });
        continue;
      }
      for await (const key of this.cache.keys(`${this.prefix}/${generation.id}`)) {
        await this.cache.delete(key);
      }
    }
    state.generations = keep;
  }

  /** Appends an immutable chunk; the manifest is durable before bytes are emitted. */
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
        throw new OpendataError("CACHE_CORRUPT", "Missing or corrupt cached chunk", {
          source: snapshot.discovery.source.url,
        });
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
        const interrupted = error instanceof OpendataError && error.code === "DOWNLOAD_INTERRUPTED";
        if (!interrupted || signal.aborted || attempt >= options.retries) {
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
    const fail = (code: string, message: string): OpendataError =>
      new OpendataError(code, message, { source: url });
    const ranged = offset > 0 && !snapshot.encoded && strongEtag(snapshot.validators.etag);
    let response = await get(
      url,
      ranged ? { Range: `bytes=${offset}-`, "If-Range": snapshot.validators.etag! } : {},
      options,
      signal,
    );
    if (response.status === 416) {
      await discard(response);
      response = await get(url, {}, options, signal);
    }
    const remote = validators(response);
    if (offset && validatorsChanged(snapshot.validators, remote)) {
      await discard(response);
      throw fail("SOURCE_CHANGED", "Source validators changed during recovery; start a new import");
    }
    let start = 0;
    let expected: number | undefined;
    let total: number | undefined;
    if (response.status === 206) {
      const match = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(response.headers.get("content-range") ?? "");
      const first = Number(match?.[1]);
      const last = Number(match?.[2]);
      const size = Number(match?.[3]);
      if (
        !ranged ||
        encoded(response) ||
        !match ||
        first !== offset ||
        last !== size - 1 ||
        remote.etag !== snapshot.validators.etag
      ) {
        await discard(response);
        throw fail("INVALID_RANGE", "Server returned an unsafe or inconsistent range response");
      }
      start = offset;
      total = size;
      expected = total - start;
      const length = contentLength(response);
      if (expected < 0 || (length !== undefined && length !== expected)) {
        await discard(response);
        throw fail("INVALID_RANGE", "Range length does not match response length");
      }
    } else if (response.status === 200) {
      expected = encoded(response) ? undefined : contentLength(response);
      total = expected;
    } else {
      await discard(response);
      throw fail("HTTP_ERROR", `HTTP ${response.status} fetching dataset`);
    }
    if (
      offset &&
      snapshot.totalBytes !== undefined &&
      total !== undefined &&
      snapshot.totalBytes !== total
    ) {
      await discard(response);
      throw fail("SOURCE_CHANGED", "Source size changed during recovery");
    }
    if (!offset) {
      snapshot.validators = remote;
      snapshot.encoded = encoded(response);
    }
    if (total !== undefined) {
      snapshot.totalBytes = total;
    }
    await this.save(snapshot);

    const report = (): void =>
      progress?.({
        phase: "download",
        snapshot: snapshot.id,
        bytes: snapshot.bytes,
        ...(total !== undefined ? { totalBytes: total } : {}),
      });
    const cached = [...snapshot.chunks];
    const body = responseBytes(response)[Symbol.asyncIterator]();
    let verify = 0;
    let received = 0;
    let position = start;
    let used = 0;
    let buffer = Buffer.allocUnsafe(snapshot.chunkBytes);
    try {
      while (true) {
        signal.throwIfAborted();
        let item: IteratorResult<Uint8Array>;
        try {
          item = await body.next();
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
          throw fail("INVALID_RESPONSE", "Dataset exceeds declared response length");
        }
        // Bytes below the cached offset are verified against cached checksums, not re-appended.
        for (let part = 0; part < item.value.length;) {
          const verifying = position < offset;
          const target = verifying ? cached[verify]!.size : snapshot.chunkBytes;
          const take = Math.min(target - used, item.value.length - part);
          buffer.set(item.value.subarray(part, part + take), used);
          used += take;
          part += take;
          if (used < target) {
            continue;
          }
          const bytes = buffer.subarray(0, used);
          if (verifying) {
            if (hash(bytes) !== cached[verify]!.hash) {
              throw fail("SOURCE_CHANGED", "Downloaded prefix differs from cached bytes");
            }
            verify++;
          } else {
            await this.chunk(snapshot, bytes);
            report();
            yield bytes;
          }
          position += used;
          used = 0;
          buffer = Buffer.allocUnsafe(snapshot.chunkBytes);
        }
      }
      if (expected !== undefined && received !== expected) {
        throw fail("DOWNLOAD_INTERRUPTED", "Dataset ended before its declared length");
      }
      if (position < offset) {
        throw fail("SOURCE_CHANGED", "Source ended before the cached prefix");
      }
      const tail = used ? buffer.subarray(0, used) : undefined;
      if (tail) {
        await this.chunk(snapshot, tail);
      }
      // EOF and length checks are complete even if the parser rejects the final chunk.
      snapshot.complete = true;
      await this.save(snapshot);
      report();
      if (tail) {
        yield tail;
      }
    } finally {
      await body.return?.(undefined);
    }
  }
}
