import { Readable } from "node:stream";
import { FileCache } from "./cache.ts";
import { type CsvRow, csvRows } from "./csv.ts";
import { metadataFrom, selectSource, structureFrom } from "./metadata.ts";
import { type EffectiveSettings, resolveSettings } from "./profiles.ts";
import {
  type CacheState,
  type Discovery,
  type Snapshot,
  SnapshotStore,
  type Validators,
  validators,
  validatorsChanged,
} from "./snapshots.ts";
import type {
  Batch,
  ConsumptionOptions,
  DataRecord,
  HookContext,
  ManagerOptions,
  Metadata,
  SourceReference,
  Structure,
} from "./types.ts";
import {
  assertJson,
  freeze,
  type HttpOptions,
  hash,
  httpUrl,
  OpendataError,
  positive,
  request,
  smallResponse,
  stable,
} from "./util.ts";

interface Cursor {
  version: 1;
  dataset: string;
  snapshot: string;
  fingerprint: string;
  nextRecord: number;
}

/** Runtime limits and transport defaults, resolved once per manager. */
interface Runtime extends HttpOptions {
  maxAgeMs: number;
  retentionMs: number;
  chunkBytes: number;
  maxMetadataBytes: number;
}

/** Mutable consumption state, reported in diagnostics when processing fails. */
interface Position {
  source?: string;
  /** Source records seen so far, including filtered and already committed records. */
  records: number;
  line: number;
}

type ExportOptions = Omit<ConsumptionOptions, "resumeFrom"> & {
  format?: "jsonl" | "json";
};

function datasetIdentifier(value: string): string {
  if (!value || value === "." || value === ".." || /[/\\?#]/.test(value)) {
    throw new OpendataError("INVALID_IDENTIFIER", "Expected a single dataset identifier");
  }
  return value;
}

function runtime(options: ManagerOptions): Runtime {
  const retries = options.retries ?? 2;
  if (!Number.isSafeInteger(retries) || retries < 0) {
    throw new OpendataError("INVALID_OPTION", "retries must be a non-negative integer");
  }
  return {
    fetch: options.fetch ?? globalThis.fetch,
    requestTimeoutMs: positive(options.requestTimeoutMs ?? 300_000, "requestTimeoutMs"),
    retries,
    retryDelayMs: positive(options.retryDelayMs ?? 250, "retryDelayMs"),
    maxAgeMs: positive(options.maxAgeMs ?? 86_400_000, "maxAgeMs"),
    retentionMs: positive(options.retentionMs ?? 604_800_000, "retentionMs"),
    chunkBytes: positive(options.chunkBytes ?? 8 * 1024 * 1024, "chunkBytes"),
    maxMetadataBytes: positive(options.maxMetadataBytes ?? 4 * 1024 * 1024, "maxMetadataBytes"),
  };
}

/** Options that would otherwise fail late, during parsing or header resolution. */
function validateSettings(settings: EffectiveSettings): void {
  positive(settings.csv?.maxRecordSize ?? 1, "csv.maxRecordSize");
  positive(settings.csv?.sampleBytes ?? 1, "csv.sampleBytes");
  if (settings.csv?.hasHeader === false && !settings.csv.headers) {
    throw new OpendataError("INVALID_OPTION", "Headerless CSV requires csv.headers");
  }
}

/** Serializes a hook result as one JSONL line; empty when the record was filtered out. */
function serialize(value: DataRecord | null, maxRecordSize: number, fromCsv: boolean): string {
  if (value === null) return "";
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new OpendataError("INVALID_TRANSFORM", "Record hook must return an object or null");
  }
  // CSV cells are always strings, so only hook results need validation.
  if (!fromCsv) assertJson(value);
  const jsonl = `${JSON.stringify(value)}\n`;
  if (Buffer.byteLength(jsonl) > maxRecordSize * 8) {
    throw new OpendataError("OUTPUT_TOO_LARGE", "Transformed record exceeds the output size limit");
  }
  return jsonl;
}

export class OpendataManager {
  readonly baseUrl: string;
  readonly identifier: string;
  private readonly options: ManagerOptions;
  private readonly settings: EffectiveSettings;
  private readonly runtime: Runtime;
  private readonly store: SnapshotStore;
  private discovery!: Discovery;
  private busy = false;
  private staleSnapshotId: string | undefined;

  private constructor(options: ManagerOptions) {
    this.baseUrl = httpUrl(options.baseUrl.endsWith("/") ? options.baseUrl : `${options.baseUrl}/`);
    this.identifier = datasetIdentifier(options.identifier);
    this.options = { ...options };
    this.settings = resolveSettings({ ...options, baseUrl: this.baseUrl });
    validateSettings(this.settings);
    this.runtime = runtime(options);
    this.store = new SnapshotStore(
      options.cache ??
        new FileCache({
          directory: options.cacheDirectory ?? ".cache/opendata",
        }),
      hash(stable([this.baseUrl, this.identifier])),
    );
  }

  public static async create(options: ManagerOptions): Promise<OpendataManager> {
    const manager = new OpendataManager(options);
    await manager.refreshInternal(
      false,
      manager.signal(options.signal),
      options.allowStale ?? false,
    );
    return manager;
  }

  public get metadata(): Metadata {
    return this.discovery.metadata;
  }

  public get source(): SourceReference {
    return this.discovery.source;
  }

  public get structure(): Structure {
    return this.discovery.structure;
  }

  private signal(signal?: AbortSignal): AbortSignal {
    return AbortSignal.any([
      ...(this.options.signal ? [this.options.signal] : []),
      ...(signal ? [signal] : []),
    ]);
  }

  private context(signal: AbortSignal, source?: SourceReference): HookContext {
    return {
      baseUrl: this.baseUrl,
      identifier: this.identifier,
      signal,
      ...(source ? { source } : {}),
    };
  }

  private warn(code: string, message: string, source?: string): void {
    this.options.onDiagnostic?.({
      code,
      message,
      ...(source ? { source } : {}),
    });
  }

  private enter(): void {
    if (this.busy) {
      throw new OpendataError("MANAGER_BUSY", "This manager already has an active operation");
    }
    this.busy = true;
  }

  /** Probes an existing source for ETag/Last-Modified changes without a body. */
  private async probe(
    source: SourceReference,
    previous: Validators,
    signal: AbortSignal,
  ): Promise<Validators> {
    const conditional = previous.etag
      ? { "If-None-Match": previous.etag }
      : previous.lastModified
        ? { "If-Modified-Since": previous.lastModified }
        : {};
    let response = await request(
      source.url,
      {
        method: "HEAD",
        headers: { "Accept-Encoding": "identity", ...conditional },
      },
      this.runtime,
      signal,
    );
    if (response.status === 405 || response.status === 501) {
      await response.body?.cancel();
      response = await request(
        source.url,
        {
          headers: {
            "Accept-Encoding": "identity",
            Range: "bytes=0-0",
            ...conditional,
          },
        },
        this.runtime,
        signal,
      );
    }
    try {
      if (response.status !== 304 && !response.ok) {
        throw new OpendataError("HTTP_ERROR", `HTTP ${response.status} validating dataset`, {
          source: source.url,
        });
      }
      return { ...previous, ...validators(response) };
    } finally {
      await response.body?.cancel();
    }
  }

  /** Discovers metadata, the selected source, advisory structure, and data validators. */
  private async discover(previous: Discovery | undefined, signal: AbortSignal): Promise<Discovery> {
    const metadataUrl = httpUrl(
      this.settings.metadataUrl ?? `opendata/${encodeURIComponent(this.identifier)}/meta.csv`,
      this.baseUrl,
    );
    const response = await request(metadataUrl, {}, this.runtime, signal);
    const metadata = await metadataFrom(
      await smallResponse(response, this.runtime.maxMetadataBytes),
      response.url || metadataUrl,
      this.settings,
      this.context(signal),
      (code, message) => this.warn(code, message, metadataUrl),
    );
    if (metadata.properties.identifier && metadata.properties.identifier !== this.identifier) {
      throw new OpendataError(
        "IDENTIFIER_MISMATCH",
        "Metadata identifier differs from the requested dataset",
        { source: metadataUrl },
      );
    }
    const chosen = this.settings.hooks?.selectSource
      ? await this.settings.hooks.selectSource.run(metadata, this.context(signal))
      : selectSource(metadata);
    const source: SourceReference = {
      ...chosen,
      url: httpUrl(chosen.url, metadata.url),
      ...(chosen.structureUrl ? { structureUrl: httpUrl(chosen.structureUrl, metadata.url) } : {}),
    };
    if (/\.(json|xml|zip|xlsx?)(?:$|\?)/i.test(source.url)) {
      throw new OpendataError(
        "UNSUPPORTED_FORMAT",
        "V1 supports CSV sources; select a CSV reference through a provider profile",
        { source: source.url },
      );
    }
    const structure = await this.structureFor(source, signal);
    // Initialization discovers metadata without issuing a data request. Revalidation probes an existing source.
    const dataValidators =
      previous && previous.source.url === source.url
        ? await this.probe(source, previous.validators, signal)
        : {};
    return {
      metadata,
      source,
      structure,
      signature: hash(
        stable({
          modified: metadata.properties.modified,
          key: source.key,
          url: source.url,
          structureKey: source.structureKey,
          structureUrl: source.structureUrl,
        }),
      ),
      checkedAt: Date.now(),
      fingerprint: this.settings.fingerprint,
      validators: dataValidators,
    };
  }

  /** Structure is advisory documentation; failures never block header-based parsing. */
  private async structureFor(source: SourceReference, signal: AbortSignal): Promise<Structure> {
    const empty: Structure = { fields: [] };
    if (this.settings.structure === "ignore") {
      return empty;
    }
    if (!source.structureUrl) {
      this.warn(
        "STRUCTURE_UNAVAILABLE",
        "No unambiguous structure reference; using the CSV header",
        source.url,
      );
      return empty;
    }
    try {
      const response = await request(source.structureUrl, {}, this.runtime, signal);
      return structureFrom(
        await smallResponse(response, this.runtime.maxMetadataBytes),
        source.structureUrl,
        this.settings.structureCsv,
      );
    } catch (error) {
      signal.throwIfAborted();
      this.warn(
        "STRUCTURE_UNAVAILABLE",
        error instanceof Error ? error.message : String(error),
        source.structureUrl,
      );
      return empty;
    }
  }

  private async refreshInternal(
    force: boolean,
    signal: AbortSignal,
    allowStale: boolean,
  ): Promise<void> {
    this.staleSnapshotId = undefined;
    signal.throwIfAborted();
    const state = await this.store.state();
    const previous =
      state.discovery?.fingerprint === this.settings.fingerprint ? state.discovery : undefined;
    if (!force && previous && Date.now() - previous.checkedAt < this.runtime.maxAgeMs) {
      this.discovery = freeze(previous);
      return;
    }
    try {
      const discovery = await this.discover(previous, signal);
      state.discovery = discovery;
      await this.store.saveState(state);
      this.discovery = freeze(discovery);
    } catch (error) {
      signal.throwIfAborted();
      const stale = allowStale ? await this.stale(state) : undefined;
      if (!stale) {
        throw error;
      }
      this.staleSnapshotId = stale.id;
      this.discovery = freeze(stale.discovery);
      this.warn(
        "STALE_CACHE",
        "Revalidation failed; explicitly using a completed cached snapshot",
        stale.discovery.source.url,
      );
    }
  }

  /** Newest cached snapshot that completed download under the current profile. */
  private async stale(state: CacheState): Promise<Snapshot | undefined> {
    const newest = [...state.generations].sort((a, b) => b.touchedAt - a.touchedAt);
    for (const { id } of newest) {
      const snapshot = await this.store.load(id);
      if (snapshot?.complete && snapshot.validatedProfiles.includes(this.settings.fingerprint)) {
        return snapshot;
      }
    }
    return undefined;
  }

  private cursor(snapshot: Snapshot, nextRecord: number): string {
    const cursor: Cursor = {
      version: 1,
      dataset: this.store.dataset,
      snapshot: snapshot.id,
      fingerprint: this.settings.fingerprint,
      nextRecord,
    };
    return Buffer.from(JSON.stringify(cursor)).toString("base64url");
  }

  private parseCursor(value: string): Cursor {
    let cursor: Cursor;
    try {
      if (value.length > 2048 || !/^[A-Za-z0-9_-]+$/.test(value))
        throw new Error("Invalid encoding");
      cursor = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as Cursor;
    } catch (error) {
      throw new OpendataError("INVALID_CURSOR", "Cannot decode cursor", {}, error);
    }
    if (
      cursor?.version !== 1 ||
      cursor.dataset !== this.store.dataset ||
      cursor.fingerprint !== this.settings.fingerprint ||
      typeof cursor.snapshot !== "string" ||
      !Number.isSafeInteger(cursor.nextRecord) ||
      cursor.nextRecord < 1
    )
      throw new OpendataError(
        "INCOMPATIBLE_CURSOR",
        "Cursor belongs to a different dataset or parsing profile, or is malformed",
      );
    return cursor;
  }

  /** Resolves the snapshot to read: the cursor's, a reusable generation, or a new one. */
  private async snapshot(
    options: ConsumptionOptions,
    signal: AbortSignal,
  ): Promise<{ snapshot: Snapshot; start: number }> {
    if (options.resumeFrom && options.forceDownload) {
      throw new OpendataError("INVALID_OPTION", "resumeFrom cannot be combined with forceDownload");
    }
    if (options.resumeFrom) {
      const cursor = this.parseCursor(options.resumeFrom);
      const pinned = await this.store.load(cursor.snapshot);
      if (!pinned) {
        throw new OpendataError("CURSOR_EXPIRED", "The cursor's snapshot is no longer cached");
      }
      await this.store.save(pinned);
      return { snapshot: pinned, start: cursor.nextRecord };
    }
    await this.refreshInternal(
      false,
      signal,
      options.allowStale ?? this.options.allowStale ?? false,
    );
    const state = await this.store.state();
    let snapshot = options.forceDownload ? undefined : await this.reusable(state);
    if (!snapshot) {
      snapshot = await this.store.create(this.discovery, this.runtime.chunkBytes);
      state.generations.push({ id: snapshot.id, touchedAt: Date.now() });
    }
    state.current = snapshot.id;
    await this.store.save(snapshot);
    await this.store.prune(state, this.runtime.retentionMs, snapshot.id);
    await this.store.saveState(state);
    return { snapshot, start: 1 };
  }

  /** A generation is reusable while its content matches and it is complete or recent. */
  private async reusable(state: CacheState): Promise<Snapshot | undefined> {
    const matches = (snapshot: Snapshot): boolean =>
      snapshot.discovery.signature === this.discovery.signature &&
      !validatorsChanged(snapshot.validators, this.discovery.validators) &&
      (snapshot.complete || Date.now() - snapshot.touchedAt < this.runtime.retentionMs);
    const preferred = this.staleSnapshotId ?? state.current;
    if (preferred) {
      const snapshot = await this.store.load(preferred);
      if (snapshot && matches(snapshot)) return snapshot;
    }
    for (const { id } of [...state.generations].reverse()) {
      const snapshot = await this.store.load(id);
      if (snapshot && matches(snapshot)) return snapshot;
    }
    return undefined;
  }

  private async *consume<F extends "objects" | "jsonl">(
    options: ConsumptionOptions & { format?: F },
  ): AsyncGenerator<Batch<F>> {
    this.enter();
    const controller = new AbortController();
    const signal = AbortSignal.any([this.signal(options.signal), controller.signal]);
    const position: Position = { records: 0, line: 0 };
    try {
      yield* this.rows(position, options, signal);
    } catch (error) {
      if (signal.aborted) {
        throw signal.reason;
      }
      throw this.failure(position, error);
    } finally {
      controller.abort();
      this.busy = false;
    }
  }

  /** Adds locations to a failure and reports it as a diagnostic. */
  private failure(position: Position, error: unknown): OpendataError {
    const detail = error as { lines?: number; records?: number };
    const known = error instanceof OpendataError ? error : undefined;
    const wrapped = new OpendataError(
      known?.code ?? "PROCESSING_ERROR",
      error instanceof Error ? error.message : String(error),
      {
        ...(position.source ? { source: position.source } : {}),
        record: detail.records !== undefined ? Math.max(1, detail.records) : position.records || 1,
        line: detail.lines ?? position.line,
        ...known?.diagnostic,
      },
      error,
    );
    this.options.onDiagnostic?.(wrapped.diagnostic);
    return wrapped;
  }

  /** Resolves, repairs, and validates the header row. */
  private async headers(
    row: CsvRow,
    snapshot: Snapshot,
    context: HookContext,
  ): Promise<readonly string[]> {
    const headers = this.settings.hooks?.headers
      ? await this.settings.hooks.headers.run(this.settings.csv?.headers ?? row.cells, context)
      : (this.settings.csv?.headers ?? row.cells);
    if (
      !headers.length ||
      headers.some((h) => typeof h !== "string" || !h.trim()) ||
      new Set(headers).size !== headers.length
    ) {
      throw new OpendataError("INVALID_HEADERS", "CSV headers must be nonempty and unique");
    }
    const { fields } = snapshot.discovery.structure;
    if (
      this.settings.structure !== "ignore" &&
      fields.length &&
      (fields.length !== headers.length || fields.some((f) => !headers.includes(f.name)))
    ) {
      this.warn(
        "STRUCTURE_MISMATCH",
        `Structure describes ${fields.length} fields; CSV contains ${headers.length}; using CSV headers`,
        snapshot.discovery.source.url,
      );
    }
    return headers;
  }

  /** Produces batches from the original bytes, resuming at the cursor's record. */
  private async *rows<F extends "objects" | "jsonl">(
    position: Position,
    options: ConsumptionOptions & { format?: F },
    signal: AbortSignal,
  ): AsyncGenerator<Batch<F>> {
    const maxRecords = positive(options.batchRecords ?? 1000, "batchRecords");
    const maxBytes = positive(options.batchBytes ?? 1024 * 1024, "batchBytes");
    const maxRecordSize = this.settings.csv?.maxRecordSize ?? 1024 * 1024;
    const hook = this.settings.hooks?.record;
    const format = (options.format ?? "objects") as F;
    const { snapshot, start } = await this.snapshot(options, signal);
    position.source = snapshot.discovery.source.url;
    const context = this.context(signal, snapshot.discovery.source);
    let bytes: AsyncIterable<Uint8Array> = this.store.bytes(
      snapshot,
      this.runtime,
      signal,
      this.options.onProgress,
    );
    if (this.settings.hooks?.repairStream) {
      bytes = this.settings.hooks.repairStream.run(bytes, context);
    }
    const report = (): void =>
      this.options.onProgress?.({
        phase: "parse",
        snapshot: snapshot.id,
        bytes: snapshot.bytes,
        records: position.records,
      });

    let headers: readonly string[] | undefined;
    let records: DataRecord[] = [];
    let lines: string[] = [];
    let size = 0;
    let batchStart = start;
    let batchEnd = start - 1;
    const batch = (): Batch<F> => ({
      format,
      data: (format === "jsonl" ? lines.join("") : records) as Batch<F>["data"],
      snapshot: snapshot.id,
      startRecord: batchStart,
      endRecord: batchEnd,
      nextCursor: this.cursor(snapshot, batchEnd + 1),
    });
    const reset = (): void => {
      records = [];
      lines = [];
      size = 0;
    };

    for await (const row of csvRows(bytes, this.settings.csv ?? {}, signal)) {
      position.line = row.line;
      if (!headers) {
        headers = await this.headers(row, snapshot, context);
        if (this.settings.csv?.hasHeader !== false) {
          continue;
        }
      }
      position.records++;
      if (row.cells.length !== headers.length) {
        throw new OpendataError(
          "INVALID_RECORD_WIDTH",
          `Expected ${headers.length} fields, received ${row.cells.length}`,
        );
      }
      const cells = Object.fromEntries(
        headers.map((header, i) => [header, row.cells[i]!]),
      ) as Record<string, string>;
      const transformed = hook
        ? await hook.run(cells, {
            ...context,
            record: position.records,
            line: row.line,
          })
        : cells;
      signal.throwIfAborted();
      // Replay hooks even for committed rows, preserving their deterministic state.
      if (position.records < start) {
        continue;
      }
      const jsonl = serialize(transformed, maxRecordSize, !hook);
      const rowSize = Buffer.byteLength(jsonl);
      if (batchEnd >= batchStart && size + rowSize > maxBytes) {
        yield batch();
        reset();
        batchStart = position.records;
      }
      batchEnd = position.records;
      if (jsonl) {
        records.push(transformed as DataRecord);
        lines.push(jsonl);
        size += rowSize;
      }
      if (batchEnd - batchStart + 1 >= maxRecords || size >= maxBytes) {
        report();
        yield batch();
        reset();
        batchStart = position.records + 1;
      }
    }
    if (!headers) {
      throw new OpendataError("MISSING_HEADER", "CSV has no header");
    }
    if (start > position.records + 1) {
      throw new OpendataError("INVALID_CURSOR", "Cursor is beyond the end of this snapshot");
    }
    if (!snapshot.complete) {
      throw new OpendataError(
        "INCOMPLETE_SOURCE",
        "Repair hook ended before the complete raw source was consumed",
      );
    }
    if (!snapshot.validatedProfiles.includes(this.settings.fingerprint)) {
      snapshot.validatedProfiles.push(this.settings.fingerprint);
    }
    await this.store.save(snapshot);
    await this.rememberValidators(snapshot);
    if (batchEnd >= batchStart) {
      yield batch();
    }
    report();
  }

  private async rememberValidators(snapshot: Snapshot): Promise<void> {
    const state = await this.store.state();
    if (
      state.current === snapshot.id &&
      state.discovery?.signature === snapshot.discovery.signature
    ) {
      // Keep the generation signature stable. Validators are used on the next scheduled check.
      state.discovery = { ...state.discovery, validators: snapshot.validators };
      await this.store.saveState(state);
      this.discovery = freeze(state.discovery);
    }
  }

  private async *exportChunks(options: ExportOptions): AsyncGenerator<string> {
    const { format = "jsonl", ...consumption } = options;
    if (format === "jsonl") {
      for await (const batch of this.batches({
        ...consumption,
        format: "jsonl",
      })) {
        if (batch.data) {
          yield batch.data;
        }
      }
      return;
    }
    yield "[";
    let first = true;
    for await (const batch of this.batches({
      ...consumption,
      format: "jsonl",
    })) {
      if (!batch.data) {
        continue;
      }
      yield (first ? "" : ",") + batch.data.trimEnd().split("\n").join(",");
      first = false;
    }
    yield "]\n";
  }

  public async refresh(
    options: { signal?: AbortSignal; allowStale?: boolean } = {},
  ): Promise<void> {
    this.enter();
    try {
      await this.refreshInternal(
        true,
        this.signal(options.signal),
        options.allowStale ?? this.options.allowStale ?? false,
      );
    } finally {
      this.busy = false;
    }
  }

  public batches<F extends "objects" | "jsonl" = "objects">(
    options: ConsumptionOptions & { format?: F } = {},
  ): AsyncGenerator<Batch<F>> {
    return this.consume(options);
  }

  public stream(options: ExportOptions = {}): Readable {
    return Readable.from(this.exportChunks(options), {
      objectMode: false,
      highWaterMark: 64 * 1024,
    });
  }

  public async toJSON(
    options: Omit<ConsumptionOptions, "resumeFrom"> & {
      maxRecords?: number;
    } = {},
  ): Promise<DataRecord[]> {
    if (options.maxRecords !== undefined) positive(options.maxRecords, "maxRecords");
    const { maxRecords, ...consumption } = options;
    const records: DataRecord[] = [];
    for await (const batch of this.batches({
      ...consumption,
      format: "objects",
    })) {
      if (maxRecords !== undefined && records.length + batch.data.length > maxRecords)
        throw new OpendataError("EXPORT_LIMIT", `Full JSON export exceeds ${maxRecords} records`);
      records.push(...batch.data);
    }
    return records;
  }
}
