import type { CacheAdapter } from "./cache.ts";

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };
export type DataRecord = { [key: string]: JsonValue };
export interface MetadataEntry {
  readonly property: string;
  readonly value: string;
  readonly attributes: Readonly<Record<string, string>>;
}
export interface SourceReference {
  readonly key: string;
  readonly url: string;
  readonly structureKey?: string;
  readonly structureUrl?: string;
}
export interface Metadata {
  /** Parsed publisher entries before a metadata repair hook runs. */
  readonly originalEntries: readonly MetadataEntry[];
  readonly entries: readonly MetadataEntry[];
  readonly properties: Readonly<Record<string, string>>;
  readonly sources: readonly SourceReference[];
  readonly url: string;
}
export interface StructureField {
  readonly name: string;
  readonly attributes: Readonly<Record<string, string>>;
}
export interface Structure {
  readonly url?: string;
  readonly fields: readonly StructureField[];
}
export interface Diagnostic {
  readonly code: string;
  readonly message: string;
  readonly source?: string;
  readonly record?: number;
  readonly line?: number;
}
export interface Progress {
  readonly phase: "download" | "parse";
  readonly snapshot: string;
  readonly bytes: number;
  readonly records?: number;
  readonly totalBytes?: number;
}
export interface HookContext {
  readonly baseUrl: string;
  readonly identifier: string;
  readonly signal: AbortSignal;
  readonly source?: SourceReference;
}
export interface RecordContext extends HookContext {
  readonly record: number;
  readonly line: number;
}
export interface VersionedHook<T> {
  readonly id: string;
  readonly version: string;
  readonly run: T;
}
export interface ProviderHooks {
  metadata?: VersionedHook<
    (
      entries: readonly MetadataEntry[],
      context: HookContext,
    ) => readonly MetadataEntry[] | Promise<readonly MetadataEntry[]>
  >;
  selectSource?: VersionedHook<
    (metadata: Metadata, context: HookContext) => SourceReference | Promise<SourceReference>
  >;
  repairStream?: VersionedHook<
    (bytes: AsyncIterable<Uint8Array>, context: HookContext) => AsyncIterable<Uint8Array>
  >;
  headers?: VersionedHook<
    (
      headers: readonly string[],
      context: HookContext,
    ) => readonly string[] | Promise<readonly string[]>
  >;
  record?: VersionedHook<
    (
      record: Readonly<Record<string, string>>,
      context: RecordContext,
    ) => DataRecord | null | Promise<DataRecord | null>
  >;
}
export interface CsvSettings {
  delimiter?: "," | ";" | "\t";
  encoding?: string;
  quote?: string | false;
  escape?: string;
  headers?: readonly string[];
  /** A header override replaces an existing header unless this is false. */
  hasHeader?: boolean;
  maxRecordSize?: number;
  sampleBytes?: number;
}
export interface ProviderSettings {
  metadataUrl?: string;
  metadataCsv?: CsvSettings;
  structureCsv?: CsvSettings;
  csv?: CsvSettings;
  structure?: "advisory" | "ignore";
  hooks?: ProviderHooks;
}
export interface ProviderProfile extends ProviderSettings {
  readonly id: string;
  readonly version: string;
  readonly origin: string;
  readonly identifier?: string;
}
export interface ManagerOptions extends ProviderSettings {
  baseUrl: string;
  identifier: string;
  profiles?: readonly ProviderProfile[];
  disabledProfiles?: readonly string[];
  cache?: CacheAdapter;
  cacheDirectory?: string;
  maxAgeMs?: number;
  retentionMs?: number;
  chunkBytes?: number;
  allowStale?: boolean;
  fetch?: typeof globalThis.fetch;
  requestTimeoutMs?: number;
  retries?: number;
  retryDelayMs?: number;
  maxMetadataBytes?: number;
  signal?: AbortSignal;
  onDiagnostic?: (diagnostic: Diagnostic) => void;
  onProgress?: (progress: Progress) => void;
}
export interface ConsumptionOptions {
  signal?: AbortSignal;
  forceDownload?: boolean;
  allowStale?: boolean;
  resumeFrom?: string;
  batchRecords?: number;
  batchBytes?: number;
}
export interface Batch<F extends "objects" | "jsonl" = "objects" | "jsonl"> {
  readonly format: F;
  readonly data: F extends "objects" ? DataRecord[] : string;
  readonly snapshot: string;
  /** Inclusive, one-based source records consumed, including filtered records. */
  readonly startRecord: number;
  readonly endRecord: number;
  readonly nextCursor: string;
}
