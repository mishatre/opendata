# @gov-services/opendata

A Node.js 22+ TypeScript library for Russian government CSV open-data publications. It discovers the latest file from `meta.csv`, caches original bytes on disk, and streams records while downloading. ESM and CommonJS are supported.

The library handles ingestion. Your service owns SQL transactions, application queries, scheduling, and coordination between workers.

## Install

```sh
npm install @gov-services/opendata
```

Node.js 22 or newer is required. The package ships ES modules, CommonJS, and TypeScript declarations; no build step or bundler is needed.

## Start an import

```ts
import OpendataManager from "@gov-services/opendata";

const manager = await OpendataManager.create({
  baseUrl: "https://minpromtorg.gov.ru/",
  identifier: "1000000012-ReestrProducts",
  cacheDirectory: "/var/cache/my-service/opendata",
  onDiagnostic: (diagnostic) => console.warn(diagnostic),
});

console.log(manager.metadata.properties.title);
console.log(manager.source.url);

for await (const batch of manager.batches({ batchRecords: 1000 })) {
  await importBatch(batch.data, batch.nextCursor);
}
```

`create()` loads metadata and advisory structure information without downloading the dataset. A fresh cached metadata document avoids those requests. Consumption starts or resumes downloading. CommonJS: `const { OpendataManager } = require("@gov-services/opendata")`.

## Outputs

```ts
// Bulk JSONL: many complete JSON lines per batch.data string.
for await (const batch of manager.batches({ format: "jsonl", batchBytes: 1024 * 1024 })) {
  await sendChunk(batch.data, batch.nextCursor);
}

// Complete JSON document without collecting the dataset in memory.
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
await pipeline(manager.stream({ format: "json" }), createWriteStream("products.json"));

// Ordinary JSONL byte stream for files and HTTP responses.
await pipeline(manager.stream({ format: "jsonl" }), createWriteStream("products.jsonl"));

// Explicitly collects records in RAM. Exceeding the limit throws.
const smallDataset = await manager.toJSON({ maxRecords: 10_000 });
```

Each logical CSV record becomes one JSON object. Quoted newlines remain inside string values and are escaped in JSONL. Default batch limits are 1,000 source records or approximately 1 MiB of serialized JSONL. A single record may exceed `batchBytes`; records are never split. Filtering can produce empty batches with meaningful checkpoints.

`batches()` exposes `format`, `data`, `snapshot`, `startRecord`, `endRecord`, and `nextCursor`. Ranges are inclusive, one-based source positions, excluding the header and blank lines and including filtered records. Use `batches()` when checkpoint boundaries matter: Node byte streams may combine or split chunks and do not carry cursors.

Streams respect backpressure. Late network, parsing, or hook errors can follow emitted batches. A JSON document is complete only after successful stream completion; use a temporary file and rename afterward for atomic file publication.

## Checkpoints and SQL

```ts
const savedCursor = await loadPendingImportCursor();
for await (const batch of manager.batches({
  ...(savedCursor ? { resumeFrom: savedCursor } : {}),
  batchRecords: 1000,
})) {
  await db.transaction(async (tx) => {
    await tx.upsertStagingRows(batch.snapshot, batch.data);
    await tx.saveImportCursor(batch.nextCursor);
  });
}
await publishCompletedImport(); // Only after successful iterator completion.
```

An emitted cursor is a proposed checkpoint. Save it in the same SQL transaction as its records, using idempotent keys or snapshot-specific staging tables. On completion, retire the checkpoint so the next scheduled import can discover a newer snapshot. See the [Moleculer-style service example](examples/moleculer-service.ts).

Cursors pin the dataset, cache generation, effective parsing profile, and next source record. Resume replays original bytes from the beginning to reconstruct CSV and repair-stream state, suppressing committed rows. Replay has bounded memory but takes time proportional to the prefix. Profile/hook version changes reject an existing cursor; batch-limit changes are compatible.

Cursors are opaque trusted application state, not authentication tokens. Evicted snapshots produce `CURSOR_EXPIRED`. Newer metadata does not redirect an existing cursor to another snapshot.

## Cache, freshness, and recovery

Filesystem caching defaults to `.cache/opendata` relative to the working directory. Original bytes are stored in approximately 8 MiB immutable chunks with SHA-256 checksums and an atomic manifest. A chunk becomes durable before its records can be emitted. Raw download completion and successful parsing under a profile are separate states.

```ts
import { FileCache } from "@gov-services/opendata";

const manager = await OpendataManager.create({
  baseUrl,
  identifier,
  cache: new FileCache({ directory: "/var/cache/opendata" }),
  maxAgeMs: 24 * 60 * 60 * 1000,
  retentionMs: 7 * 24 * 60 * 60 * 1000,
});

await manager.refresh(); // Explicit metadata and HTTP-validator check.
for await (const batch of manager.batches({ forceDownload: true })) {
  await importBatch(batch.data, batch.nextCursor);
}
```

Freshness is checked on use after 24 hours by default. The module compares metadata modification value, selected version, data URL, and structure reference, and probes ETag/Last-Modified when revalidating the same source. HEAD is preferred; unsupported HEAD falls back to a small range GET. No timer runs in the background.

Unchanged metadata and validators cannot reveal every publisher correction; use `forceDownload` when needed. Without trustworthy lengths or validators, a syntactically valid truncated file cannot always be identified.

Outages fail by default while preserving cache. `allowStale: true`, on the manager or consumption call, permits using a completed snapshot when revalidation fails and emits `STALE_CACHE`. It never switches a replacement stream to old data after output begins.

Inactive generations and abandoned partial downloads are retained for seven days by default; cleanup occurs when starting imports. Current reusable and active generations are preserved. Choose retention longer than the longest intended interruption; this cache is not a permanent archive.

Recovery uses `Range`/`If-Range` only with a strong ETag and compatible transport encoding, validating the returned range and length. Otherwise, including ignored range requests, it redownloads and verifies every cached prefix chunk before accepting new bytes. Changed prefixes, validators, or known sizes fail with `SOURCE_CHANGED`.

One worker must own a dataset at a time, including across manager instances sharing storage. A manager rejects overlapping operations with `MANAGER_BUSY`. Distributed coordination belongs in your service.

### Custom storage

Custom backends implement `CacheAdapter`: `read`, atomic `write`, idempotent `delete`, and recursive `keys(prefix)`. These are storage infrastructure operations; the manager has no dataset search/list/get API. Backends must provide read-after-write consistency and preserve exact bytes. Pass any adapter as `cache`; `FileCache` is the default.

## Provider and dataset overrides

Generic behavior follows actual metadata URLs, normalizes equivalent timestamps such as `20260906` and `20260906T0000`, deduplicates identical references, and reports conflicts. Ambiguous latest-source selection fails unless a profile resolves it. `metadata.originalEntries` preserves parsed publisher entries before repair; `metadata.entries` contains the effective entries.

CSV headers define keys. Structure files are advisory and never drop columns or coerce values. Missing, malformed, and incomplete structures produce diagnostics. Strings, leading zeros, whitespace, empty strings, and `-` placeholders remain intact. Duplicate/empty headers, width mismatches, malformed quoting, invalid decoding, and oversized records fail unless repaired explicitly.

Delimiter detection considers comma, semicolon, and tab using a bounded sample; ambiguity requires an override. UTF-8 is the default, UTF-16 BOMs are recognized, and `csv.encoding` supports Windows-1251 and other Node `TextDecoder` encodings. `metadataCsv` and `structureCsv` independently configure those documents.

```ts
import type { ProviderProfile } from "@gov-services/opendata";

const profile: ProviderProfile = {
  id: "my-service/products",
  version: "1",
  origin: "https://minpromtorg.gov.ru",
  identifier: "1000000012-ReestrProducts", // Omit for all datasets on this origin.
  csv: { delimiter: ",", encoding: "utf-8" },
  hooks: {
    record: {
      id: "my-service/product-fields",
      version: "1",
      run(record) {
        return { ...record, INN: record.INN?.trim() ?? "" };
      },
    },
  },
};

const manager = await OpendataManager.create({ baseUrl, identifier, profiles: [profile] });
```

The example is an application-specific transform. The supplied Minpromtorg file needs no built-in repair. Tested publisher-specific rules can be added through the same mechanism.

Precedence: defaults → provider profile → dataset profile → explicit options. One profile may match each scope. Origins include scheme and port; identifiers match exactly. `disabledProfiles` ignores profile IDs. CSV settings merge by property; hooks replace earlier hooks rather than composing implicitly.

| Hook           | Input → output                                              |
| -------------- | ----------------------------------------------------------- |
| `metadata`     | Parsed metadata entries → repaired entries                  |
| `selectSource` | Normalized metadata → source reference                      |
| `repairStream` | Original byte async iterable → repaired byte async iterable |
| `headers`      | Header array → corrected header array                       |
| `record`       | String-valued record and position → JSON object or `null`   |

Each hook has an `id`, `version`, and `run` function. Metadata, source, header, and record hooks may be asynchronous and must be pure and deterministic. Raw-stream hooks may initialize per-invocation state, must handle arbitrary byte boundaries and backpressure, and must consume the full source. Original bytes are cached before repair. Line positions refer to the repaired CSV if a hook changes line breaks. Increment profile/hook versions whenever behavior changes; captured configuration is not introspected.

`csv.headers` replaces an existing header. For headerless sources also set `csv.hasHeader: false`. `csv.quote` and `csv.escape` customize quoting. No record skipping or repair is enabled implicitly.

## Operational controls

- `signal`: accepted for creation, refresh, and consumption. Cancellation preserves durable progress; pending atomic writes finish before cleanup.
- `onDiagnostic`: structured code, message, and available source/record/physical-line positions. Diagnostics never enter JSON output.
- `onProgress`: durable download bytes and parsed source records. Callbacks should be fast; their errors fail the operation.
- `requestTimeoutMs`: 300,000 per HTTP attempt including its body. Increase for slow sources or consumers.
- `retries`: 2 for transient requests and interrupted transfers; `retryDelayMs`: 250 with exponential backoff.
- `maxMetadataBytes`: 4 MiB per metadata/structure document. `csv.maxRecordSize`: 1 MiB of parser-buffer characters. Transformed JSON is capped at eight times that limit in bytes per record.
- `csv.sampleBytes`: 65,536 decoded characters for detection. `chunkBytes`: 8 MiB of original bytes for caching. Both are configurable.
- `fetch`: inject a compatible transport that honors abort signals and streams response bodies.

Errors use `OpendataError` with `code`, `diagnostic`, and `cause`; cancellation propagates the caller's abort reason. Failed imports must not publish staged rows.

## Development

```sh
pnpm install
pnpm check        # formatting, lint, and type checking
pnpm test         # unit and integration tests against a local fixture server
pnpm build        # ESM, CJS, and declaration output in dist/
pnpm package:check # publint and arethetypeswrong on the packed tarball

# Requires the supplied files in example/; writes .cache/full-sample-report.json.
pnpm test:full
```

Regular tests use a local HTTP fixture server without external services. The opt-in full-sample test asserts 513,856 records, 27 fields, 15,571 multiline records, output before EOF, and peak heap below 256 MiB.

Continuous integration runs `pnpm check`, `pnpm test`, `pnpm build`, and `pnpm package:check` on Node.js 22 and 24. Only distribution artifacts, README, license, and changelog are packaged; datasets, tests, credentials, and caches are excluded. Building and testing do not publish the package.

To release, bump the version and tag (`pnpm release`), push the tag, and let the publish workflow create a provenance-signed npm release. Manual publishing requires membership in the `gov-services` npm organization:

```sh
pnpm publish
```

`prepublishOnly` refuses to publish unless checks, tests, the build, `publint`, and `arethetypeswrong` all pass.
