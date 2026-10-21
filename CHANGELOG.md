# Changelog

All notable changes to this project are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.1.0 — 2026-10-21

Initial public release.

### Added

- `OpendataManager` discovery through `meta.csv`, with advisory structure handling and configurable provider and dataset repairs.
- Streaming object batches, bulk JSONL, and complete JSON documents without collecting the dataset in memory.
- Resumable filesystem caching with immutable chunks, SHA-256 checksums, atomic manifests, and ranged recovery.
- Import cursors that pin the dataset, cache generation, parsing profile, and next source record, plus `CURSOR_EXPIRED` and `SOURCE_CHANGED` failure modes.
- Metadata freshness revalidation (24 hours by default), explicit `refresh()`, `forceDownload`, and opt-in stale-snapshot fallback.
- Structured diagnostics, progress callbacks, cancellation, retries, and typed `OpendataError` failures.
- Node.js 22+, ESM, CommonJS, and TypeScript declarations.

### Notes

- Custom backends implement the exported `CacheAdapter` contract; `FileCache` is the default.
- Profiles come from `profiles` only, applied provider scope then dataset scope, before explicit options.
- Publisher-specific repairs are user-supplied hooks. The supplied Minpromtorg dataset needs none.
- One worker must own a dataset at a time; distributed coordination belongs in the consuming service.

[0.1.0]: https://github.com/mishatre/opendata/releases/tag/v0.1.0
