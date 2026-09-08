import { Readable } from "node:stream";
import { FileCache } from "./cache.ts";
import { csvRows } from "./csv.ts";
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
	Diagnostic,
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

interface Settings extends HttpOptions {
	maxAgeMs: number;
	retentionMs: number;
	chunkBytes: number;
	maxMetadataBytes: number;
}

export class OpendataManager {
	readonly baseUrl: string;
	readonly identifier: string;
	private readonly options: ManagerOptions;
	private readonly settings: EffectiveSettings;
	private readonly runtime: Settings;
	private readonly store: SnapshotStore;
	private discovery!: Discovery;
	private busy = false;
	private staleSnapshotId: string | undefined;

	private constructor(options: ManagerOptions) {
		this.baseUrl = httpUrl(
			options.baseUrl.endsWith("/") ? options.baseUrl : `${options.baseUrl}/`,
		);
		if (
			!options.identifier ||
			options.identifier === "." ||
			options.identifier === ".." ||
			/[/\\?#]/.test(options.identifier)
		) {
			throw new OpendataError(
				"INVALID_IDENTIFIER",
				"Expected a single dataset identifier",
			);
		}
		this.identifier = options.identifier;
		this.options = { ...options };
		this.settings = resolveSettings({ ...options, baseUrl: this.baseUrl });
		this.runtime = {
			fetch: options.fetch ?? globalThis.fetch,
			requestTimeoutMs: positive(
				options.requestTimeoutMs ?? 300_000,
				"requestTimeoutMs",
			),
			retries: options.retries ?? 2,
			retryDelayMs: positive(options.retryDelayMs ?? 250, "retryDelayMs"),
			maxAgeMs: positive(options.maxAgeMs ?? 86_400_000, "maxAgeMs"),
			retentionMs: positive(options.retentionMs ?? 604_800_000, "retentionMs"),
			chunkBytes: positive(options.chunkBytes ?? 8 * 1024 * 1024, "chunkBytes"),
			maxMetadataBytes: positive(
				options.maxMetadataBytes ?? 4 * 1024 * 1024,
				"maxMetadataBytes",
			),
		};
		if (
			!Number.isSafeInteger(this.runtime.retries) ||
			this.runtime.retries < 0
		) {
			throw new OpendataError(
				"INVALID_OPTION",
				"retries must be a non-negative integer",
			);
		}
		positive(this.settings.csv?.maxRecordSize ?? 1, "csv.maxRecordSize");
		positive(this.settings.csv?.sampleBytes ?? 1, "csv.sampleBytes");
		if (this.settings.csv?.hasHeader === false && !this.settings.csv.headers) {
			throw new OpendataError(
				"INVALID_OPTION",
				"Headerless CSV requires csv.headers",
			);
		}
		this.store = new SnapshotStore(
			options.cache ??
				new FileCache({
					directory: options.cacheDirectory ?? ".cache/opendata",
				}),
			hash(stable([this.baseUrl, this.identifier])),
		);
	}

	public static async create(
		options: ManagerOptions,
	): Promise<OpendataManager> {
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
			throw new OpendataError(
				"MANAGER_BUSY",
				"This manager already has an active operation",
			);
		}
		this.busy = true;
	}

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
				throw new OpendataError(
					"HTTP_ERROR",
					`HTTP ${response.status} validating dataset`,
					{
						source: source.url,
					},
				);
			}
			return { ...previous, ...validators(response) };
		} finally {
			await response.body?.cancel();
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
			state.discovery?.fingerprint === this.settings.fingerprint
				? state.discovery
				: undefined;
		if (
			!force &&
			previous &&
			Date.now() - previous.checkedAt < this.runtime.maxAgeMs
		) {
			this.discovery = freeze(previous);
			return;
		}
		try {
			const metadataUrl = httpUrl(
				this.settings.metadataUrl ??
					`opendata/${encodeURIComponent(this.identifier)}/meta.csv`,
				this.baseUrl,
			);
			const response = await request(metadataUrl, {}, this.runtime, signal);
			const bytes = await smallResponse(
				response,
				this.runtime.maxMetadataBytes,
			);
			const metadata = await metadataFrom(
				bytes,
				response.url || metadataUrl,
				this.settings,
				this.context(signal),
				(code, message) => this.warn(code, message, metadataUrl),
			);
			if (
				metadata.properties.identifier &&
				metadata.properties.identifier !== this.identifier
			) {
				throw new OpendataError(
					"IDENTIFIER_MISMATCH",
					"Metadata identifier differs from the requested dataset",
					{ source: metadataUrl },
				);
			}
			const chosen = this.settings.hooks?.selectSource
				? await this.settings.hooks.selectSource.run(
						metadata,
						this.context(signal),
					)
				: selectSource(metadata);
			const source: SourceReference = {
				...chosen,
				url: httpUrl(chosen.url, metadata.url),
				...(chosen.structureUrl
					? { structureUrl: httpUrl(chosen.structureUrl, metadata.url) }
					: {}),
			};
			if (/\.(json|xml|zip|xlsx?)(?:$|\?)/i.test(source.url)) {
				throw new OpendataError(
					"UNSUPPORTED_FORMAT",
					"V1 supports CSV sources; select a CSV reference through a provider profile",
					{ source: source.url },
				);
			}
			let structure: Structure = { fields: [] };
			if (this.settings.structure !== "ignore") {
				if (source.structureUrl) {
					try {
						structure = structureFrom(
							await smallResponse(
								await request(source.structureUrl, {}, this.runtime, signal),
								this.runtime.maxMetadataBytes,
							),
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
					}
				} else
					this.warn(
						"STRUCTURE_UNAVAILABLE",
						"No unambiguous structure reference; using the CSV header",
						source.url,
					);
			}
			// Initialization discovers metadata without issuing a data request. Revalidation probes an existing source.
			const dataValidators =
				previous && previous.source.url === source.url
					? await this.probe(source, previous.validators, signal)
					: {};
			const signature = hash(
				stable({
					modified: metadata.properties.modified,
					key: source.key,
					url: source.url,
					structureKey: source.structureKey,
					structureUrl: source.structureUrl,
				}),
			);
			const discovery: Discovery = {
				metadata,
				source,
				structure,
				signature,
				checkedAt: Date.now(),
				fingerprint: this.settings.fingerprint,
				validators: dataValidators,
			};
			state.discovery = discovery;
			await this.store.saveState(state);
			this.discovery = freeze(discovery);
		} catch (error) {
			signal.throwIfAborted();
			if (allowStale) {
				const candidates = [...state.generations].sort(
					(a, b) => b.touchedAt - a.touchedAt,
				);
				for (const candidate of candidates) {
					const snapshot = await this.store.load(candidate.id);
					if (
						snapshot?.complete &&
						snapshot.validatedProfiles.includes(this.settings.fingerprint)
					) {
						this.staleSnapshotId = snapshot.id;
						this.discovery = freeze(snapshot.discovery);
						this.warn(
							"STALE_CACHE",
							"Revalidation failed; explicitly using a completed cached snapshot",
							snapshot.discovery.source.url,
						);
						return;
					}
				}
			}
			throw error;
		}
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
			cursor = JSON.parse(
				Buffer.from(value, "base64url").toString("utf8"),
			) as Cursor;
		} catch (error) {
			throw new OpendataError(
				"INVALID_CURSOR",
				"Cannot decode cursor",
				{},
				error,
			);
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

	private async snapshot(
		options: ConsumptionOptions,
		signal: AbortSignal,
	): Promise<{ snapshot: Snapshot; start: number }> {
		if (options.resumeFrom && options.forceDownload)
			throw new OpendataError(
				"INVALID_OPTION",
				"resumeFrom cannot be combined with forceDownload",
			);
		if (options.resumeFrom) {
			const cursor = this.parseCursor(options.resumeFrom);
			const snapshot = await this.store.load(cursor.snapshot);
			if (!snapshot)
				throw new OpendataError(
					"CURSOR_EXPIRED",
					"The cursor's snapshot is no longer cached",
				);
			await this.store.save(snapshot);
			return { snapshot, start: cursor.nextRecord };
		}
		await this.refreshInternal(
			false,
			signal,
			options.allowStale ?? this.options.allowStale ?? false,
		);
		const state = await this.store.state();
		let snapshot: Snapshot | undefined;
		if (!options.forceDownload) {
			const preferred = this.staleSnapshotId ?? state.current;
			if (preferred) snapshot = await this.store.load(preferred);
			const reusable = (s: Snapshot): boolean =>
				s.discovery.signature === this.discovery.signature &&
				!validatorsChanged(s.validators, this.discovery.validators) &&
				(s.complete || Date.now() - s.touchedAt < this.runtime.retentionMs);
			if (snapshot && !reusable(snapshot)) snapshot = undefined;
			if (!snapshot)
				for (const generation of [...state.generations].reverse()) {
					const candidate = await this.store.load(generation.id);
					if (candidate && reusable(candidate)) {
						snapshot = candidate;
						break;
					}
				}
		}
		if (!snapshot) {
			snapshot = await this.store.create(
				this.discovery,
				this.runtime.chunkBytes,
			);
			state.generations.push({ id: snapshot.id, touchedAt: Date.now() });
		}
		state.current = snapshot.id;
		await this.store.save(snapshot);
		await this.store.prune(state, this.runtime.retentionMs, snapshot.id);
		await this.store.saveState(state);
		return { snapshot, start: 1 };
	}

	private async *consume<F extends "objects" | "jsonl">(
		options: ConsumptionOptions & { format?: F },
	): AsyncGenerator<Batch<F>> {
		this.enter();
		const controller = new AbortController();
		const signal = AbortSignal.any([
			this.signal(options.signal),
			controller.signal,
		]);
		let currentSource: string | undefined;
		let count = 0;
		let line = 0;
		try {
			const maxRecords = positive(options.batchRecords ?? 1000, "batchRecords");
			const maxBytes = positive(
				options.batchBytes ?? 1024 * 1024,
				"batchBytes",
			);
			const { snapshot, start } = await this.snapshot(options, signal);
			currentSource = snapshot.discovery.source.url;
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
			let headers: readonly string[] | undefined;
			let records: DataRecord[] = [];
			let lines: string[] = [];
			let size = 0;
			let batchStart = start;
			let batchEnd = start - 1;
			const format = options.format ?? "objects";
			const batch = (): Batch<F> => ({
				format: format as F,
				data: (format === "jsonl"
					? lines.join("")
					: records) as Batch<F>["data"],
				snapshot: snapshot.id,
				startRecord: batchStart,
				endRecord: batchEnd,
				nextCursor: this.cursor(snapshot, batchEnd + 1),
			});
			for await (const row of csvRows(bytes, this.settings.csv ?? {}, signal)) {
				line = row.line;
				if (!headers) {
					headers = this.settings.csv?.headers ?? row.cells;
					if (this.settings.hooks?.headers) {
						headers = await this.settings.hooks.headers.run(headers, context);
					}
					if (
						!headers.length ||
						headers.some((h) => typeof h !== "string" || !h.trim()) ||
						new Set(headers).size !== headers.length
					) {
						throw new OpendataError(
							"INVALID_HEADERS",
							"CSV headers must be nonempty and unique",
						);
					}
					const fields = snapshot.discovery.structure.fields;
					if (
						this.settings.structure !== "ignore" &&
						fields.length &&
						(fields.length !== headers.length ||
							fields.some((f) => !headers!.includes(f.name)))
					) {
						this.warn(
							"STRUCTURE_MISMATCH",
							`Structure describes ${fields.length} fields; CSV contains ${headers.length}; using CSV headers`,
							currentSource,
						);
					}
					if (this.settings.csv?.hasHeader !== false) {
						continue;
					}
				}
				count++;
				if (row.cells.length !== headers.length) {
					throw new OpendataError(
						"INVALID_RECORD_WIDTH",
						`Expected ${headers.length} fields, received ${row.cells.length}`,
					);
				}
				const record = Object.fromEntries(
					headers.map((header, i) => [header, row.cells[i]!]),
				) as Record<string, string>;
				const transformed = this.settings.hooks?.record
					? await this.settings.hooks.record.run(record, {
							...context,
							record: count,
							line,
						})
					: record;
				signal.throwIfAborted();
				// Replay hooks even for committed rows, preserving their deterministic state.
				if (count < start) {
					continue;
				}
				let serialized = "";
				if (transformed !== null) {
					if (
						!transformed ||
						typeof transformed !== "object" ||
						Array.isArray(transformed)
					) {
						throw new OpendataError(
							"INVALID_TRANSFORM",
							"Record hook must return an object or null",
						);
					}
					if (this.settings.hooks?.record) {
						assertJson(transformed);
					}
					serialized = JSON.stringify(transformed) + "\n";
					if (
						Buffer.byteLength(serialized) >
						(this.settings.csv?.maxRecordSize ?? 1024 * 1024) * 8
					) {
						throw new OpendataError(
							"OUTPUT_TOO_LARGE",
							"Transformed record exceeds the output size limit",
						);
					}
				}
				const rowSize = Buffer.byteLength(serialized);
				if (batchEnd >= batchStart && size + rowSize > maxBytes) {
					yield batch();
					records = [];
					lines = [];
					size = 0;
					batchStart = count;
				}
				batchEnd = count;
				if (transformed !== null) {
					records.push(transformed);
					lines.push(serialized);
					size += rowSize;
				}
				if (batchEnd - batchStart + 1 >= maxRecords || size >= maxBytes) {
					this.options.onProgress?.({
						phase: "parse",
						snapshot: snapshot.id,
						bytes: snapshot.bytes,
						records: count,
					});
					yield batch();
					records = [];
					lines = [];
					size = 0;
					batchStart = count + 1;
				}
			}
			if (!headers) {
				throw new OpendataError("MISSING_HEADER", "CSV has no header");
			}
			if (start > count + 1) {
				throw new OpendataError(
					"INVALID_CURSOR",
					"Cursor is beyond the end of this snapshot",
				);
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
			this.options.onProgress?.({
				phase: "parse",
				snapshot: snapshot.id,
				bytes: snapshot.bytes,
				records: count,
			});
		} catch (error) {
			if (signal.aborted) {
				throw signal.reason;
			}
			const csvError = error as {
				lines?: number;
				records?: number;
				code?: string;
				message?: string;
			};
			const diagnostic: Omit<Diagnostic, "code" | "message"> = {
				...(currentSource ? { source: currentSource } : {}),
				record:
					csvError.records !== undefined
						? Math.max(1, csvError.records)
						: count || 1,
				line: csvError.lines ?? line,
			};
			const wrapped = new OpendataError(
				error instanceof OpendataError ? error.code : "PROCESSING_ERROR",
				error instanceof Error ? error.message : String(error),
				{
					...diagnostic,
					...(error instanceof OpendataError ? error.diagnostic : {}),
				},
				error,
			);
			this.options.onDiagnostic?.(wrapped.diagnostic);
			throw wrapped;
		} finally {
			controller.abort();
			this.busy = false;
		}
	}

	private async rememberValidators(snapshot: Snapshot): Promise<void> {
		const state: CacheState = await this.store.state();
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

	private async *exportChunks(
		options: Omit<ConsumptionOptions, "resumeFrom"> & {
			format?: "jsonl" | "json";
		},
	): AsyncGenerator<string> {
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
		} else {
			yield "[";
			let first = true;
			for await (const batch of this.batches({
				...consumption,
				format: "jsonl",
			})) {
				if (!batch.data) {
					continue;
				}
				const body = batch.data.slice(0, -1).split("\n").join(",");
				yield (first ? "" : ",") + body;
				first = false;
			}
			yield "]\n";
		}
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

	public stream(
		options: Omit<ConsumptionOptions, "resumeFrom"> & {
			format?: "jsonl" | "json";
		} = {},
	): Readable {
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
		if (options.maxRecords !== undefined)
			positive(options.maxRecords, "maxRecords");
		const { maxRecords, ...consumption } = options;
		const records: DataRecord[] = [];
		for await (const batch of this.batches({
			...consumption,
			format: "objects",
		})) {
			if (
				maxRecords !== undefined &&
				records.length + batch.data.length > maxRecords
			)
				throw new OpendataError(
					"EXPORT_LIMIT",
					`Full JSON export exceeds ${maxRecords} records`,
				);
			records.push(...batch.data);
		}
		return records;
	}
}
