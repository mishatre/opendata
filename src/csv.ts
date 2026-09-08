import type { TransformOptions } from "node:stream";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { type Options, parse } from "csv-parse";
import { parse as parseSync } from "csv-parse/sync";
import type { CsvSettings } from "./types.ts";
import { OpendataError } from "./util.ts";

function encoding(bytes: Uint8Array, explicit?: string): string {
	if (explicit) return explicit;
	if (bytes[0] === 0xff && bytes[1] === 0xfe) return "utf-16le";
	if (bytes[0] === 0xfe && bytes[1] === 0xff) return "utf-16be";
	return "utf-8";
}

export function decodeSmall(bytes: Uint8Array, explicit?: string): string {
	try {
		return new TextDecoder(encoding(bytes, explicit), { fatal: true }).decode(
			bytes,
		);
	} catch (error) {
		throw new OpendataError(
			"INVALID_ENCODING",
			"Cannot decode source; set an explicit encoding override if necessary",
			{},
			error,
		);
	}
}

async function* decode(
	bytes: AsyncIterable<Uint8Array>,
	explicit?: string,
): AsyncGenerator<string> {
	let decoder: TextDecoder | undefined;
	let prefix = Buffer.alloc(0);
	for await (const part of bytes) {
		let input: Uint8Array = part;
		if (!decoder) {
			prefix = Buffer.concat([prefix, part]);
			if (prefix.length < 3) continue;
			decoder = new TextDecoder(encoding(prefix, explicit), { fatal: true });
			input = prefix;
			prefix = Buffer.alloc(0);
		}
		let text: string;
		try {
			text = decoder.decode(input, { stream: true });
		} catch (error) {
			throw new OpendataError(
				"INVALID_ENCODING",
				"Invalid source encoding",
				{},
				error,
			);
		}
		if (text) yield text;
	}
	if (!decoder) {
		yield decodeSmall(prefix, explicit);
		return;
	}
	try {
		const end = decoder.decode();
		if (end) yield end;
	} catch (error) {
		throw new OpendataError(
			"INVALID_ENCODING",
			"Truncated encoded character",
			{},
			error,
		);
	}
}

/** End of the last complete quoted record within a bounded sample. */
function completePrefix(
	text: string,
	quote: string | false,
	escape: string,
): string {
	let quoted = false;
	let end = 0;
	for (let i = 0; i < text.length; i++) {
		const c = text[i];
		if (quote && quoted && c === escape && text[i + 1] === quote) {
			i++;
			continue;
		}
		if (quote && c === quote) {
			quoted = !quoted;
		}
		if (!quoted && (c === "\n" || c === "\r")) {
			end = i + 1;
		}
	}
	return text.slice(0, end);
}

export function detectDelimiter(
	sample: string,
	settings: CsvSettings = {},
	ended = false,
): "," | ";" | "\t" {
	if (settings.delimiter) return settings.delimiter;
	const quote = settings.quote ?? '"';
	const escape = settings.escape ?? '"';
	const text = ended ? sample : completePrefix(sample, quote, escape);
	const candidates = ([",", ";", "\t"] as const).filter((delimiter) => {
		try {
			const rows = parseSync(text, {
				delimiter,
				quote,
				escape,
				bom: true,
				skip_empty_lines: true,
				to: 10,
			}) as string[][];
			return rows.length > 0 && (rows[0]?.length ?? 0) > 1;
		} catch {
			return false;
		}
	});
	if (candidates.length !== 1)
		throw new OpendataError(
			"AMBIGUOUS_DELIMITER",
			"Cannot determine a unique CSV delimiter from the bounded sample; supply csv.delimiter",
		);
	return candidates[0]!;
}

export function smallCsv(
	bytes: Uint8Array,
	settings: CsvSettings = {},
): string[][] {
	const text = decodeSmall(bytes, settings.encoding);
	try {
		return parseSync(text, {
			delimiter: detectDelimiter(text, settings, true),
			bom: true,
			skip_empty_lines: true,
			max_record_size: settings.maxRecordSize ?? 1024 * 1024,
			quote: settings.quote ?? '"',
			escape: settings.escape ?? '"',
		}) as string[][];
	} catch (error) {
		if (error instanceof OpendataError) throw error;
		throw new OpendataError(
			"INVALID_CSV",
			"Cannot parse CSV document",
			{},
			error,
		);
	}
}

export interface CsvRow {
	cells: string[];
	line: number;
}

export async function* csvRows(
	bytes: AsyncIterable<Uint8Array>,
	settings: CsvSettings,
	signal: AbortSignal,
): AsyncGenerator<CsvRow> {
	const iterator = decode(bytes, settings.encoding)[Symbol.asyncIterator]();
	let sample = "";
	let rest = "";
	let ended = false;
	const limit = settings.sampleBytes ?? 64 * 1024;
	try {
		while (sample.length < limit) {
			const next = await iterator.next();
			if (next.done) {
				ended = true;
				break;
			}
			const remaining = limit - sample.length;
			sample += next.value.slice(0, remaining);
			rest = next.value.slice(remaining);
			if (settings.delimiter || rest) break;
		}
		if (/^\s*<(?:!doctype\s+html|html\b|head\b|body\b)/i.test(sample))
			throw new OpendataError(
				"UNEXPECTED_CONTENT",
				"Publisher returned an HTML page instead of CSV",
			);
		const delimiter = detectDelimiter(sample, settings, ended);
		const input = Readable.from(
			(async function* () {
				if (sample) yield sample;
				if (rest) yield rest;
				while (!ended) {
					signal.throwIfAborted();
					const next = await iterator.next();
					if (next.done) break;
					yield next.value;
				}
			})(),
			{ objectMode: false, highWaterMark: 64 * 1024 },
		);
		let physicalLine = 1;
		// Width is validated against repaired headers by the manager, without dropping surplus cells.
		const parserOptions: Options & TransformOptions = {
			delimiter,
			quote: settings.quote ?? '"',
			escape: settings.escape ?? '"',
			bom: true,
			cast: false,
			info: true,
			raw: true,
			on_record(item) {
				const row = item as unknown as { raw: string; info: { lines: number } };
				const breaks = row.raw.match(/\r\n|\r|\n/g)?.length ?? 0;
				row.info.lines =
					physicalLine + breaks - (/[\r\n]$/.test(row.raw) ? 1 : 0);
				physicalLine += breaks;
				return item;
			},
			relax_column_count: true,
			skip_empty_lines: true,
			max_record_size: settings.maxRecordSize ?? 1024 * 1024,
			readableHighWaterMark: 16,
			writableHighWaterMark: 64 * 1024,
		};
		const parser = parse(parserOptions);
		let failure: unknown;
		const done = pipeline(input, parser, { signal }).catch((error: unknown) => {
			failure = error;
		});
		try {
			for await (const item of parser) {
				signal.throwIfAborted();
				const row = item as { record: string[]; info: { lines: number } };
				yield { cells: row.record, line: row.info.lines };
			}
			await done;
			if (failure) throw failure;
		} catch (error) {
			const detail = error as { raw?: string; lines?: number };
			if (typeof detail.raw === "string")
				detail.lines =
					physicalLine + (detail.raw.match(/\r\n|\r|\n/g)?.length ?? 0);
			throw error;
		} finally {
			input.destroy();
			parser.destroy();
			await done;
		}
	} finally {
		await iterator.return?.(undefined);
	}
}
