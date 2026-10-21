import type { TransformOptions } from "node:stream";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { type Options, parse } from "csv-parse";
import { parse as parseSync } from "csv-parse/sync";
import type { CsvSettings } from "./types.ts";
import { OpendataError } from "./util.ts";

/** At most three bytes are needed to recognize a byte-order mark. */
const BOM_BYTES = 3;
const newline = /\r\n|\r|\n/g;

function encoding(bytes: Uint8Array, explicit?: string): string {
  if (explicit) return explicit;
  if (bytes[0] === 0xff && bytes[1] === 0xfe) return "utf-16le";
  if (bytes[0] === 0xfe && bytes[1] === 0xff) return "utf-16be";
  return "utf-8";
}

function invalidEncoding(error: unknown): OpendataError {
  return new OpendataError(
    "INVALID_ENCODING",
    "Cannot decode source; set an explicit encoding override if necessary",
    {},
    error,
  );
}

export function decodeSmall(bytes: Uint8Array, explicit?: string): string {
  try {
    return new TextDecoder(encoding(bytes, explicit), { fatal: true }).decode(bytes);
  } catch (error) {
    throw invalidEncoding(error);
  }
}

function feed(decoder: TextDecoder, chunk?: Uint8Array): string {
  try {
    return chunk ? decoder.decode(chunk, { stream: true }) : decoder.decode();
  } catch (error) {
    throw invalidEncoding(error);
  }
}

/** Decodes a byte stream, choosing the encoding from its first bytes. */
async function* decode(
  bytes: AsyncIterable<Uint8Array>,
  explicit?: string,
): AsyncGenerator<string> {
  const source = bytes[Symbol.asyncIterator]();
  let head = Buffer.alloc(0);
  let done = false;
  try {
    while (!done && head.length < BOM_BYTES) {
      const next = await source.next();
      done = next.done === true;
      if (!done) head = Buffer.concat([head, next.value]);
    }
    let decoder: TextDecoder;
    try {
      decoder = new TextDecoder(encoding(head, explicit), { fatal: true });
    } catch (error) {
      throw invalidEncoding(error);
    }
    for (let chunk: Uint8Array | undefined = head; ;) {
      if (chunk?.length) {
        const text = feed(decoder, chunk);
        if (text) yield text;
      }
      if (done) break;
      const next = await source.next();
      done = next.done === true;
      chunk = done ? undefined : next.value;
    }
    const tail = feed(decoder);
    if (tail) yield tail;
  } finally {
    await source.return?.(undefined);
  }
}

/** End of the last complete quoted record within a bounded sample. */
function completePrefix(text: string, quote: string | false, escape: string): string {
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

export function smallCsv(bytes: Uint8Array, settings: CsvSettings = {}): string[][] {
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
    throw new OpendataError("INVALID_CSV", "Cannot parse CSV document", {}, error);
  }
}

export interface CsvRow {
  cells: string[];
  /** One-based physical line on which the record ends. */
  line: number;
}

/**
 * Streams rows while reporting physical end lines. Width is validated against
 * repaired headers by the manager, so surplus cells are preserved here.
 */
export async function* csvRows(
  bytes: AsyncIterable<Uint8Array>,
  settings: CsvSettings,
  signal: AbortSignal,
): AsyncGenerator<CsvRow> {
  const decoded = decode(bytes, settings.encoding)[Symbol.asyncIterator]();
  let line = 1;
  try {
    // Read a bounded prefix to detect the dialect, then parse the remainder as it arrives.
    const limit = settings.sampleBytes ?? 64 * 1024;
    let sample = "";
    let ended = false;
    while (sample.length < limit) {
      const next = await decoded.next();
      if (next.done) {
        ended = true;
        break;
      }
      sample += next.value;
    }
    const rest = sample.slice(limit);
    sample = sample.slice(0, limit);
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
          const next = await decoded.next();
          if (next.done) break;
          yield next.value;
        }
      })(),
      { objectMode: false, highWaterMark: 64 * 1024 },
    );
    const parserOptions: Options & TransformOptions = {
      delimiter,
      quote: settings.quote ?? '"',
      escape: settings.escape ?? '"',
      bom: true,
      info: true,
      raw: true,
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
        const row = item as { record: string[]; info: { raw: string } };
        const breaks = row.info.raw.match(newline)?.length ?? 0;
        const endsWithBreak = /[\r\n]$/.test(row.info.raw);
        yield { cells: row.record, line: line + breaks - (endsWithBreak ? 1 : 0) };
        line += breaks;
      }
      await done;
      if (failure) throw failure;
    } catch (error) {
      const detail = error as { raw?: string; lines?: number };
      if (typeof detail.raw === "string")
        detail.lines = line + (detail.raw.match(newline)?.length ?? 0);
      throw error;
    } finally {
      input.destroy();
      parser.destroy();
      await done;
    }
  } finally {
    await decoded.return?.(undefined);
  }
}
