import { smallCsv } from "./csv.ts";
import type { EffectiveSettings } from "./profiles.ts";
import type {
  CsvSettings,
  HookContext,
  Metadata,
  MetadataEntry,
  SourceReference,
  Structure,
} from "./types.ts";
import { freeze, httpUrl, OpendataError } from "./util.ts";

const STRUCTURE_PREFIX = "structure-";
const DATA_PREFIX = "data-";

/** Normalizes compact and extended publisher timestamps to `YYYYMMDDTHHMM`. */
function timestamp(value: string): string | undefined {
  const match = /^(\d{4})-?(\d{2})-?(\d{2})(?:T(\d{2})-?(\d{2}))?$/.exec(value);
  if (!match) {
    return undefined;
  }
  const [, y, m, d, hh = "00", mm = "00"] = match;
  const date = new Date(`${y}-${m}-${d}T${hh}:${mm}:00Z`);
  if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== `${y}-${m}-${d}`) {
    return undefined;
  }
  return `${y}${m}${d}T${hh}${mm}`;
}

/** Splits a versioned reference such as `data-20260906-structure-20210405`. */
export function versionParts(
  key: string,
): { date: string; structure: string; normalized: string } | undefined {
  const match = /^data-(.+)-structure-(.+)$/i.exec(key);
  if (!match) {
    return undefined;
  }
  const date = timestamp(match[1]!);
  const structure = timestamp(match[2]!);
  return date && structure
    ? { date, structure, normalized: `data-${date}-structure-${structure}` }
    : undefined;
}

function add<T>(map: Map<string, Set<T>>, key: string, values: Iterable<T>): void {
  const known = map.get(key) ?? new Set<T>();
  for (const value of values) {
    known.add(value);
  }
  map.set(key, known);
}

export async function metadataFrom(
  bytes: Uint8Array,
  url: string,
  settings: EffectiveSettings,
  context: HookContext,
  warn: (code: string, message: string) => void,
): Promise<Metadata> {
  const rows = smallCsv(bytes, settings.metadataCsv);
  const headers = rows.shift()?.map((s) => s.trim().toLowerCase()) ?? [];
  const propertyIndex = headers.indexOf("property");
  const valueIndex = headers.indexOf("value");
  if (propertyIndex < 0 || valueIndex < 0 || new Set(headers).size !== headers.length) {
    throw new OpendataError(
      "INVALID_METADATA",
      "Metadata requires unique property and value columns",
      { source: url },
    );
  }
  const originalEntries = freeze(
    rows.map((row) => ({
      property: row[propertyIndex]!.trim(),
      value: row[valueIndex]!,
      attributes: Object.fromEntries(headers.map((h, i) => [h, row[i]!])),
    })),
  );
  const entries: readonly MetadataEntry[] = settings.hooks?.metadata
    ? await settings.hooks.metadata.run(originalEntries, context)
    : originalEntries;

  const properties: Record<string, string> = Object.create(null) as Record<string, string>;
  /** Reference property → distinct URLs. */
  const references = new Map<string, Set<string>>();
  for (const entry of entries) {
    if (/^(data-|structure-)/i.test(entry.property)) {
      add(references, entry.property, [httpUrl(entry.value.trim(), url)]);
    } else if (
      properties[entry.property] !== undefined &&
      properties[entry.property] !== entry.value
    ) {
      warn(
        "METADATA_CONFLICT",
        `Conflicting metadata property: ${entry.property}; first value retained`,
      );
    } else {
      properties[entry.property] = entry.value;
    }
  }

  /** Structure reference or normalized timestamp → distinct URLs. */
  const structures = new Map<string, Set<string>>();
  for (const [key, urls] of references) {
    if (!key.startsWith(STRUCTURE_PREFIX)) {
      continue;
    }
    const stamp = timestamp(key.slice(STRUCTURE_PREFIX.length));
    for (const id of stamp ? [key, stamp] : [key]) {
      add(structures, id, urls);
    }
  }

  const sources: SourceReference[] = [];
  for (const [key, urls] of references) {
    if (urls.size > 1) {
      warn("METADATA_CONFLICT", `Conflicting reference: ${key}`);
    }
    if (!key.startsWith(DATA_PREFIX)) {
      continue;
    }
    const suffix = key.includes(`-${STRUCTURE_PREFIX}`)
      ? key.split(`-${STRUCTURE_PREFIX}`)[1]
      : undefined;
    const structureKey = suffix ? `${STRUCTURE_PREFIX}${suffix}` : undefined;
    const stamp = suffix ? timestamp(suffix) : undefined;
    const structureUrls = new Set([
      ...(structureKey ? (structures.get(structureKey) ?? []) : []),
      ...(stamp ? (structures.get(stamp) ?? []) : []),
    ]);
    if (structureUrls.size > 1) {
      warn("STRUCTURE_CONFLICT", `Ambiguous structure for ${key}; advisory structure omitted`);
    }
    for (const dataUrl of urls) {
      sources.push({
        key,
        url: dataUrl,
        ...(structureKey ? { structureKey } : {}),
        ...(structureUrls.size === 1 ? { structureUrl: [...structureUrls][0]! } : {}),
      });
    }
  }

  return freeze({
    originalEntries,
    entries: [...entries],
    properties,
    sources,
    url,
  });
}

export function selectSource(metadata: Metadata): SourceReference {
  const hint = metadata.properties.provenance?.trim();
  let matches = hint ? metadata.sources.filter((s) => s.key === hint || s.url === hint) : [];
  if (!matches.length && hint) {
    const normalized = versionParts(hint)?.normalized;
    if (normalized) {
      matches = metadata.sources.filter((s) => versionParts(s.key)?.normalized === normalized);
    }
  }
  if (!matches.length) {
    const dated = metadata.sources
      .map((source) => ({ source, date: versionParts(source.key)?.date }))
      .filter((s) => s.date)
      .sort((a, b) => b.date!.localeCompare(a.date!));
    if (dated.length) {
      matches = dated.filter((s) => s.date === dated[0]!.date).map((s) => s.source);
    } else if (metadata.sources.length === 1) {
      matches = [...metadata.sources];
    }
  }
  const unique = [...new Map(matches.map((s) => [`${s.url}|${s.structureUrl ?? ""}`, s])).values()];
  if (unique.length !== 1) {
    throw new OpendataError(
      "AMBIGUOUS_SOURCE",
      "Cannot select a unique data source; provide a selectSource hook",
      { source: metadata.url },
    );
  }
  return unique[0]!;
}

export function structureFrom(
  bytes: Uint8Array,
  url: string,
  settings: CsvSettings = {},
): Structure {
  const rows = smallCsv(bytes, settings);
  const headers = rows.shift()?.map((s) => s.trim().toLowerCase()) ?? [];
  const index = headers.findIndex((h) => ["field name", "field", "name"].includes(h));
  if (index < 0) {
    throw new OpendataError("INVALID_STRUCTURE", "Structure has no recognizable field-name column");
  }
  const fields = rows.map((row) => ({
    name: row[index]!,
    attributes: Object.fromEntries(headers.map((h, i) => [h, row[i]!])),
  }));
  if (fields.some((f) => !f.name) || new Set(fields.map((f) => f.name)).size !== fields.length) {
    throw new OpendataError(
      "INVALID_STRUCTURE",
      "Structure contains empty or duplicate field names",
    );
  }
  return freeze({ url, fields });
}
