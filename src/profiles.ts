import type { ManagerOptions, ProviderProfile, ProviderSettings, VersionedHook } from "./types.ts";
import { freeze, hash, httpUrl, OpendataError, stable } from "./util.ts";

export interface EffectiveSettings extends ProviderSettings {
  /** Identifies the effective parsing configuration; part of cursor compatibility. */
  fingerprint: string;
}

const defaults: ProviderSettings = {
  csv: { maxRecordSize: 1024 * 1024, sampleBytes: 64 * 1024, hasHeader: true },
  structure: "advisory",
};

/** Later layers win per property; hooks replace earlier hooks instead of composing. */
function merge(base: ProviderSettings, layer: ProviderSettings): ProviderSettings {
  return {
    ...(layer.metadataUrl !== undefined ? { metadataUrl: layer.metadataUrl } : {}),
    ...(layer.structure !== undefined ? { structure: layer.structure } : {}),
    csv: { ...base.csv, ...layer.csv },
    metadataCsv: { ...base.metadataCsv, ...layer.metadataCsv },
    structureCsv: { ...base.structureCsv, ...layer.structureCsv },
    hooks: { ...base.hooks, ...layer.hooks },
  };
}

/**
 * Profiles for this origin, ordered provider scope then dataset scope. At most one
 * profile may match each scope, so precedence stays predictable.
 */
function applicable(options: ManagerOptions): readonly ProviderProfile[] {
  const profiles = options.profiles ?? [];
  const seen = new Set<string>();
  for (const profile of profiles) {
    if (!profile.id || !profile.version) {
      throw new OpendataError("INVALID_PROFILE", "Profiles require a stable ID and version");
    }
    if (seen.has(profile.id)) {
      throw new OpendataError("AMBIGUOUS_PROFILE", `Duplicate profile ID: ${profile.id}`);
    }
    seen.add(profile.id);
  }
  const origin = new URL(options.baseUrl).origin;
  const matches = profiles.filter(
    (profile) =>
      !options.disabledProfiles?.includes(profile.id) &&
      new URL(httpUrl(profile.origin)).origin === origin &&
      (profile.identifier === undefined || profile.identifier === options.identifier),
  );
  const scope = (dataset: boolean): readonly ProviderProfile[] => {
    const layers = matches.filter((profile) => (profile.identifier !== undefined) === dataset);
    if (layers.length > 1) {
      throw new OpendataError(
        "AMBIGUOUS_PROFILE",
        `Only one ${dataset ? "dataset" : "provider"} profile may match`,
      );
    }
    return layers;
  };
  return [...scope(false), ...scope(true)];
}

function hookVersions(hooks: ProviderSettings["hooks"]): [string, string][] {
  const versions: [string, string][] = [];
  const entries = Object.entries(hooks ?? {}) as [string, VersionedHook<unknown> | undefined][];
  for (const [key, hook] of entries) {
    if (!hook?.id || !hook.version || typeof hook.run !== "function") {
      throw new OpendataError(
        "INVALID_PROFILE",
        `Hook ${key} requires an ID, version, and run function`,
      );
    }
    versions.push([hook.id, hook.version]);
  }
  return versions;
}

export function resolveSettings(options: ManagerOptions): EffectiveSettings {
  const profiles = applicable(options);
  let settings = defaults;
  for (const layer of [...profiles, options]) {
    settings = merge(settings, layer);
  }
  const { hooks, ...rest } = settings;
  return freeze({
    ...rest,
    ...(hooks ? { hooks } : {}),
    fingerprint: hash(
      stable({
        settings: rest,
        hooks: hookVersions(hooks),
        profiles: profiles.map((profile) => [profile.id, profile.version]),
      }),
    ),
  });
}
