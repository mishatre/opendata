import type {
	ManagerOptions,
	ProviderProfile,
	ProviderSettings,
} from "./types.ts";
import { freeze, hash, httpUrl, OpendataError, stable } from "./util.ts";

/** Generic parsing handles the current Minpromtorg fixture; no publisher-specific repair is necessary. */
export const builtinProfiles: readonly ProviderProfile[] = Object.freeze([]);

export interface EffectiveSettings extends ProviderSettings {
	fingerprint: string;
	profileIds: readonly string[];
}

export function resolveSettings(options: ManagerOptions): EffectiveSettings {
	const profiles = new Map(
		builtinProfiles.map((p) => [p.id, { profile: p, custom: false }]),
	);
	const customIds = new Set<string>();
	for (const p of options.profiles ?? []) {
		if (customIds.has(p.id)) {
			throw new OpendataError(
				"AMBIGUOUS_PROFILE",
				`Duplicate custom profile ID: ${p.id}`,
			);
		}
		customIds.add(p.id);
		profiles.set(p.id, { profile: p, custom: true });
	}
	for (const id of options.disabledProfiles ?? []) {
		profiles.delete(id);
	}
	const origin = new URL(options.baseUrl).origin;
	const matching = [...profiles.values()].filter(
		({ profile: p }) =>
			new URL(httpUrl(p.origin)).origin === origin &&
			(p.identifier === undefined || p.identifier === options.identifier),
	);
	const layers = matching
		.map((p) => ({
			...p,
			priority:
				Number(p.custom) * 2 + Number(p.profile.identifier !== undefined),
		}))
		.sort((a, b) => a.priority - b.priority);
	if (new Set(layers.map((p) => p.priority)).size !== layers.length) {
		throw new OpendataError(
			"AMBIGUOUS_PROFILE",
			"Only one provider and one dataset profile may match per built-in/custom layer",
		);
	}
	const sorted = layers.map((p) => p.profile);
	let settings: ProviderSettings = {
		csv: {
			maxRecordSize: 1024 * 1024,
			sampleBytes: 64 * 1024,
			hasHeader: true,
		},
		structure: "advisory",
	};
	for (const layer of [...sorted, options]) {
		if ("id" in layer && (!layer.id || !layer.version)) {
			throw new OpendataError(
				"INVALID_PROFILE",
				"Profiles require a stable ID and version",
			);
		}
		settings = {
			...settings,
			...(layer.metadataUrl !== undefined
				? { metadataUrl: layer.metadataUrl }
				: {}),
			...(layer.structure !== undefined ? { structure: layer.structure } : {}),
			csv: { ...settings.csv, ...layer.csv },
			metadataCsv: { ...settings.metadataCsv, ...layer.metadataCsv },
			structureCsv: { ...settings.structureCsv, ...layer.structureCsv },
			hooks: { ...settings.hooks, ...layer.hooks },
		};
	}
	const hookVersions = Object.fromEntries(
		Object.entries(settings.hooks ?? {}).map(([key, hook]) => {
			if (!hook.id || !hook.version || typeof hook.run !== "function") {
				throw new OpendataError(
					"INVALID_PROFILE",
					`Hook ${key} requires an ID, version, and run function`,
				);
			}
			return [key, [hook.id, hook.version]];
		}),
	);
	return freeze({
		...settings,
		fingerprint: hash(
			stable({
				...settings,
				hooks: hookVersions,
				profiles: sorted.map((p) => [p.id, p.version]),
			}),
		),
		profileIds: sorted.map((p) => p.id),
	});
}
