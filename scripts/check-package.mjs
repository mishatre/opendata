import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

/**
 * Packs the package once and runs publint and arethetypeswrong against that
 * exact tarball.
 *
 * The tarball is built with dry-run explicitly disabled: `npm publish --dry-run`
 * exports `npm_config_dry_run`, which would otherwise be inherited by a nested
 * `npm pack` and leave attw looking for a file that was never written.
 */
const env = { ...process.env, npm_config_dry_run: "false" };

const bin = (name) =>
  join(process.cwd(), "node_modules", ".bin", process.platform === "win32" ? `${name}.cmd` : name);

const directory = mkdtempSync(join(tmpdir(), "opendata-package-check-"));

try {
  // Build here rather than through `npm pack`'s prepack hook: lifecycle stdout
  // would be mixed into the JSON report that npm pack writes to stdout.
  execFileSync("npm", ["run", "build"], { env, stdio: "inherit" });

  const stdout = execFileSync(
    "npm",
    ["pack", "--json", "--ignore-scripts", "--pack-destination", directory],
    { encoding: "utf8", env, stdio: ["ignore", "pipe", "inherit"] },
  );
  const [packed] = JSON.parse(stdout);
  if (packed?.filename === undefined) {
    throw new Error("npm pack did not report a tarball filename");
  }
  const tarball = join(directory, packed.filename);

  for (const tool of ["publint", "attw"]) {
    execFileSync(bin(tool), [tarball], { env, stdio: "inherit" });
  }

  console.log(`Package checks passed for ${packed.filename}`);
} finally {
  rmSync(directory, { recursive: true, force: true });
}
