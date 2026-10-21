import { defineConfig, type UserConfig } from "vite-plus";

const config: UserConfig = defineConfig({
  pack: {
    entry: ["src/index.ts"],
    format: ["esm", "cjs"],
    target: "node22",
    outputOptions: { exports: "named" },
    dts: {
      tsgo: true,
    },
    // Keep explicit, type-aware import/require conditions in package.json.
    exports: false,
  },
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
});

export default config;
