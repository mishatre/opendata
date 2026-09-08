import { defineConfig, type UserConfig } from "vite-plus";

const config: UserConfig = defineConfig({
  pack: {
    entry: ["src/index.ts", "src/s3.ts"],
    format: ["esm", "cjs"],
    target: "node22",
    outputOptions: { exports: "named" },
    dts: {
      tsgo: true,
    },
    exports: true,
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
