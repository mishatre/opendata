import { defineConfig, type UserConfig } from "vite-plus";

const config: UserConfig = defineConfig({
	pack: {
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
