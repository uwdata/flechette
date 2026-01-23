import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		expect: { requireAssertions: true },
		include: ["test/**/*.test.js"],
	}
});
