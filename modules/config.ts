import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type { JobsOptions } from "bullmq";

export type QueueDefaults = Partial<JobsOptions>;

/** Identity helper for authoring `stepqueue.config.ts` with type inference. */
export function defineQueueDefaults(defaults: QueueDefaults): QueueDefaults {
	return defaults;
}

const CONFIG_FILENAMES = [
	"stepqueue.config.ts",
	"stepqueue.config.js",
	"stepqueue.config.mjs",
	"stepqueue.config.cjs",
];

let resolvedDefaults: Promise<QueueDefaults> | undefined;

/**
 * Resolves user-configured job-option defaults, checked once per process and
 * cached thereafter — every call after the first (including concurrent calls
 * made before it settles) awaits the same promise instead of touching the
 * filesystem again.
 *
 * Looks for `stepqueue.config.{ts,js,mjs,cjs}` in `process.cwd()`, falling
 * back to a `stepQueueDefaults` field in `package.json`.
 */
export function resolveQueueDefaults(): Promise<QueueDefaults> {
	if (!resolvedDefaults) {
		resolvedDefaults = loadQueueDefaults();
	}
	return resolvedDefaults;
}

async function loadQueueDefaults(): Promise<QueueDefaults> {
	const cwd = process.cwd();

	for (const filename of CONFIG_FILENAMES) {
		const path = resolve(cwd, filename);
		if (existsSync(path)) {
			const mod = await import(pathToFileURL(path).href);
			return (mod.default ?? {}) as QueueDefaults;
		}
	}

	const pkgPath = resolve(cwd, "package.json");
	if (existsSync(pkgPath)) {
		const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
		if (pkg.stepQueueDefaults) {
			return pkg.stepQueueDefaults as QueueDefaults;
		}
	}

	return {};
}
