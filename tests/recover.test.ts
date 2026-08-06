import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { z } from "zod";
import { NameFactory } from "../modules/nameFactory.ts";
import { QueueSystem } from "../modules/queueSystem.ts";
import type { TypedJob } from "../modules/types.ts";
import {
	closeAll,
	connection,
	ensureRedis,
	runToCompletion,
	track,
	uniqueName,
} from "./support.ts";

describe("recover action error (end-to-end via BullMQ)", () => {
	before(ensureRedis);
	after(closeAll);

	test("spawns a child job, waits, and retries the same step once it completes", async () => {
		const state = { fixed: false };
		const visited: string[] = [];

		const importBrandQueue = track(
			new QueueSystem(NameFactory({ name: uniqueName("import-brand") }))
				.defineInputSchema(z.object({ brandId: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({
						name: "import",
						handler: async () => {
							state.fixed = true;
							return { imported: true };
						},
					}),
				),
		);

		const pushQueue = track(
			new QueueSystem(NameFactory({ name: uniqueName("push-to-channel") }))
				.defineInputSchema(z.object({ brandId: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "push",
							handler: async (job) => {
								visited.push("push");
								if (!state.fixed) {
									throw new job.actionErrors.recover(importBrandQueue, {
										brandId: job.data.brandId,
									});
								}
								return { pushed: true };
							},
						})
						.addFinalStep({
							handler: async (job) => ({
								done: true,
								pushed: job.data.pushed,
							}),
						}),
				),
		);

		const out = await runToCompletion(pushQueue, { brandId: "brand-1" });
		assert.deepStrictEqual(out, { done: true, pushed: true });
		// "push" ran once, failed, waited on the fix job, then ran again and succeeded.
		assert.deepStrictEqual(visited, ["push", "push"]);
		assert.strictEqual(state.fixed, true);
	});

	test("gives up after maxAttempts and fails the job", async () => {
		const visited: string[] = [];

		const importBrandQueue = track(
			new QueueSystem(NameFactory({ name: uniqueName("import-brand-fail") }))
				.defineInputSchema(z.object({ brandId: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({
						name: "import",
						// The fix job runs, but never actually resolves the problem.
						handler: async () => ({ imported: true }),
					}),
				),
		);

		const pushQueue = track(
			new QueueSystem(NameFactory({ name: uniqueName("push-to-channel-fail") }))
				.defineInputSchema(z.object({ brandId: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "push",
							handler: async (job) => {
								visited.push("push");
								throw new job.actionErrors.recover(
									importBrandQueue,
									{ brandId: job.data.brandId },
									{ maxAttempts: 2 },
								);
							},
						})
						.addFinalStep({ handler: async () => ({ done: true }) }),
				),
		);

		await assert.rejects(
			() => runToCompletion(pushQueue, { brandId: "brand-2" }),
			/Step "push" exceeded its recover maxAttempts \(2\)/,
		);
		// 2 recovery attempts (spawned a fix job each time) plus the final failing run.
		assert.deepStrictEqual(visited, ["push", "push", "push"]);
	});

	test("child data is type-checked against the target queue's input schema", () => {
		const importBrandQueue = track(
			new QueueSystem(NameFactory({ name: uniqueName("import-brand-typed") }))
				.defineInputSchema(z.object({ brandId: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({ name: "import", handler: async () => ({}) }),
				),
		);

		// Never invoked — exists purely so `tsc` checks the call shapes below.
		const _typeCheck = (job: TypedJob<{ brandId: string }>) => {
			const _ok = new job.actionErrors.recover(importBrandQueue, {
				brandId: job.data.brandId,
			});
			const _badKey = new job.actionErrors.recover(importBrandQueue, {
				// @ts-expect-error - `brand` isn't a key of importBrandQueue's input schema
				brand: job.data.brandId,
			});
			const _badType = new job.actionErrors.recover(importBrandQueue, {
				// @ts-expect-error - `brandId` must be a string, not a number
				brandId: 1,
			});
			return { _ok, _badKey, _badType };
		};

		assert.strictEqual(typeof _typeCheck, "function");
	});
});
