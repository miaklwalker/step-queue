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

	test("a recovery child's result never leaks into job data or a later childQueue step", async () => {
		const fixQueue = track(
			new QueueSystem(NameFactory({ name: uniqueName("fix-leak-check") }))
				.defineInputSchema(z.object({ marker: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({
						name: "fix",
						// Deliberately data-bearing, to prove this never reaches the
						// parent's job data or a later childQueue step's results.
						handler: async () => ({ secret: "leaked-from-fix" }),
					}),
				),
		);

		const doubleQueue = track(
			new QueueSystem(NameFactory({ name: uniqueName("double-leak-check") }))
				.defineInputSchema(z.object({ n: z.number() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({
						name: "double",
						handler: async (job) => ({ doubled: job.data.n * 2 }),
					}),
				),
		);

		let firstAttempt = true;

		const mainQueue = track(
			new QueueSystem(NameFactory({ name: uniqueName("recover-then-fanout") }))
				.defineInputSchema(z.object({ start: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "push",
							handler: async (job) => {
								if (firstAttempt) {
									firstAttempt = false;
									throw new job.actionErrors.recover(fixQueue, {
										marker: job.data.start,
									});
								}
								return { recovered: true };
							},
						})
						.addStep({
							// No `singleChild` — the results key holds an array of every
							// unconsumed child's value. If the recovery child's key were
							// never marked consumed, it would show up here as a second,
							// unwanted array entry regardless of BullMQ's key sort order.
							name: "fanOut",
							childQueue: doubleQueue,
							handler: async (job) => {
								await doubleQueue.addChildJob(job, { n: 21 });
							},
						})
						.addFinalStep({
							handler: async (job) => {
								const raw = job.data as unknown as Record<string, unknown>;
								return {
									fanOutResults: job.data["fanOut-results"],
									leakedSecret: "secret" in raw,
								};
							},
						}),
				),
		);

		const out = await runToCompletion(mainQueue, { start: "go" });
		// Exactly one entry — doubleQueue's own child (its accumulated data:
		// input `n` plus its own `doubled` output). The earlier recovery
		// child's return value was consumed and discarded, not merged in
		// alongside it.
		assert.deepStrictEqual(out.fanOutResults, [{ n: 21, doubled: 42 }]);
		assert.strictEqual(out.leakedSecret, false);
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
