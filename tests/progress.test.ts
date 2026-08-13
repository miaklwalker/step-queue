import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { z } from "zod";
import { NameFactory } from "../modules/nameFactory.ts";
import { QueueSystem } from "../modules/queueSystem.ts";
import {
	closeAll,
	connection,
	ensureRedis,
	track,
	uniqueName,
} from "./support.ts";

const empty = z.object({});

type ProgressShape = {
	step?: string;
	progress?: number;
	steps?: Record<string, string>;
};

describe("progress.steps tracking", () => {
	before(ensureRedis);
	after(closeAll);

	test("finished steps report elapsed time; unreached steps stay not_started", async () => {
		let releaseB: () => void = () => {};
		const bGate = new Promise<void>((resolve) => {
			releaseB = resolve;
		});

		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("progress-steps") }))
				.defineInputSchema(empty)
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({ name: "A", handler: async () => ({}) })
						.addStep({
							name: "B",
							handler: async () => {
								await bGate;
								return {};
							},
						})
						.addStep({ name: "C", handler: async () => ({}) })
						.addFinalStep({ handler: async () => ({ done: true }) }),
				),
		);

		const job = await queue.addJob({});

		let progress: ProgressShape | undefined;
		for (let i = 0; i < 50; i++) {
			const fetched = await queue.queue.getJob(job.id as string);
			progress = fetched?.progress as ProgressShape | undefined;
			if (progress?.steps?.B === "running") break;
			await new Promise((resolve) => setTimeout(resolve, 20));
		}

		assert.match(progress?.steps?.A ?? "", /^Finished in \d+ms$/);
		assert.strictEqual(progress?.steps?.B, "running");
		assert.strictEqual(progress?.steps?.C, "not_started");

		releaseB();
		const out = await job.waitUntilFinished(queue.QueueEvents, 15_000);
		assert.deepStrictEqual(out, { done: true });

		const finished = await queue.queue.getJob(job.id as string);
		const finalProgress = finished?.progress as ProgressShape | undefined;
		assert.match(finalProgress?.steps?.A ?? "", /^Finished in \d+ms$/);
		assert.match(finalProgress?.steps?.B ?? "", /^Finished in \d+ms$/);
		assert.match(finalProgress?.steps?.C ?? "", /^Finished in \d+ms$/);
		assert.match(finalProgress?.steps?.final ?? "", /^Finished in \d+ms$/);
		assert.strictEqual(finalProgress?.progress, 100);
		// The final step's internal bookkeeping name must never leak in as its
		// own entry — it should only ever appear under the "final" alias.
		assert.strictEqual(finalProgress?.steps?.__final__step__, undefined);
		assert.strictEqual(Object.keys(finalProgress?.steps ?? {}).length, 4);
	});

	test("a skipped step is marked Skipped, and the bypassed step stays not_started", async () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("progress-skip") }))
				.defineInputSchema(empty)
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "A",
							handler: async (job) => {
								throw new job.actionErrors.skip("C");
							},
						})
						.addStep({ name: "B", handler: async () => ({}) })
						.addStep({ name: "C", handler: async () => ({}) })
						.addFinalStep({ handler: async () => ({ ok: true }) }),
				),
		);

		const job = await queue.addJob({});
		await job.waitUntilFinished(queue.QueueEvents, 15_000);

		const finished = await queue.queue.getJob(job.id as string);
		const progress = finished?.progress as ProgressShape | undefined;
		assert.match(progress?.steps?.A ?? "", /^Skipped after \d+ms$/);
		assert.strictEqual(progress?.steps?.B, "not_started");
		assert.match(progress?.steps?.C ?? "", /^Finished in \d+ms$/);
	});

	test("a failed step is marked Failed, and later steps stay not_started", async () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("progress-fail") }))
				.defineInputSchema(empty)
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({ name: "A", handler: async () => ({}) })
						.addStep({
							name: "B",
							handler: async () => {
								throw new Error("boom");
							},
						})
						.addStep({ name: "C", handler: async () => ({}) })
						.addFinalStep({ handler: async () => ({}) }),
				),
		);

		const job = await queue.addJob({});
		await assert.rejects(
			() => job.waitUntilFinished(queue.QueueEvents, 15_000),
			/boom/,
		);

		const finished = await queue.queue.getJob(job.id as string);
		const progress = finished?.progress as ProgressShape | undefined;
		assert.match(progress?.steps?.A ?? "", /^Finished in \d+ms$/);
		assert.match(progress?.steps?.B ?? "", /^Failed after \d+ms$/);
		assert.strictEqual(progress?.steps?.C, "not_started");
	});
});
