import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { z } from "zod";
import {
	AddStepError,
	NameFactory,
	ProcessorBuilder,
	QueueSystem,
} from "../modules/index.ts";
import {
	closeAll,
	connection,
	ensureRedis,
	runToCompletion,
	track,
	uniqueName,
} from "./support.ts";

describe("ProcessorBuilder (end-to-end via BullMQ)", () => {
	before(ensureRedis);
	after(closeAll);

	test("runs steps in order and accumulates data", async () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("order") }))
				.defineInputSchema(z.object({ start: z.number() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "double",
							handler: async (job) => ({ doubled: job.data.start * 2 }),
						})
						.addStep({
							name: "label",
							handler: async (job) => ({ label: `=${job.data.doubled}` }),
						})
						.addFinalStep({
							handler: async (job) => ({ result: job.data.label }),
						}),
				),
		);

		const out = await runToCompletion(queue, { start: 5 });
		assert.deepStrictEqual(out, { result: "=10" });
	});

	test("inserts an implicit final step returning accumulated data", async () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("implicit") }))
				.defineInputSchema(z.object({ a: z.number() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({
						name: "add-b",
						handler: async (job) => ({ b: job.data.a + 1 }),
					}),
				),
		);

		const out = await runToCompletion(queue, { a: 1 });
		// user data is present...
		assert.strictEqual(out.a, 1);
		assert.strictEqual(out.b, 2);
		// ...but internal bookkeeping keys are stripped
		assert.ok(!("step" in out));
		assert.ok(!("completedSteps" in out));
		assert.ok(!("consumedKeys" in out));
		assert.ok(!("previousStep" in out));
	});
});

describe("ProcessorBuilder (build-time validation)", () => {
	test("throws on duplicate step name", () => {
		assert.throws(() => {
			ProcessorBuilder.init<Record<string, never>>()
				.addStep({ name: "dup", handler: async () => ({}) })
				.addStep({ name: "dup", handler: async () => ({}) });
		}, AddStepError);
	});

	test("throws when building with no steps", () => {
		assert.throws(() => {
			ProcessorBuilder.init<Record<string, never>>().build();
		}, AddStepError);
	});

	test("cannot add a step after a final step (runtime guard)", () => {
		assert.throws(() => {
			ProcessorBuilder.init<Record<string, never>>()
				.addFinalStep({ handler: async () => ({}) })
				.addStep({ name: "after", handler: async () => ({}) });
		}, AddStepError);
	});
});
