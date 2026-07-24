import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { z } from "zod";
import {
	NameFactory,
	ProcessorBuilder,
	QueueSystem,
} from "../modules/index.ts";
import {
	closeAll,
	connection,
	ensureRedis,
	type Equal,
	type Expect,
	runToCompletion,
	track,
	uniqueName,
} from "./support.ts";

describe("clean feature (end-to-end via BullMQ)", () => {
	before(ensureRedis);
	after(closeAll);

	test("deletes cleaned keys from job data at runtime", async () => {
		const seenInLaterStep: Record<string, unknown> = {};
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("clean-secret") }))
				.defineInputSchema(z.object({ secret: z.string(), keep: z.number() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "use-secret",
							handler: async (job) => ({ derived: job.data.secret.length }),
							// we no longer need the raw secret past this point
							clean: ["secret"],
						})
						.addStep({
							name: "later",
							handler: async (job) => {
								Object.assign(seenInLaterStep, job.data);
								return {};
							},
						}),
				),
		);

		const out = await runToCompletion(queue, { secret: "hunter2", keep: 1 });

		// The later step never saw the cleaned key...
		assert.ok(!("secret" in seenInLaterStep));
		assert.strictEqual(seenInLaterStep.keep, 1);
		assert.strictEqual(seenInLaterStep.derived, 7);

		// ...and it's gone from the final result too.
		assert.ok(!("secret" in out));
		assert.strictEqual(out.keep, 1);
		assert.strictEqual(out.derived, 7);
	});

	test("cleans an input key while the step adds new data", async () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("clean-temp") }))
				.defineInputSchema(z.object({ a: z.number() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({
						name: "temp",
						handler: async (job) => ({
							temp: job.data.a * 10,
							b: job.data.a + 1,
						}),
						clean: ["a"],
					}),
				),
		);

		const out = await runToCompletion(queue, { a: 2 });
		assert.ok(!("a" in out));
		assert.strictEqual(out.temp, 20);
		assert.strictEqual(out.b, 3);
	});
});

describe("clean feature (type-level)", () => {
	test("cleaned key is removed from the accumulated type", () => {
		const processor = ProcessorBuilder.init<{
			secret: string;
			keep: number;
		}>()
			.addStep({
				name: "s",
				handler: async (job) => ({ derived: job.data.secret.length }),
				clean: ["secret"],
			})
			.build();

		type Accumulated = Awaited<ReturnType<typeof processor>>;
		type _cleaned = Expect<
			Equal<Accumulated, { keep: number; derived: number }>
		>;

		assert.ok(true as _cleaned);
	});

	test("a following step no longer sees the cleaned key", () => {
		const builder = ProcessorBuilder.init<{
			secret: string;
			keep: number;
		}>().addStep({
			name: "s",
			handler: async (job) => ({ derived: job.data.secret.length }),
			clean: ["secret"],
		});

		builder.addStep({
			name: "next",
			handler: async (job) => {
				// @ts-expect-error - `secret` was cleaned away
				job.data.secret;
				return {};
			},
		});

		assert.ok(true);
	});

	test("clean only accepts keys of the step's input data", () => {
		const builder = ProcessorBuilder.init<{ a: number }>();
		builder.addStep({
			name: "s",
			handler: async () => {},
			clean: [
				// @ts-expect-error - "nope" is not a key of the step's input data
				"nope",
			],
		});
		assert.ok(true);
	});

	test("clean does not accept keys the same step adds", () => {
		const builder = ProcessorBuilder.init<{ a: number }>();
		builder.addStep({
			name: "s",
			handler: async (job) => ({ temp: job.data.a * 10 }),
			clean: [
				// @ts-expect-error - "temp" is added by this step, not part of its input
				"temp",
			],
		});
		assert.ok(true);
	});
});
