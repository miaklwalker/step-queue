import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { z } from "zod";
import {
	defineStep,
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

describe("rollbackKeys (end-to-end via BullMQ)", () => {
	before(ensureRedis);
	after(closeAll);

	test("rollback receives only the declared keys", async () => {
		let seenInRollback: Record<string, unknown> | undefined;
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("rollback-keys") }))
				.defineInputSchema(z.object({ a: z.number(), secret: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "one",
							handler: async (job) => ({ made: job.data.a + 1 }),
							rollbackKeys: ["a", "made"],
							rollback: async (job) => {
								seenInRollback = { ...job.data };
							},
						})
						.addStep({
							name: "two",
							handler: async () => {
								throw new Error("boom");
							},
						})
						.addFinalStep({ handler: async () => ({}) }),
				),
		);

		await assert.rejects(
			() => runToCompletion(queue, { a: 2, secret: "hush" }),
			/boom/,
		);
		assert.deepStrictEqual(seenInRollback, { a: 2, made: 3 });
	});
});

describe("rollbackKeys (type-level and build-time)", () => {
	test("rollback's job.data is narrowed to the declared keys", () => {
		ProcessorBuilder.init<{ a: number; secret: string }>().addStep({
			name: "s",
			handler: async (job) => ({ made: job.data.a + 1 }),
			rollbackKeys: ["a", "made"],
			rollback: async (job) => {
				type _narrowed = Expect<
					Equal<typeof job.data, { a: number; made: number }>
				>;
				assert.ok(true as _narrowed);
			},
		});
		assert.ok(true);
	});

	test("without rollbackKeys, rollback sees no job data", () => {
		ProcessorBuilder.init<{ a: number }>().addStep({
			name: "s",
			handler: async (job) => ({ made: job.data.a + 1 }),
			rollback: async (job) => {
				// biome-ignore lint/complexity/noBannedTypes: <This is for the test>
				type _empty = Expect<Equal<typeof job.data, {}>>;
				assert.ok(true as _empty);
			},
		});
		assert.ok(true);
	});

	test("rollbackKeys only accepts keys of the step's input or output", () => {
		const builder = ProcessorBuilder.init<{ a: number }>();
		builder.addStep({
			name: "s",
			handler: async () => ({ made: 1 }),
			// @ts-expect-error - "nope" is neither input nor output of this step
			rollbackKeys: ["nope"],
			rollback: async () => {},
		});
		assert.ok(true);
	});

	test("later steps cannot clean a rollback-reserved key", () => {
		assert.throws(
			() =>
				ProcessorBuilder.init<{ a: number; b: string }>()
					.addStep({
						name: "one",
						handler: async () => {},
						rollbackKeys: ["a"],
						rollback: async () => {},
					})
					.addStep({
						name: "two",
						handler: async () => {},
						clean: [
							// @ts-expect-error - "a" is reserved by step one's rollbackKeys
							"a",
						],
					}),
			/reserved by rollbackKeys/,
		);
	});

	test("standalone steps support rollbackKeys with the same narrowing", () => {
		const makeResource = defineStep<{ userId: string }>()({
			name: "makeResource",
			handler: async () => ({ resourceId: "r1" }),
			rollbackKeys: ["resourceId"],
			rollback: async (job) => {
				type _picked = Expect<Equal<typeof job.data, { resourceId: string }>>;
				assert.ok(true as _picked);
			},
		});

		const builder = ProcessorBuilder.init<{ userId: string }>().addStep(
			makeResource,
		);
		// the standalone step's rollbackKeys are protected downstream too
		assert.throws(
			() =>
				builder.addStep({
					name: "next",
					handler: async () => {},
					clean: [
						// @ts-expect-error - "resourceId" is reserved by rollbackKeys
						"resourceId",
					],
				}),
			/reserved by rollbackKeys/,
		);
	});

	test("a step cannot clean its own rollbackKeys", () => {
		assert.throws(
			() =>
				ProcessorBuilder.init<{ a: number; b: string }>().addStep({
					name: "one",
					handler: async () => {},
					rollbackKeys: ["a"],
					rollback: async () => {},
					clean: ["a"],
				}),
			/reserved by rollbackKeys/,
		);
	});
});
