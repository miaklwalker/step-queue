import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { z } from "zod";
import { NameFactory } from "../modules/nameFactory.ts";
import { QueueSystem, type QueueOutput } from "../modules/queueSystem.ts";
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

describe("QueueSystem builder (no connection needed)", () => {
	test("carries name/displayName from NameFactory", () => {
		const q = new QueueSystem(NameFactory({ name: "MyCoolQueue", env: "dev" }));
		assert.strictEqual(q.name, "dev-MyCoolQueue");
		assert.strictEqual(q.displayName, "MyCoolQueue");
	});

	test("defineInputSchema derives a JSON schema", () => {
		const q = new QueueSystem(NameFactory({ name: "Q" })).defineInputSchema(
			z.object({ greeting: z.string() }),
		);
		const schema = q.jsonSchema as {
			type: string;
			properties: Record<string, unknown>;
		};
		assert.strictEqual(schema.type, "object");
		assert.ok("greeting" in schema.properties);
	});

	test("defineProcessor throws if connection was not defined", () => {
		const q = new QueueSystem(NameFactory({ name: "Q" })).defineInputSchema(
			z.object({ greeting: z.string() }),
		);
		assert.throws(
			() =>
				q.defineProcessor((step) =>
					step.addStep({ name: "s", handler: async () => ({}) }),
				),
			/defineConnection/,
		);
	});

	test("addJob throws before the queue is initialized", async () => {
		const q = new QueueSystem(NameFactory({ name: "Q" })).defineInputSchema(
			z.object({ greeting: z.string() }),
		);
		await assert.rejects(() => q.addJob({ greeting: "hi" }), /not initialized/);
	});
});

describe("QueueSystem (end-to-end via BullMQ)", () => {
	before(ensureRedis);
	after(closeAll);

	test("processes a job through the real worker, typed end to end", async () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("e2e"), env: "test" }))
				.defineInputSchema(z.object({ greeting: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "shout",
							handler: async (job) => ({
								shout: job.data.greeting.toUpperCase(),
							}),
						})
						.addFinalStep({
							handler: async (job) => ({ done: job.data.shout }),
						}),
				),
		);

		type Out = QueueOutput<typeof queue>;
		type _ = Expect<Equal<Out, { done: string }>>;

		const out = await runToCompletion(queue, { greeting: "hi" });
		assert.deepStrictEqual(out, { done: "HI" });
	});

	test("validates job input against the schema at enqueue time", async () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("validate") }))
				.defineInputSchema(z.object({ n: z.number() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({ name: "noop", handler: async () => ({}) }),
				),
		);

		// A value whose runtime type violates the schema (cast past the compiler
		// on purpose, without using `any`).
		const bad = { n: "not a number" } as unknown as { n: number };
		await assert.rejects(() => queue.addJob(bad));
	});
});
