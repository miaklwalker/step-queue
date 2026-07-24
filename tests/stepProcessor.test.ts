import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { z } from "zod";
import { NameFactory } from "../modules/nameFactory.ts";
import { QueueSystem } from "../modules/queueSystem.ts";
import {
	closeAll,
	connection,
	ensureRedis,
	runToCompletion,
	track,
	uniqueName,
} from "./support.ts";

const empty = z.object({});

describe("Step control flow (end-to-end via BullMQ)", () => {
	before(ensureRedis);
	after(closeAll);

	test("skip jumps to a named later step", async () => {
		const ran: string[] = [];
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("skip") }))
				.defineInputSchema(empty)
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "A",
							handler: async (job) => {
								ran.push("A");
								throw new job.actionErrors.skip("C");
							},
						})
						.addStep({
							name: "B",
							handler: async () => {
								ran.push("B");
								return {};
							},
						})
						.addStep({
							name: "C",
							handler: async () => {
								ran.push("C");
								return {};
							},
						})
						.addFinalStep({ handler: async () => ({ ok: true }) }),
				),
		);

		const out = await runToCompletion(queue, {});
		assert.deepStrictEqual(out, { ok: true });
		assert.deepStrictEqual(ran, ["A", "C"]); // B was skipped
	});

	test("earlyExit returns immediately", async () => {
		const ran: string[] = [];
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("early") }))
				.defineInputSchema(empty)
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "A",
							handler: async (job) => {
								ran.push("A");
								throw new job.actionErrors.earlyExit({ bailed: true });
							},
						})
						.addFinalStep({ handler: async () => ({ ok: true }) }),
				),
		);

		const out = await runToCompletion(queue, {});
		assert.deepStrictEqual(out, { bailed: true });
		assert.deepStrictEqual(ran, ["A"]);
	});

	test("rollback runs for completed steps on failure", async () => {
		const rolledBack: string[] = [];
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("rollback") }))
				.defineInputSchema(empty)
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "one",
							handler: async () => ({ one: true }),
							rollback: async () => {
								rolledBack.push("one");
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

		await assert.rejects(() => runToCompletion(queue, {}), /boom/);
		assert.deepStrictEqual(rolledBack, ["one"]);
	});
});
