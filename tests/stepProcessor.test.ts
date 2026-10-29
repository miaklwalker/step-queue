import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { UnrecoverableError } from "bullmq";
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

	test("a retried step resumes without rolling back earlier steps", async () => {
		const ran: string[] = [];
		const rolledBack: string[] = [];
		let twoAttempts = 0;
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("retry-no-rollback") }))
				.defineInputSchema(empty)
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "one",
							handler: async () => {
								ran.push("one");
								return { one: true };
							},
							rollback: async () => {
								rolledBack.push("one");
							},
						})
						.addStep({
							name: "two",
							handler: async () => {
								ran.push("two");
								twoAttempts += 1;
								if (twoAttempts < 3) {
									throw new Error("flaky");
								}
								return {};
							},
						})
						.addFinalStep({ handler: async (job) => ({ one: job.data.one }) }),
				),
		);

		// Fails twice, succeeds on the third (final) default attempt.
		const out = await runToCompletion(queue, {});
		assert.deepStrictEqual(out, { one: true });
		assert.deepStrictEqual(ran, ["one", "two", "two", "two"]);
		// Nothing rolled back: the earlier attempts' failures were retried.
		assert.deepStrictEqual(rolledBack, []);
	});

	test("an UnrecoverableError rolls back immediately, without retries", async () => {
		const ran: string[] = [];
		const rolledBack: string[] = [];
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("unrecoverable") }))
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
								ran.push("two");
								throw new UnrecoverableError("give up");
							},
						})
						.addFinalStep({ handler: async () => ({}) }),
				),
		);

		await assert.rejects(() => runToCompletion(queue, {}), /give up/);
		assert.deepStrictEqual(ran, ["two"]);
		assert.deepStrictEqual(rolledBack, ["one"]);
	});
});
