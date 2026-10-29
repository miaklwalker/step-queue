import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import { z } from "zod";
import { NameFactory } from "../modules/nameFactory.ts";
import { ProcessorBuilder } from "../modules/processorBuilder.ts";
import { type QueueOutput, QueueSystem } from "../modules/queueSystem.ts";
import { type JSONLike, SchemaValidationError } from "../modules/types.ts";
import {
	closeAll,
	connection,
	type Equal,
	type Expect,
	ensureRedis,
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

	test("accepts a non-Zod Standard Schema and leaves jsonSchema unset", () => {
		// A hand-rolled Standard Schema — any spec-compliant library works.
		const custom: StandardSchemaV1<unknown, { n: number }> = {
			"~standard": {
				version: 1,
				vendor: "hand-rolled",
				validate: (value) => {
					const v = value as { n?: unknown };
					return typeof v?.n === "number"
						? { value: { n: v.n } }
						: { issues: [{ message: "n must be a number", path: ["n"] }] };
				},
			},
		};
		const q = new QueueSystem(NameFactory({ name: "Q" })).defineInputSchema(
			custom,
		);
		type In = Parameters<typeof q.addJob>[0];
		type _ = Expect<Equal<In, { n: number }>>;
		// The vendor exposes no JSON Schema converter, so none is derived.
		assert.strictEqual(q.jsonSchema, undefined);
	});

	test("defineProcessor without a connection is a type error, and throws", () => {
		const q = new QueueSystem(NameFactory({ name: "Q" })).defineInputSchema(
			z.object({ greeting: z.string() }),
		);
		assert.throws(
			() =>
				// @ts-expect-error - `this` requires a connected system
				q.defineProcessor((step) =>
					step.addStep({ name: "s", handler: async () => ({}) }),
				),
			/defineConnection/,
		);
	});

	test("the ProcessorBuilder overload requires a connection too", () => {
		const q = new QueueSystem(NameFactory({ name: "Q" }));
		assert.throws(
			() =>
				// @ts-expect-error - `this` requires a connected system
				q.defineProcessor(
					ProcessorBuilder.init<{ greeting: string }>().addStep({
						name: "s",
						handler: async () => ({}),
					}),
				),
			/defineConnection/,
		);
	});

	test("defineHandler without a connection is a type error, and throws", () => {
		const q = new QueueSystem(NameFactory({ name: "Q" }));
		assert.throws(
			() =>
				// @ts-expect-error - `this` requires a connected system
				q.defineHandler(async () => "ok"),
			/defineConnection/,
		);
	});

	test("addJob throws before the queue is initialized", async () => {
		const q = new QueueSystem(NameFactory({ name: "Q" })).defineInputSchema(
			z.object({ greeting: z.string() }),
		);
		await assert.rejects(() => q.addJob({ greeting: "hi" }), /not initialized/);
	});

	test("defineConnection collapses `queue`, defineProcessor collapses `worker`", () => {
		// Type-only: touching `.queue` here would open a real connection.
		const fresh = new QueueSystem(NameFactory({ name: "Q" })).defineInputSchema(
			z.object({ greeting: z.string() }),
		);
		type FreshQueue = typeof fresh.queue;
		type FreshWorker = typeof fresh.worker;
		type _QueueOptional = Expect<
			Equal<Extract<FreshQueue, undefined>, undefined>
		>;
		type _WorkerOptional = Expect<
			Equal<Extract<FreshWorker, undefined>, undefined>
		>;

		// defineConnection narrows `queue` only — the worker doesn't exist yet.
		const connected = fresh.defineConnection(connection);
		type ConnectedQueue = typeof connected.queue;
		type _QueueDefined = Expect<
			Equal<Extract<ConnectedQueue, undefined>, never>
		>;
		type _SameQueue = Expect<Equal<ConnectedQueue, NonNullable<FreshQueue>>>;
		type _WorkerStillOptional = Expect<
			Equal<Extract<typeof connected.worker, undefined>, undefined>
		>;

		// defineProcessor narrows both.
		type Built = ReturnType<typeof connected.defineProcessor<{ ok: boolean }>>;
		type _BuiltQueue = Expect<Equal<Extract<Built["queue"], undefined>, never>>;
		type _BuiltWorker = Expect<
			Equal<Extract<Built["worker"], undefined>, never>
		>;

		// Explicitly-`false` flags are exactly `undefined`, not `T | undefined`.
		type Neither = QueueSystem<
			{ greeting: string },
			JSONLike,
			"Q",
			false,
			false
		>;
		type _NoQueue = Expect<Equal<Neither["queue"], undefined>>;
		type _NoWorker = Expect<Equal<Neither["worker"], undefined>>;

		// A narrowed system still satisfies a plain three-argument annotation.
		const asAny: QueueSystem<{ greeting: string }, JSONLike, string> =
			connected;
		assert.strictEqual(asAny.name, "Q");
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
		// The narrowed `queue` is a real, single BullMQ queue instance.
		assert.strictEqual(queue.queue.name, queue.name);
		assert.strictEqual(queue.queue, queue.queue);
		const out = await runToCompletion(queue, { greeting: "hi" });
		assert.deepStrictEqual(out, { done: "HI" });
	});

	test("processes a job defined via a standalone ProcessorBuilder", async () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("builder"), env: "test" }))
				.defineConnection(connection)
				.defineProcessor(
					ProcessorBuilder.init<{ greeting: string }>()
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

		// Input AND output types are inferred from the builder.
		type Out = QueueOutput<typeof queue>;
		type _ = Expect<Equal<Out, { done: string }>>;

		const out = await runToCompletion(queue, { greeting: "hey" });
		assert.deepStrictEqual(out, { done: "HEY" });
	});

	test("starting a second worker on the same queue throws", () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("twice") }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({ name: "s", handler: async () => ({}) }),
				),
		);
		assert.throws(
			() => queue.defineHandler(async () => "again"),
			/already has a worker/,
		);
		assert.throws(
			() =>
				queue.defineProcessor((step) =>
					step.addStep({ name: "s", handler: async () => ({}) }),
				),
			/already has a worker/,
		);
	});

	test("processes a job with a plain defineHandler", async () => {
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("handler") }))
				.defineInputSchema(z.object({ greeting: z.string() }))
				.defineConnection(connection)
				.defineHandler(async (job) => {
					type _In = Expect<Equal<typeof job.data.greeting, string>>;
					return job.data.greeting.toUpperCase();
				}),
		);

		// The output is exactly what the handler returns — not limited to objects.
		type Out = QueueOutput<typeof queue>;
		type _ = Expect<Equal<Out, string>>;
		assert.ok(queue.worker);

		const out = await runToCompletion(queue, { greeting: "hi" });
		assert.strictEqual(out, "HI");
	});

	test("a defineHandler queue works as a typed child queue", async () => {
		const child = track(
			new QueueSystem(NameFactory({ name: uniqueName("handlerChild") }))
				.defineInputSchema(z.object({ n: z.number() }))
				.defineConnection(connection)
				.defineHandler(async (job) => ({ doubled: job.data.n * 2 })),
		);
		const parent = track(
			new QueueSystem(NameFactory({ name: uniqueName("handlerParent") }))
				.defineInputSchema(z.object({ ns: z.array(z.number()) }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "fan",
							childQueue: child,
							handler: async (job) => {
								await child.addBulkChildJobs(
									job,
									job.data.ns.map((n) => ({ data: { n } })),
								);
							},
						})
						.addFinalStep({
							handler: async (job) => ({
								total: job.data["fan-results"].reduce(
									(sum, r) => sum + r.doubled,
									0,
								),
							}),
						}),
				),
		);

		const out = await runToCompletion(parent, { ns: [1, 2, 3] });
		assert.deepStrictEqual(out, { total: 12 });
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
		await assert.rejects(() => queue.addJob(bad), SchemaValidationError);
	});

	test("validates via a non-Zod Standard Schema at enqueue time", async () => {
		const custom: StandardSchemaV1<unknown, { n: number }> = {
			"~standard": {
				version: 1,
				vendor: "hand-rolled",
				validate: (value) => {
					const v = value as { n?: unknown };
					return typeof v?.n === "number"
						? { value: { n: v.n } }
						: { issues: [{ message: "n must be a number", path: ["n"] }] };
				},
			},
		};
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("std-schema") }))
				.defineInputSchema(custom)
				.defineConnection(connection)
				.defineProcessor((step) =>
					step.addStep({ name: "noop", handler: async () => ({}) }),
				),
		);

		const bad = { n: "nope" } as unknown as { n: number };
		await assert.rejects(
			() => queue.addJob(bad),
			(err: unknown) =>
				err instanceof SchemaValidationError &&
				err.message === "n: n must be a number",
		);
		await queue.addJob({ n: 1 });
	});
});

describe("processor action errors (control flow)", () => {
	before(ensureRedis);
	after(closeAll);

	test("earlyExit returns its value and bypasses every later step", async () => {
		const visited: string[] = [];
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("early-exit") }))
				.defineInputSchema(z.object({ email: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "validate",
							handler: async (job) => {
								visited.push("validate");
								throw new job.actionErrors.earlyExit({
									status: "rejected",
									reason: "invalid email",
								});
							},
						})
						.addStep({
							name: "enrich",
							handler: async () => {
								visited.push("enrich");
								return { enriched: true };
							},
						})
						.addFinalStep({
							handler: async () => {
								visited.push("final");
								return { status: "created" };
							},
						}),
				),
		);

		const out = await runToCompletion(queue, { email: "nope" });
		// earlyExit's value is returned verbatim as the job result.
		assert.deepStrictEqual(out, {
			status: "rejected",
			reason: "invalid email",
		});
		// No later step ran — not even the final step.
		assert.deepStrictEqual(visited, ["validate"]);
	});

	test("skip jumps to the named step, bypassing the ones in between", async () => {
		const visited: string[] = [];
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("skip-named") }))
				.defineInputSchema(z.object({ start: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "a",
							handler: async (job) => {
								visited.push("a");
								throw new job.actionErrors.skip("c", { emailed: false });
							},
						})
						.addStep({
							name: "b",
							handler: async () => {
								visited.push("b");
								return { ran_b: true };
							},
						})
						.addStep({
							name: "c",
							handler: async () => {
								visited.push("c");
								return { ran_c: true };
							},
						})
						.addFinalStep({
							handler: async (job) => {
								visited.push("final");
								const data = job.data as Record<string, unknown>;
								return {
									emailed: data.emailed,
									ran_b: data.ran_b ?? false,
									ran_c: data.ran_c ?? false,
								};
							},
						}),
				),
		);

		const out = await runToCompletion(queue, { start: "go" });
		// "b" is skipped; "c" and the final step still run.
		assert.deepStrictEqual(visited, ["a", "c", "final"]);
		// skip's result payload is merged into the job data before the target runs.
		assert.deepStrictEqual(out, {
			emailed: false,
			ran_b: false,
			ran_c: true,
		});
	});

	test("skipToFinal jumps straight to the final step, merging its result", async () => {
		const visited: string[] = [];
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("skip-final") }))
				.defineInputSchema(z.object({ start: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "a",
							handler: async (job) => {
								visited.push("a");
								throw new job.actionErrors.skipToFinal(null, {
									jumped: true,
								});
							},
						})
						.addStep({
							name: "b",
							handler: async () => {
								visited.push("b");
								return { ran_b: true };
							},
						})
						.addStep({
							name: "c",
							handler: async () => {
								visited.push("c");
								return { ran_c: true };
							},
						})
						.addFinalStep({
							handler: async (job) => {
								visited.push("final");
								const data = job.data as Record<string, unknown>;
								return { done: true, jumped: data.jumped };
							},
						}),
				),
		);

		const out = await runToCompletion(queue, { start: "go" });
		// Everything between the throwing step and the final step is bypassed.
		assert.deepStrictEqual(visited, ["a", "final"]);
		assert.deepStrictEqual(out, { done: true, jumped: true });
	});

	test("a rejected step runs rollbacks in reverse and fails the job", async () => {
		const visited: string[] = [];
		const rolledBack: string[] = [];
		const queue = track(
			new QueueSystem(NameFactory({ name: uniqueName("rollback-reverse") }))
				.defineInputSchema(z.object({ start: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "a",
							rollbackKeys: ["ran_a"],
							handler: async () => {
								visited.push("a");
								return { ran_a: true };
							},
							rollback: async (job) => {
								const data = job.data as Record<string, unknown>;
								rolledBack.push(`a:${String(data.ran_a)}`);
							},
						})
						.addStep({
							name: "b",
							handler: async () => {
								visited.push("b");
								throw new Error("boom");
							},
						})
						.addFinalStep({
							handler: async () => {
								visited.push("final");
								return { done: true };
							},
						}),
				),
		);

		await assert.rejects(() => runToCompletion(queue, { start: "go" }), /boom/);
		// "b" throws on every attempt (3 by default), so the final step never
		// runs. Each retry resumes on "b": "a" ran once and is never re-run.
		assert.deepStrictEqual(visited, ["a", "b", "b", "b"]);
		// Rollbacks wait for the final failure, so "a" rolls back exactly once,
		// with its requested keys.
		assert.deepStrictEqual(rolledBack, ["a:true"]);
	});
});
