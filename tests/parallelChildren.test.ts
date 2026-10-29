import assert from "node:assert";
import test, { after, before, describe } from "node:test";
import { z } from "zod";
import {
	AddStepError,
	NameFactory,
	ProcessorBuilder,
	type QueueOutput,
	QueueSystem,
} from "../modules/index.ts";
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

describe("parallel child steps (builder, no connection needed)", () => {
	const a = new QueueSystem(NameFactory({ name: "a" }));
	const b = new QueueSystem(NameFactory({ name: "b" }));

	test("rejects the same queue listed twice in childQueues", () => {
		assert.throws(
			() =>
				ProcessorBuilder.init<{ id: string }>().addStep({
					name: "list",
					childQueues: { first: a, second: a },
					handler: async () => {},
				}),
			AddStepError,
		);
	});

	test("rejects childQueue and childQueues together", () => {
		assert.throws(
			() =>
				// @ts-expect-error - childQueue and childQueues are mutually exclusive
				ProcessorBuilder.init<{ id: string }>().addStep({
					name: "list",
					childQueues: { a, b },
					childQueue: a,
					handler: async () => {},
				}),
			AddStepError,
		);
	});

	test("rejects a singleChild key that isn't in childQueues", () => {
		assert.throws(
			() =>
				// @ts-expect-error - "c" is not one of the childQueues
				ProcessorBuilder.init<{ id: string }>().addStep({
					name: "list",
					childQueues: { a, b },
					singleChild: { a: true, c: true },
					handler: async () => {},
				}),
			/singleChild/,
		);
	});

	test("rejects inherited object keys in singleChild", () => {
		// A JavaScript caller (or a cast) can pass any key; prototype keys like
		// `constructor` must not slip past the own-key check.
		const singleChild = { constructor: true } as unknown as { a: true };
		assert.throws(
			() =>
				ProcessorBuilder.init<{ id: string }>().addStep({
					name: "list",
					childQueues: { a, b },
					singleChild,
					handler: async () => {},
				}),
			/singleChild/,
		);
	});

	test("a non-literal flag widens to both result shapes", () => {
		const flag = Math.random() > 0.5;
		ProcessorBuilder.init<{ id: string }>()
			.addStep({
				name: "list",
				childQueues: { a },
				singleChild: { a: flag },
				handler: async () => {},
			})
			.addFinalStep({
				handler: async (job) => {
					type _ = Expect<
						Equal<
							(typeof job.data)["list-results"]["a"],
							QueueOutput<typeof a> | undefined | QueueOutput<typeof a>[]
						>
					>;
					return {};
				},
			});
	});
});

describe("parallel child steps (end-to-end via BullMQ)", () => {
	before(ensureRedis);
	after(closeAll);

	const parentName = NameFactory({ name: uniqueName("listProduct") });

	const ebay = track(
		new QueueSystem(
			NameFactory({ name: uniqueName("ebay"), parent: parentName }),
		)
			.defineInputSchema(z.object({ sku: z.string() }))
			.defineConnection(connection)
			.defineHandler(async (job) => `ebay-${job.data.sku}`),
	);
	const shopify = track(
		new QueueSystem(
			NameFactory({ name: uniqueName("shopify"), parent: parentName }),
		)
			.defineInputSchema(z.object({ sku: z.string(), price: z.number() }))
			.defineConnection(connection)
			.defineProcessor((step) =>
				step.addFinalStep({
					handler: async (job) => ({
						listingId: `shopify-${job.data.sku}`,
						price: job.data.price,
					}),
				}),
			),
	);

	test("groups results by queue, each typed by its own queue", async () => {
		const parent = track(
			new QueueSystem(NameFactory({ name: uniqueName("parallel") }))
				.defineInputSchema(z.object({ skus: z.array(z.string()) }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "list",
							childQueues: { ebay, shopify },
							handler: async (job) => {
								await Promise.all([
									ebay.addBulkChildJobs(
										job,
										job.data.skus.map((sku) => ({ data: { sku } })),
									),
									shopify.addChildJob(job, { sku: "all", price: 10 }),
								]);
							},
						})
						.addFinalStep({
							handler: async (job) => {
								const results = job.data["list-results"];
								type _ = Expect<
									Equal<
										typeof results,
										{
											ebay: string[];
											shopify: { listingId: string; price: number }[];
										}
									>
								>;
								return {
									ebay: [...results.ebay].sort(),
									shopify: results.shopify,
								};
							},
						}),
				),
		);

		const out = await runToCompletion(parent, { skus: ["b", "a"] });
		assert.deepStrictEqual(out, {
			ebay: ["ebay-a", "ebay-b"],
			shopify: [{ listingId: "shopify-all", price: 10 }],
		});
	});

	test("singleChild unwraps per queue; a skipped queue is undefined", async () => {
		const parent = track(
			new QueueSystem(NameFactory({ name: uniqueName("parallelSingle") }))
				.defineInputSchema(z.object({ sku: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "list",
							childQueues: { ebay, shopify },
							singleChild: true,
							handler: async (job) => {
								await ebay.addChildJob(job, { sku: job.data.sku });
							},
							processChildResult: (results) => {
								type _ = Expect<
									Equal<
										typeof results,
										{
											ebay: string | undefined;
											shopify: { listingId: string; price: number } | undefined;
										}
									>
								>;
								return {
									ebay: results.ebay ?? null,
									shopify: results.shopify?.listingId ?? null,
								};
							},
						})
						.addFinalStep({
							handler: async (job) => job.data["list-results"],
						}),
				),
		);

		type Out = QueueOutput<typeof parent>;
		type _ = Expect<
			Equal<Out, { ebay: string | null; shopify: string | null }>
		>;

		const out = await runToCompletion(parent, { sku: "x" });
		assert.deepStrictEqual(out, { ebay: "ebay-x", shopify: null });
	});

	test("a step that adds no children still stores empty results", async () => {
		const parent = track(
			new QueueSystem(NameFactory({ name: uniqueName("parallelEmpty") }))
				.defineInputSchema(z.object({ sku: z.string() }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "list",
							childQueues: { ebay, shopify },
							singleChild: { ebay: true },
							// Nothing to list on any channel.
							handler: async () => {},
						})
						.addStep({
							name: "single",
							childQueue: ebay,
							handler: async () => {},
						})
						.addStep({
							name: "mapped",
							childQueue: ebay,
							handler: async () => {},
							processChildResult: (results) => ({ count: results.length }),
						})
						.addFinalStep({
							handler: async (job) => ({
								list: job.data["list-results"],
								single: job.data["single-results"],
								mapped: job.data["mapped-results"],
							}),
						}),
				),
		);

		const out = await runToCompletion(parent, { sku: "x" });
		// `ebay: undefined` doesn't survive JSON, so the key is absent.
		assert.deepStrictEqual(out, {
			list: { shopify: [] },
			single: [],
			mapped: { count: 0 },
		});
	});

	test("singleChild can be set per queue", async () => {
		const parent = track(
			new QueueSystem(NameFactory({ name: uniqueName("parallelMixed") }))
				.defineInputSchema(z.object({ skus: z.array(z.string()) }))
				.defineConnection(connection)
				.defineProcessor((step) =>
					step
						.addStep({
							name: "list",
							childQueues: { ebay, shopify },
							singleChild: { ebay: true },
							handler: async (job) => {
								await Promise.all([
									ebay.addChildJob(job, { sku: "parent" }),
									shopify.addBulkChildJobs(
										job,
										job.data.skus.map((sku) => ({ data: { sku, price: 5 } })),
									),
								]);
							},
						})
						.addFinalStep({
							handler: async (job) => {
								const results = job.data["list-results"];
								type _ = Expect<
									Equal<
										typeof results,
										{
											ebay: string | undefined;
											shopify: { listingId: string; price: number }[];
										}
									>
								>;
								return {
									ebay: results.ebay,
									shopify: results.shopify.map((r) => r.listingId).sort(),
								};
							},
						}),
				),
		);

		const out = await runToCompletion(parent, { skus: ["red", "blue"] });
		assert.deepStrictEqual(out, {
			ebay: "ebay-parent",
			shopify: ["shopify-blue", "shopify-red"],
		});
	});
});
