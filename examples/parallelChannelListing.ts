import { z } from "zod";
import { NameFactory, QueueSystem } from "../modules/index.ts";
import { connection } from "../tests/support.ts";

/**
 * Parallel child queues: one step that fans out to several different queues
 * at once, with every queue's results still fully typed.
 *
 *   ListProduct (root)
 *     step "list" -> childQueues, all running in parallel:
 *        ebay     — 1 listing for the parent product   (singleChild)
 *        shopify  — 1 listing per variant              (array of results)
 *        amazon   — 1 listing, only for eligible items (singleChild, may be skipped)
 *     step "summarize" reads job.data["list-results"]
 *
 * Doing this with three sequential `childQueue` steps would also be type-safe,
 * but the parent would wait for eBay, then Shopify, then Amazon in turn. With
 * `childQueues` it waits once, for as long as the slowest channel takes.
 *
 * Everything is mock data — no real marketplace APIs.
 */

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const rootName = NameFactory({ name: "ListProduct", env: "demo" });

// ---- channel queues ----
// Each channel is an ordinary QueueSystem with its own input schema and output
// type. Simple API-call channels use defineHandler; one that needs steps can
// use defineProcessor — a parallel step doesn't care which.

export const ebayQueue = new QueueSystem(
	NameFactory({ name: "EbayListing", parent: rootName }),
)
	.defineInputSchema(z.object({ sku: z.string(), title: z.string() }))
	.defineConnection(connection)
	.defineHandler(async (job) => {
		await sleep(300); // mock API latency
		return {
			itemId: `EBAY-${job.data.sku}`,
			url: `https://ebay.example/${job.data.sku}`,
		};
	});

export const shopifyQueue = new QueueSystem(
	NameFactory({ name: "ShopifyVariantListing", parent: rootName }),
)
	.defineInputSchema(
		z.object({ sku: z.string(), size: z.string(), price: z.number() }),
	)
	.defineConnection(connection)
	.defineConcurrency(4)
	.defineProcessor((step) =>
		step
			.addStep({
				name: "upsertVariant",
				handler: async (job) => {
					await sleep(200);
					return { variantId: `SHOP-${job.data.sku}-${job.data.size}` };
				},
			})
			.addFinalStep({
				handler: async (job) => ({
					variantId: job.data.variantId,
					size: job.data.size,
					price: job.data.price,
				}),
			}),
	);

export const amazonQueue = new QueueSystem(
	NameFactory({ name: "AmazonListing", parent: rootName }),
)
	.defineInputSchema(z.object({ sku: z.string(), asin: z.string() }))
	.defineConnection(connection)
	.defineHandler(async (job) => {
		await sleep(400);
		return { asin: job.data.asin, status: "active" as const };
	});

// ---- the parent ----

const productSchema = z.object({
	sku: z.string(),
	title: z.string(),
	asin: z.string().optional(),
	variants: z.array(z.object({ size: z.string(), price: z.number() })),
});

export const listProductQueue = new QueueSystem(rootName)
	.defineInputSchema(productSchema)
	.defineConnection(connection)
	.defineProcessor((step) =>
		step
			.addStep({
				name: "list",
				// Keys here become the keys of job.data["list-results"].
				childQueues: {
					ebay: ebayQueue,
					shopify: shopifyQueue,
					amazon: amazonQueue,
				},
				// eBay and Amazon run at most one child each, so store their output
				// unwrapped. Shopify isn't listed, so it keeps an array.
				singleChild: { ebay: true, amazon: true },
				handler: async (job) => {
					const { sku, title, asin, variants } = job.data;
					// Enqueue everything at once; the processor then parks the parent
					// in waiting-children until every child, on every queue, completes.
					await Promise.all([
						ebayQueue.addChildJob(job, { sku, title }),
						shopifyQueue.addBulkChildJobs(
							job,
							variants.map((v) => ({
								data: { sku, size: v.size, price: v.price },
							})),
						),
						// Not every product is on Amazon. Skipping a queue is fine: its
						// key is `undefined` (singleChild) or `[]` (array).
						asin ? amazonQueue.addChildJob(job, { sku, asin }) : undefined,
					]);
				},
			})
			.addFinalStep({
				handler: async (job) => {
					// Typed as:
					// {
					//   ebay: { itemId: string; url: string } | undefined;
					//   shopify: { variantId: string; size: string; price: number }[];
					//   amazon: { asin: string; status: "active" } | undefined;
					// }
					const { ebay, shopify, amazon } = job.data["list-results"];
					return {
						sku: job.data.sku,
						ebayItemId: ebay?.itemId ?? null,
						shopifyVariantIds: shopify.map((v) => v.variantId).sort(),
						amazonAsin: amazon?.asin ?? null,
					};
				},
			}),
	);
listProductQueue.addToSystem("Examples");

async function main() {
	const started = Date.now();
	const job = await listProductQueue.addJob({
		sku: "TEE-001",
		title: "Classic Crew Tee",
		asin: "B0EXAMPLE1",
		variants: [
			{ size: "sm", price: 20 },
			{ size: "md", price: 22 },
			{ size: "lg", price: 24 },
		],
	});
	const result = await job.waitUntilFinished(
		listProductQueue.QueueEvents,
		30_000,
	);
	console.log(result);
	// Roughly the slowest channel (~400ms), not the sum of all three.
	console.log(`Listed on every channel in ${Date.now() - started}ms`);

	const noAmazon = await listProductQueue.addJob({
		sku: "TEE-002",
		title: "Vintage Wash Tee",
		variants: [{ size: "md", price: 25 }],
	});
	console.log(
		await noAmazon.waitUntilFinished(listProductQueue.QueueEvents, 30_000),
	);
}

main()
	.catch((err) => {
		console.error(err);
		process.exitCode = 1;
	})
	.finally(async () => {
		await Promise.all([
			listProductQueue.close(),
			ebayQueue.close(),
			shopifyQueue.close(),
			amazonQueue.close(),
		]);
	});
