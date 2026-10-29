import { z } from "zod";
import { NameFactory, QueueSystem } from "../modules/index.ts";
import { connection } from "../tests/support.ts";

/**
 * Simple handler queues: `defineHandler` instead of `defineProcessor`.
 *
 * Some queues are just commands — call an API, send a webhook, write an audit
 * row. Splitting that into steps adds nothing, but you still want the job in a
 * queue: retries, concurrency, and visibility in a dashboard like Bull Board.
 * `defineHandler` starts the worker with one plain function, the same as a raw
 * BullMQ `Worker`, while keeping the typed input and typed output.
 *
 *   ExchangeRate   — returns an object
 *   SlackNotify    — returns a plain string (outputs don't have to be objects)
 *   PriceInCurrency (steps) -> ExchangeRate (child, defineHandler)
 *
 * Everything is mock data — no real rate service or Slack workspace.
 */

const MOCK_RATES: Record<string, number> = { EUR: 0.92, GBP: 0.79, JPY: 151 };

// ---- a handler that returns an object ----
export const exchangeRateQueue = new QueueSystem(
	NameFactory({ name: "ExchangeRate", env: "demo" }),
)
	.defineInputSchema(z.object({ currency: z.string() }))
	.defineConnection(connection)
	.defineConcurrency(5)
	// `job.data` is typed `{ currency: string }` from the schema.
	// The queue's output type is exactly what this function returns:
	// `{ currency: string; rate: number; fetchedAt: string }`.
	.defineHandler(async (job) => {
		const rate = MOCK_RATES[job.data.currency];
		if (rate === undefined) {
			// Throwing fails the job like any BullMQ processor. With the default
			// job options (attempts: 3) it is retried twice, re-running this whole
			// handler, before it shows up as failed in Bull Board.
			throw new Error(`No rate for ${job.data.currency}`);
		}
		return {
			currency: job.data.currency,
			rate,
			fetchedAt: new Date().toISOString(),
		};
	});

// ---- a handler that returns a plain string ----
export const slackNotifyQueue = new QueueSystem(
	NameFactory({ name: "SlackNotify", env: "demo" }),
)
	.defineInputSchema(z.object({ channel: z.string(), text: z.string() }))
	.defineConnection(connection)
	.defineHandler(async (job) => {
		// Logs and progress work exactly as they do on a raw BullMQ job.
		await job.log(`posting to #${job.data.channel}`);
		await job.updateProgress(100);
		return `ts-${Date.now()}`; // QueueOutput<typeof slackNotifyQueue> = string
	});

// ---- a handler queue used as a typed child queue ----
// A defineHandler queue is a QueueSystem like any other, so a step processor
// can fan out to it and its return type flows into `<step>-results`.
export const priceInCurrencyQueue = new QueueSystem(
	NameFactory({ name: "PriceInCurrency", env: "demo" }),
)
	.defineInputSchema(
		z.object({ usdPrice: z.number(), currencies: z.array(z.string()) }),
	)
	.defineConnection(connection)
	.defineProcessor((step) =>
		step
			.addStep({
				name: "fetchRates",
				childQueue: exchangeRateQueue,
				handler: async (job) => {
					await exchangeRateQueue.addBulkChildJobs(
						job,
						job.data.currencies.map((currency) => ({ data: { currency } })),
					);
				},
			})
			.addFinalStep({
				handler: async (job) => {
					// typed { currency: string; rate: number; fetchedAt: string }[]
					const rates = job.data["fetchRates-results"];
					return Object.fromEntries(
						rates.map((r) => [
							r.currency,
							Math.round(job.data.usdPrice * r.rate * 100) / 100,
						]),
					);
				},
			}),
	);

async function main() {
	const rateJob = await exchangeRateQueue.addJob({ currency: "EUR" });
	const rate = await rateJob.waitUntilFinished(
		exchangeRateQueue.QueueEvents,
		10_000,
	);
	console.log(`EUR rate: ${rate.rate}`);

	const slackJob = await slackNotifyQueue.addJob({
		channel: "pricing",
		text: "Rates refreshed",
	});
	const messageTs = await slackJob.waitUntilFinished(
		slackNotifyQueue.QueueEvents,
		10_000,
	);
	console.log(`Slack message id: ${messageTs}`);

	const priceJob = await priceInCurrencyQueue.addJob({
		usdPrice: 25,
		currencies: ["EUR", "GBP", "JPY"],
	});
	const prices = await priceJob.waitUntilFinished(
		priceInCurrencyQueue.QueueEvents,
		10_000,
	);
	console.log("$25 in other currencies:", prices);
}

main()
	.catch((err) => {
		console.error(err);
		process.exitCode = 1;
	})
	.finally(async () => {
		await Promise.all([
			exchangeRateQueue.close(),
			slackNotifyQueue.close(),
			priceInCurrencyQueue.close(),
		]);
	});
