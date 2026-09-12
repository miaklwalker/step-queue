import {
	auditLogQueue,
	BAD_PRODUCT_ID,
	priceValidationQueue,
	productRepriceQueue,
	tshirtRepriceQueue,
	variantRepriceQueue,
} from "./tshirtReprice.ts";

const PRODUCT_IDS = Array.from(
	{ length: 20 },
	(_, i) => `tshirt-${String(i + 1).padStart(3, "0")}`,
);

async function main() {
	console.log(
		`Seeding TshirtReprice demo — ${PRODUCT_IDS.length} products, base price $20 (expect ${BAD_PRODUCT_ID} to fail)`,
	);
	const job = await tshirtRepriceQueue.addJob({
		productIds: PRODUCT_IDS,
		basePrice: 20,
	});
	console.log(`Enqueued root job ${job.id}`);

	try {
		const result = await job.waitUntilFinished(
			tshirtRepriceQueue.QueueEvents,
			5 * 60 * 1000,
		);
		console.log(
			`Reprice complete: ${result.repriced}/${result.requested} products repriced`,
		);
	} catch (err) {
		console.log(`Root job did not finish cleanly: ${(err as Error).message}`);
	}
}

main()
	.catch((err) => {
		console.error(err);
		process.exitCode = 1;
	})
	.finally(async () => {
		await Promise.all([
			tshirtRepriceQueue.close(),
			productRepriceQueue.close(),
			variantRepriceQueue.close(),
			priceValidationQueue.close(),
			auditLogQueue.close(),
		]);
	});
