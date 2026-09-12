import { z } from "zod";
import { NameFactory, QueueSystem } from "../modules/index.ts";
import { connection } from "../tests/support.ts";

/**
 * Ecommerce t-shirt repricing demo, built to exercise a queue visualizer
 * against a deep, wide job tree:
 *
 *   TshirtReprice (root)
 *     -> ProductReprice        (100 children — one per product id)
 *          -> VariantReprice   (4 children — sm/md/lg/xl)
 *               -> PriceValidation  (1 child)
 *                    -> AuditLog    (1 child, leaf)
 *
 * Five queue levels deep, and one step ("fanOutProducts") with exactly 100
 * children. Everything is mock data — no real catalog or pricing service.
 */

export const SIZES = ["sm", "md", "lg", "xl"] as const;
export type Size = (typeof SIZES)[number];

const SIZE_MULTIPLIERS: Record<Size, number> = {
	sm: 1,
	md: 1.1,
	lg: 1.2,
	xl: 1.35,
};

const STYLES = [
	"Classic Crew",
	"Vintage Wash",
	"Graphic Print",
	"Long Sleeve",
	"V-Neck",
	"Henley",
	"Pocket Tee",
	"Raglan",
	"Oversized",
	"Slim Fit",
];

const MIN_PRICE = 5;
const MAX_PRICE = 200;

/** Deliberately missing from the mock catalog, so one of the 100 fan-out children fails. */
export const BAD_PRODUCT_ID = "tshirt-017";

// ---- queue names, nested outward from the root ----
const rootName = NameFactory({ name: "TshirtReprice", env: "demo" });
const productName = NameFactory({ name: "ProductReprice", env: "demo"});
const variantName = NameFactory({
	name: "VariantReprice",
	env: "demo"
});
const priceValidationName = NameFactory({
	name: "PriceValidation",
	env: "demo"
});
const auditLogName = NameFactory({
	name: "AuditLog",
	env: "demo"
});

// ---- level 5 (leaf): write a mock audit entry for one variant's new price ----
export const auditLogQueue = new QueueSystem(auditLogName)
	.defineConnection(connection)
	.defineConcurrency(20)
	.addToSystem("Ecommerce Demo")
	.addDescription("Writes a mock audit log entry for a single variant reprice")
	.defineInputSchema(
		z.object({
			productId: z.string(),
			variant: z.enum(SIZES),
			price: z.number(),
		}),
	)
	.defineProcessor((processor) =>
		processor
			.addStep({
				name: "writeAuditEntry",
				handler: async (job) => {
					const auditId = `audit-${job.data.productId}-${job.data.variant}-${Math.random()
						.toString(36)
						.slice(2, 8)}`;
					return { auditId };
				},
			})
			.addFinalStep({
				handler: async (job) => ({ auditId: job.data.auditId }),
			}),
	);

// ---- level 4: clamp the computed price into business bounds, then audit it ----
export const priceValidationQueue = new QueueSystem(priceValidationName)
	.defineConnection(connection)
	.defineConcurrency(20)
	.addToSystem("Ecommerce Demo")
	.addDescription(
		"Clamps a variant's computed price to business bounds and audits it",
	)
	.defineInputSchema(
		z.object({
			productId: z.string(),
			variant: z.enum(SIZES),
			price: z.number(),
		}),
	)
	.defineProcessor((processor) =>
		processor
			.addStep({
				name: "checkPriceBounds",
				handler: async (job) => ({
					price: Math.min(Math.max(job.data.price, MIN_PRICE), MAX_PRICE),
				}),
			})
			.addStep({
				name: "logAudit",
				childQueue: auditLogQueue,
				singleChild: true,
				handler: async (job) => {
					await auditLogQueue.addChildJob(job, {
						productId: job.data.productId,
						variant: job.data.variant,
						price: job.data.price,
					});
				},
				processChildResult: (result) => ({ auditId: result.auditId }),
			})
			.addFinalStep({
				handler: async (job) => ({
					price: job.data.price,
					auditId: job.data["logAudit-results"].auditId,
				}),
			}),
	);

// ---- level 3: compute one variant's price, then validate + audit it ----
export const variantRepriceQueue = new QueueSystem(variantName)
	.defineConnection(connection)
	.defineConcurrency(20)
	.addToSystem("Ecommerce Demo")
	.addDescription(
		"Computes and validates the new price for a single size variant",
	)
	.defineInputSchema(
		z.object({
			productId: z.string(),
			variant: z.enum(SIZES),
			basePrice: z.number().positive(),
		}),
	)
	.defineProcessor((processor) =>
		processor
			.addStep({
				name: "computeVariantPrice",
				handler: async (job) => ({
					price:
						Math.round(
							job.data.basePrice * SIZE_MULTIPLIERS[job.data.variant] * 100,
						) / 100,
				}),
			})
			.addStep({
				name: "validatePrice",
				childQueue: priceValidationQueue,
				singleChild: true,
				handler: async (job) => {
					await priceValidationQueue.addChildJob(job, {
						productId: job.data.productId,
						variant: job.data.variant,
						price: job.data.price,
					});
				},
			})
			.addFinalStep({
				handler: async (job) => ({
					variant: job.data.variant,
					price: job.data["validatePrice-results"].price,
				}),
			}),
	);

// ---- level 2: look up the product, then fan out its four size variants ----
export const productRepriceQueue = new QueueSystem(productName)
	.defineConnection(connection)
	.defineConcurrency(10)
	.addToSystem("Ecommerce Demo")
	.addDescription("Looks up one product and reprices all of its size variants")
	.defineInputSchema(
		z.object({
			productId: z.string(),
			basePrice: z.number().positive(),
		}),
	)
	.defineProcessor((processor) =>
		processor
			.addStep({
				name: "lookupProduct",
				handler: async (job) => {
					// Mock catalog lookup — one product id is intentionally missing.
					if (job.data.productId === BAD_PRODUCT_ID) {
						throw new Error(
							`Product ${job.data.productId} was not found in the catalog`,
						);
					}
					const index = Number(job.data.productId.split("-")[1] ?? "0");
					return { name: STYLES[index % STYLES.length] ?? "Classic Tee" };
				},
			})
			.addStep({
				name: "fanOutVariants",
				childQueue: variantRepriceQueue,
				handler: async (job) => {
					await variantRepriceQueue.addBulkChildJobs(
						job,
						SIZES.map((variant) => ({
							data: {
								productId: job.data.productId,
								variant,
								basePrice: job.data.basePrice,
							},
						})),
					);
				},
			})
			.addFinalStep({
				handler: async (job) => ({
					productId: job.data.productId,
					name: job.data.name,
					variants: job.data["fanOutVariants-results"],
				}),
			}),
	);

// ---- level 1 (root): validate the request, then fan out to 100 products ----
export const tshirtRepriceQueue = new QueueSystem(rootName)
	.defineConnection(connection)
	.defineConcurrency(5)
	.addToSystem("Ecommerce Demo")
	.addDescription("Reprices every variant of a batch of t-shirt product ids")
	.defineInputSchema(
		z.object({
			productIds: z.array(z.string()).min(1),
			basePrice: z.number().positive(),
		}),
	)
	.defineProcessor((processor) =>
		processor
			.addStep({
				name: "validateRequest",
				handler: async (job) => ({
					requestedCount: job.data.productIds.length,
				}),
			})
			.addStep({
				name: "fanOutProducts",
				childQueue: productRepriceQueue,
				handler: async (job) => {
					// One bad product id fails its job; ignoreDependencyOnFailure keeps
					// that from blocking the other 99 (and this root job) forever.
					await productRepriceQueue.addBulkChildJobs(
						job,
						job.data.productIds.map((productId) => ({
							data: { productId, basePrice: job.data.basePrice },
							opts: { },
						})),
					);
				},
			})
			.addFinalStep({
				handler: async (job) => ({
					requested: job.data.requestedCount,
					repriced: job.data["fanOutProducts-results"].length,
					products: job.data["fanOutProducts-results"],
				}),
			}),
	);
