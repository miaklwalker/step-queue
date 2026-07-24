import { z } from "zod";
import {
	defineStep,
	defineSteps,
	NameFactory,
	QueueSystem,
} from "../modules/index.ts";

/**
 * Steps don't have to be written inline. `defineStep` builds a reusable,
 * fully-typed step you can drop into any processor with `.addStep(theStep)`.
 * `defineSteps` groups a few together.
 *
 * TypeScript enforces that the processor's accumulated data satisfies each
 * step's required input before you're allowed to add it.
 *
 * Run it:  node --experimental-transform-types examples/reusableSteps.ts
 */
const connection = {
	host: process.env.REDIS_HOST ?? "127.0.0.1",
	port: Number(process.env.REDIS_PORT ?? 6379),
};

// A step that needs `{ userId: string }` and adds `{ user }`.
const loadUser = defineStep<{ userId: string }>()({
	name: "loadUser",
	handler: async (job) => {
		// imagine a DB lookup here
		return { user: { id: job.data.userId, name: "Ada" } };
	},
});

// A little library of steps that only need what they declare.
const common = defineSteps({
	stampReceivedAt: defineStep<{ userId: string }>()({
		name: "stampReceivedAt",
		handler: async () => ({ receivedAt: "2026-07-23T00:00:00Z" }),
	}),
	audit: defineStep<{ user: { name: string } }>()({
		name: "audit",
		handler: async (job) => {
			console.log(`[audit] processed ${job.data.user.name}`);
			return {};
		},
	}),
});

const onboarding = new QueueSystem(
	NameFactory({ name: "onboarding", env: "example" }),
)
	.defineInputSchema(z.object({ userId: z.string() }))
	.defineConnection(connection)
	.defineProcessor((step) =>
		step
			.addStep(common.stampReceivedAt)
			.addStep(loadUser)
			.addStep(common.audit)
			.addFinalStep({
				handler: async (job) => ({
					welcome: `Welcome, ${job.data.user.name}!`,
					receivedAt: job.data.receivedAt,
				}),
			}),
	);

const job = await onboarding.addJob({ userId: "u_42" });
console.log(
	"result:",
	await job.waitUntilFinished(onboarding.QueueEvents, 10_000),
);

await onboarding.close();
process.exit(0);
