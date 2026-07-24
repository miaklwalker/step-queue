import { z } from "zod";
import { defineStep, defineSteps, NameFactory, QueueSystem } from "../modules";
import { connection } from "../tests/support.ts";

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
onboarding.addToSystem("Examples");
