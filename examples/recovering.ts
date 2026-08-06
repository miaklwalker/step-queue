import { Redis } from "ioredis";
import { z } from "zod";
import { NameFactory, QueueSystem } from "../modules/index.ts";
import { connection } from "../tests/support.ts";

// A plain Redis client standing in for "some external system's state" — the
// same shape as an API that reports whether a brand has been imported into a
// channel yet. It lives outside job data on purpose: `gate` can't fix it
// itself, only `flipFlag` can.
const redis = new Redis(connection);

const flipFlag = new QueueSystem(
	NameFactory({ name: "flipFlag", env: "example" }),
)
	.defineInputSchema(z.object({ key: z.string() }))
	.defineConnection(connection)
	.defineProcessor((step) =>
		step.addStep({
			name: "flip",
			handler: async (job) => {
				await redis.set(job.data.key, "true");
			},
		}),
	);
flipFlag.addToSystem("Examples");

const gate = new QueueSystem(NameFactory({ name: "gate", env: "example" }))
	.defineInputSchema(z.object({ key: z.string() }))
	.defineConnection(connection)
	.defineProcessor((step) =>
		step
			.addStep({
				name: "setFalse",
				handler: async (job) => {
					// Whatever this key represents starts out not ready.
					await redis.set(job.data.key, "false");
				},
			})
			.addStep({
				name: "checkFlag",
				handler: async (job) => {
					const value = await redis.get(job.data.key);
					if (value !== "true") {
						// Not ready — spawn `flipFlag` as a child job, park this job,
						// and retry this exact step once the child completes. No
						// QueueEvents listener, and no BullMQ 'failed' event fires.
						throw new job.actionErrors.recover(flipFlag, {
							key: job.data.key,
						});
					}
					return { ready: true };
				},
			})
			.addFinalStep({
				handler: async (job) => ({ key: job.data.key, ready: job.data.ready }),
			}),
	);
console.log("Adding Job")
gate.addToSystem("Examples");
// Enqueueing { key: "some-unique-key" } on `gate` runs "setFalse", fails
// "checkFlag" once, waits on a "flip" child job, then retries "checkFlag" —
// which now finds "true" and finishes normally.
