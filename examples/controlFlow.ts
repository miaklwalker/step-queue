import { z } from "zod";
import { NameFactory, QueueSystem } from "../modules";
import { connection } from "../tests/support.ts";

const attempts = new Map<string, number>();

const signup = new QueueSystem(NameFactory({ name: "signup", env: "example" }))
	.defineInputSchema(
		z.object({ email: z.string(), optOutOfEmail: z.boolean() }),
	)
	.defineConnection(connection)
	.defineProcessor((step) =>
		step
			.errors({
				NO_VALID_USER: "There is no valid user"
			})
			.addStep({
				name: "validate",
				handler: async (job) => {
					if (!job.data.email.includes("@")) {
						// bail out of the entire job with our own result shape
						if(!job.data.email.includes('.')){
							throw job.errors.NO_VALID_USER;
						}
						throw new job.actionErrors.earlyExit({
							status: "rejected",
							reason: "invalid email",
						});
					}
					return { validated: true };
				},
			})
			.addStep({
				name: "reserveUsername",
				handler: async (job) => {
					// pretend the username service is flaky on the first try. Throwing
					// a plain error lets BullMQ retry the job natively (enqueue with
					// `attempts` > 1); it resumes from this step, since the processor
					// persists its position in the job data.
					const tries = (attempts.get(job.data.email) ?? 0) + 1;
					attempts.set(job.data.email, tries);
					if (tries < 2) {
						throw new Error("username service unavailable, retrying");
					}
					return { username: job.data.email.split("@")[0] ?? "user" };
				},
			})
			.addStep({
				name: "sendWelcomeEmail",
				handler: async (job) => {
					if (job.data.optOutOfEmail) {
						// jump straight to "audit", skipping grantTrial
						throw new job.actionErrors.skip("audit", { emailed: false });
					}
					return { emailed: true };
				},
			})
			.addStep({
				name: "grantTrial",
				handler: async () => ({ trialDays: 30 }),
			})
			.addStep({
				name: "audit",
				handler: async (job) => {
					console.log(
						`[audit] ${job.data.username} — emailed=${job.data.emailed}`,
					);
					return {};
				},
			})
			.addFinalStep({
				handler: async (job) => ({
					status: "created",
					username: job.data.username,
				}),
			}),
	);
signup.addToSystem("Examples");
