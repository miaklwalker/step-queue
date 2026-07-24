import { z } from "zod";
import { NameFactory, QueueSystem } from "../modules/index.ts";
import { connection } from "../tests/support.ts";

const attempts = new Map<string, number>();

const signup = new QueueSystem(NameFactory({ name: "signup", env: "example" }))
	.defineInputSchema(
		z.object({ email: z.string(), optOutOfEmail: z.boolean() }),
	)
	.defineConnection(connection)
	.defineProcessor((step) =>
		step
			.addStep({
				name: "validate",
				handler: async (job) => {
					if (!job.data.email.includes("@")) {
						// bail out of the entire job with our own result shape
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
					// pretend the username service is flaky on the first try
					const tries = (attempts.get(job.data.email) ?? 0) + 1;
					attempts.set(job.data.email, tries);
					if (tries < 2) {
						throw new job.actionErrors.retry("reserveUsername", {}, 3);
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

async function run(input: { email: string; optOutOfEmail: boolean }) {
	const job = await signup.addJob(input);
	const result = await job.waitUntilFinished(signup.QueueEvents, 10_000);
	console.log(`${input.email} ->`, result);
}

await run({ email: "not-an-email", optOutOfEmail: false }); // earlyExit
await run({ email: "ada@example.com", optOutOfEmail: false }); // retry, full path
await run({ email: "grace@example.com", optOutOfEmail: true }); // skip grantTrial

await signup.close();
process.exit(0);
