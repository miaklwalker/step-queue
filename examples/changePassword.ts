import { z } from "zod";
import { NameFactory, QueueSystem } from "../modules";
import { connection } from "../tests/support.ts";

export const myQueue = new QueueSystem(
	NameFactory({ name: "myQueue", env: "dev" }),
)
	.defineConnection(connection)
	.addToSystem("Admin Queue")
	.defineInputSchema(
		z.object({
			user: z.string(),
			originalPassword: z.string().min(8).max(255),
			newPassword: z.string().min(8).max(255),
		}),
	)
	.defineProcessor((processor) =>
		processor
			.errors({
				MATCHING_PASSWORDS: "Your new password must match",
			})
			.addStep({
				name: "Validate Password",
				handler: async (job) => {
					const data = job.data;
					// verify passwords not match
					if (data.newPassword === data.originalPassword) {
						throw job.errors.MATCHING_PASSWORDS;
					}
					return { valid: true };
				},
			})
			.addStep({
				name: "Check User Password",
				clean: ["originalPassword", "newPassword"],
				handler: async (job) => {
					const data = job.data;
					return { userMatch: data.user === "Michael" };
				},
			})
			.addStep({
				name: "Report Success",
				handler: async (job) => {
					const data = job.data;
					return {
						message:
							data.userMatch && data.valid
								? "Your new password has been changed successfully"
								: "Your new password has not been changed successfully",
					};
				},
			})
			.addFinalStep({
				handler: async (job) => {
					return {
						greeting: job.data.message,
					};
				},
			}),
	);
myQueue.addDescription("this is a description");
// console.log("Adding job");
// const job = await myQueue.addJob({
// 	user: "Michael",
// 	originalPassword: "Your password must match",
// 	newPassword: "Your password must not match!!",
// });
// console.log(job.id);
