import { z } from "zod";
import { NameFactory, QueueSystem } from "../modules";

const connection = {
	host: process.env.REDIS_HOST ?? "127.0.0.1",
	port: Number(process.env.REDIS_PORT ?? 6379),
};

const importer = new QueueSystem(
	NameFactory({ name: "importer", env: "example" }),
)
	.defineInputSchema(z.object({ rows: z.number() }))
	.defineConnection(connection)
	.defineProcessor((step) =>
		step
			.addStep({
				name: "import",
				progressReport: async (_job, _token, stepName, status) => {
					console.log(`[step] ${stepName} -> ${status}`);
				},
				handler: async (job) => {
					for (let done = 0; done <= job.data.rows; done += 25) {
						const percent = Math.round((done / job.data.rows) * 100);
						await job.updateStepProgress({ imported: done, percent });
					}
					return { imported: job.data.rows };
				},
			})
			.addFinalStep({
				handler: async (job) => ({ imported: job.data.imported }),
			}),
	);
importer.addToSystem("Examples");
// // Listen for the worker's progress events (typed listener, no `any`).
// importer.addWorkerEventListener("progress", (_job, progress) => {
// 	console.log("[progress]", progress);
// });
//
// const job = await importer.addJob({ rows: 100 });
// console.log(
// 	"result:",
// 	await job.waitUntilFinished(importer.QueueEvents, 10_000),
// );
//
// await importer.close();
// process.exit(0);
