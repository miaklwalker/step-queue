import { z } from "zod";
import { NameFactory, QueueSystem } from "../modules";
import {connection} from "../tests/support.ts";


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
