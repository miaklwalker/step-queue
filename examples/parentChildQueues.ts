import {NameFactory, QueueSystem} from "../modules";
import {connection} from "../tests/support.ts";
import {z} from "zod";

const childInputSchema = z.object({name: z.string()});
const childQueue = new QueueSystem(
    NameFactory({
        name: "childQueue",
        parent: NameFactory({name: "parentQueue"}),
    }),
)
    .defineInputSchema(childInputSchema)
    .defineConnection(connection)
    .defineProcessor((processor) =>
        processor.addStep({
            name: "Step One",
            handler: async (job) => {
                const data = job.data;
                const name = data.name;
                return {x: 5, sillyName: `${name}silly`};
            },
        }),
    );
const parentInputSchema = z.object({joke: z.string()});
const parentQueue = new QueueSystem(NameFactory({name: "parentQueue"}))
    .defineConnection(connection)
	.defineInputSchema(parentInputSchema)
    .defineProcessor((processor) =>
        processor
			.errors({
				NO_ERRORS_FOUND: "Error: Not Found",
			})
			.addStep({
				name: "Step One",
				handler: async (_job) => {
					return {y: 5};
				},
			})
            .addStep({
                name: "Test Children",
                childQueue: childQueue,
                handler: async (job) => {
                    await childQueue.addChildJob(job, {name: "frank"});
                },
            })
			.addStep({
				name: "Test Child",
				childQueue: childQueue,
				singleChild:true,
				handler: async (job) => {
					await childQueue.addChildJob(job, {name: "jeff"});
				},
			})
			.addStep({
				name: "Test processed Child",
				childQueue: childQueue,
				singleChild:true,
				handler: async (job) => {
					await childQueue.addChildJob(job, {name: "beans"});
				},
				processChildResult: results => {
					return { reallySillyName : `${results.sillyName} ${results.name}` }
				}
			})
			.addStep({
				name: "Step Three",
				handler: async (job) => {
					const data = job.data;
					await job.log(`${data}`);
					return {z: 5};
				},
			})
			.addStep({
				name: "Test Clean",
				clean: ['joke','y','Test Children-results','Test Child-results'],
				handler: async (job) => {
					const data = job.data;
					return {z: 5};
				},
			})
			.addStep({
				name: "nother one",
				handler: async (job) => {
					const data = job.data;
				},
			})
		,
    );
