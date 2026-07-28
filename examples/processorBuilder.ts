import { NameFactory, ProcessorBuilder, QueueSystem } from "../modules";
import { connection } from "../tests/support.ts";

const manualBuilder = new QueueSystem(NameFactory({ name: "test" }))
	.defineConnection(connection)
	.defineProcessor(
		ProcessorBuilder.init<{ foo: string }>()
			.addStep({
				name: "check",
				handler: async (_job) => {
					return { bar: "bar" };
				},
			})
			.addStep({
				name: "please",
				handler: async (_job) => {
					return { baz: "baz" };
				},
			})
			.addFinalStep({
				handler: async (job) => {
					const data = job.data;

					const foo = data.foo;
					const bar = data.bar;
					const baz = data.baz;

					await job.log(
						JSON.stringify({
							foo,
							bar,
							baz,
						}),
					);

					return {
						qux: "qux",
					};
				},
			}),
	);
manualBuilder.addToSystem("Examples");
