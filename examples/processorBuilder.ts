import {NameFactory, ProcessorBuilder, QueueSystem} from "../modules";

new QueueSystem(
    NameFactory({ name: "test" }),
)

ProcessorBuilder
    .init<{ foo: string }>()
    .addStep({
        name: 'check',
        handler: async (job) => {
            return {bar: "bar"};
        }
    })
    .addStep({
        name: 'please',
        handler: async (job) => {
            return {baz: "baz"};
        }
    })
    .addFinalStep({
        handler: async (job) => {
            const data = job.data;

            const foo = data.foo;
            const bar = data.bar;
            const baz = data.baz;

            await job.log(JSON.stringify({
                foo,
                bar,
                baz
            }));

            return {
                qux: 'qux'
            }
        }
    })
    .build();



