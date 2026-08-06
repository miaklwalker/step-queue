---
title: "Typed Input & Schema Validation"
description: "How defineInputSchema types the first step and validates job data at enqueue time using Standard Schema."
---

`defineInputSchema` takes any [Standard Schema](https://standardschema.dev) — Zod, Valibot, ArkType, Effect Schema, and other compliant libraries all work. It does two things:

1. Infers the initial shape of `job.data` for the first step, so the rest of the processor is typed from it without any manual generic annotation.
2. Validates every job's data against the schema at `addJob` / `addBulkJobs` time — invalid data is rejected with a `SchemaValidationError` (carrying the standard `issues` array) before it ever reaches a worker.

```ts
const q = new QueueSystem(NameFactory({ name: "Orders" }))
  .defineInputSchema(z.object({ orderId: z.string(), amount: z.number() }))
  .defineConnection(connection)
  .defineProcessor((processor) =>
    processor.addStep({ name: "charge", handler: async () => ({}) }),
  );

// throws SchemaValidationError — amount fails z.number()
await q.addJob({ orderId: "o_1", amount: "not a number" } as unknown as {
  orderId: string;
  amount: number;
});
```

## Catching validation errors

`SchemaValidationError` carries the schema library's own `issues` array (per the Standard Schema spec), and its `message` is a semicolon-joined summary of every issue:

```ts
import { SchemaValidationError } from "@michaelrwalker/step-queue";

try {
  await q.addJob(badData);
} catch (err) {
  if (err instanceof SchemaValidationError) {
    console.error(err.issues); // ReadonlyArray<StandardSchemaV1.Issue>
  }
}
```

Because validation runs before `queue.add(...)`, a rejected job never touches Redis — it fails synchronously in the calling process.

## Deriving a JSON Schema

If the schema library supports Standard Schema's JSON Schema extension (`StandardJSONSchemaV1` — Zod 4 does, for example), the schema is also converted to a JSON Schema and exposed as `q.jsonSchema`, useful for documenting or externally validating a queue's expected input without pulling in the schema library on the reading side. For libraries without that support, `q.jsonSchema` stays `undefined`.

```ts
const q = new QueueSystem(NameFactory({ name: "Orders" })).defineInputSchema(
  z.object({ orderId: z.string() }),
);

q.jsonSchema;
// { type: "object", properties: { orderId: { type: "string" } }, ... }
```

## Skipping schema validation

`defineInputSchema` is optional. Without it, `job.data`'s initial type has to come from somewhere else — typically by constructing the processor with `ProcessorBuilder.init<T>()` and passing it to `defineProcessor` directly (see [ProcessorBuilder reference](../reference/processor-builder)) — and job data is enqueued without runtime validation. This is also the case for `QueueOnly`, which never validates: the assumption is that whichever process defines the schema (a `QueueSystem` on the consuming side) is the source of truth for the shape. See [QueueOnly reference](../reference/queue-only).

## Standalone `ProcessorBuilder` and the input type

`defineProcessor` also accepts a `ProcessorBuilder` you construct yourself, useful when the steps are defined away from the queue (shared, tested, or composed elsewhere). Don't call `.build()` on it — `defineProcessor` builds the processor in both forms. When a standalone builder is passed, the queue's input type is inferred from `ProcessorBuilder.init<T>()`, not from `defineInputSchema`:

```ts
import { QueueSystem, NameFactory, ProcessorBuilder } from "@michaelrwalker/step-queue";

const processor = ProcessorBuilder.init<{ greeting: string }>()
  .addStep({
    name: "shout",
    handler: async (job) => ({ shout: job.data.greeting.toUpperCase() }),
  })
  .addFinalStep({
    handler: async (job) => ({ done: job.data.shout }),
  });

const myQueue = new QueueSystem(NameFactory({ name: "MyCoolQueue", env: "dev" }))
  .defineConnection(connection)
  .defineProcessor(processor);

// addJob is typed { greeting: string }; results are typed { done: string }
```

A standalone builder passed to `defineProcessor` does not get runtime validation from a schema, even alongside `defineInputSchema` — only `defineInputSchema` wires that up.
