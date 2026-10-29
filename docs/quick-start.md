---
title: "Quick Start"
description: "Define a queue, add steps, enqueue a job, and read back a fully typed result."
---

This walks through one complete job: define an input schema, add two steps, enqueue a job, and wait for the typed result.

## Define the queue

```ts
import { z } from "zod";
import { QueueSystem, NameFactory } from "@michaelrwalker/step-queue";

const myInput = z.object({ greeting: z.string() });

const myQueue = new QueueSystem(NameFactory({ name: "MyCoolQueue", env: "dev" }))
  .defineInputSchema(myInput)
  .defineConnection({ host: "localhost", port: 6379 })
  .defineProcessor((processor) =>
    processor
      .addStep({
        name: "shout",
        // job.data.greeting is typed as string (inferred from the schema)
        handler: async (job) => ({ shout: job.data.greeting.toUpperCase() }),
      })
      .addFinalStep({
        // job.data.shout is typed as string
        handler: async (job) => ({ done: job.data.shout }),
      }),
  );
```

`NameFactory` builds the queue's internal BullMQ name and its display name. See [NameFactory reference](reference/name-factory). `defineInputSchema`'s callback infers the shape of `job.data` for the first step; `defineProcessor`'s callback receives a `ProcessorBuilder` already typed with that schema.

`addStep` runs `"shout"` first. It reads `job.data.greeting` (typed `string` from the schema) and returns `{ shout: ... }`, which is merged into the accumulated data. `addFinalStep` marks the end of the pipeline. Its return value becomes the job's result, and `job.data.shout` is typed `string` because the `"shout"` step declared it.

## Enqueue a job

```ts
// typed: { greeting: string }
const job = await myQueue.addJob({ greeting: "hi" });
```

`addJob` validates `jobData` against the schema passed to `defineInputSchema` before enqueueing. Invalid data throws `SchemaValidationError` immediately: the job never reaches Redis or a worker. See [Typed Input & Schema Validation](guides/schema-validation).

## Wait for the result

```ts
const result = await job.waitUntilFinished(myQueue.QueueEvents, 10_000);
// result is typed { done: string } — the final step's return type
console.log(result); // { done: "HI" }
```

`job.waitUntilFinished` is BullMQ's own API; `myQueue.QueueEvents` is a lazily-created `QueueEvents` instance that Step Queue manages for you.

## Close it down

When a process is shutting down (tests, scripts, graceful server shutdown), close the queue system to release its Redis connections:

```ts
await myQueue.close();
```

`close()` gracefully closes the worker, the queue, and any `QueueEvents` instance that was created.

## Where this goes next

- [Steps & Data Accumulation](guides/steps): how `addStep`, `addFinalStep`, and `clean` shape the accumulated type.
- [Resuming After Failure](guides/resuming): what happens when a step throws and the job retries.
- [Waiting on Child Jobs](guides/child-queues): fanning work out to another queue mid-pipeline, or to several queues in parallel.
- [Simple Handler Queues](guides/handlers): a queue processed by one plain function, for commands where steps don't make sense.
- [Recovering From a Failure](guides/recovering): fixing a step's failure with a child job instead of a `QueueEvents` listener.
