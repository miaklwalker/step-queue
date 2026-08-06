---
title: "Overview"
description: "Why Step Queue exists and the core idea behind it: type-safe, accumulating steps built on top of BullMQ."
---

Step Queue is a type-safe, step-based wrapper around [BullMQ](https://docs.bullmq.io/). It lets you define a BullMQ job as a sequence of typed steps instead of one opaque processor function. Job data accumulates as it moves through the pipeline, so each step is typed against exactly what the steps before it produced. If a job fails and BullMQ retries it, the job resumes on the exact step it left off at, which makes it possible to write steps that are idempotent instead of re-running the whole job from the start.

## The problem with raw BullMQ multi-step jobs

BullMQ's own docs recommend handling a multi-step job by keeping a `step` field on the job data, switching over an enum of step names, and — anywhere the job needs to wait on children — manually calling `job.moveToWaitingChildren` and throwing `WaitingChildrenError`:

```ts
import { WaitingChildrenError, Worker } from "bullmq";

enum Step {
  Initial,
  Second,
  Third,
  Finish,
}

const worker = new Worker(
  "parentQueueName",
  async (job, token) => {
    let step = job.data.step;
    while (step !== Step.Finish) {
      switch (step) {
        case Step.Initial: {
          await doInitialStepStuff();
          await childrenQueue.add(
            "child-1",
            { foo: "bar" },
            { parent: { id: job.id, queue: job.queueQualifiedName } },
          );
          await job.updateData({ step: Step.Second });
          step = Step.Second;
          break;
        }
        case Step.Second: {
          // ...
          break;
        }
        case Step.Third: {
          const shouldWait = await job.moveToWaitingChildren(token);
          if (!shouldWait) {
            await job.updateData({ step: Step.Finish });
            return Step.Finish;
          }
          throw new WaitingChildrenError();
        }
        default: {
          throw new Error("invalid step");
        }
      }
    }
  },
  { connection },
);
```

This is fine for a short job. It stops being fine once the logic gets long and has to wait on children more than once: the step enum, the `switch`, and the `moveToWaitingChildren` / `WaitingChildrenError` pair have to be repeated and kept in sync at every wait point, `job.data` is untyped inside every `case`, and it is easy to forget the `await job.updateData(...)` before a step that can throw, which silently breaks resumption on the next retry.

## The core idea

Step Queue turns that same pattern into a builder. `.addStep(...)` replaces a `case`, the step-tracking and the `moveToWaitingChildren` / `WaitingChildrenError` handling for any step with a `childQueue` is done once by the processor instead of by hand at every wait point, and `job.data` inside each step is typed against exactly what the steps before it produced.

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
        // job.data.greeting is typed as string, inferred from the schema
        handler: async (job) => ({ shout: job.data.greeting.toUpperCase() }),
      })
      .addFinalStep({
        // job.data.shout is typed as string
        handler: async (job) => ({ done: job.data.shout }),
      }),
  );
```

A handler either returns an object, which is merged into the accumulated job data for every later step, or returns nothing, leaving the data unchanged. Because `ProcessorBuilder` tracks that accumulated shape at the type level, a step that reads a field before an earlier step produces it is a compile error, not a runtime `undefined`.

## Where to go next

- [Installation](installation): add the package and the `bullmq` peer dependency.
- [Quick Start](quick-start): a complete worked job end to end.
- [Steps & Data Accumulation](guides/steps): the mechanics of `addStep`, `addFinalStep`, and `clean`.
- [Resuming After Failure](guides/resuming): how a retried job picks up where it left off.
- [Recovering From a Failure](guides/recovering): fixing a step's failure with a child job instead of a `QueueEvents` listener.
