---
title: "Resuming After Failure"
description: "How a retried job picks up on the exact step it failed on, and what that means for writing idempotent steps."
---

The reason Step Queue exists is to make resumable, multi-step jobs easy to write correctly. This page covers the mechanics: how the processor remembers where it was, and what that implies for how you write a handler.

## How resumption works

The processor tracks its own position in the job data using a few reserved keys: `step` (the current step name), `previousStep`, `consumedKeys` (child results already collected), `completedSteps` (used for rollback ordering), and `recoverStep` / `recoverAttempts` (used by [`job.actionErrors.recover`](recovering)). After every step completes, and before moving on, the processor calls BullMQ's `job.updateData(...)` to persist that position, including the merged result of the step that just ran.

When a worker picks up a job, whether for the first time or as a BullMQ retry, it reads `job.data.step`. If it's unset, the job starts at the first declared step. If it's set (because a previous attempt got partway through), the job resumes from that exact step, with `job.data` already containing everything every prior step produced.

This means a retried job does not restart from the input. It restarts from wherever it left off, with the accumulated data intact.

## Enabling retries

Every job added through `addJob` / `addBulkJobs` picks up these defaults unless overridden per call:

```ts
{
  removeOnComplete: { count: 1000 },
  removeOnFail: { count: 3000 },
  attempts: 1,
}
```

`attempts: 1` means BullMQ-level retries are off by default. To enable retries, raise `attempts` per job:

```ts
await myQueue.addJob({ email: "user@example.com" }, { attempts: 5 });
```

To change these defaults project-wide instead of per call, add a `stepqueue.config.ts` next to `package.json`:

```ts
import { defineQueueDefaults } from "@michaelrwalker/step-queue";

export default defineQueueDefaults({
  removeOnComplete: { count: 5000 },
  attempts: 3,
});
```

(`stepqueue.config.js`/`.mjs`/`.cjs`, or a `"stepQueueDefaults"` field in `package.json`, work the same way — the config file takes priority if both exist.) It's read once per process and cached, so it adds no overhead beyond the first `addJob`/`addBulkJobs` call.

Because the processor persists its position in the job data on every step transition, a retried job resumes from the step that failed rather than restarting from its input.

```ts
const attempts = new Map<string, number>();

const signup = new QueueSystem(NameFactory({ name: "signup", env: "example" }))
  .defineInputSchema(z.object({ email: z.string(), optOutOfEmail: z.boolean() }))
  .defineConnection(connection)
  .defineProcessor((step) =>
    step
      .addStep({
        name: "validate",
        handler: async (job) => ({ validated: true }),
      })
      .addStep({
        name: "reserveUsername",
        handler: async (job) => {
          // If the username service is flaky, throw a plain error — BullMQ
          // retries the job (enqueue with `attempts` > 1), and it resumes
          // from this exact step, since the processor persists its position
          // in the job data.
          const tries = (attempts.get(job.data.email) ?? 0) + 1;
          attempts.set(job.data.email, tries);
          if (tries < 2) {
            throw new Error("username service unavailable, retrying");
          }
          return { username: job.data.email.split("@")[0] ?? "user" };
        },
      })
      .addFinalStep({
        handler: async (job) => ({ status: "created", username: job.data.username }),
      }),
  );
```

On the first attempt, `"validate"` runs and completes; `"reserveUsername"` throws. BullMQ marks the job failed and, because `attempts` is greater than 1, schedules a retry. The retry re-enters the processor with `job.data.step === "reserveUsername"` and `job.data.validated === true` already present. `"validate"` does not run again.

## Writing idempotent steps

Because a step can run more than once (its own retries, or a worker restart mid-step before `job.updateData` committed), a handler should be safe to re-run with the same input. Concretely:

- Prefer operations that are naturally idempotent: an upsert instead of an insert, a `PUT` instead of a `POST` that creates a new resource each time.
- If a step calls an external API that isn't idempotent, key the call on something derived from the job (a job ID, an input field) so a duplicate call is detected and short-circuited on the far end.
- Don't rely on side effects from earlier in the *same* step persisting if the step throws partway through: until the handler returns and `job.updateData` runs, nothing it did is recorded in `job.data`.

## What is *not* automatically retried

A handler that throws one of `job.actionErrors` (`skip`, `skipToFinal`, `earlyExit`) is not a failure: those are control-flow signals the processor handles itself, not something BullMQ retries. See [Errors, Control Flow & Rollbacks](errors). Only an error that reaches BullMQ as a job failure triggers a retry (subject to `attempts`), and rollbacks for every completed step run first, in reverse order, before the error is re-thrown.

`job.actionErrors.recover` is similar but not identical: while it's waiting on its fix job, the job is parked in BullMQ's `WaitingChildren` state, not failed, so it doesn't consume an `attempts` retry either. It only becomes a real, rollback-triggering failure if the step exceeds its own `maxAttempts`. See [Recovering From a Failure](recovering).

## Limits

A processor is capped at 100 steps; a pathological `skip` chain that loops back on itself throws `ProcessorStepError` rather than looping forever.
