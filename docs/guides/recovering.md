---
title: "Recovering From a Failure"
description: "Fixing the cause of a step's failure with a child job, then automatically retrying the same step — no QueueEvents listener required."
---

Some failures aren't really failures — they're a precondition that's temporarily unmet, and there's a job that can fix it. A step that fails because a brand hasn't been imported into a channel yet is really telling you "run the import-brand job, then try me again."

The usual way to handle this with raw BullMQ is a `QueueEvents` listener on `'failed'`, decoupled from the step that actually knows what went wrong: it has to re-inspect the failure to decide whether it's fixable, enqueue the fix job itself, and separately track when to retry the original job. `job.actionErrors.recover` puts that logic on the step instead, right next to the code that produces the error.

## Basic usage

```ts
throw new job.actionErrors.recover(childQueue, childData, options?);
```

- `childQueue` — the `QueueSystem` to run as a fix. Any queue works, as long as you can provide its input.
- `childData` — the data for that child job. Checked at compile time against `childQueue`'s own input schema (see [Type safety](#type-safety) below).
- `options?.maxAttempts` — how many times this step may recover before giving up. Defaults to `3`.

```ts
const importBrandQueue = new QueueSystem(NameFactory({ name: "importBrand" }))
  .defineInputSchema(z.object({ brandId: z.string(), channel: z.string() }))
  .defineConnection(connection)
  .defineProcessor((step) =>
    step.addStep({
      name: "import",
      handler: async (job) => {
        await importBrand(job.data.brandId, job.data.channel);
      },
    }),
  );

const pushToChannel = new QueueSystem(NameFactory({ name: "pushToChannel" }))
  .defineInputSchema(z.object({ brandId: z.string(), channel: z.string() }))
  .defineConnection(connection)
  .defineProcessor((step) =>
    step
      .addStep({
        name: "push",
        handler: async (job) => {
          try {
            return await pushToChannelApi(job.data);
          } catch (err) {
            if (isBrandMissingError(err)) {
              throw new job.actionErrors.recover(importBrandQueue, {
                brandId: job.data.brandId,
                channel: job.data.channel,
              });
            }
            throw err; // anything else fails the step normally
          }
        },
      })
      .addFinalStep({ handler: async (job) => ({ pushed: true }) }),
  );
```

The first time `"push"` throws that error, the processor adds `importBrandQueue` as a child job, pauses `pushToChannel`'s job, and waits. Once the child completes, `"push"` runs again from the top — no separate listener, and nothing else in the pipeline has to know a fix happened at all.

## What happens at runtime

Recovering reuses the exact `moveToWaitingChildren` / `WaitingChildrenError` mechanism described in [Waiting on Child Jobs](child-queues) and [the overview](../overview) — the difference is *when* it fires (from a caught error, inside the handler's own `catch`) and what happens once the child finishes (the *same* step runs again, rather than the pipeline moving on to `nextStep`).

Because parking a job in `WaitingChildren` is not the same as failing it, none of this touches BullMQ's own retry machinery: no `'failed'` event fires, and the job's `attempts` budget isn't spent. A step can recover several times over without ever looking like a flaky job to anything watching the queue.

## Giving up: `maxAttempts`

Only the specific error you choose to catch and recover from should trigger this — a step should still let unrelated errors fail it normally (rollbacks run, BullMQ retries), rather than treating every failure as fixable.

Even a targeted match can loop forever if the fix job runs but doesn't actually resolve the problem. Each step tracks its own recovery attempts; once a step exceeds `maxAttempts` (default `3`), the processor stops recovering, runs rollbacks for every completed step, and fails the job with a `RecoverAttemptsExceededError`:

```ts
throw new job.actionErrors.recover(
  importBrandQueue,
  { brandId: job.data.brandId, channel: job.data.channel },
  { maxAttempts: 5 },
);
```

## Type safety

`childData` is checked against `childQueue`'s own input schema, inferred from `childQueue` itself at the throw site — the same guarantee `childQueue.addChildJob(job, data)` already gives you for ordinary child steps:

```ts
// importBrandQueue expects { brandId: string; channel: string }
throw new job.actionErrors.recover(importBrandQueue, {
  brand: job.data.brandId, // ❌ compile error — no `brand` key on importBrandQueue's input
});
```

## What recovering does *not* do

Unlike a `childQueue` step, a recovery child's result isn't collected or merged into the job data — it's treated as a side effect, not a data source. If the fix job produces something the retried step actually needs to read, have the step re-derive it itself (typically by re-running the same lookup that failed the first time) rather than relying on the child's output.

## See also

- [Waiting on Child Jobs](child-queues): the fan-out mechanism `recover` builds on.
- [Errors, Control Flow & Rollbacks](errors): where `recover` sits among the other `job.actionErrors`.
- [Resuming After Failure](resuming): how `job.data` position-tracking interacts with recovering.
