---
title: "Waiting on Child Jobs"
description: "Fanning a step's work out to a separate queue and pausing the parent job until every child job finishes."
---

A step can fan work out to a separate `QueueSystem` by attaching a `childQueue`. 
The step's own handler is responsible for enqueuing the child jobs — typically via `childQueue.addChildJob(job, data)` or `childQueue.addBulkChildJobs(job, jobs)` — and the processor takes care of pausing the parent job until every child finishes.

## Defining a child queue and fanning out

```ts
const childInputSchema = z.object({ name: z.string() });
const childQueue = new QueueSystem(
  NameFactory({ name: "childQueue", parent: NameFactory({ name: "parentQueue" }) }),
)
  .defineInputSchema(childInputSchema)
  .defineConnection(connection)
  .defineProcessor((step) =>
    step.addStep({
      name: "greet",
      handler: async (job) => ({ greeting: `hello, ${job.data.name}` }),
    }),
  );

const parentQueue = new QueueSystem(NameFactory({ name: "parentQueue" }))
  .defineInputSchema(z.object({ names: z.array(z.string()) }))
  .defineConnection(connection)
  .defineProcessor((step) =>
    step
      .addStep({
        name: "fanOut",
        childQueue,
        handler: async (job) => {
          await childQueue.addBulkChildJobs(
            job,
            job.data.names.map((name) => ({ data: { name } })),
          );
        },
      })
      .addFinalStep({
        // job.data["fanOut-results"] holds every child job's output
        handler: async (job) => ({ greetings: job.data["fanOut-results"] }),
      }),
  );
```

`NameFactory`'s `parent` option is the usual way to name a child queue — passing the parent's own `{ name, displayName }` object bakes the parent's `env` prefix into the child's name without applying it twice. See [NameFactory reference](../reference/name-factory).
Though Child Queue Can be any Queue, as long as you are able to provide the correct input.
## What happens at runtime

Internally, a step with a `childQueue` moves the parent job into BullMQ's "waiting-children" state after the handler runs, via `moveToWaitingChildren` — the same mechanism raw BullMQ uses, but driven by the processor instead of by hand. Once every child job completes, the processor collects `job.getChildrenValues()`, applies `singleChild` / `processChildResult` (below), stores the result, and continues to the next step — deduplicating against previously consumed children so a step is never double-processed if the worker restarts mid-wait.

This is the exact `moveToWaitingChildren` / `WaitingChildrenError` dance described in [the overview](../overview), handled once per step instead of hand-written at every wait point.

## Options on a child step

- `singleChild: true` — when exactly one child job is expected, its output is stored unwrapped instead of as a single-element array.
- `processChildResult` — maps the raw child output(s) (one value if `singleChild`, otherwise an array) into whatever shape should actually be stored.
- The results are always written back to the job data under the key `${stepName}-results`, both at runtime and in the accumulated type — so a step named `"fanOut"` produces `job.data["fanOut-results"]` for every later step.

```ts
.addStep({
  name: "fetchProfile",
  childQueue: profileQueue,
  singleChild: true,
  handler: async (job) => {
    await profileQueue.addChildJob(job, { userId: job.data.userId });
  },
  processChildResult: (result) => ({
    displayName: result.name,
  }),
})
// job.data["fetchProfile-results"] is typed { displayName: string }
```

## Rolling back a child step

A `childQueue` step can declare `rollback` and `rollbackKeys` the same way a plain step does — the requested keys can include the step's own `${stepName}-results` key, since by the time a later step fails, the children have already completed and their results are part of the job data.

## Producing child jobs from a non-worker process

If the process enqueuing child jobs shouldn't itself run a worker — an API route, a script — use `QueueOnly` instead of `QueueSystem` for the child queue's producer side. See [Producer-Only Queues](../reference/queue-only).
