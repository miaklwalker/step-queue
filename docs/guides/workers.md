---
title: "Workers, Concurrency & Progress"
description: "How QueueSystem starts a BullMQ worker, tuning concurrency, listening to worker events, and reporting step progress."
---

`defineProcessor` is what actually starts a BullMQ `Worker`: everything before it (`defineInputSchema`, `defineConnection`, `defineConcurrency`) only configures what that worker will be.

## Starting the worker

```ts
const myQueue = new QueueSystem(NameFactory({ name: "MyCoolQueue", env: "dev" }))
  .defineInputSchema(z.object({ greeting: z.string() }))
  .defineConnection({ host: "localhost", port: 6379 })
  .defineProcessor((processor) =>
    processor.addStep({
      name: "greet",
      handler: async (job) => ({ shout: job.data.greeting.toUpperCase() }),
    }),
  );
```

`defineProcessor` requires a connected system: `defineConnection` must run first. This is enforced at the type level: calling `defineProcessor` before `defineConnection` is a compile error (`this` parameter requires `TConnected` to be `true`), with a matching runtime guard for JavaScript callers or code that casts past the type.

`defineProcessor` also narrows `worker` from `Worker | undefined` to `Worker`, and `queue` was already narrowed to `Queue` by `defineConnection`:

```ts
const pending = new QueueSystem(NameFactory({ name: "MyCoolQueue" }));
pending.queue; // Queue | undefined
pending.worker; // Worker | undefined

const connected = pending.defineConnection({ host: "localhost", port: 6379 });
connected.queue; // Queue
connected.worker; // Worker | undefined — not started yet

const running = connected.defineProcessor((step) =>
  step.addStep({ name: "greet", handler: async () => ({ ok: true }) }),
);
running.queue; // Queue
running.worker; // Worker
```

## Concurrency

`defineConcurrency(n)` sets how many jobs the worker processes at once. It defaults to `1`, and must be called before `defineProcessor` (the value it sets is read when the `Worker` is constructed):

```ts
const myQueue = new QueueSystem(NameFactory({ name: "MyCoolQueue" }))
  .defineConnection(connection)
  .defineConcurrency(10)
  .defineProcessor((processor) =>
    processor.addStep({ name: "work", handler: async () => ({}) }),
  );
```

## Listening to worker events

`addWorkerEventListener` / `removeWorkerEventListener` are typed wrappers around the underlying `Worker`'s event emitter: no `any` in the callback signature.

```ts
importer.addWorkerEventListener("progress", (_job, progress) => {
  console.log("[progress]", progress);
});

importer.addWorkerEventListener("failed", (job, err) => {
  console.error(`job ${job?.id} failed:`, err.message);
});
```

`addWorkerEventListener` throws if called before `defineProcessor` has created the worker.

## `progress.steps`: per-step status for free

Every job's BullMQ progress carries a `steps` object, maintained automatically — no configuration needed. Each key is a step's name; each value is one of:

- `"not_started"` — the step hasn't run yet
- `"running"` — the step's handler is currently executing (or, for a step with a `childQueue`, waiting on its children)
- `"Finished in <N>ms"` — the step completed successfully
- `"Skipped after <N>ms"` — `job.actionErrors.skip`/`skipToFinal` bypassed it
- `"Failed after <N>ms"` — the handler threw and the job is failing
- `"Recovering (attempt <n>/<max>)"` — a `RecoverWithChildError` child job is in flight for this step

The final step (implicit or explicit) is reported under the key `"final"`.

```ts
const job = await importer.addJob({ rows: 100 });
// while it's running:
const inFlight = await importer.queue.getJob(job.id);
inFlight?.progress;
// { step: "import", progress: 0, steps: { import: "running", final: "not_started" } }
```

This survives a job pausing in `waiting-children` or recovering via a child job: the status and elapsed time recorded before the pause are read back from the job's persisted progress and carried forward, not reset.

## Reporting progress from a step

There are two other, independent ways to surface progress from a step:

- `job.updateStepProgress({...})`, called from inside a handler, merges custom data into the job's BullMQ progress and emits a `"progress"` event on the worker.
- A `progressReport` callback on a step, invoked automatically at `"start"`, `"finish"`, `"skip"`, and (for a step with a `childQueue`) `"child-finish"`, useful for logging or pushing updates without cluttering the handler itself.

```ts
const importer = new QueueSystem(NameFactory({ name: "importer", env: "example" }))
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
      .addFinalStep({ handler: async (job) => ({ imported: job.data.imported }) }),
  );

// Typed worker event listener — no `any`.
importer.addWorkerEventListener("progress", (_job, progress) => {
  console.log("[progress]", progress);
});
```

## Step lifecycle logging

`ProcessorBuilder`'s `debug()` method opts a processor in to step lifecycle logging (start, finish, rollback, and similar transitions) via BullMQ's own `job.log`. It's off by default:

```ts
processor.debug().addStep({ name: "greet", handler: async () => ({}) });

// pass false to explicitly disable it again
processor.debug(false);
```

## Closing a queue system

`close()` gracefully closes the worker, the queue, and any `QueueEvents` instance that was created:

```ts
await myQueue.close();
```

Call it during process shutdown, or between tests, to avoid leaking Redis connections.
