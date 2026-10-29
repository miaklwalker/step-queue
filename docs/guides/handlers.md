---
title: "Simple Handler Queues"
description: "Starting a worker with one plain, typed function via defineHandler, for command queues where steps don't make sense."
---

Not every queue is a pipeline. Some are just commands: call an API, send a webhook, post a Slack message, write an audit row. Splitting that work into steps adds nothing, but you still want it in a queue for retries, concurrency, rate limiting, and visibility in a dashboard like Bull Board.

`defineHandler` is the step-less alternative to `defineProcessor`. It starts the queue's BullMQ `Worker` with a single function, exactly as if you had passed that function to `new Worker(...)` yourself, while keeping the parts of `QueueSystem` that matter for a command queue: typed, validated input, a typed output, and the same `addJob` / `addChildJob` / `QueueEvents` / `close` API as every other queue.

## Defining a handler queue

```ts
const exchangeRateQueue = new QueueSystem(NameFactory({ name: "ExchangeRate" }))
  .defineInputSchema(z.object({ currency: z.string() }))
  .defineConnection(connection)
  .defineConcurrency(5)
  .defineHandler(async (job) => {
    // job.data is typed { currency: string }, inferred from the schema
    const rate = await ratesApi.get(job.data.currency);
    return { currency: job.data.currency, rate };
  });

// QueueOutput<typeof exchangeRateQueue> is { currency: string; rate: number }
const job = await exchangeRateQueue.addJob({ currency: "EUR" });
const result = await job.waitUntilFinished(exchangeRateQueue.QueueEvents, 10_000);
result.rate; // number
```

The builder calls before `defineHandler` work the same as they do before `defineProcessor`:

- `defineInputSchema` types `job.data` and validates input when a job is added. See [Typed Input & Schema Validation](schema-validation). It's optional; without it `job.data` is typed as a plain `Record<string, unknown>` and nothing is validated, so a handler queue almost always wants one.
- `defineConnection` is required, and must come first. Calling `defineHandler` on an unconnected system is a compile error (the `this` parameter requires `TConnected` to be `true`), with a matching runtime guard (`defineConnection(...) must be called before defineHandler(...)`) for JavaScript callers.
- `defineConcurrency` sets how many jobs the worker runs at once. It must be called before `defineHandler`, since that's when the `Worker` is constructed.

Like `defineProcessor`, `defineHandler` narrows both lifecycle flags, so `queue` and `worker` are non-optional on the result.

## What the handler receives

The handler's signature is `(job, token?) => Promise<TOutput>`, the same as a raw BullMQ processor:

- `job` is a plain BullMQ `Job` whose `data` is typed from the input schema. Everything on a BullMQ job is available: `job.log(...)`, `job.updateProgress(...)`, `job.attemptsMade`, `job.id`, and so on.
- `token` is the worker's lock token, passed through unchanged.

It is **not** a `TypedJob`. A handler queue has no steps, so the step-specific additions a step handler gets don't apply: there is no `job.errors` registry, no `job.actionErrors` (skip, early exit, recover), no `updateStepProgress`, no per-step `progress.steps`, and no rollbacks. If you find yourself wanting any of those, that is the sign to switch to `defineProcessor`.

## The output type

The queue's output type is exactly what the handler resolves to, inferred from its return statements; there is no final step and no accumulated data. Unlike a step processor, whose output is always an object, a handler can return anything BullMQ can store as a job's return value:

```ts
const slackNotifyQueue = new QueueSystem(NameFactory({ name: "SlackNotify" }))
  .defineInputSchema(z.object({ channel: z.string(), text: z.string() }))
  .defineConnection(connection)
  .defineHandler(async (job) => {
    const res = await slack.chat.postMessage(job.data);
    return res.ts; // QueueOutput<typeof slackNotifyQueue> is string
  });
```

A handler that returns nothing gives the queue an output type of `void`. Return values go through BullMQ's JSON serialization, so return plain data (objects, arrays, strings, numbers, booleans, `null`) rather than class instances, `Date`s, or `Map`s, which come back as their JSON form.

## Failures and retries

A handler fails its job the same way any BullMQ processor does: by throwing (or rejecting). The job is retried according to its job options: [the defaults](resuming) (`attempts: 3`, so up to two retries), merged with your `stepqueue.config.ts` (if any) and any options passed to `addJob`. Since a handler has no steps, a retry runs the whole function again from the top. Keep handlers idempotent, or small enough that re-running them is harmless. If part of the work must not repeat on retry, that is the case for splitting it into steps with `defineProcessor`, where a retry resumes on the step that failed.

## Using a handler queue as a child queue

A handler queue is a `QueueSystem` like any other, so a step processor can fan out to it with `childQueue` (or `childQueues`), and its output type flows into the parent's `<step>-results`:

```ts
const priceInCurrencyQueue = new QueueSystem(NameFactory({ name: "PriceInCurrency" }))
  .defineInputSchema(z.object({ usdPrice: z.number(), currencies: z.array(z.string()) }))
  .defineConnection(connection)
  .defineProcessor((step) =>
    step
      .addStep({
        name: "fetchRates",
        childQueue: exchangeRateQueue,
        handler: async (job) => {
          await exchangeRateQueue.addBulkChildJobs(
            job,
            job.data.currencies.map((currency) => ({ data: { currency } })),
          );
        },
      })
      .addFinalStep({
        handler: async (job) =>
          // typed { currency: string; rate: number }[]
          job.data["fetchRates-results"].map((r) => job.data.usdPrice * r.rate),
      }),
  );
```

This is the usual way to combine the two: a step processor for the orchestration, with handler queues as the leaves that do one thing each. See [Waiting on Child Jobs](child-queues).

## `defineHandler` or `defineProcessor`?

| Use `defineHandler` when... | Use `defineProcessor` when... |
| --- | --- |
| The job is one action (an API call, a send, a write). | The job has several stages that should resume independently on retry. |
| Re-running the whole job on retry is safe. | Some work must not repeat once it has succeeded. |
| You want the result to be any value, including a string or `void`. | You want data to accumulate across stages, with `clean` and `rollbackKeys`. |
| No child jobs, or only as a leaf other queues fan out to. | The job itself waits on child jobs, or needs skip / early exit / recover / rollbacks. |

A runnable version of every snippet on this page is in [`examples/simpleHandler.ts`](https://github.com/miaklwalker/step-queue/blob/main/examples/simpleHandler.ts).
