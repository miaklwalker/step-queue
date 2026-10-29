---
title: "Waiting on Child Jobs"
description: "Fanning a step's work out to one or several separate queues and pausing the parent job until every child job finishes."
---

A step can fan work out to a separate `QueueSystem` by attaching a `childQueue`, or to several different queues at once by attaching `childQueues` (see [Fanning out to several queues in parallel](#fanning-out-to-several-queues-in-parallel)).
The step's own handler is responsible for enqueuing the child jobs, typically via `childQueue.addChildJob(job, data)` or `childQueue.addBulkChildJobs(job, jobs)`, and the processor takes care of pausing the parent job until every child finishes.

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

`NameFactory`'s `parent` option is the usual way to name a child queue: passing the parent's own `{ name, displayName }` object bakes the parent's `env` prefix into the child's name without applying it twice. See [NameFactory reference](../reference/name-factory).
Though Child Queue Can be any Queue, as long as you are able to provide the correct input.
## What happens at runtime

Internally, a step with a `childQueue` moves the parent job into BullMQ's "waiting-children" state after the handler runs, via `moveToWaitingChildren`, the same mechanism raw BullMQ uses, but driven by the processor instead of by hand. Once every child job completes, the processor collects `job.getChildrenValues()`, applies `singleChild` / `processChildResult` (below), stores the result, and continues to the next step, deduplicating against previously consumed children so a step is never double-processed if the worker restarts mid-wait.

This is the exact `moveToWaitingChildren` / `WaitingChildrenError` dance described in [the overview](../overview), handled once per step instead of hand-written at every wait point.

## Options on a child step

- `singleChild: true`: when exactly one child job is expected, its output is stored unwrapped instead of as a single-element array.
- `processChildResult`: maps the raw child output(s) (one value if `singleChild`, otherwise an array) into whatever shape should actually be stored.
- The results are always written back to the job data under the key `${stepName}-results`, both at runtime and in the accumulated type, so a step named `"fanOut"` produces `job.data["fanOut-results"]` for every later step.
- This holds even if the handler adds no child jobs at all: the step stores an empty array (or `undefined` with `singleChild`), and `processChildResult` still runs on that empty value, so the value later steps read always matches its type.

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

## Fanning out to several queues in parallel

A `childQueue` step is typed against exactly one queue, so `<step>-results` is an array of that queue's output. When one step needs to hand work to several *different* queues at the same time (listing a product on eBay, Shopify, and Amazon at once, say), use `childQueues` instead: a record of queues, keyed by whatever names you want the results grouped under.

```ts
const listProductQueue = new QueueSystem(NameFactory({ name: "ListProduct" }))
  .defineInputSchema(productSchema)
  .defineConnection(connection)
  .defineProcessor((step) =>
    step
      .addStep({
        name: "list",
        childQueues: { ebay: ebayQueue, shopify: shopifyQueue },
        handler: async (job) => {
          await Promise.all([
            ebayQueue.addChildJob(job, { sku: job.data.sku, title: job.data.title }),
            shopifyQueue.addBulkChildJobs(
              job,
              job.data.variants.map((v) => ({
                data: { sku: job.data.sku, size: v.size, price: v.price },
              })),
            ),
          ]);
        },
      })
      .addFinalStep({
        handler: async (job) => {
          // typed { ebay: EbayOutput[]; shopify: ShopifyOutput[] }
          const { ebay, shopify } = job.data["list-results"];
          return { ebayItemIds: ebay.map((l) => l.itemId), variants: shopify.length };
        },
      }),
  );
```

As with `childQueue`, the step's handler is responsible for enqueuing the child jobs, and each `addChildJob` / `addBulkChildJobs` call is checked against its own queue's input schema. The difference is what happens after the children finish: instead of one flat array, `<step>-results` is an object with one key per entry in `childQueues`, and each key is typed by *that* queue's output.

### Why not sequential steps?

You could get the same type safety with one `childQueue` step per channel. But each of those steps parks the parent until its own children finish before the next step even starts enqueuing, so the parent's total wait is the *sum* of every channel's time. With `childQueues`, every child is enqueued up front, they all run at once on their own workers, and the parent waits once, for as long as the *slowest* channel takes. In [`examples/parallelChannelListing.ts`](https://github.com/miaklwalker/step-queue/blob/main/examples/parallelChannelListing.ts), three channels taking roughly 300ms, 200ms, and 400ms finish in about 400ms together instead of about 900ms in sequence.

Use sequential steps when a later channel genuinely depends on an earlier one's result; use `childQueues` when the children are independent.

### The shape of the results

By default, every key holds an **array** of that queue's outputs, one entry per child job added to that queue. A queue the handler added no jobs to holds an empty array, and if the handler adds no jobs to *any* queue, the step still stores the full grouped object (every array empty, every single queue `undefined`) and still calls `processChildResult` with it.

`singleChild` unwraps queues that run at most one child, storing that child's output directly instead of a one-element array. It accepts two forms:

- `singleChild: true` unwraps **every** queue in the step.
- `singleChild: { ebay: true }` unwraps only the queues named; every other queue keeps its array. Keys must name entries in `childQueues`: a misspelled key is a compile error, and `addStep` also throws `AddStepError` at runtime.

An unwrapped queue is typed `T | undefined` rather than `T`, because the handler may legitimately skip a queue (a product that isn't sold on Amazon, say), and then there is no output to unwrap. This is the common mixed case: one listing for the parent product on one channel, one listing per variant on another:

```ts
.addStep({
  name: "list",
  childQueues: { ebay: ebayQueue, shopify: shopifyQueue, amazon: amazonQueue },
  singleChild: { ebay: true, amazon: true },
  handler: async (job) => {
    const { sku, title, asin, variants } = job.data;
    await Promise.all([
      ebayQueue.addChildJob(job, { sku, title }),
      shopifyQueue.addBulkChildJobs(
        job,
        variants.map((v) => ({ data: { sku, size: v.size, price: v.price } })),
      ),
      // Not every product is on Amazon; skipping a queue is fine.
      asin ? amazonQueue.addChildJob(job, { sku, asin }) : undefined,
    ]);
  },
})
// job.data["list-results"] is typed:
// {
//   ebay: EbayOutput | undefined;
//   shopify: ShopifyOutput[];
//   amazon: AmazonOutput | undefined;
// }
```

If a `singleChild` flag is a `boolean` variable rather than a literal `true` or `false`, the type can't know which shape it will be at runtime, so that key is typed as the union of both (`T | undefined | T[]`).

`processChildResult` works exactly as it does for a single `childQueue`, except that it receives the grouped object (with `singleChild` already applied) instead of an array. Whatever it returns is what's stored under `<step>-results`:

```ts
.addStep({
  name: "list",
  childQueues: { ebay: ebayQueue, shopify: shopifyQueue },
  singleChild: { ebay: true },
  handler: async (job) => { /* ... */ },
  processChildResult: ({ ebay, shopify }) => ({
    ebayItemId: ebay?.itemId ?? null,
    shopifyVariantIds: shopify.map((v) => v.variantId),
  }),
})
// job.data["list-results"] is typed { ebayItemId: string | null; shopifyVariantIds: string[] }
```

Within a single queue's array, results are **not** guaranteed to be in the order the jobs were enqueued. Sort them, or match them up by a field in the output (like `sku` or `size` above), when order matters.

### What happens at runtime

A `childQueues` step uses exactly the same waiting mechanism as a `childQueue` step: after the handler returns, the parent moves into BullMQ's waiting-children state via `moveToWaitingChildren`, and stays there until **every** child, on every queue, has completed. Once they have, the processor reads `job.getChildrenValues()`, which keys each child's return value by that child's Redis job key (`<prefix>:<queueName>:<jobId>`). Each result is assigned to the `childQueues` entry whose queue name prefixes its key, `singleChild` and `processChildResult` are applied, and the result is stored under `<step>-results` before the processor moves on to the next step. The same deduplication against previously consumed children applies, so restarting the worker mid-wait never processes a child twice.

Because results are matched by queue, a few things are checked when the step is added, and `addStep` throws `AddStepError` if:

- the step has both `childQueue` and `childQueues` (also a compile error);
- the same queue appears under two keys in `childQueues`, since its results couldn't be split between them;
- `singleChild` names a key that isn't in `childQueues`.

A child job added to a queue that is *not* listed in `childQueues` still holds the parent in waiting-children until it completes, but its result isn't included under any key.

### Failures and retries

Failures behave the same as for any child step. If a child job fails, the parent stays in waiting-children: it doesn't fail, and it doesn't move on with partial results. Once that child is retried (by its own job options, or by hand from a dashboard like Bull Board) and completes, the parent picks up where it left off. The children that had already succeeded are not re-run.

The step's handler itself is a different matter: if the handler throws partway through enqueuing (say the eBay child was added but the Shopify call failed), the parent job fails before reaching waiting-children, and a retry runs the handler again from the top, which enqueues the eBay child a second time. Use deterministic `jobId`s in the child job options if a duplicate child must be impossible, since BullMQ ignores an add whose `jobId` already exists.

## Rolling back a child step

A `childQueue` or `childQueues` step can declare `rollback` and `rollbackKeys` the same way a plain step does: the requested keys can include the step's own `${stepName}-results` key, since by the time a later step fails, the children have already completed and their results are part of the job data.

## Producing child jobs from a non-worker process

If the process enqueuing child jobs shouldn't itself run a worker — an API route, a script — use `QueueOnly` instead of `QueueSystem` for the child queue's producer side. See [Producer-Only Queues](../reference/queue-only).

## Waiting on a child job to fix a failure

The same waiting mechanism also powers `job.actionErrors.recover`, which adds a child job from inside a `catch` block and retries the *same* step once it finishes, instead of a normal step's handler moving on to `nextStep`. See [Recovering From a Failure](recovering).
