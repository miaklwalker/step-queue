---
title: "QueueOnly"
description: "Full API reference for QueueOnly, a producer-only counterpart to QueueSystem that enqueues jobs without starting a worker."
---

`QueueOnly` is a producer-only counterpart to `QueueSystem`. It exposes the same `addJob` / `addBulkJobs` / `addChildJob` / `addBulkChildJobs` surface without constructing a `Worker`, for a process that should only enqueue jobs — an API route, a script, a cron job — and never run one.

```ts
class QueueOnly<
  TInput extends Record<string, unknown> = Record<string, unknown>,
  TOutput extends Record<string, unknown> = Record<string, unknown>,
  TName extends string = string,
>
```

```ts
import { QueueOnly, NameFactory } from "@michaelrwalker/step-queue";

const producer = new QueueOnly(
  NameFactory({ name: "MyCoolQueue", env: "dev" }),
  { host: "localhost", port: 6379 },
);
await producer.addJob({ greeting: "hi" });
```

`QueueOnly` does not take an input schema, so it does not validate job data at enqueue time: the assumption is that whatever process defines the schema (via a `QueueSystem` on the consuming side) is the source of truth for the shape.

## Constructor

```ts
new QueueOnly(
  nameConfig: { name: string; displayName: string },
  connection: ConnectionOptions,
)
```

`nameConfig` is typically the output of [`NameFactory`](name-factory). Unlike `QueueSystem`, the connection is required up front: `QueueOnly` constructs its `Queue` immediately.

## Methods

### `addJob(data, options?)`

```ts
addJob(jobData: TInput, options?: JobsOptions): Promise<Job<TInput, TOutput, TName>>
```

Enqueues one job, merging [default job options](../guides/resuming) with any `options` passed. No schema validation is performed.

### `addBulkJobs(jobs)`

```ts
addBulkJobs(
  jobs: Array<{ data: TInput; opts?: JobsOptions }>,
): Promise<Job<TInput, TOutput, TName>[]>
```

Enqueues many jobs in one call.

### `addChildJob(parentJob, data, options?)`

```ts
addChildJob(
  parentJob: Job | TypedJob<JSONLike, ErrorRegistry>,
  jobData: TInput,
  options?: JobsOptions,
): Promise<Job<TInput, TOutput, TName>>
```

Enqueues a job as a BullMQ child of `parentJob`. Throws if `parentJob` lacks an `id` or `queueQualifiedName`.

### `addBulkChildJobs(parentJob, jobs)`

```ts
addBulkChildJobs(
  parentJob: Job | TypedJob<JSONLike, ErrorRegistry>,
  jobs: Array<{ data: TInput; opts?: JobsOptions }>,
): Promise<Job<TInput, TOutput, TName>[]>
```

Bulk version of `addChildJob`.

### `close()`

```ts
close(): Promise<void>
```

Closes the queue and any `QueueEvents` instance that was created.

## Properties

| Property | Type | Notes |
| --- | --- | --- |
| `name` | `TName` | The BullMQ queue key (from `NameFactory`). |
| `displayName` | `string` | Human-readable name (from `NameFactory`). |
| `queue` | `Queue<TInput, TOutput, TName, TInput, TOutput, TName>` | The underlying BullMQ queue, always present, unlike `QueueSystem.queue`, since `QueueOnly` constructs it immediately. |
| `QueueEvents` | `QueueEvents` (getter) | Lazily-created `QueueEvents` instance. |

## `QueueOnlyInput<T>` / `QueueOnlyOutput<T>`

```ts
type QueueOnlyInput<T> = T extends QueueOnly<infer I, any, any> ? I : never;
type QueueOnlyOutput<T> = T extends QueueOnly<any, infer O, any> ? O : never;
```

Extract a `QueueOnly` producer's input/output types, mirroring `QueueInput` / `QueueOutput` for `QueueSystem`.
