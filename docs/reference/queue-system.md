---
title: "QueueSystem"
description: "Full API reference for QueueSystem, the fluent builder that owns a BullMQ Queue and Worker."
---

`QueueSystem` is the fluent builder that owns a BullMQ `Queue` and `Worker`. It enqueues jobs and defines the steps that process them.

```ts
class QueueSystem<
  TInput extends JSONLike = JSONLike,
  TOutput extends JSONLike = JSONLike,
  TName extends string = string,
  TConnected extends boolean = boolean,
  TStarted extends boolean = boolean,
>
```

## Constructor

```ts
new QueueSystem(nameConfig: { name: string; displayName: string })
```

`nameConfig` is typically the output of [`NameFactory`](name-factory).

## Methods

### `defineInputSchema(schema)`

```ts
defineInputSchema<TSchema extends JSONLike>(
  schema: StandardSchemaV1<unknown, TSchema>,
): QueueSystem<TSchema, TOutput, TName, TConnected, TStarted>
```

Sets the [Standard Schema](https://standardschema.dev) that types and validates job input. Infers the initial `job.data` type for the first step, and — if the schema's vendor supports JSON Schema conversion — populates `jsonSchema`. See [Typed Input & Schema Validation](../guides/schema-validation).

### `defineConnection(connection)`

```ts
defineConnection(
  connection: ConnectionOptions,
): QueueSystem<TInput, TOutput, TName, true, TStarted>
```

Sets the BullMQ `ConnectionOptions` (Redis). Required before `defineProcessor`. Narrows `queue` from `Queue | undefined` to `Queue`.

### `defineConcurrency(n)`

```ts
defineConcurrency(concurrency: number): this
```

Sets worker concurrency. Defaults to `1`. Must be set before `defineProcessor`, since that's when the `Worker` is constructed.

### `defineProcessor(buildOrBuilder)`

```ts
defineProcessor<TNewOutput extends JSONLike>(
  this: QueueSystem<TInput, TOutput, TName, true, TStarted>,
  build: (
    builder: ProcessorBuilder<TInput, TInput, TInput>,
  ) => ProcessorBuilder<TInput, TInput, TNewOutput, ErrorRegistry>,
): QueueSystem<TInput, TNewOutput, TName, true, true>;

defineProcessor<TNewInput extends TInput, TNewOutput extends JSONLike>(
  this: QueueSystem<TInput, TOutput, TName, true, TStarted>,
  builder: ProcessorBuilder<TNewInput, TNewInput, TNewOutput, ErrorRegistry>,
): QueueSystem<TNewInput, TNewOutput, TName, true, true>;
```

Declares steps and starts the `Queue` + `Worker`. Takes either a callback receiving a typed `ProcessorBuilder`, or a standalone `ProcessorBuilder` instance (un-built, via `ProcessorBuilder.init<T>()`). Requires a connected system — calling it before `defineConnection` is a compile error via the `this` parameter, with a matching runtime guard (`defineConnection(...) must be called before defineProcessor(...)`) for callers that bypass the type check. Narrows `queue` and `worker` to non-optional. See [ProcessorBuilder reference](processor-builder).

### `addDescription(text)` / `addToSystem(text)`

```ts
addDescription(description: string): this
addToSystem(system: string): this
```

Free-form metadata fields for organizing queues; not used by the engine itself. Stored on `description` and `system`.

### `addJob(data, options?)`

```ts
addJob(jobData: TInput, options?: JobsOptions): Promise<Job<TInput, TOutput, TName>>
```

Validates (if `defineInputSchema` was called) and enqueues one job, merging [default job options](../guides/resuming) with any `options` passed. Throws `SchemaValidationError` on invalid data, or a plain `Error` if the queue hasn't been initialized yet (`defineConnection` not called).

### `addBulkJobs(jobs)`

```ts
addBulkJobs(
  jobs: Array<{ data: TInput; opts?: JobsOptions }>,
): Promise<Job<TInput, TOutput, TName>[]>
```

Validates and enqueues many jobs in one call.

### `addChildJob(parentJob, data, options?)`

```ts
addChildJob(
  parentJob: Job | TypedJob<JSONLike, ErrorRegistry>,
  jobData: TInput,
  options?: JobsOptions,
): Promise<Job<TInput, TOutput, TName>>
```

Enqueues a job as a BullMQ child of `parentJob`. Throws if `parentJob` lacks an `id` or `queueQualifiedName`. See [Waiting on Child Jobs](../guides/child-queues).

### `addBulkChildJobs(parentJob, jobs)`

```ts
addBulkChildJobs(
  parentJob: Job | TypedJob<JSONLike, ErrorRegistry>,
  jobs: Array<{ data: TInput; opts?: JobsOptions }>,
): Promise<Job<TInput, TOutput, TName>[]>
```

Bulk version of `addChildJob`.

### `addWorkerEventListener(event, cb)` / `removeWorkerEventListener(event, cb)`

```ts
addWorkerEventListener<E extends keyof WorkerListener>(
  eventName: E,
  callback: WorkerListener<TInput, TOutput, TName>[E],
): void

removeWorkerEventListener<E extends keyof WorkerListener>(
  eventName: E,
  callback: WorkerListener<TInput, TOutput, TName>[E],
): void
```

Typed wrappers around the underlying `Worker`'s event emitter. `addWorkerEventListener` throws if the worker isn't initialized yet (before `defineProcessor`). See [Workers, Concurrency & Progress](../guides/workers).

### `close()`

```ts
close(): Promise<void>
```

Gracefully closes the worker, queue, and queue events.

## Properties

| Property | Type | Notes |
| --- | --- | --- |
| `name` | `TName` | The BullMQ queue key (from `NameFactory`). |
| `displayName` | `string` | Human-readable name (from `NameFactory`). |
| `system` | `string \| undefined` | Set via `addToSystem`. |
| `description` | `string \| undefined` | Set via `addDescription`. |
| `jsonSchema` | `Record<string, unknown> \| undefined` | Populated by `defineInputSchema` when the schema vendor supports JSON Schema conversion. |
| `queue` | `Queue \| undefined`, narrows to `Queue` after `defineConnection` | The underlying BullMQ queue, created on first access once a connection has been defined. |
| `worker` | `Worker \| undefined`, narrows to `Worker` after `defineProcessor` | The underlying BullMQ worker. |
| `QueueEvents` | `QueueEvents` (getter) | Lazily-created `QueueEvents` instance, needed by `job.waitUntilFinished(queue.QueueEvents, timeout)`. Throws if accessed before `defineConnection`. |

## Lifecycle narrowing

`QueueSystem` carries two optional lifecycle type parameters that track how far the builder has been driven, so `queue` and `worker` stop being optional once the call that creates them has run — no `?.` and no `!` needed.

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

Both parameters default to `boolean`, meaning "either state", so an existing annotation like `QueueSystem<In, Out, Name>` still accepts a system at any stage and behaves exactly as it did before. Write them explicitly only when you want to require a stage:

```ts
// Only accepts a system that has been connected.
function enqueueOnly<In extends JSONLike>(q: QueueSystem<In, JSONLike, string, true>) {
  return q.queue; // Queue, no narrowing needed
}
```

The same mechanism enforces call order. `defineProcessor` declares a `this` parameter requiring a connected system, so getting the order wrong fails to compile rather than throwing at startup:

```ts
new QueueSystem(NameFactory({ name: "MyCoolQueue" })).defineProcessor((step) =>
  step.addStep({ name: "greet", handler: async () => ({ ok: true }) }),
);
// ^ error: The 'this' context of type 'QueueSystem<..., boolean, boolean>' is not
//   assignable to method's 'this' of type 'QueueSystem<..., true, boolean>'.
```

## `QueueInput<T>` / `QueueOutput<T>`

```ts
type QueueInput<T> = T extends QueueSystem<infer I, any, any, any, any> ? I : never;
type QueueOutput<T> = T extends QueueSystem<any, infer O, any, any, any> ? O : never;
```

Extract a queue's input/output types for use elsewhere — for example, typing a function that only enqueues jobs for an already-built queue.

```ts
import type { QueueInput, QueueOutput } from "@michaelrwalker/step-queue";

type MyInput = QueueInput<typeof myQueue>;
type MyOutput = QueueOutput<typeof myQueue>;
```
