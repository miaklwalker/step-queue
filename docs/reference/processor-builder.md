---
title: "ProcessorBuilder"
description: "Full API reference for ProcessorBuilder, TypedJob, standalone steps, and the error/control-flow classes."
---

`ProcessorBuilder` accumulates steps and their types. It's usually received via `defineProcessor`'s callback, but can also be constructed standalone with `ProcessorBuilder.init<T>()` and passed to `defineProcessor` directly. See [Typed Input & Schema Validation](../guides/schema-validation).

```ts
class ProcessorBuilder<
  TInitialData,
  TCurrentData,
  TLastReturnType,
  TErrors extends ErrorRegistry = Record<PropertyKey, string>,
  TProtected extends PropertyKey = never,
>
```

## Static

### `ProcessorBuilder.init<T>()`

```ts
static init<TInitialData>(): ProcessorBuilder<TInitialData, TInitialData, TInitialData>
```

Constructs a standalone builder typed with `T` as the initial job data shape, independent of any `QueueSystem`'s `defineInputSchema`.

## Instance methods

### `errors(registry)`

```ts
errors<const E extends ErrorRegistry>(
  registry: E,
): ProcessorBuilder<TInitialData, TCurrentData, TLastReturnType, E, TProtected>
```

Registers named error codes usable from any step's handler via `job.errors.CODE`. See [Errors, Control Flow & Rollbacks](../guides/errors).

### `debug(enabled?)`

```ts
debug(enabled?: boolean): ProcessorBuilder<...>
```

Opts a processor in to step lifecycle logging (start, finish, rollback, and similar transitions) via BullMQ's own `job.log`. Off by default; call `debug()` (or `debug(true)`) to enable, `debug(false)` to explicitly disable it again.

### `addStep(stepConfig)`

Two overloads, distinguished by whether `stepConfig` includes a `childQueue`.

**Plain step:**

```ts
addStep<
  TStepOutput,
  const TRollbackKeys extends readonly PropertyKey[] = readonly [],
  const TClean extends readonly PropertyKey[] = readonly [],
>(stepConfig: {
  name: string;
  handler: (job: TypedJob<TLastReturnType, TErrors>, token: Job["token"]) => Promise<TStepOutput>;
  rollbackKeys?: TRollbackKeys;
  rollback?: (job: TypedJob<Pick<..., TRollbackKeys[number]>, TErrors>, token: Job["token"]) => void | Promise<void>;
  clean?: TClean;
  progressReport?: (job, token, stepName, status) => void | Promise<void>;
}): ProcessorBuilder<TInitialData, TCurrentData, /* new accumulated type */, TErrors, TProtected | TRollbackKeys[number]>
```

**Child step** (adds `childQueue` and its related options):

```ts
addStep<
  TName extends string,
  TStepOutput,
  TChildQueue extends AnyChildQueue,
  TSingle extends boolean = false,
  TChildResult = ChildResults<TChildQueue, TSingle>,
  const TRollbackKeys extends readonly PropertyKey[] = readonly [],
  const TClean extends readonly PropertyKey[] = readonly [],
>(stepConfig: {
  name: TName;
  handler: (job, token) => Promise<TStepOutput>;
  childQueue: TChildQueue;
  singleChild?: TSingle;
  processChildResult?: (results: ChildResults<TChildQueue, TSingle>) => TChildResult | Promise<TChildResult>;
  rollbackKeys?: TRollbackKeys;
  rollback?: (job, token) => void | Promise<void>;
  clean?: TClean;
  progressReport?: (job, token, stepName, status) => void | Promise<void>;
}): ProcessorBuilder<..., TErrors, TProtected | TRollbackKeys[number]>
```

Runs `_push` internally, which throws `AddStepError` synchronously (at build time, not job-run time) for:

- A duplicate step name.
- A `clean` entry that names a key reserved by this or an earlier step's `rollbackKeys`.

See [Steps & Data Accumulation](../guides/steps) and [Waiting on Child Jobs](../guides/child-queues).

### `addFinalStep(stepConfig)`

```ts
addFinalStep<TStepOutput>(stepConfig: {
  handler: (job: TypedJob<TLastReturnType, TErrors>, token: Job["token"]) => Promise<TStepOutput>;
}): ProcessorBuilder<TInitialData, TCurrentData, TStepOutput, TErrors>
```

Marks the end of the pipeline. Throws `AddStepError` if a step is added after it, or if a step is (re-)named `__final__step__`: the reserved internal name for an implicit final step.

### `build()`

```ts
build(): Processor<TInitialData, TLastReturnType>
```

Compiles the accumulated steps into the function BullMQ's `Worker` actually runs, inserting an implicit final step if `addFinalStep` was never called. **Don't call this yourself** when passing a builder to `QueueSystem.defineProcessor`: it calls `build()` for you in both the callback and standalone-builder forms. Throws `AddStepError` if the builder has no steps at all.

## `TypedJob<TData, TErrors>`

The `job` a handler receives, a normal BullMQ `Job` with `data` narrowed to the type accumulated so far, plus a few additions:

```ts
type TypedJob<TData, TErrors extends ErrorRegistry = Record<string, string>> = Omit<Job, "data"> & {
  readonly data: TData;
  readonly errors: { readonly [K in keyof TErrors]: ProcessorStepError };
  readonly actionErrors: {
    readonly skip: typeof SkipStepError;
    readonly earlyExit: typeof EarlyExitError;
    readonly skipToFinal: typeof SkipToFinalError;
    readonly recover: typeof RecoverWithChildError;
  };
  updateStepProgress: (data: Record<string, unknown>) => Promise<void>;
};
```

`errors` and `actionErrors` are runtime proxies: accessing an unregistered key on `errors` throws immediately (pointing at the missing `.errors()` registration) rather than returning `undefined`.

## Standalone steps

### `defineStep<TRequired>()`

```ts
function defineStep<TRequired>(): <
  TOutput = void,
  const TRollbackKeys extends readonly PropertyKey[] = readonly [],
>(
  config: StandaloneStep<TRequired, TOutput, TRollbackKeys>,
) => StandaloneStep<TRequired, TOutput, TRollbackKeys>
```

Produces a step scoped only to the minimum shape of data it needs (`TRequired`), independent of any specific processor's accumulated type. `addStep` enforces at compile time that the processor's accumulated data satisfies `TRequired` before the step can be added. See [Steps & Data Accumulation](../guides/steps).

### `defineSteps(steps)`

```ts
function defineSteps<TSteps extends Record<string, StandaloneStep<any, any>>>(
  steps: TSteps,
): TSteps
```

Groups several standalone steps under one namespace: purely an organizational identity function; it does not merge or validate the steps against each other.

## Error and control-flow classes

| Class | Thrown by | Meaning |
| --- | --- | --- |
| `SchemaValidationError` | `QueueSystem.addJob` / `addBulkJobs` | Job data failed the schema passed to `defineInputSchema`. Carries `issues: ReadonlyArray<StandardSchemaV1.Issue>`. |
| `AddStepError` | `ProcessorBuilder.addStep` / `addFinalStep` / `build` | A structural problem in the processor definition itself (duplicate name, reserved key, empty processor), thrown while building, before any job runs. |
| `ProcessorStepError` | The running processor | A runtime processor invariant was violated (missing step config, too many steps, no next step after a step that isn't final). Also the underlying class behind registered `.errors()` codes. |
| `SkipStepError` | `job.actionErrors.skip` | Thrown by handler code to jump to a named step. Caught internally by the processor, not a job failure. |
| `SkipToFinalError` | `job.actionErrors.skipToFinal` | Thrown by handler code to jump straight to the final step. Caught internally, not a job failure. |
| `EarlyExitError` | `job.actionErrors.earlyExit` | Thrown by handler code to end the processor immediately with a given return value. Caught internally, not a job failure. |
| `RecoverWithChildError` | `job.actionErrors.recover` | Thrown by handler code to add a child job and retry this same step once it completes. Caught internally; only becomes a job failure if `maxAttempts` is exceeded. |
| `RecoverAttemptsExceededError` | The running processor | A step's `recover` calls exceeded `maxAttempts`. Rollbacks run first, then the job fails with this error. |

See [Errors, Control Flow & Rollbacks](../guides/errors) and [Recovering From a Failure](../guides/recovering) for how these are used from inside a handler.
