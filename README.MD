# Step Queue

A type-safe, step-based wrapper around [BullMQ](https://docs.bullmq.io/).

Step Queue lets you define BullMQ jobs as a set of completely type-safe steps.
Job data accumulates as it moves through the pipeline, so each step is typed
against exactly what the steps before it produced. If a job fails and BullMQ
retries it, the job resumes on the exact step it left off at, which makes it
possible to write steps that are fully idempotent instead of re-running the
whole job from the start.

---

## Why this exists

BullMQ's own docs recommend handling a multi-step job by keeping a `step`
field on the job data, switching over an enum of step names, and — anywhere
the job needs to wait on children — manually calling `job.moveToWaitingChildren`
and throwing `WaitingChildrenError`:

```ts
import { WaitingChildrenError, Worker } from 'bullmq';

enum Step {
  Initial,
  Second,
  Third,
  Finish,
}

const worker = new Worker(
  'parentQueueName',
  async (job: Job, token?: string) => {
    let step = job.data.step;
    while (step !== Step.Finish) {
      switch (step) {
        case Step.Initial: {
          await doInitialStepStuff();
          await childrenQueue.add(
            'child-1',
            { foo: 'bar' },
            { parent: { id: job.id, queue: job.queueQualifiedName } },
          );
          await job.updateData({ step: Step.Second });
          step = Step.Second;
          break;
        }
        case Step.Second: {
          await doSecondStepStuff();
          await childrenQueue.add(
            'child-2',
            { foo: 'bar' },
            { parent: { id: job.id, queue: job.queueQualifiedName } },
          );
          await job.updateData({ step: Step.Third });
          step = Step.Third;
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
          throw new Error('invalid step');
        }
      }
    }
  },
  { connection },
);
```

I built my first several queues exactly this way, and for a short job it is
fine. It stops being fine once the logic gets long and has to wait on children
more than once: the step enum, the `switch`, and the
`moveToWaitingChildren`/`WaitingChildrenError` pair have to be repeated and
kept in sync at every wait point, `job.data` is untyped inside every `case`,
and it is easy to forget the `await job.updateData(...)` before a step that
can throw, which silently breaks resumption on the next retry.

Step Queue is that same pattern turned into a builder. `.addStep(...)`
replaces a `case`, the step-tracking and the
`moveToWaitingChildren`/`WaitingChildrenError` handling for any step with a
`childQueue` is done once by the processor instead of by hand at every wait
point, and `job.data` inside each step is typed against exactly what the
steps before it produced.

---

## Installation

```bash
npm install @michaelrwalker/step-queue bullmq
```

`bullmq` is a peer dependency. Input validation is powered by
[Standard Schema](https://standardschema.dev), so bring whichever compliant
schema library you prefer — Zod, Valibot, ArkType, Effect Schema, and others
all work (the examples below use Zod).

---

## Core concepts

| Concept | Role |
| --- | --- |
| `QueueSystem` | Fluent builder that owns a BullMQ `Queue` and `Worker`. Enqueues jobs and defines the steps that process them. |
| `ProcessorBuilder` | Accumulates steps and their types. Usually received via `defineProcessor`'s callback, but can also be constructed standalone with `ProcessorBuilder.init<T>()` and passed to `defineProcessor` directly. |
| A step | One unit of work: a `handler`, plus optional `rollback`, `clean`, `progressReport`, and (for fan-out) `childQueue`. |
| `TypedJob` | The `job` a handler receives — a normal BullMQ `Job` with `data` narrowed to the type accumulated so far, plus `errors`, `actionErrors`, and `updateStepProgress`. |
| `QueueOnly` | A producer-only counterpart to `QueueSystem` — enqueues jobs without starting a worker. |
| `NameFactory` | Builds a queue's internal `name` (used for the BullMQ queue key) and a human-readable `displayName`. |

Data flows through steps by accumulation: the first step sees the validated job
input; each later step sees the input merged with everything every prior step
returned, minus anything any prior step `clean`ed away. `ProcessorBuilder`
tracks this shape at the type level, so `job.data` inside a handler is exactly
what will be present at runtime when that handler executes — nothing has to be
cast or asserted.

---

## Quick start

```ts
import {z} from "zod";
import {QueueSystem, NameFactory} from "@michaelrwalker/step-queue";

const myInput = z.object({greeting: z.string()});

const myQueue = new QueueSystem(NameFactory({name: "MyCoolQueue", env: "dev"}))
    .defineInputSchema(myInput)
    .defineConnection({host: "localhost", port: 6379})
    .defineProcessor((processor) =>
        processor
            .addStep({
                name: "shout",
                // job.data.greeting is typed as string (inferred from the schema)
                handler: async (job) => ({shout: job.data.greeting.toUpperCase()}),
            })
            .addFinalStep({
                // job.data.shout is typed as string
                handler: async (job) => ({done: job.data.shout}),
            }),
    );

// typed: { greeting: string }
const job = await myQueue.addJob({greeting: "hi"});
const result = await job.waitUntilFinished(myQueue.QueueEvents, 10_000);
// result is typed { done: string }
```

`defineProcessor`'s callback receives a `ProcessorBuilder`; since an input
schema was provided via `defineInputSchema`, the first step added is typed
with that schema's data. `defineConnection` has to be called before
`defineProcessor` — calling it out of order throws immediately.

`defineProcessor` also accepts a `ProcessorBuilder` you construct yourself,
which is useful when the steps are defined away from the queue (shared,
tested, or composed elsewhere). Don't call `.build()` — `defineProcessor`
builds the processor in both forms. When a standalone builder is passed, the
queue's input type is inferred from `ProcessorBuilder.init<T>()`:

```ts
import {QueueSystem, NameFactory, ProcessorBuilder} from "@michaelrwalker/step-queue";

const processor = ProcessorBuilder.init<{greeting: string}>()
    .addStep({
        name: "shout",
        handler: async (job) => ({shout: job.data.greeting.toUpperCase()}),
    })
    .addFinalStep({
        handler: async (job) => ({done: job.data.shout}),
    });

const myQueue = new QueueSystem(NameFactory({name: "MyCoolQueue", env: "dev"}))
    .defineConnection({host: "localhost", port: 6379})
    .defineProcessor(processor);

// addJob is typed { greeting: string }; results are typed { done: string }
```

---

## Defining input and the type pipeline

`defineInputSchema` takes any [Standard Schema](https://standardschema.dev)
(Zod, Valibot, ArkType, Effect Schema, ...). It does two things:

1. Infers the initial shape of `job.data` for the first step, so the rest of the
   processor is typed from it without any manual generic annotation.
2. Validates every job's data against the schema at `addJob` / `addBulkJobs`
   time — invalid data is rejected with a `SchemaValidationError` (carrying the
   standard `issues` array) before it ever reaches a worker.

```ts
const q = new QueueSystem(NameFactory({name: "Orders"}))
    .defineInputSchema(z.object({orderId: z.string(), amount: z.number()}))
    .defineConnection(connection)
    .defineProcessor((processor) =>
        processor
            .addStep({name: "charge", handler: async () => ({})}),
    );

// throws — amount fails z.number()
await q.addJob({orderId: "o_1", amount: "not a number"} as unknown as {
    orderId: string;
    amount: number;
});
```

If the schema library supports Standard Schema's JSON Schema extension
(`StandardJSONSchemaV1` — Zod 4 does, for example), the schema is also
converted to a JSON Schema and exposed as `q.jsonSchema`, useful for
documenting or externally validating a queue's expected input without pulling
in the schema library on the reading side. For libraries without that support,
`q.jsonSchema` stays `undefined`.

---

## Steps

### Adding a step

```ts
processor
        .addStep({
          name: "hash",
          handler: async (job) => ({hash: await hashIt(job.data.password)}),
        });
```

A handler either returns an object — merged into the accumulated job data for
every later step — or returns nothing, leaving the data unchanged.

Steps run in the order `addStep` is called in. Each step's `job.data` type is
computed from every previous step's declared output (and `clean`), so a step
that reads a field before an earlier step produces it is a compile error, not a
runtime `undefined`.

### The final step

```ts
processor
	.addStep({ name: "shout", handler: async (job) => ({ shout: job.data.greeting.toUpperCase() }) })
	.addFinalStep({ handler: async (job) => ({ done: job.data.shout }) });
```

`addFinalStep` marks the end of the pipeline; its return value becomes the
job's result (what `job.waitUntilFinished` resolves to). A final step cannot
have a next step, and no step may be added after it.

If `addFinalStep` is never called, one is inserted automatically. Its result is
the accumulated job data with Step Queue's own bookkeeping keys (`step`,
`previousStep`, `consumedKeys`, `completedSteps`) stripped out — so the queue's
output type matches whatever the last declared step produced, without those
internal fields leaking into it.

### The `clean` feature

If a step produces data that later steps do not need, listing those keys in
`clean` deletes them from the job data at runtime **and** removes them from the
type that flows into subsequent steps.

```ts
const q = new QueueSystem(NameFactory({ name: "Signup" }))
	.defineInputSchema(z.object({ password: z.string(), email: z.string() }))
	.defineConnection(connection)
	.defineProcessor((processor) =>
		processor
			.addStep({
				name: "hash",
				handler: async (job) => ({ hash: await hashIt(job.data.password) }),
				// no longer needed past this point
				clean: ["password"],
			})
			.addStep({
				name: "persist",
				handler: async (job) => {
					job.data.hash; // string
					job.data.email; // string
					// @ts-expect-error — `password` was cleaned away
					job.data.password;
					return {};
				},
			}),
	);
```

Rules that apply to `clean`:

- Keys are type-checked against the data available at that step — an unknown
  key is a compile error, and the editor autocompletes valid keys.
- A step can clean a pre-existing input key or a key it just added, but not a
  key produced by a later step (it does not exist yet).
- A key reserved by any step's `rollbackKeys` (see below) can never be cleaned
  — by that step or by any step after it — since a rollback might still need
  it. Attempting to do so throws `AddStepError` while the processor is being
  built, before any job ever runs.

---

## Reusable steps

`defineStep` produces a step scoped only to the minimum shape of data it
needs, independent of any specific processor's accumulated type. `defineSteps`
groups several under one namespace.

```ts
import { defineStep, defineSteps } from "@michaelrwalker/step-queue";

const loadUser = defineStep<{ userId: string }>()({
	name: "loadUser",
	handler: async (job) => ({ user: { id: job.data.userId, name: "Ada" } }),
});

const common = defineSteps({
	stampReceivedAt: defineStep<{ userId: string }>()({
		name: "stampReceivedAt",
		handler: async () => ({ receivedAt: new Date().toISOString() }),
	}),
	audit: defineStep<{ user: { name: string } }>()({
		name: "audit",
		handler: async (job) => {
			console.log(`[audit] processed ${job.data.user.name}`);
		},
	}),
});

const onboarding = new QueueSystem(NameFactory({ name: "onboarding" }))
	.defineInputSchema(z.object({ userId: z.string() }))
	.defineConnection(connection)
	.defineProcessor((processor) =>
		processor
			.addStep(common.stampReceivedAt)
			.addStep(loadUser)
			.addStep(common.audit)
			.addFinalStep({
				handler: async (job) => ({ welcome: `Welcome, ${job.data.user.name}!` }),
			}),
	);
```

`addStep` is where enforcement happens: TypeScript checks that the processor's
accumulated data at that point satisfies the step's declared `TRequired` shape
before it compiles. A step defined with `defineStep<{ userId: string }>()`
cannot be dropped into a processor that has not yet produced a `userId` field.

---

## Control flow

A handler controls what happens next by throwing one of `job.actionErrors`,
available on every `TypedJob`.

| Action | How | Effect |
| --- | --- | --- |
| Skip ahead to a named step | `throw new job.actionErrors.skip(stepName, result?)` | Jumps directly to `stepName`, merging `result` into the job data first. Any steps between the current one and the target are not run. |
| Skip straight to the final step | `throw new job.actionErrors.skipToFinal(null, result?)` | Jumps to the final step regardless of what comes next in the declared order. |
| Exit early with a value | `throw new job.actionErrors.earlyExit(value)` | Ends the processor immediately; `value` becomes the job's result, bypassing every later step (including the final step). |


```ts
const signup = new QueueSystem(NameFactory({ name: "signup", env: "example" }))
	.defineInputSchema(z.object({ email: z.string(), optOutOfEmail: z.boolean() }))
	.defineConnection(connection)
	.defineProcessor((processor) =>
		processor
			.addStep({
				name: "validate",
				handler: async (job) => {
					if (!job.data.email.includes("@")) {
						// bail out of the entire job with a custom result shape
						throw new job.actionErrors.earlyExit({
							status: "rejected",
							reason: "invalid email",
						});
					}
					return { validated: true };
				},
			})
			.addStep({
				name: "reserveUsername",
				handler: async (job) => {
					// if the username service is down, throw — BullMQ retries the job
					// (enqueue with `attempts` > 1) and resumes from this step.
					const reserved = await reserveUsername(job.data.email);
					if (!reserved) {
						throw new Error("username service unavailable");
					}
					return { username: job.data.email.split("@")[0] ?? "user" };
				},
			})
			.addStep({
				name: "sendWelcomeEmail",
				handler: async (job) => {
					if (job.data.optOutOfEmail) {
						// jump straight to "audit", skipping grantTrial
						throw new job.actionErrors.skip("audit", { emailed: false });
					}
					return { emailed: true };
				},
			})
			.addStep({
				name: "grantTrial",
				handler: async () => ({ trialDays: 30 }),
			})
			.addStep({
				name: "audit",
				handler: async (job) => {
					console.log(`[audit] ${job.data.username} — emailed=${job.data.emailed}`);
				},
			})
			.addFinalStep({
				handler: async (job) => ({ status: "created", username: job.data.username }),
			}),
	);
```

---

## Typed errors

Beyond control-flow actions, a processor can register its own error codes with
`.errors(...)` and throw them by name from any step. Unlike `actionErrors`,
these are thrown directly — `job.errors.CODE` is already an error instance, not
a class.

```ts
const q = new QueueSystem(NameFactory({ name: "ChangePassword" }))
	.defineInputSchema(
		z.object({ originalPassword: z.string(), newPassword: z.string() }),
	)
	.defineConnection(connection)
	.defineProcessor((processor) =>
		processor
			.errors({
				MATCHING_PASSWORDS: "Your new password must match",
			})
			.addStep({
				name: "validate",
				handler: async (job) => {
					if (job.data.newPassword === job.data.originalPassword) {
						throw job.errors.MATCHING_PASSWORDS;
					}
					return { valid: true };
				},
			}),
	);
```

Throwing an unregistered code (a typo, or a code from a different processor)
throws immediately with a message pointing at the missing `.errors()`
registration, rather than silently producing `undefined`.

An error thrown from a handler that is not one of the control-flow actions
above (including a registered typed error) fails the step: the processor runs
rollbacks for every completed step, in reverse order, and re-throws — which
BullMQ records as a failed job.

---

## Rollbacks

A step can declare a `rollback`, run only if a *later* step in the same job
fails. Rollbacks run in reverse completion order — the most recently completed
step rolls back first.

By default a rollback receives no job data at all: it has to explicitly ask for
what it needs via `rollbackKeys`. Requesting a key does two things — it narrows
`job.data` inside `rollback` to exactly those keys, and it reserves those keys
so neither this step nor any later step can `clean` them, guaranteeing they
still exist if the rollback actually runs.

```ts
const queue = new QueueSystem(NameFactory({ name: "charge" }))
	.defineInputSchema(z.object({ accountId: z.string(), amount: z.number() }))
	.defineConnection(connection)
	.defineProcessor((processor) =>
		processor
			.addStep({
				name: "reserveFunds",
				handler: async (job) => ({ reservationId: await reserve(job.data.accountId, job.data.amount) }),
				rollbackKeys: ["accountId", "reservationId"],
				rollback: async (job) => {
					// job.data is narrowed to { accountId: string; reservationId: string }
					await releaseReservation(job.data.accountId, job.data.reservationId);
				},
			})
			.addStep({
				name: "chargeCard",
				handler: async () => {
					throw new Error("payment gateway timeout");
				},
			}),
	);

// "chargeCard" fails → reserveFunds's rollback runs before the job is marked failed.
```

Attempting to name a key in `rollbackKeys` that is neither part of the step's
input nor its own output is a compile error, and attempting to `clean` a
reserved key (from this or an earlier step) throws `AddStepError` at build
time:

```ts
ProcessorBuilder.init<{ a: number; b: string }>()
	.addStep({
		name: "one",
		handler: async () => {},
		rollbackKeys: ["a"],
		rollback: async () => {},
	})
	.addStep({
		name: "two",
		handler: async () => {},
		// throws AddStepError: "a" is reserved by step one's rollbackKeys
		clean: ["a"],
	});
```

---

## Progress reporting

There are two independent ways to surface progress from a step:

- `job.updateStepProgress({...})`, called from inside a handler, merges custom
  data into the job's BullMQ progress and emits a `"progress"` event on the
  worker.
- A `progressReport` callback on a step, invoked automatically at `"start"`,
  `"finish"`, `"skip"`, and (for a step with a child queue) `"child-finish"` —
  useful for logging or pushing updates without cluttering the handler itself.

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

---

## Child queues (fan-out)

A step can fan work out to a separate `QueueSystem` by attaching a
`childQueue`. The step's own handler is responsible for enqueuing the child
jobs — typically via `childQueue.addChildJob(job, data)` — and the processor
takes care of pausing the parent job until every child finishes.

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

Options on a child step:

- `singleChild: true` — when exactly one child job is expected, its output is
  stored unwrapped instead of as a single-element array.
- `processChildResult` — maps the raw child output(s) (one value if
  `singleChild`, otherwise an array) into whatever shape should actually be
  stored.
- The results are always written back to the job data under the key
  `${stepName}-results`, both at runtime and in the accumulated type — so a
  step named `"fanOut"` produces `job.data["fanOut-results"]` for every later
  step.

Internally, a step with a `childQueue` moves the parent job into BullMQ's
"waiting-children" state after the handler runs (via `moveToWaitingChildren`).
Once every child job completes, the processor collects `job.getChildrenValues()`,
applies `singleChild` / `processChildResult`, stores the result, and continues
to the next step — deduplicating against previously consumed children so a
step is never double-processed if the worker restarts mid-wait.

---

## Producers-only queues

A process that should only enqueue jobs — an API route, a script, a cron job —
and never run a worker can use `QueueOnly` instead of `QueueSystem`. It exposes
the same `addJob` / `addBulkJobs` / `addChildJob` / `addBulkChildJobs` surface
without constructing a `Worker`.

```ts
import { QueueOnly, NameFactory } from "@michaelrwalker/step-queue";

const producer = new QueueOnly(
	NameFactory({ name: "MyCoolQueue", env: "dev" }),
	{ host: "localhost", port: 6379 },
);
await producer.addJob({ greeting: "hi" });
```

`QueueOnly` does not take an input schema, so it does not validate job data at
enqueue time — the assumption is that whatever process defines the schema (via
a `QueueSystem` on the consuming side) is the source of truth for the shape.

---

## Naming queues

`NameFactory` builds the pair of names every `QueueSystem` / `QueueOnly` /
child queue needs: an internal `name` (the actual BullMQ queue key, which
should be unique and environment-scoped) and a `displayName` (just the given
name, for logging or UI).

```ts
NameFactory({ name: "Orders" });
// { name: "Orders", displayName: "Orders" }

NameFactory({ name: "Orders", env: "dev" });
// { name: "dev-Orders", displayName: "Orders" }

NameFactory({ name: "child", parent: "Orders", env: "dev" });
// { name: "dev-Orders -child", displayName: "child" }

const parent = NameFactory({ name: "parentQueue" });
NameFactory({ name: "childQueue", parent });
// { name: "parentQueue -childQueue", displayName: "childQueue" }
```

Passing another queue's own `{ name, displayName }` object as `parent` (rather
than a bare string) is the usual pattern for child queues, since the parent's
`env` prefix is already baked into `parent.name` and is not applied twice.

---

## `QueueSystem` reference

| Method | Purpose |
| --- | --- |
| `defineInputSchema(schema)` | Sets the Standard Schema (Zod, Valibot, ArkType, ...) that types and validates job input. |
| `defineConnection(options)` | Sets the BullMQ `ConnectionOptions` (Redis). Required before `defineProcessor`. |
| `defineConcurrency(n)` | Sets worker concurrency. Defaults to `1`. |
| `defineProcessor(buildOrBuilder)` | Declares steps and starts the `Queue` + `Worker`. Takes either a callback receiving a typed `ProcessorBuilder`, or a standalone `ProcessorBuilder` instance (un-built). |
| `addDescription(text)` / `addToSystem(text)` | Free-form metadata fields for organizing queues; not used by the engine itself. |
| `addJob(data, options?)` | Validates and enqueues one job. |
| `addBulkJobs(jobs)` | Validates and enqueues many jobs in one call. |
| `addChildJob(parentJob, data, options?)` | Enqueues a job as a BullMQ child of `parentJob`. |
| `addBulkChildJobs(parentJob, jobs)` | Bulk version of `addChildJob`. |
| `QueueEvents` | Lazily-created `QueueEvents` instance, needed by `job.waitUntilFinished(queue.QueueEvents, timeout)`. |
| `addWorkerEventListener(event, cb)` / `removeWorkerEventListener(event, cb)` | Typed wrappers around the underlying `Worker`'s event emitter. |
| `close()` | Gracefully closes the worker, queue, and queue events. |

`QueueInput<T>` and `QueueOutput<T>` extract a queue's input/output types for
use elsewhere — for example, typing a function that only enqueues jobs for an
already-built queue.

```ts
import type { QueueInput, QueueOutput } from "@michaelrwalker/step-queue";

type MyInput = QueueInput<typeof myQueue>;
type MyOutput = QueueOutput<typeof myQueue>;
```

---

## Default job options

Every job added through `addJob` / `addBulkJobs` (on both `QueueSystem` and
`QueueOnly`) picks up these defaults unless overridden per call:

```ts
{
	removeOnComplete: { age: 60 * 60 * 24, count: 1000 }, // 1 day, capped at 1000
	removeOnFail: { age: 60 * 60 * 24 * 7 },               // 7 days
	attempts: 1,
}
```

`attempts: 1` means BullMQ-level retries are off by default. To enable retries,
raise `attempts` per job — because the processor persists its position in the
job data, a retried job resumes from the step that failed rather than restarting
from its input.

---

## Internal bookkeeping and limits

The processor tracks its own position in the job data using a few reserved
keys: `step` (the current step name), `previousStep`, `consumedKeys` (child
results already collected), and `completedSteps` (for rollback ordering).
These are written to `job.data` at runtime so a job can resume correctly after
a worker restart, but they are stripped from the result of an implicit final
step and should not be relied on by handler code — a step name accidentally
matching one of `"step"`, `"previousStep"`, `"consumedKeys"`, or
`"completedSteps"` would collide with them.

A processor is capped at 100 steps; exceeding that throws
`ProcessorStepError` at job-run time as a guard against accidental infinite
step graphs (for example, a `skip` chain that loops back on itself).

---

## Development

The test suite exercises real BullMQ against a real Redis (there are no mocks
— BullMQ relies on Lua scripts and blocking commands a mock cannot provide).
Start a Redis first, then run the tests:

```bash
npm test          # expects Redis on 127.0.0.1:6379
npm run typecheck
npm run format
npm run fix
```

If Redis is elsewhere, point the suite at it — it fails fast with a clear
message if it can't connect, rather than hanging:

```bash
REDIS_HOST=127.0.0.1 REDIS_PORT=55578 npm test
```

Tests run with `node --test --experimental-transform-types`, so TypeScript runs
directly with no build step. Each test builds a `QueueSystem`, enqueues a job,
and asserts on what the worker returns via `waitUntilFinished`, covering the
step engine — accumulation, `clean`, skips, retries, rollbacks, `rollbackKeys`
— end to end. Type-level guarantees are asserted inline with `@ts-expect-error`
and checked by `npm run typecheck`.
