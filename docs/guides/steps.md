---
title: "Steps & Data Accumulation"
description: "How addStep and addFinalStep build up a fully typed job data shape, and how clean removes keys from it."
---

A processor is a sequence of steps, each contributing a `handler` and (optionally) `rollback`, `clean`, `progressReport`, and a `childQueue`. Steps run in the order `addStep` is called, and each step's `job.data` type is computed from every previous step's declared output — a step that reads a field before an earlier step produces it is a compile error, not a runtime `undefined`.

## Adding a step

```ts
processor.addStep({
  name: "hash",
  handler: async (job) => ({ hash: await hashIt(job.data.password) }),
});
```

A handler either returns an object, which is merged into the accumulated job data for every later step, or returns nothing (`void`), leaving the data unchanged.

## The final step

```ts
processor
  .addStep({ name: "shout", handler: async (job) => ({ shout: job.data.greeting.toUpperCase() }) })
  .addFinalStep({ handler: async (job) => ({ done: job.data.shout }) });
```

`addFinalStep` marks the end of the pipeline; its return value becomes the job's result — what `job.waitUntilFinished` resolves to. A final step cannot have a next step, and no step may be added after it.

If `addFinalStep` is never called, one is inserted automatically. Its result is the accumulated job data with Step Queue's own bookkeeping keys (`step`, `previousStep`, `consumedKeys`, `completedSteps`) stripped out, so the queue's output type matches whatever the last declared step produced without those internal fields leaking into it.

## The `clean` feature

If a step produces data that later steps do not need, listing those keys in `clean` deletes them from the job data at runtime **and** removes them from the type that flows into subsequent steps.

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

- Keys are type-checked against the data available at that step — an unknown key is a compile error, and the editor autocompletes valid keys.
- A step can clean a pre-existing input key or a key it just added, but not a key produced by a later step (it does not exist yet).
- A key reserved by any step's `rollbackKeys` (see [Errors, Control Flow & Rollbacks](errors)) can never be cleaned — by that step or by any step after it — since a rollback might still need it. Attempting to do so throws `AddStepError` while the processor is being built, before any job ever runs.

## Reusable steps

`defineStep` produces a step scoped only to the minimum shape of data it needs, independent of any specific processor's accumulated type. `defineSteps` groups several under one namespace.

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

`addStep` is where enforcement happens: TypeScript checks that the processor's accumulated data at that point satisfies the step's declared required shape before it compiles. A step defined with `defineStep<{ userId: string }>()` cannot be dropped into a processor that has not yet produced a `userId` field.

## Step limits

A processor is capped at 100 steps. Exceeding that throws `ProcessorStepError` at job-run time — a guard against accidental infinite step graphs, such as a `skip` chain that loops back on itself. See [Errors, Control Flow & Rollbacks](errors) for `skip`.
