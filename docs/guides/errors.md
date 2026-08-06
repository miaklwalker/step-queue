---
title: "Errors, Control Flow & Rollbacks"
description: "Directing a job's flow with actionErrors, registering typed error codes, and rolling back completed steps on failure."
---

A handler has three ways to end a step: return normally, throw one of `job.actionErrors` to direct the processor's flow, or throw anything else, which fails the step.

## Control-flow actions

A handler controls what happens next by throwing one of `job.actionErrors`, available on every `TypedJob`.

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
          console.log(`[audit] emailed=${job.data.emailed}`);
        },
      })
      .addFinalStep({
        handler: async (job) => ({ status: "created" }),
      }),
  );
```

None of these three are treated as a job failure — BullMQ never sees them as an error and never retries because of them.

## Typed errors

Beyond control-flow actions, a processor can register its own error codes with `.errors(...)` and throw them by name from any step. Unlike `actionErrors`, these are thrown directly — `job.errors.CODE` is already an error instance, not a class.

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

Throwing an unregistered code (a typo, or a code from a different processor) throws immediately with a message pointing at the missing `.errors()` registration, rather than silently producing `undefined`.

A typed error is not a control-flow action — it fails the step exactly like any other thrown error, described next.

## What happens when a step fails

An error thrown from a handler that is not one of the control-flow actions above (including a registered typed error) fails the step: the processor runs rollbacks for every completed step, in reverse order, and re-throws — which BullMQ records as a failed job. If the job's `attempts` option allows another try, BullMQ retries it and the processor resumes from the failed step; see [Resuming After Failure](resuming).

## Rollbacks

A step can declare a `rollback`, run only if a *later* step in the same job fails. Rollbacks run in reverse completion order — the most recently completed step rolls back first.

By default a rollback receives no job data at all: it has to explicitly ask for what it needs via `rollbackKeys`. Requesting a key does two things — it narrows `job.data` inside `rollback` to exactly those keys, and it reserves those keys so neither this step nor any later step can `clean` them, guaranteeing they still exist if the rollback actually runs.

```ts
const queue = new QueueSystem(NameFactory({ name: "charge" }))
  .defineInputSchema(z.object({ accountId: z.string(), amount: z.number() }))
  .defineConnection(connection)
  .defineProcessor((processor) =>
    processor
      .addStep({
        name: "reserveFunds",
        handler: async (job) => ({
          reservationId: await reserve(job.data.accountId, job.data.amount),
        }),
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

Attempting to name a key in `rollbackKeys` that is neither part of the step's input nor its own output is a compile error, and attempting to `clean` a key reserved by `rollbackKeys` (from this or an earlier step) throws `AddStepError` at build time:

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

Rollbacks are also declarable on a `childQueue` step and on a standalone step created with `defineStep` — the same `rollbackKeys` narrowing applies in both cases.
