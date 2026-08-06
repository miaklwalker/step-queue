---
title: "Installation"
description: "Install Step Queue and its bullmq peer dependency, and connect it to Redis."
---

## Install the package

```bash
npm install @michaelrwalker/step-queue bullmq
```

`bullmq` is a **peer dependency**, not a bundled dependency — install it alongside Step Queue explicitly. Step Queue targets `bullmq ^5.0.0`.

## Install a schema library

Input validation is powered by [Standard Schema](https://standardschema.dev), so bring whichever compliant schema library you prefer — Zod, Valibot, ArkType, Effect Schema, and others all work. The examples throughout this documentation use Zod:

```bash
npm install zod
```

A schema library isn't required for a `QueueOnly` producer, or for a `QueueSystem` that skips `defineInputSchema` — but without one, job data isn't validated at enqueue time and the initial step type has to come from `ProcessorBuilder.init<T>()` instead.

## Connect to Redis

Step Queue doesn't manage Redis itself — it passes a BullMQ `ConnectionOptions` object straight through to the `Queue` and `Worker` it creates. Point it at a running Redis instance:

```ts
import { QueueSystem, NameFactory } from "@michaelrwalker/step-queue";

const connection = { host: "localhost", port: 6379 };

const myQueue = new QueueSystem(NameFactory({ name: "MyCoolQueue", env: "dev" }))
  .defineConnection(connection);
```

`defineConnection` has to be called before `defineProcessor` — calling it out of order is a compile error (and throws at runtime if the type check is bypassed). See [QueueSystem reference](reference/queue-system) for the full lifecycle.

For local development, run Redis however you normally would (a local install, Docker, or a managed instance) and point `host` / `port` at it. Step Queue does not start, stop, or configure Redis on your behalf.

## Next

Continue to the [Quick Start](quick-start) for a complete worked job.
