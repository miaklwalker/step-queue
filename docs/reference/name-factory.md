---
title: "NameFactory"
description: "Full API reference for NameFactory, which builds a queue's internal BullMQ name and its display name."
---

`NameFactory` builds the pair of names every `QueueSystem` / `QueueOnly` / child queue needs: an internal `name` (the actual BullMQ queue key, which should be unique and environment-scoped) and a `displayName` (just the given name, for logging or UI).

```ts
function NameFactory(config: {
  name: string;
  parent?: string | { name: string; displayName: string };
  env?: string;
}): { name: string; displayName: string }
```

## Behavior

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

`displayName` is always just the `name` you passed in — it never includes the `env` prefix or the parent segment. `name` is what's actually used as the BullMQ queue key.

## The two forms of `parent`

- **A bare string** (`parent: "Orders"`) — `env` is still applied to the combined name: `${env}-${parent}-${name}`, or `${parent}-${name}` with no `env`.
- **Another queue's own `{ name, displayName }` object** (`parent: someQueue` or `parent: NameFactory({ ... })`) — the parent's `env` prefix is already baked into `parent.name`, so it is not applied again. This is the usual pattern for [child queues](../guides/child-queues), since it guarantees the child's name is derived from the parent's actual queue key rather than a second, possibly inconsistent, string.

## Type-level overloads

`NameFactory` has three overloads that track the literal string types involved, so `name` and `displayName` on the result stay as specific template-literal types rather than widening to `string` — useful if you rely on `TName` elsewhere (for example, an explicit `QueueSystem<In, Out, "dev-Orders">` annotation).

```ts
// Object parent — env is NOT re-applied (already baked into parent.name)
function NameFactory<TName extends string, TParentName extends string>(config: {
  name: TName;
  parent: { name: TParentName; displayName: string };
  env?: string;
}): { name: `${TParentName}-${TName}`; displayName: TName };

// String parent — env IS applied
function NameFactory<TName extends string, TParent extends string, TEnv extends string | undefined>(config: {
  name: TName;
  parent: TParent;
  env?: TEnv;
}): {
  name: TEnv extends string ? `${TEnv}-${TParent}-${TName}` : `${TParent}-${TName}`;
  displayName: TName;
};

// No parent
function NameFactory<TName extends string, TEnv extends string | undefined>(config: {
  name: TName;
  env?: TEnv;
}): {
  name: TEnv extends string ? `${TEnv}-${TName}` : TName;
  displayName: TName;
};
```
