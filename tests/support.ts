import { Queue } from "bullmq";
import type { QueueSystem } from "../modules/index.ts";

/**
 * Connection used by the whole suite. Points at a real Redis — start one and
 * (if it isn't on the default) set REDIS_HOST / REDIS_PORT before running.
 */
export const connection: { host: string; port: number } = {
	host: process.env.REDIS_HOST ?? "127.0.0.1",
	port: process.env.REDIS_PORT ? Number(process.env.REDIS_PORT) : 60866,
};

let counter = 0;
/** A queue name unique to this process + call, so tests never collide. */
export function uniqueName(base: string): string {
	counter += 1;
	return `${base}-${process.pid}-${counter}`;
}

/** Fail fast with a friendly message if Redis isn't reachable. */
export async function ensureRedis(): Promise<void> {
	const probe = new Queue(uniqueName("probe"), { connection });
	try {
		await Promise.race([
			probe.waitUntilReady(),
			new Promise<never>((_, reject) => {
				const t = setTimeout(() => reject(new Error("timed out")), 3000);
				t.unref();
			}),
		]);
	} catch {
		throw new Error(
			`Cannot reach Redis at ${String(connection.host)}:${String(connection.port)}. ` +
				`Start a Redis instance (and set REDIS_HOST / REDIS_PORT if it isn't on 127.0.0.1:6379).`,
		);
	} finally {
		await probe.close();
	}
}

type Closable = { close: () => Promise<void> };

const open: Closable[] = [];

/** Register a QueueSystem (or anything closable) for automatic teardown. */
export function track<T extends Closable>(closable: T): T {
	open.push(closable);
	return closable;
}

/** Close everything registered with {@link track}. Call from afterEach. */
export async function closeAll(): Promise<void> {
	const toClose = open.splice(0);
	await Promise.all(toClose.map((c) => c.close()));
}

/**
 * Enqueue a job and resolve with the processor's typed result once the real
 * worker finishes. Rejects (with the failure reason) if the job fails.
 */
export async function runToCompletion<
	TInput extends Record<string, unknown>,
	TOutput extends Record<string, unknown>,
	TName extends string,
>(queue: QueueSystem<TInput, TOutput, TName>, input: TInput): Promise<TOutput> {
	const job = await queue.addJob(input);
	return await job.waitUntilFinished(queue.QueueEvents, 15_000);
}

// ---- compile-time assertion helpers (checked by `npm run typecheck`) ----
export type Equal<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
export type Expect<T extends true> = T;
