import type { JobsOptions } from "bullmq";

/**
 * Default options applied to every job added through a QueueSystem.
 * Override per-job by passing `options` to `addJob`.
 */
export const DEFAULT_JOB_OPTIONS: JobsOptions = {
	removeOnComplete: { age: 60 * 60 * 24, count: 1000 },
	removeOnFail: { age: 60 * 60 * 24 * 7 },
	attempts: 1,
};
