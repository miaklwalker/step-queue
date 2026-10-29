import type { JobsOptions } from "bullmq";

/**
 * Default options applied to every job added through a QueueSystem.
 * Override per-job by passing `options` to `addJob`.
 */
export const DEFAULT_JOB_OPTIONS: JobsOptions = {
	removeOnComplete: { count: 1000 },
	removeOnFail: { count: 3000 },
	attempts: 3,
};
