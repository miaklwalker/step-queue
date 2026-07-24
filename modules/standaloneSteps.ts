import type { Prettify, TypedJob, ProgressStatus } from "./types.ts";

// When TOutput is void, the rollback handler should only see TRequired (not merge with void).
type VoidSafe<T> = T extends void ? Record<never, never> : T;

/**
 * The shape of a standalone (reusable) step.
 * TRequired - the minimum job data this step needs to run.
 * TOutput   - what this step adds to the job data (void if it adds nothing).
 */
export type StandaloneStep<TRequired, TOutput = void> = {
	name: string;
	handler: (
		job: TypedJob<TRequired>,
		token: string,
	) => TOutput | Promise<TOutput>;
	rollback?: (
		job: TypedJob<Prettify<TRequired & VoidSafe<TOutput>>>,
		token: string,
	) => void | Promise<void>;
	progressReport?: (
		job: TypedJob<TRequired>,
		token: string,
		stepName: string,
		status: ProgressStatus,
	) => void | Promise<void>;
};

/**
 * Define a reusable step with full type safety.
 *
 * Usage:
 *   const getConditions = defineStep<{ userId: string }>()({
 *       name: "Get Conditions",
 *       handler: async (job) => {
 *           // job.data.userId is typed
 *           return { conditions: [...] };
 *       }
 *   });
 *
 *   // Drop-in replacement for .addStep({...}):
 *   ProcessorBuilder.init<{ userId: string }>()
 *       .addStep(getConditions)
 *
 * TypeScript enforces that the processor's accumulated data satisfies
 * the step's TRequired before it can be used — same as a regular step.
 */
export function defineStep<TRequired = {}>() {
	return <TOutput = void>(
		config: StandaloneStep<TRequired, TOutput>,
	): StandaloneStep<TRequired, TOutput> => config;
}

/**
 * Group related standalone steps into a named collection.
 *
 * Usage:
 *   const commonSteps = defineSteps({
 *       getConditions: defineStep<{ userId: string }>()({ ... }),
 *       logAudit:      defineStep<{}>()({ ... }),
 *   });
 *
 *   .addStep(commonSteps.getConditions)
 */
export function defineSteps<
	TSteps extends Record<string, StandaloneStep<any, any>>,
>(steps: TSteps): TSteps {
	return steps;
}
