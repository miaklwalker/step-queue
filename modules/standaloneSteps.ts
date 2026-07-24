import type {Job} from "bullmq";
import type { Prettify, ProgressStatus, TypedJob } from "./types.ts";

// When TOutput is void, the rollback handler should only see TRequired (not merge with void).
type VoidSafe<T> = T extends void ? Record<never, never> : T;

/**
 * The shape of a standalone (reusable) step.
 * TRequired     - the minimum job data this step needs to run.
 * TOutput       - what this step adds to the job data (void if it adds nothing).
 * TRollbackKeys - the keys the rollback requested; its job data is exactly
 *                 these (empty when none are declared), same as inline steps.
 */
export type StandaloneStep<
	TRequired,
	TOutput = void,
	TRollbackKeys extends readonly PropertyKey[] = readonly [],
> = {
	name: string;
	handler: (
		job: TypedJob<TRequired>,
		token: Job['token'],
	) => TOutput | Promise<TOutput>;
	rollbackKeys?: TRollbackKeys &
		readonly (keyof (TRequired & VoidSafe<TOutput>))[];
	rollback?: (
		job: TypedJob<
			Prettify<
				Pick<
					TRequired & VoidSafe<TOutput>,
					Extract<TRollbackKeys[number], keyof (TRequired & VoidSafe<TOutput>)>
				>
			>
		>,
		token: Job['token'],
	) => void | Promise<void>;
	progressReport?: (
		job: TypedJob<TRequired>,
		token: Job['token'],
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
 *   ProcessorBuilder.init<{ userId: string }>()
 *       .addStep(getConditions)
 *
 * TypeScript enforces that the processor's accumulated data satisfies
 * the step's TRequired before it can be used — same as a regular step.
 */
export function defineStep<TRequired>() {
	return <
		TOutput = void,
		const TRollbackKeys extends readonly PropertyKey[] = readonly [],
	>(
		config: StandaloneStep<TRequired, TOutput, TRollbackKeys>,
	): StandaloneStep<TRequired, TOutput, TRollbackKeys> => config;
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
	// biome-ignore lint/suspicious/noExplicitAny: <I believe any is correct here>
		TSteps extends Record<string, StandaloneStep<any, any>>,
>(steps: TSteps): TSteps {
	return steps;
}
