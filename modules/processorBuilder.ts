import type { Job } from "bullmq";
import type { QueueOutput, QueueSystem } from "./queueSystem.ts";
import { StepProcessor } from "./stepProcessor.ts";
import {
	AddStepError,
	type Cleaned,
	type ErrorRegistry,
	INTERNAL_KEYS,
	type Prettify,
	type ProgressStatus,
	type StepConfig,
	type TypedJob,
} from "./types.ts";

export type RegularBase<TInput, TErrors extends ErrorRegistry> = {
	name: string;
	hasChildren?: boolean;
	progressReport?: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: Job["token"],
		stepName: string,
		status: ProgressStatus,
	) => void | Promise<void>;
};

/**
 * A handler either adds data (returns an object) or adds nothing (returns
 * void). Rather than one overload per case, `TStepOutput` is inferred from the
 * handler and merged conditionally: void-ish returns leave the data unchanged.
 */

// biome-ignore lint/suspicious/noConfusingVoidType: <Users can return undefined or not return at all.>
type MergeOutput<TInput, TOutput> = [TOutput] extends [void | undefined]
	? TInput
	: TInput & TOutput;

// biome-ignore lint/suspicious/noExplicitAny: matching a queue structurally; its generics are captured by TChildQueue itself
export type AnyChildQueue = QueueSystem<any, any, any, any, any>;

/**
 * What `processChildResult` receives — and what `<name>-results` holds when no
 * `processChildResult` is given: one child's output when `singleChild: true`,
 * otherwise the array of all child outputs.
 */
type ChildResults<TChildQueue, TSingle extends boolean> = TSingle extends true
	? QueueOutput<TChildQueue>
	: QueueOutput<TChildQueue>[];

/** Job data after a child step: handler output merged in, plus `<name>-results`. */
type ChildStepData<
	TName extends string,
	TInput,
	TStepOutput,
	TChildResult,
> = MergeOutput<TInput, TStepOutput> & {
	[K in `${TName}-results`]: TChildResult;
};

/**
 * The job data visible inside `rollback` via `rollbackKeys`,
 * With no `rollbackKeys` the rollback gets an empty data object — a rollback must explicitly request what it needs.
 * Requested keys are protected from `clean` in this and all later steps, so
 * they are guaranteed to still exist when the rollback runs.
 */
type RollbackData<TAll, TKeys extends readonly PropertyKey[]> = Pick<
	TAll,
	Extract<TKeys[number], keyof TAll>
>;

export type PlainStep<
	TInput,
	TStepOutput,
	TRollbackKeys extends readonly PropertyKey[],
	TErrors extends ErrorRegistry,
> = {
	handler: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: Job["token"],
	) => Promise<TStepOutput>;
	/**
	 * The keys of the job data this step's `rollback` needs. Rollback receives
	 * ONLY these keys (none by default), and they become ineligible for `clean`
	 * in this and all later steps, so they still exist if the rollback runs.
	 */
	rollbackKeys?: TRollbackKeys & readonly (keyof (TInput & TStepOutput))[];
	rollback?: (
		job: TypedJob<
			Prettify<RollbackData<MergeOutput<TInput, TStepOutput>, TRollbackKeys>>,
			TErrors
		>,
		token: Job["token"],
	) => void | Promise<void>;
} & RegularBase<Prettify<TInput>, TErrors>;

export type ChildStep<
	TName extends string,
	TInput,
	TStepOutput,
	TChildQueue extends AnyChildQueue,
	TSingle extends boolean,
	TChildResult,
	TRollbackKeys extends readonly PropertyKey[],
	TErrors extends ErrorRegistry,
> = {
	name: TName;
	handler: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: Job["token"],
	) => Promise<TStepOutput>;
	childQueue: TChildQueue;
	hasChildren?: true;
	/** `true` = exactly one child job; its output is stored unwrapped. */
	singleChild?: TSingle;
	/** Map the child output(s) before storing them under `<name>-results`. */
	processChildResult?: (
		results: ChildResults<TChildQueue, TSingle>,
	) => TChildResult | Promise<TChildResult>;
	/**
	 * The keys of the job data this step's `rollback` needs. Rollback receives
	 * ONLY these keys (none by default), and they become ineligible for `clean`
	 * in this and all later steps, so they still exist if the rollback runs.
	 */
	rollbackKeys?: TRollbackKeys &
		readonly (keyof (TInput &
			TStepOutput & { [K in `${TName}-results`]: TChildResult }))[];
	rollback?: (
		job: TypedJob<
			Prettify<
				RollbackData<
					ChildStepData<TName, TInput, TStepOutput, TChildResult>,
					TRollbackKeys
				>
			>,
			TErrors
		>,
		token: Job["token"],
	) => void | Promise<void>;
	progressReport?: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: Job["token"],
		stepName: string,
		status: ProgressStatus,
	) => void | Promise<void>;
};

/**
 * Optional `clean` field: type-safe keys to drop from the job data once the
 * step finishes. Keys are validated against the step's INPUT data — the same
 * type `job.data` has inside the handler — minus any keys earlier steps
 * reserved via `rollbackKeys` (TProtected). Both types are fully resolved
 * before `addStep` is called, so `clean` never interacts with handler
 * inference. `TClean` captures the literal tuple (for the return-type Omit);
 * the value type validates element-wise — a valid key checks against itself,
 * an invalid one is expected to be the valid-key union — so only the
 * offending string is underlined, and the editor autocompletes the keys.
 */
type CleanField<
	TData,
	TProtected extends PropertyKey,
	TClean extends readonly PropertyKey[],
> = {
	clean?: {
		[I in keyof TClean]: TClean[I] extends Exclude<keyof TData, TProtected>
			? TClean[I]
			: Exclude<keyof TData, TProtected>;
	};
};

// final step must return a result, to be used as the return value of the entire processor.
// It cannot have a next step, and it must be the last step in the processor.
export type FinalStep<TInput, TOutput, TErrors extends ErrorRegistry> = {
	handler: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: Job["token"],
	) => Promise<TOutput>;
};

export const FINAL_STEP_NAME = "__final__step__";

export class ProcessorBuilder<
	TInitialData,
	TCurrentData,
	TLastReturnType,
	TErrors extends ErrorRegistry = Record<PropertyKey, string>,
	TProtected extends PropertyKey = never,
> {
	constructor(
		private readonly _steps: Record<string, StepConfig>,
		private readonly _stepOrder: string[],
		private readonly _errors: TErrors,
		private readonly _protectedKeys: readonly string[] = [],
		private readonly _debug: boolean = false,
	) {}

	static init<TInitialData>() {
		return new ProcessorBuilder<
			TInitialData,
			TInitialData,
			TInitialData,
			Record<PropertyKey, string>
		>({}, [], {});
	}

	errors<const E extends ErrorRegistry>(
		registry: E,
	): ProcessorBuilder<
		TInitialData,
		TCurrentData,
		TLastReturnType,
		E,
		TProtected
	> {
		return new ProcessorBuilder(
			this._steps,
			this._stepOrder,
			registry,
			this._protectedKeys,
			this._debug,
		);
	}

	/**
	 * Opt in to step lifecycle logging (start/finish/rollback/etc.) via
	 * `job.log`. Off by default. Pass `false` to explicitly disable it again.
	 */
	debug(
		enabled = true,
	): ProcessorBuilder<
		TInitialData,
		TCurrentData,
		TLastReturnType,
		TErrors,
		TProtected
	> {
		return new ProcessorBuilder(
			this._steps,
			this._stepOrder,
			this._errors,
			this._protectedKeys,
			enabled,
		);
	}

	private _push(config: StepConfig) {
		const clonedSteps = { ...this._steps };

		const stepName = config.name;
		if (clonedSteps[stepName]) {
			throw new AddStepError(`Duplicate step name: ${stepName}`);
		}

		// Keys reserved by rollbackKeys (this step's or an earlier step's) must
		// survive until a potential rollback, so they can never be cleaned.
		const rollbackKeys = config.rollbackKeys ?? [];
		const conflicts = (config.clean ?? []).filter(
			(key) => this._protectedKeys.includes(key) || rollbackKeys.includes(key),
		);
		if (conflicts.length > 0) {
			throw new AddStepError(
				`Step "${stepName}" cannot clean key(s) ${conflicts.join(", ")}: they are reserved by rollbackKeys`,
			);
		}
		const protectedKeys = [...this._protectedKeys, ...rollbackKeys];

		const previousStep = this._stepOrder[this._stepOrder.length - 1];

		if (!previousStep) {
			if (this._stepOrder.length > 0) {
				throw new AddStepError(
					"Invalid step order: Has entries but no previous step",
				);
			}
			clonedSteps[stepName] = config;
			return new ProcessorBuilder(
				clonedSteps,
				[stepName],
				this._errors,
				protectedKeys,
				this._debug,
			);
		}
		const previousStepConfig = clonedSteps[previousStep];
		if (!previousStepConfig) {
			throw new AddStepError(
				`Invalid step order: Previous step ${previousStep} not found in steps`,
			);
		}
		if (previousStepConfig.type === "final") {
			throw new AddStepError(
				`Invalid step order: Previous step ${previousStep} is a final step and cannot have a next step`,
			);
		}
		clonedSteps[previousStep] = {
			...previousStepConfig,
			nextStep: config.name,
			hasNextStep: true,
		};

		clonedSteps[stepName] = config;
		return new ProcessorBuilder(
			clonedSteps,
			[...this._stepOrder, stepName],
			this._errors,
			protectedKeys,
			this._debug,
		);
	}

	// [child step]
	// Has a `childQueue`
	// If you know there will only be one child job
	// You may mark it as single child, this will tell the processor to return a single result
	// Instead of a results array
	// You may also define a processChildJob property
	// This function will take the results from the child job and process them before proceeding to the next step
	addStep<
		TName extends string,
		TStepOutput,
		TChildQueue extends AnyChildQueue,
		TSingle extends boolean = false,
		TChildResult = ChildResults<TChildQueue, TSingle>,
		const TRollbackKeys extends readonly PropertyKey[] = readonly [],
		const TClean extends readonly PropertyKey[] = readonly [],
	>(
		stepConfig: ChildStep<
			TName,
			TLastReturnType,
			TStepOutput,
			TChildQueue,
			TSingle,
			TChildResult,
			TRollbackKeys,
			TErrors
		> &
			CleanField<TLastReturnType, TProtected, TClean>,
	): ProcessorBuilder<
		TInitialData,
		TCurrentData,
		Cleaned<
			ChildStepData<TName, TLastReturnType, TStepOutput, TChildResult>,
			TClean[number]
		>,
		TErrors,
		TProtected | TRollbackKeys[number]
	>;

	// [plain step]
	// This step does not contain a child queue.
	// Its return type is added to the job data and is visible in all future steps
	addStep<
		TStepOutput,
		const TRollbackKeys extends readonly PropertyKey[] = readonly [],
		const TClean extends readonly PropertyKey[] = readonly [],
	>(
		stepConfig: PlainStep<
			TLastReturnType,
			TStepOutput,
			TRollbackKeys,
			TErrors
		> &
			CleanField<TLastReturnType, TProtected, TClean>,
	): ProcessorBuilder<
		TInitialData,
		TCurrentData,
		Cleaned<MergeOutput<TLastReturnType, TStepOutput>, TClean[number]>,
		TErrors,
		TProtected | TRollbackKeys[number]
	>;

	// biome-ignore lint/suspicious/noExplicitAny: <Overloaded add step will never it any condition>
	addStep(stepConfig: any): any {
		return this._push({
			hasChildren: !!stepConfig.childQueue,
			...stepConfig,
			type: "regular" as const,
		});
	}

	addFinalStep<TStepOutput>(
		stepConfig: FinalStep<TLastReturnType, TStepOutput, TErrors>,
	): ProcessorBuilder<TInitialData, TCurrentData, TStepOutput, TErrors>;

	addFinalStep(stepConfig: Pick<StepConfig, "handler">): unknown {
		if (this._steps[FINAL_STEP_NAME]) {
			throw new AddStepError(
				"Step name __final__step__ is reserved for the final step and cannot be used as a regular step name",
			);
		}
		const baseConfig = {
			name: FINAL_STEP_NAME,
			type: "final" as const,
			hasChildren: false,
			hasNextStep: false,
			handler: stepConfig.handler,
		};
		return this._push(baseConfig);
	}

	build() {
		const steps = { ...this._steps };
		let stepOrder = [...this._stepOrder];

		const lastStepName = stepOrder[stepOrder.length - 1];
		if (!lastStepName) {
			throw new AddStepError("Cannot build processor with no steps");
		}

		// If no explicit final step was declared, insert one automatically.
		// Its result is the accumulated job data (minus internal bookkeeping),
		// matching TLastReturnType.
		if (lastStepName !== FINAL_STEP_NAME) {
			const lastStep = steps[lastStepName];
			if (!lastStep) {
				throw new AddStepError(`Step "${lastStepName}" not found`);
			}
			steps[lastStepName] = {
				...lastStep,
				nextStep: FINAL_STEP_NAME,
				hasNextStep: true,
			};
			steps[FINAL_STEP_NAME] = {
				name: FINAL_STEP_NAME,
				type: "final",
				hasChildren: false,
				hasNextStep: false,
				handler: async (job: { data: Record<string, unknown> }) => {
					const clone: Record<string, unknown> = { ...job.data };
					for (const key of INTERNAL_KEYS) {
						delete clone[key];
					}
					return clone;
				},
			};
			stepOrder = [...stepOrder, FINAL_STEP_NAME];
		}

		return StepProcessor<TInitialData, TLastReturnType>({
			steps,
			stepOrder,
			errors: this._errors,
			debug: this._debug,
		});
	}
}
