import type { QueueOutput, QueueSystem } from "./queueSystem.ts";
import {
	AddStepError,
	type Cleaned,
	type ErrorRegistry,
	INTERNAL_KEYS,
	type JSONLike,
	type Prettify,
	type ProgressStatus,
	type StepConfig,
	type TypedJob,
} from "./types.ts";
import { StepProcessor } from "./stepProcessor.ts";

export type RegularBase<TInput, TErrors extends ErrorRegistry> = {
	name: string;
	hasChildren?: boolean;
	progressReport?: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: string,
		stepName: string,
		status: ProgressStatus,
	) => void | Promise<void>;
};

/**
 * A handler either adds data (returns an object) or adds nothing (returns
 * void). Rather than one overload per case, `TStepOutput` is inferred from the
 * handler and merged conditionally: void-ish returns leave the data unchanged.
 */
type MergeOutput<TInput, TOutput> = [TOutput] extends [void | undefined]
	? TInput
	: TInput & TOutput;

/** Any child queue, regardless of its own input/output/name types. */
// biome-ignore lint/suspicious/noExplicitAny: matching a queue structurally; its generics are captured by TChildQueue itself
export type AnyChildQueue = QueueSystem<any, any, any>;

/**
 * What `processChildResult` receives — and what `<name>-results` holds when no
 * `processChildResult` is given: one child's output when `singleChild: true`,
 * otherwise the array of all child outputs.
 */
type ChildResults<
	TChildQueue,
	TSingle extends boolean,
> = TSingle extends true ? QueueOutput<TChildQueue> : QueueOutput<TChildQueue>[];

/** Job data after a child step: handler output merged in, plus `<name>-results`. */
type ChildStepData<
	TName extends string,
	TInput,
	TStepOutput,
	TChildResult,
> = MergeOutput<TInput, TStepOutput> & {
	[K in `${TName}-results`]: TChildResult;
};

export type PlainStep<TInput, TStepOutput, TErrors extends ErrorRegistry> = {
	handler: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: string,
	) => TStepOutput | Promise<TStepOutput>;
	rollback?: (
		job: TypedJob<Prettify<MergeOutput<TInput, TStepOutput>>, TErrors>,
		token: string,
	) => void | Promise<void>;
} & RegularBase<Prettify<TInput>, TErrors>;

export type ChildStep<
	TName extends string,
	TInput,
	TStepOutput,
	TChildQueue extends AnyChildQueue,
	TSingle extends boolean,
	TChildResult,
	TErrors extends ErrorRegistry,
> = {
	name: TName;
	handler: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: string,
	) => TStepOutput | Promise<TStepOutput>;
	childQueue: TChildQueue;
	hasChildren?: true;
	/** `true` = exactly one child job; its output is stored unwrapped. */
	singleChild?: TSingle;
	/** Map the child output(s) before storing them under `<name>-results`. */
	processChildResult?: (
		results: ChildResults<TChildQueue, TSingle>,
	) => TChildResult | Promise<TChildResult>;
	rollback?: (
		job: TypedJob<
			Prettify<ChildStepData<TName, TInput, TStepOutput, TChildResult>>,
			TErrors
		>,
		token: string,
	) => void | Promise<void>;
	progressReport?: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: string,
		stepName: string,
		status: ProgressStatus,
	) => void | Promise<void>;
};

/**
 * Optional `clean` field: type-safe keys to drop from the job data once the
 * step finishes. Keys are validated against the step's INPUT data — the same
 * type `job.data` has inside the handler. That type is fully resolved before
 * `addStep` is called, so `clean` never interacts with handler inference.
 * `TClean` captures the literal tuple (for the return-type Omit); the value
 * type validates element-wise — a valid key checks against itself, an invalid
 * one is expected to be `keyof TData` — so only the offending string is
 * underlined, and the editor autocompletes the current keys.
 */
type CleanField<TData, TClean extends readonly PropertyKey[]> = {
	clean?: {
		[I in keyof TClean]: TClean[I] extends keyof TData
			? TClean[I]
			: keyof TData;
	};
};

// final step must return a result, to be used as the return value of the entire processor.
// It cannot have a next step, and it must be the last step in the processor.
export type FinalStep<TInput, TOutput, TErrors extends ErrorRegistry> = {
	handler: (
		job: TypedJob<Prettify<TInput>, TErrors>,
		token: string,
	) => TOutput | Promise<TOutput>;
};

export const FINAL_STEP_NAME = "__final__step__";

export class ProcessorBuilder<
	TInitialData,
	TCurrentData,
	TLastReturnType,
	TErrors extends ErrorRegistry = Record<PropertyKey, string>,
> {
	constructor(
		private readonly _steps: Record<string, StepConfig>,
		private readonly _stepOrder: string[],
		private readonly _errors: TErrors,
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
	): ProcessorBuilder<TInitialData, TCurrentData, TLastReturnType, E> {
		return new ProcessorBuilder(this._steps, this._stepOrder, registry);
	}

	private _push(config: StepConfig) {
		const clonedSteps = { ...this._steps };

		const stepName = config.name;
		if (clonedSteps[stepName]) {
			throw new AddStepError(`Duplicate step name: ${stepName}`);
		}

		const previousStep = this._stepOrder[this._stepOrder.length - 1];

		if (!previousStep) {
			if (this._stepOrder.length > 0) {
				throw new AddStepError(
					"Invalid step order: Has entries but no previous step",
				);
			}
			clonedSteps[stepName] = config;
			return new ProcessorBuilder(clonedSteps, [stepName], this._errors);
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
		);
	}


	// [child step]
	// Has a `childQueue`. Every sub-variant is folded into this one signature
	// instead of eight overloads:
	//  - adds data vs adds nothing → TStepOutput inferred from the handler;
	//    MergeOutput skips the merge when it returns void
	//  - single vs many children   → TSingle inferred from `singleChild`
	//    (defaults to false); ChildResults picks output vs output[]
	//  - processChildResult or not → TChildResult is inferred from the callback
	//    when present, otherwise defaults to the raw child result(s)
	//  - `clean` keys              → TClean, optional on every variant
	addStep<
		TName extends string,
		TStepOutput,
		TChildQueue extends AnyChildQueue,
		TSingle extends boolean = false,
		TChildResult = ChildResults<TChildQueue, TSingle>,
		const TClean extends readonly PropertyKey[] = readonly [],
	>(
		stepConfig: ChildStep<
			TName,
			TLastReturnType,
			TStepOutput,
			TChildQueue,
			TSingle,
			TChildResult,
			TErrors
		> &
			CleanField<TLastReturnType, TClean>,
	): ProcessorBuilder<
		TInitialData,
		TCurrentData,
		Cleaned<
			ChildStepData<TName, TLastReturnType, TStepOutput, TChildResult>,
			TClean[number]
		>,
		TErrors
	>;

	// [plain step]
	// No child queue. The handler's return value (if any) is merged into the job
	// data; `clean` drops keys afterwards.
	addStep<
		TStepOutput,
		const TClean extends readonly PropertyKey[] = readonly [],
	>(
		stepConfig: PlainStep<TLastReturnType, TStepOutput, TErrors> &
			CleanField<TLastReturnType, TClean>,
	): ProcessorBuilder<
		TInitialData,
		TCurrentData,
		Cleaned<MergeOutput<TLastReturnType, TStepOutput>, TClean[number]>,
		TErrors
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

	addFinalStep(stepConfig: any): any {
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
		});
	}
}
