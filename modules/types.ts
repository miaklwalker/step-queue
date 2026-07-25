import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { Job } from "bullmq";
import type { QueueSystem } from "./queueSystem.ts";

/** Flattens an intersection into a single, readable object type. */
export type Prettify<T> = { [K in keyof T]: T[K] } & {};

/** The shape of job data flowing through a processor. */
export type JSONLike = Record<string, unknown>;

/** A user-defined registry mapping error codes to messages. */
export type ErrorRegistry = Record<string, string>;

export type ProgressStatus = "start" | "finish" | "skip" | "child-finish";

/**
 * Remove `TKeys` from `TData` and flatten the result.
 * Used by the `clean` step feature to drop keys from the job data type
 * once a step declares it no longer needs them.
 */
export type Cleaned<TData, TKeys extends PropertyKey> = Prettify<
	Omit<TData, TKeys>
>;

/**
 * Internal bookkeeping keys the processor writes onto the job data.
 * They are stripped from the result of an implicit final step.
 */
export const INTERNAL_KEYS = [
	"step",
	"previousStep",
	"consumedKeys",
	"completedSteps",
] as const;

export type StepConfig = {
	name: string;
	type: "regular" | "final";
	nextStep?: string | null;
	hasNextStep: boolean;
	hasChildren: boolean;
	/** Keys to delete from the job data once this step finishes. */
	clean?: readonly string[];
	/**
	 * The only keys of the job data exposed to this step's rollback. Reserved:
	 * neither this step nor any later step may list them in `clean`.
	 */
	rollbackKeys?: readonly string[];
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	rollback?: (job: any, token: Job["token"]) => Promise<void>;
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	handler: (job: any, token: Job["token"]) => Promise<any>;
	progressReport?: (
		// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
		job: any,
		token: Job["token"],
		stepName: string,
		status: ProgressStatus,
	) => Promise<void>;
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	childQueue?: QueueSystem<any, any, any>;
	singleChild?: boolean;
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	processChildResult?: (results: any) => any;
};

/** Thrown when job data fails validation against the queue's input schema. */
export class SchemaValidationError extends Error {
	constructor(public readonly issues: ReadonlyArray<StandardSchemaV1.Issue>) {
		super(
			issues
				.map((issue) => {
					const path = issue.path
						?.map((segment) =>
							String(
								typeof segment === "object" && segment !== null
									? segment.key
									: segment,
							),
						)
						.join(".");
					return path ? `${path}: ${issue.message}` : issue.message;
				})
				.join("; "),
		);
		this.name = "SchemaValidationError";
	}
}

export class AddStepError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "AddStepError";
	}
}
export class ProcessorStepError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ProcessorStepError";
	}
}

export class SkipStepError extends Error {
	constructor(
		public readonly targetStep: string | null = null,
		// biome-ignore lint/suspicious/noExplicitAny: <todo: fix this one>
		public readonly result: Record<string, any> = {},
	) {
		super(`SkipStep:${targetStep ?? "final"}`);
		this.name = "SkipStepError";
	}
}
export class SkipToFinalError extends Error {
	constructor(
		public readonly targetStep: string | null = null,
		// biome-ignore lint/suspicious/noExplicitAny: <todo: fix this one>
		public readonly result: Record<string, any> = {},
	) {
		super(`SkipStep:${targetStep ?? "final"}`);
		this.name = "SkipStepError";
	}
}

export class EarlyExitError extends Error {
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	constructor(public readonly returnValue: any = {}) {
		super("EarlyExit");
		this.name = "EarlyExitError";
	}
}

export const actionErrors = {
	skip: SkipStepError,
	earlyExit: EarlyExitError,
	skipToFinal: SkipToFinalError,
};

export type TypedJob<
	TData,
	TErrors extends ErrorRegistry = Record<string, string>,
> = Omit<Job, "data"> & {
	readonly data: TData;
	readonly errors: { readonly [K in keyof TErrors]: ProcessorStepError };
	readonly actionErrors: {
		readonly [K in keyof typeof actionErrors]: (typeof actionErrors)[K];
	};
	updateStepProgress: (data: Record<string, unknown>) => Promise<void>;
};
