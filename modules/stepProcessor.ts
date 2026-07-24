import { type Job, WaitingChildrenError } from "bullmq";
import {
	type ErrorRegistry,
	type StepConfig,
	type TypedJob,
	ProcessorStepError,
	SkipStepError,
	EarlyExitError,
	RetryStepError,
	actionErrors,
	SkipToFinalError,
	INTERNAL_KEYS,
} from "./types.ts";
import { FINAL_STEP_NAME } from "./processorBuilder.ts";

export interface ProcessorConfig {
	steps: Record<string, StepConfig>;
	stepOrder: string[];
	errors: ErrorRegistry;
}

export function createJobProxy<TInput, TErrors extends ErrorRegistry>(
	job: Job<TInput>,
	errors: TErrors,
): TypedJob<TInput, TErrors> {
	const errorsProxy = new Proxy(errors, {
		get(target, prop: string) {
			const code = target[prop];
			if (!code) {
				throw new Error(
					`Error code ${prop} not found in error registry. Did you remember to register it with .errors()`,
				);
			}
			return new ProcessorStepError(code);
		},
	});
	const actionErrorsProxy = new Proxy(actionErrors, {
		get(target, prop: string) {
			const ErrorClass = target[prop as keyof typeof actionErrors];
			if (!ErrorClass) {
				throw new Error(
					`Action error ${prop} not found. Available action errors are: ${Object.keys(actionErrors).join(", ")}`,
				);
			}
			return ErrorClass;
		},
	});
	return new Proxy(job as any, {
		get(target, prop) {
			if (prop === "errors") {
				return errorsProxy;
			}
			if (prop === "actionErrors") {
				return actionErrorsProxy;
			}
			if (prop === "updateStepProgress") {
				return async (customData: Record<string, unknown>) => {
					const currentProgress =
						(target.progress as Record<string, unknown>) ?? {};
					return target.updateProgress({ ...currentProgress, ...customData });
				};
			}
			const val = target[prop];
			return typeof val === "function" ? val.bind(target) : val;
		},
	}) as TypedJob<TInput, TErrors>;
}

/**
 * Strip the processor's internal bookkeeping keys from job data.
 * Used to shape the result of an implicit (auto-inserted) final step.
 */
export function stripInternalKeys<T extends Record<string, unknown>>(
	data: T,
): Record<string, unknown> {
	const clone: Record<string, unknown> = { ...data };
	for (const key of INTERNAL_KEYS) {
		delete clone[key];
	}
	return clone;
}

const MAX_STEP_COUNT = 100;
const WAIT_CHILDREN = "__WAIT_CHILDREN__";
const STEP_PROCESSOR_HEADER = () => `[StepProcessor]`;

async function executeRollbacks(
	job: Job,
	jobProxy: any,
	token: string,
	config: ProcessorConfig,
): Promise<void> {
	const completedSteps = job.data?.completedSteps ?? [];
	for (let i = completedSteps.length - 1; i >= 0; i--) {
		const stepName = completedSteps[i];
		const stepConfig = config.steps[stepName];
		if (stepConfig?.rollback) {
			// A rollback only sees the keys it requested via rollbackKeys —
			// with none requested, its data is empty. Matches the static type.
			const data = (job.data ?? {}) as Record<string, unknown>;
			const picked: Record<string, unknown> = {};
			for (const key of stepConfig.rollbackKeys ?? []) {
				if (key in data) {
					picked[key] = data[key];
				}
			}
			const rollbackProxy = new Proxy(jobProxy, {
				get(target, prop) {
					return prop === "data" ? picked : target[prop];
				},
			});
			await stepConfig.rollback(rollbackProxy, token);
			await job.log(`${STEP_PROCESSOR_HEADER()} Rolled back step: ${stepName}`);
		}
	}
}

export function StepProcessor<TInput, TResult>(
	processorConfig: ProcessorConfig,
) {
	return async (
		job: Job<TInput, TResult>,
		token: string | undefined,
	): Promise<TResult> => {
		const jobProxy = createJobProxy(job, processorConfig.errors);
		type StepNames = keyof typeof processorConfig.steps;
		const firstStep = processorConfig.stepOrder[0] as StepNames | undefined;
		if (!firstStep) {
			throw new ProcessorStepError("No steps defined in processor");
		}
		const stepLog = async (message: string) =>
			await job.log(`${STEP_PROCESSOR_HEADER()} ${message}`);

		if (processorConfig.stepOrder.length > MAX_STEP_COUNT) {
			throw new ProcessorStepError(
				`Too many steps defined in processor. Max is ${MAX_STEP_COUNT}`,
			);
		}
		if (!processorConfig || !processorConfig.steps) {
			throw new ProcessorStepError("Invalid processor configuration");
		}
		const firstStepName = processorConfig.steps[firstStep]!
			.name as StepNames[0];
		type ResultsName = `${string}-results`;

		let jobData: {
			step?: StepNames;
			previousStep?: StepNames;
			consumedKeys?: string[];
			completedSteps?: StepNames[];
			[key: ResultsName]: unknown;
		} = {
			step: undefined as StepNames | undefined,
			previousStep: undefined as StepNames | undefined,
			consumedKeys: [] as string[],
			completedSteps: [] as StepNames[],
			...job.data,
		};

		let currentStep = (jobData.step ?? firstStepName) as StepNames;
		await stepLog(`Starting Step: ${String(currentStep)}`);
		let previousStep: StepNames | null = jobData?.previousStep ?? null;

		const stepRetryCount: Record<keyof typeof processorConfig.steps, number> =
			{};

		try {
			while (currentStep) {
				if (currentStep === WAIT_CHILDREN) {
					if (!token) {
						throw new ProcessorStepError("Missing token for waiting children");
					}

					// WAIT_CHILDREN can never be the first step, so previousStep must be defined here.
					if (!previousStep) {
						throw new ProcessorStepError(
							"Invalid state: previousStep is null while waiting for children",
						);
					}
					const stepConfig = processorConfig.steps[previousStep];
					if (!stepConfig) {
						throw new ProcessorStepError(
							`Step config not found for previous step ${String(previousStep)} while waiting for children`,
						);
					}
					const shouldWait = await job.moveToWaitingChildren(token);
					if (shouldWait) {
						await stepLog(`StepProcessor: Child Job Completed`);
						await stepConfig?.progressReport?.(
							jobProxy,
							token,
							String(previousStep),
							"child-finish",
						);
						throw new WaitingChildrenError();
					}
					await stepLog(`StepProcessor: All Children Completed`);
					await stepConfig?.progressReport?.(
						jobProxy,
						token,
						String(previousStep),
						"finish",
					);

					// If shouldWait is false, that means all children have finished.
					let consumedKeys = jobData.consumedKeys || [];
					const childrenJobValues = await job.getChildrenValues();
					const unconsumedKeys = Object.keys(childrenJobValues)
						.filter((key) => !consumedKeys.includes(key))
						.sort();
					if (unconsumedKeys.length > 0) {
						let values: any = unconsumedKeys.map(
							(key) => childrenJobValues[key],
						);
						consumedKeys = [...consumedKeys, ...unconsumedKeys];

						if (stepConfig.singleChild) {
							values = values[0];
						}

						if (stepConfig.processChildResult) {
							values = await stepConfig.processChildResult(values);
						}

						const resultsKey = `${String(previousStep)}-results` as ResultsName;
						jobData.consumedKeys = consumedKeys;
						jobData[resultsKey] = values;
						await job.updateData({
							...jobData,
							[resultsKey]: values,
						} as TInput);
					}

					// Jobs With Children can never be the final step so there must be a next step
					const next = stepConfig.nextStep;
					if (!next) {
						throw new ProcessorStepError(
							`Invalid state: No next step found after waiting for children in step ${String(previousStep)}`,
						);
					}
					currentStep = next;
					jobData.step = currentStep;
					await job.updateData(jobData as TInput);
					continue;
				}
				const stepConfig = processorConfig.steps[currentStep];
				if (!stepConfig) {
					throw new ProcessorStepError(
						`Step config not found for step ${String(currentStep)}`,
					);
				}
				await stepLog(
					`Starting Step: ${currentStep === FINAL_STEP_NAME ? "Final Step" : String(currentStep)}`,
				);
				await stepConfig?.progressReport?.(
					jobProxy,
					token ?? "",
					String(currentStep),
					"start",
				);

				let skipOccurred = false;
				let skipTarget: StepNames | null = null;
				let earlyExit = false;
				let retrying = false;
				await job.updateProgress({ step: stepConfig.name, progress: 0 });
				const result = await stepConfig
					.handler(jobProxy, token ?? "")
					.catch(async (err: unknown) => {
						if (err instanceof SkipStepError) {
							skipOccurred = true;
							skipTarget = err.targetStep as StepNames | null;
							return err.result ?? {};
						}
						if (err instanceof SkipToFinalError) {
							skipOccurred = true;
							skipTarget = FINAL_STEP_NAME as StepNames;
							return err.result ?? {};
						}
						if (err instanceof EarlyExitError) {
							earlyExit = true;
							return err.returnValue;
						}
						if (err instanceof RetryStepError) {
							stepRetryCount[currentStep] =
								(stepRetryCount[currentStep] ?? 0) + 1;
							if (stepRetryCount[currentStep]! > err.maxRetries) {
								await executeRollbacks(job, jobProxy, token!, processorConfig);
								throw new Error(
									`Max retries (${err.maxRetries}) reached for "${String(currentStep)}" on job ${job.id}`,
								);
							}
							retrying = true;
							currentStep = err.step as StepNames;
							jobData.step = currentStep;
							await job.updateData({
								...jobData,
								...(err.currentData ?? {}),
							});
							return undefined;
						}
						await executeRollbacks(job, jobProxy, token!, processorConfig);
						throw err;
					});

				if (earlyExit) {
					return result as TResult;
				}
				if (retrying) {
					continue;
				}
				if (
					result &&
					typeof result === "object" &&
					stepConfig.type !== "final"
				) {
					jobData = {
						...jobData,
						...result,
					};
					await job.updateData(jobData as TInput);
				}

				const completedSteps = jobData.completedSteps || [];
				if (!completedSteps.includes(currentStep)) {
					completedSteps.push(currentStep);
					jobData.completedSteps = completedSteps;
					await job.updateData(jobData as TInput);
				}

				// `clean`: drop keys the step declared it no longer needs, so later
				// steps neither see them at runtime nor in their static type.
				if (
					stepConfig.type !== "final" &&
					stepConfig.clean &&
					stepConfig.clean.length > 0
				) {
					for (const key of stepConfig.clean) {
						delete (jobData as Record<string, unknown>)[key];
					}
					await job.updateData(jobData as TInput);
					await stepLog(
						`Cleaned keys after step ${String(currentStep)}: ${stepConfig.clean.join(", ")}`,
					);
				}

				await stepLog(
					`${skipOccurred ? `Finished Step: ${String(currentStep)} (Skipped → ${String(skipTarget)})` : `Finished Step: ${String(currentStep)}`}`,
				);
				if (stepConfig.type === "final") {
					await stepLog(
						`Finished Final Step: ${String(currentStep)}. Returning result.`,
					);
					await job.updateProgress({ step: stepConfig.name, progress: 100 });
					return result as TResult;
				}

				if (skipOccurred) {
					const nextStep = skipTarget ?? stepConfig.nextStep;
					if (!nextStep) {
						await stepLog(
							`No next step found after skip in step ${String(currentStep)}. Ending processor.`,
						);
						return result as TResult;
					}

					currentStep = nextStep as StepNames;
					jobData.step = currentStep;
					await job.updateData(jobData as TInput);
					continue;
				}

				// Handle has children
				const hasChildren = stepConfig.hasChildren;
				if (hasChildren) {
					previousStep = currentStep;
					currentStep = WAIT_CHILDREN as StepNames;
					jobData.step = currentStep;
					jobData.previousStep = previousStep;
					await job.updateData(jobData as TInput);
					continue;
				}
				await stepConfig?.progressReport?.(
					jobProxy,
					token!,
					String(currentStep),
					skipOccurred ? "skip" : "finish",
				);

				const next = stepConfig.nextStep;
				if (!next) {
					await stepLog(
						`No next step found after step ${String(currentStep)}. Ending processor.`,
					);
					throw new ProcessorStepError(
						`No next step found after step ${String(currentStep)}. Did you forget to set nextStep or mark it as final?`,
					);
				}

				currentStep = next as StepNames;
				jobData.step = currentStep;
				await job.updateData(jobData as TInput);
			}
			throw new ProcessorStepError(
				"Processor finished without hitting a final step. Please ensure the last step is marked as final or has a next step.",
			);
		} catch (err: any) {
			throw err;
		}
	};
}
