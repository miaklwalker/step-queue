import { type Job, WaitingChildrenError } from "bullmq";
import { FINAL_STEP_NAME } from "./processorBuilder.ts";
import {
	actionErrors,
	DEFAULT_RECOVER_MAX_ATTEMPTS,
	EarlyExitError,
	type ErrorRegistry,
	ProcessorStepError,
	RecoverAttemptsExceededError,
	RecoverWithChildError,
	SkipStepError,
	SkipToFinalError,
	type StepConfig,
	type TypedJob,
} from "./types.ts";

export interface ProcessorConfig {
	steps: Record<string, StepConfig>;
	stepOrder: string[];
	errors: ErrorRegistry;
	debug?: boolean;
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
	const proxyTarget = job as Job<TInput> & Record<string | symbol, unknown>;
	return new Proxy(proxyTarget, {
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
	}) as unknown as TypedJob<TInput, TErrors>;
}

const MAX_STEP_COUNT = 100;
const WAIT_CHILDREN = "__WAIT_CHILDREN__";
const WAIT_RECOVER_CHILDREN = "__WAIT_RECOVER_CHILDREN__";
const STEP_PROCESSOR_HEADER = () => `[StepProcessor]`;

const STEPS_PROGRESS_KEY = "steps";
const STEP_TIMINGS_KEY = "__stepTimings__";
const FINAL_STEP_DISPLAY_NAME = "final";

/** Reads `job.progress` as a plain object, or `{}` if it isn't one yet. */
function readProgressObject(job: Job): Record<string, unknown> {
	const current = job.progress;
	return current && typeof current === "object" && !Array.isArray(current)
		? (current as Record<string, unknown>)
		: {};
}

async function executeRollbacks(
	job: Job,
	jobProxy: TypedJob<unknown>,
	token: Job["token"],
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
			const rollbackTarget = jobProxy as TypedJob<unknown> &
				Record<string | symbol, unknown>;
			const rollbackProxy = new Proxy(rollbackTarget, {
				get(target, prop) {
					return prop === "data" ? picked : target[prop];
				},
			});
			await stepConfig.rollback(rollbackProxy, token);
			if (config.debug) {
				await job.log(
					`${STEP_PROCESSOR_HEADER()} Rolled back step: ${stepName}`,
				);
			}
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
		const stepLog = async (message: string) => {
			if (!processorConfig.debug) return;
			await job.log(`${STEP_PROCESSOR_HEADER()} ${message}`);
		};

		if (processorConfig.stepOrder.length > MAX_STEP_COUNT) {
			throw new ProcessorStepError(
				`Too many steps defined in processor. Max is ${MAX_STEP_COUNT}`,
			);
		}
		if (!processorConfig?.steps) {
			throw new ProcessorStepError("Invalid processor configuration");
		}
		const firstStepName = processorConfig.steps[firstStep]
			?.name as StepNames[0];
		type ResultsName = `${string}-results`;

		let jobData: {
			step?: StepNames;
			previousStep?: StepNames;
			consumedKeys?: string[];
			completedSteps?: StepNames[];
			recoverStep?: StepNames;
			recoverAttempts?: Record<string, number>;
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

		// `steps`/timings survive resumes (WAIT_CHILDREN, WAIT_RECOVER_CHILDREN)
		// because they're re-read from job.progress, which is persisted in Redis
		// across the separate processor invocations a resume triggers.
		const initialProgress = readProgressObject(job);
		const stepsState: Record<string, string> = {
			...(initialProgress[STEPS_PROGRESS_KEY] as
				| Record<string, string>
				| undefined),
		};
		const stepTimings: Record<string, number> = {
			...(initialProgress[STEP_TIMINGS_KEY] as
				| Record<string, number>
				| undefined),
		};
		for (const name of processorConfig.stepOrder) {
			if (name === FINAL_STEP_NAME || name in stepsState) continue;
			stepsState[name] = jobData.completedSteps?.includes(name as StepNames)
				? "finished"
				: "not_started";
		}
		const reportStep = async (
			name: string,
			status: string,
			progress?: number,
		) => {
			stepsState[name] = status;
			const patch: Record<string, unknown> = {
				...readProgressObject(job),
				step: name,
				steps: { ...stepsState },
				[STEP_TIMINGS_KEY]: { ...stepTimings },
			};
			if (progress !== undefined) {
				patch.progress = progress;
			}
			await job.updateProgress(patch);
		};

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
				const childrenElapsed =
					Date.now() - (stepTimings[String(previousStep)] ?? Date.now());
				await reportStep(
					String(previousStep),
					`Finished in ${childrenElapsed}ms`,
				);
				await stepConfig?.progressReport?.(
					jobProxy,
					token,
					String(previousStep),
					"finish",
				);

				// If shouldWait is false, that means all children have finished.
				let consumedKeys = jobData.consumedKeys || [];
				const childrenJobValues = await job.getChildrenValues<unknown>();
				const unconsumedKeys = Object.keys(childrenJobValues)
					.filter((key) => !consumedKeys.includes(key))
					.sort();
				if (unconsumedKeys.length > 0) {
					const childValues = unconsumedKeys.map(
						(key) => childrenJobValues[key],
					);
					consumedKeys = [...consumedKeys, ...unconsumedKeys];

					let values: unknown = stepConfig.singleChild
						? childValues[0]
						: childValues;

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
			if (currentStep === WAIT_RECOVER_CHILDREN) {
				if (!token) {
					throw new ProcessorStepError(
						"Missing token for waiting on a recovery child",
					);
				}
				const resumeStep = jobData.recoverStep;
				if (!resumeStep) {
					throw new ProcessorStepError(
						"Invalid state: recoverStep is missing while waiting for a recovery child",
					);
				}
				const shouldWait = await job.moveToWaitingChildren(token);
				if (shouldWait) {
					await stepLog(
						`StepProcessor: Waiting on recovery job for step: ${String(resumeStep)}`,
					);
					throw new WaitingChildrenError();
				}
				await stepLog(
					`StepProcessor: Recovery job(s) completed for step: ${String(resumeStep)} — retrying`,
				);

				// `getChildrenValues()` is cumulative over the whole job, not scoped
				// to this wait — mark the recovery child's key(s) consumed so they
				// can never be picked up (and merged into `<name>-results`) by a
				// later `childQueue` step's own WAIT_CHILDREN pass. The value itself
				// is discarded: a recovery child is a side effect, not a data source.
				const consumedKeys = jobData.consumedKeys ?? [];
				const childrenJobValues = await job.getChildrenValues<unknown>();
				const unconsumedKeys = Object.keys(childrenJobValues).filter(
					(key) => !consumedKeys.includes(key),
				);
				if (unconsumedKeys.length > 0) {
					jobData.consumedKeys = [...consumedKeys, ...unconsumedKeys];
				}

				currentStep = resumeStep;
				jobData.step = currentStep;
				jobData.recoverStep = undefined;
				await job.updateData(jobData as TInput);
				continue;
			}
			const stepConfig = processorConfig.steps[currentStep];
			if (!stepConfig) {
				throw new ProcessorStepError(
					`Step config not found for step ${String(currentStep)}`,
				);
			}
			// Steps report under their own name, except the (internally-named)
			// final step, which reports under the friendly key "final".
			const stepDisplayName =
				currentStep === FINAL_STEP_NAME
					? FINAL_STEP_DISPLAY_NAME
					: String(currentStep);
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
			let recovering = false;
			stepTimings[String(currentStep)] = Date.now();
			await reportStep(stepDisplayName, "running", 0);
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
					if (!token) {
						throw new Error("Missing token for next step");
					}
					if (err instanceof RecoverWithChildError) {
						const maxAttempts =
							err.options?.maxAttempts ?? DEFAULT_RECOVER_MAX_ATTEMPTS;
						const recoverAttempts = { ...(jobData.recoverAttempts ?? {}) };
						const attempts = (recoverAttempts[String(currentStep)] ?? 0) + 1;
						if (attempts > maxAttempts) {
							await executeRollbacks(job, jobProxy, token, processorConfig);
							await reportStep(stepDisplayName, "Failed");
							throw new RecoverAttemptsExceededError(
								String(currentStep),
								maxAttempts,
							);
						}
						recoverAttempts[String(currentStep)] = attempts;
						jobData.recoverAttempts = recoverAttempts;
						jobData.recoverStep = currentStep;
						jobData.step = WAIT_RECOVER_CHILDREN as StepNames;
						await job.updateData(jobData as TInput);
						await err.childQueue.addChildJob(job, err.childData);
						await reportStep(
							stepDisplayName,
							`Recovering (attempt ${attempts}/${maxAttempts})`,
						);
						const shouldWait = await job.moveToWaitingChildren(token);
						if (shouldWait) {
							await stepLog(
								`StepProcessor: Recovering step "${String(currentStep)}" via ${err.childQueue.name} (attempt ${attempts}/${maxAttempts})`,
							);
							throw new WaitingChildrenError();
						}
						recovering = true;
						return undefined;
					}
					await executeRollbacks(job, jobProxy, token, processorConfig);
					const elapsedFail =
						Date.now() - (stepTimings[String(currentStep)] ?? Date.now());
					await reportStep(stepDisplayName, `Failed after ${elapsedFail}ms`);
					throw err;
				});

			if (earlyExit) {
				return result as TResult;
			}
			if (recovering) {
				// The child already finished by the time we checked (rare, but
				// possible) — jobData.step is already WAIT_RECOVER_CHILDREN and
				// persisted; loop back through that branch so the child-value
				// bookkeeping above runs from a single place.
				currentStep = WAIT_RECOVER_CHILDREN as StepNames;
				continue;
			}
			if (result && typeof result === "object" && stepConfig.type !== "final") {
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
				const elapsedFinal =
					Date.now() - (stepTimings[String(currentStep)] ?? Date.now());
				await reportStep(stepDisplayName, `Finished in ${elapsedFinal}ms`, 100);
				return result as TResult;
			}

			if (skipOccurred) {
				const elapsedSkip =
					Date.now() - (stepTimings[String(currentStep)] ?? Date.now());
				await reportStep(stepDisplayName, `Skipped after ${elapsedSkip}ms`);
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
			const elapsedFinish =
				Date.now() - (stepTimings[String(currentStep)] ?? Date.now());
			await reportStep(stepDisplayName, `Finished in ${elapsedFinish}ms`);
			await stepConfig?.progressReport?.(
				jobProxy,
				token,
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
	};
}
