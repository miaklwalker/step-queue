import {
	type ConnectionOptions,
	type Job,
	type JobsOptions,
	MetricsTime,
	type Processor,
	Queue,
	QueueEvents,
	Worker,
	type WorkerListener,
} from "bullmq";
import { type ZodType, z } from "zod";
import { DEFAULT_JOB_OPTIONS } from "./const.ts";
import { ProcessorBuilder } from "./processorBuilder.ts";
import type { ErrorRegistry, JSONLike, TypedJob } from "./types.ts";

/**
 * A fully type-safe, step-based wrapper around a BullMQ queue + worker.
 *
 * ```ts
 * const myInput = z.object({ greeting: z.string() });
 *
 * const myQueue = new QueueSystem(NameFactory({ name: "MyCoolQueue", env: "dev" }))
 *   .defineInputSchema(myInput)
 *   .defineConnection({ host: "localhost", port: 6379 })
 *   .defineProcessor((processor) =>
 *     processor
 *       .addStep({
 *         name: "greet",
 *         handler: async (job) => ({ shout: job.data.greeting.toUpperCase() }),
 *       })
 *       .addFinalStep({ handler: async (job) => ({ done: job.data.shout }) }),
 *   );
 * ```
 *
 * `defineProcessor` receives a `ProcessorBuilder` already typed with the input
 * schema — no `.init<T>()`. If you omit
 * `addFinalStep`, one is inserted for you that returns the accumulated job data.
 */
export class QueueSystem<
	TInput extends JSONLike = JSONLike,
	TOutput extends JSONLike = JSONLike,
	TName extends string = string,
> {
	readonly name: TName;
	readonly displayName: string;
	system: string | undefined;
	description: string | undefined;
	jsonSchema: Record<string, unknown> | undefined;

	queue: Queue<TInput, TOutput, TName, TInput, TOutput, TName> | undefined;
	worker: Worker<TInput, TOutput, TName> | undefined;

	private _schema: ZodType | undefined;
	private _connection: ConnectionOptions | undefined;
	private _concurrency = 1;
	private _queueEvents: QueueEvents | undefined;

	constructor(nameConfig: { name: TName; displayName: string }) {
		this.name = nameConfig.name;
		this.displayName = nameConfig.displayName;
		this.description = undefined;
		this.system = undefined;
		this.jsonSchema = undefined;
	}

	/**
	 * Provide a Zod schema describing the job input. The processor's initial
	 * data type is inferred from it, and jobs are validated against it at runtime.
	 */
	defineInputSchema<TSchema extends JSONLike>(
		schema: ZodType<TSchema>,
	): QueueSystem<TSchema, TOutput, TName> {
		this._schema = schema;
		this.jsonSchema = z.toJSONSchema(schema) as Record<string, unknown>;
		return this as unknown as QueueSystem<TSchema, TOutput, TName>;
	}

	/** Provide the Redis/BullMQ connection this queue + worker should use. */
	defineConnection(connection: ConnectionOptions): this {
		this._connection = connection;
		return this;
	}

	/** Set the worker concurrency (defaults to 1). */
	defineConcurrency(concurrency: number): this {
		this._concurrency = concurrency;
		return this;
	}

	addDescription(description: string): this {
		this.description = description;
		return this;
	}

	addToSystem(system: string): this {
		this.system = system;
		return this;
	}

	/**
	 * Define the processing steps and start the worker. Must be called after
	 * `defineConnection`. Returns the runnable queue, typed with the output of
	 * the final step.
	 *
	 * Accepts either a callback that receives a `ProcessorBuilder` already typed
	 * with the input schema, or a `ProcessorBuilder` you constructed yourself via
	 * `ProcessorBuilder.init<TInput>()`. Do not call `.build()` — `defineProcessor`
	 * builds the processor for you in both forms.
	 */
	defineProcessor<TNewOutput extends JSONLike>(
		build: (
			builder: ProcessorBuilder<TInput, TInput, TInput>,
		) => ProcessorBuilder<TInput, TInput, TNewOutput, ErrorRegistry>,
	): QueueSystem<TInput, TNewOutput, TName>;

	defineProcessor<TNewInput extends TInput, TNewOutput extends JSONLike>(
		builder: ProcessorBuilder<TNewInput, TNewInput, TNewOutput, ErrorRegistry>,
	): QueueSystem<TNewInput, TNewOutput, TName>;

	defineProcessor<TNewOutput extends JSONLike>(
		builderOrBuild:
			| ProcessorBuilder<TInput, TInput, TNewOutput, ErrorRegistry>
			| ((
					builder: ProcessorBuilder<TInput, TInput, TInput>,
			  ) => ProcessorBuilder<TInput, TInput, TNewOutput, ErrorRegistry>),
	): QueueSystem<TInput, TNewOutput, TName> {
		if (!this._connection) {
			throw new Error(
				"defineConnection(...) must be called before defineProcessor(...)",
			);
		}
		const builder =
			builderOrBuild instanceof ProcessorBuilder
				? builderOrBuild
				: builderOrBuild(ProcessorBuilder.init<TInput>());
		const processor = builder.build();

		this.queue = new Queue<TInput, TOutput, TName, TInput, TOutput, TName>(
			this.name,
			{ connection: this._connection },
		);
		this.worker = new Worker<TInput, TOutput, TName>(
			this.name,
			processor as unknown as Processor<TInput, TOutput, TName>,
			{
				connection: this._connection,
				concurrency: this._concurrency,
				metrics: { maxDataPoints: MetricsTime.ONE_WEEK * 2 },
			},
		);

		return this as unknown as QueueSystem<TInput, TNewOutput, TName>;
	}

	private _requireQueue(): Queue<
		TInput,
		TOutput,
		TName,
		TInput,
		TOutput,
		TName
	> {
		if (!this.queue) {
			throw new Error(
				"Queue is not initialized. Call defineConnection(...).defineProcessor(...) first.",
			);
		}
		return this.queue;
	}

	private _validate(jobData: TInput): TInput {
		return this._schema ? (this._schema.parse(jobData) as TInput) : jobData;
	}

	async addJob(jobData: TInput, options?: JobsOptions) {
		const queue = this._requireQueue();
		return await queue.add(this.name, this._validate(jobData), {
			...DEFAULT_JOB_OPTIONS,
			...options,
		});
	}

	async addBulkJobs(jobs: Array<{ data: TInput; opts?: JobsOptions }>) {
		const queue = this._requireQueue();
		return await queue.addBulk(
			jobs.map((job) => ({
				name: this.name,
				data: this._validate(job.data),
				opts: { ...DEFAULT_JOB_OPTIONS, ...job.opts },
			})),
		);
	}

	async addChildJob(
		parentJob: Job | TypedJob<JSONLike, ErrorRegistry>,
		jobData: TInput,
		options?: JobsOptions,
	) {
		if (!parentJob.id || !parentJob.queueQualifiedName) {
			throw new Error(
				"Parent job must have an id and a queueQualifiedName to add a child job",
			);
		}
		return await this.addJob(jobData, {
			...DEFAULT_JOB_OPTIONS,
			...options,
			parent: {
				id: parentJob.id,
				queue: parentJob.queueQualifiedName,
			},
		});
	}

	async addBulkChildJobs(
		parentJob: Job | TypedJob<JSONLike, ErrorRegistry>,
		jobs: Array<{ data: TInput; opts?: JobsOptions }>,
	) {
		const parentId = parentJob.id;
		if (!parentId || !parentJob.queueQualifiedName) {
			throw new Error(
				"Parent job must have an id and a queueQualifiedName to add child jobs",
			);
		}
		return await this.addBulkJobs(
			jobs.map((job) => ({
				data: job.data,
				opts: {
					...job?.opts,
					parent: {
						id: parentId,
						queue: parentJob.queueQualifiedName,
					},
				},
			})),
		);
	}

	get QueueEvents(): QueueEvents {
		if (!this._connection) {
			throw new Error("defineConnection(...) must be called first");
		}
		if (!this._queueEvents) {
			this._queueEvents = new QueueEvents(this.name, {
				connection: this._connection,
			});
		}
		return this._queueEvents;
	}

	addWorkerEventListener<E extends keyof WorkerListener>(
		eventName: E,
		callback: WorkerListener<TInput, TOutput, TName>[E],
	) {
		if (!this.worker) {
			throw new Error(
				"Worker is not initialized. Call defineProcessor(...) first.",
			);
		}
		this.worker.on(eventName, callback);
	}

	removeWorkerEventListener<E extends keyof WorkerListener>(
		eventName: E,
		callback: WorkerListener<TInput, TOutput, TName>[E],
	) {
		if (!this.worker) {
			return;
		}
		this.worker.off(eventName, callback);
	}

	/** Gracefully close the worker, queue, and any queue events. */
	async close(): Promise<void> {
		await this.worker?.close();
		await this.queue?.close();
		await this._queueEvents?.close();
	}
}

export type QueueInput<T> =
// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	T extends QueueSystem<infer I, any, any> ? I : never;
export type QueueOutput<T> =
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	T extends QueueSystem<any, infer O, any> ? O : never;
