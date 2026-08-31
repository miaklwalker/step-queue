import type {
	StandardJSONSchemaV1,
	StandardSchemaV1,
} from "@standard-schema/spec";
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
import { resolveQueueDefaults } from "./config.ts";
import { DEFAULT_JOB_OPTIONS } from "./const.ts";
import { ProcessorBuilder } from "./processorBuilder.ts";
import {
	type ErrorRegistry,
	type JSONLike,
	SchemaValidationError,
	type TypedJob,
} from "./types.ts";

/**
 * The BullMQ queue backing a {@link QueueSystem} with the given generics.
 */
type QueueOf<
	TInput extends JSONLike,
	TOutput extends JSONLike,
	TName extends string,
> = Queue<TInput, TOutput, TName, TInput, TOutput, TName>;

/**
 * `T` when the lifecycle flag is `true`, `undefined` when it is `false`.
 *
 * The flags default to `boolean`, over which this conditional distributes back
 * to `T | undefined` — so an un-narrowed `QueueSystem` behaves exactly as
 * before, while a narrowed one collapses to a plain `T`.
 */
type WhenTrue<TFlag extends boolean, T> = TFlag extends true ? T : undefined;

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
 *
 * Two lifecycle type parameters track how far the builder has been driven:
 * `TConnected` flips on `defineConnection`, collapsing `queue` from
 * `Queue | undefined` to `Queue`; `TStarted` flips on `defineProcessor`,
 * collapsing `worker` from `Worker | undefined` to `Worker`. Both default to
 * `boolean` ("either state"), so plain `QueueSystem<In, Out, Name>` annotations
 * keep accepting a system at any stage.
 *
 * Both flags are explicitly marked covariant (`out`). They are used only inside
 * a deferred conditional type, which TypeScript cannot measure variance for; it
 * would otherwise fall back to a bivariant comparison and quietly accept
 * `QueueSystem<..., boolean>` where `QueueSystem<..., true>` is required —
 * defeating the `this` guard on `defineProcessor`. Covariance gives what we
 * want: a narrowed system satisfies a wider annotation, but not the reverse.
 */
export class QueueSystem<
	TInput extends JSONLike = JSONLike,
	TOutput extends JSONLike = JSONLike,
	TName extends string = string,
	out TConnected extends boolean = boolean,
	out TStarted extends boolean = boolean,
> {
	readonly name: TName;
	readonly displayName: string;
	system: string | undefined;
	description: string | undefined;
	jsonSchema: Record<string, unknown> | undefined;

	private _worker: Worker<TInput, TOutput, TName> | undefined;
	private _queue: QueueOf<TInput, TOutput, TName> | undefined;
	private _schema: StandardSchemaV1 | undefined;
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
	 * The underlying BullMQ queue. Created on first access once a connection has
	 * been defined, so this is `undefined` only before `defineConnection(...)`.
	 */
	get queue(): WhenTrue<TConnected, QueueOf<TInput, TOutput, TName>> {
		return this._ensureQueue() as WhenTrue<
			TConnected,
			QueueOf<TInput, TOutput, TName>
		>;
	}

	/**
	 * The underlying BullMQ worker. Started by `defineProcessor(...)`, so this is
	 * `undefined` only before that call.
	 */
	get worker(): WhenTrue<TStarted, Worker<TInput, TOutput, TName>> {
		return this._worker as WhenTrue<TStarted, Worker<TInput, TOutput, TName>>;
	}

	/** Create the queue on demand once a connection is known. */
	private _ensureQueue(): QueueOf<TInput, TOutput, TName> | undefined {
		if (!this._queue && this._connection) {
			this._queue = new Queue<TInput, TOutput, TName, TInput, TOutput, TName>(
				this.name,
				{ connection: this._connection },
			);
		}
		return this._queue;
	}

	/**
	 * Provide a [Standard Schema](https://standardschema.dev) describing the job
	 * input — any compliant library works (Zod, Valibot, ArkType, Effect Schema,
	 * ...). The processor's initial data type is inferred from the schema's
	 * output type, and jobs are validated against it at runtime.
	 *
	 * If the schema's vendor supports JSON Schema conversion
	 * (`StandardJSONSchemaV1`), `jsonSchema` is populated as well.
	 */
	defineInputSchema<TSchema extends JSONLike>(
		schema: StandardSchemaV1<unknown, TSchema>,
	): QueueSystem<TSchema, TOutput, TName, TConnected, TStarted> {
		this._schema = schema;
		const props = schema["~standard"];
		if ("jsonSchema" in props) {
			try {
				this.jsonSchema = (
					props as StandardJSONSchemaV1.Props<unknown, TSchema>
				).jsonSchema.input({ target: "draft-2020-12" });
			} catch {
				// The vendor doesn't support this target; leave jsonSchema unset.
			}
		}
		return this as unknown as QueueSystem<
			TSchema,
			TOutput,
			TName,
			TConnected,
			TStarted
		>;
	}

	/**
	 * Provide the Redis/BullMQ connection this queue + worker should use.
	 *
	 * Narrows the returned type so `queue` is a `Queue` rather than
	 * `Queue | undefined`.
	 */
	defineConnection(
		connection: ConnectionOptions,
	): QueueSystem<TInput, TOutput, TName, true, TStarted> {
		this._connection = connection;
		return this as unknown as QueueSystem<
			TInput,
			TOutput,
			TName,
			true,
			TStarted
		>;
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
	 *
	 * Calling this before `defineConnection(...)` is a type error: the `this`
	 * parameter requires a system whose `TConnected` flag is `true`. The runtime
	 * guard below is kept for JavaScript callers and for anyone who casts past it.
	 *
	 * Because a connection is therefore always present, the result is narrowed on
	 * both lifecycle flags: `queue` and `worker` are non-optional.
	 */
	defineProcessor<TNewOutput extends JSONLike>(
		this: QueueSystem<TInput, TOutput, TName, true, TStarted>,
		build: (
			builder: ProcessorBuilder<TInput, TInput, TInput>,
		) => ProcessorBuilder<TInput, TInput, TNewOutput, ErrorRegistry>,
	): QueueSystem<TInput, TNewOutput, TName, true, true>;

	defineProcessor<TNewInput extends TInput, TNewOutput extends JSONLike>(
		this: QueueSystem<TInput, TOutput, TName, true, TStarted>,
		builder: ProcessorBuilder<TNewInput, TNewInput, TNewOutput, ErrorRegistry>,
	): QueueSystem<TNewInput, TNewOutput, TName, true, true>;

	defineProcessor<TNewOutput extends JSONLike>(
		builderOrBuild:
			| ProcessorBuilder<TInput, TInput, TNewOutput, ErrorRegistry>
			| ((
					builder: ProcessorBuilder<TInput, TInput, TInput>,
			  ) => ProcessorBuilder<TInput, TInput, TNewOutput, ErrorRegistry>),
	): QueueSystem<TInput, TNewOutput, TName, true, true> {
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

		this._ensureQueue();
		this._worker = new Worker<TInput, TOutput, TName>(
			this.name,
			processor as unknown as Processor<TInput, TOutput, TName>,
			{
				connection: this._connection,
				concurrency: this._concurrency,
				metrics: { maxDataPoints: MetricsTime.ONE_WEEK * 2 },
			},
		);

		return this as unknown as QueueSystem<
			TInput,
			TNewOutput,
			TName,
			true,
			true
		>;
	}

	private _requireQueue(): QueueOf<TInput, TOutput, TName> {
		const queue = this._ensureQueue();
		if (!queue) {
			throw new Error(
				"Queue is not initialized. Call defineConnection(...).defineProcessor(...) first.",
			);
		}
		return queue;
	}

	private async _validate(jobData: TInput): Promise<TInput> {
		if (!this._schema) {
			return jobData;
		}
		const result = await this._schema["~standard"].validate(jobData);
		if (result.issues) {
			throw new SchemaValidationError(result.issues);
		}
		return result.value as TInput;
	}

	async addJob(jobData: TInput, options?: JobsOptions) {
		const queue = this._requireQueue();
		const configDefaults = await resolveQueueDefaults();
		return await queue.add(this.name, await this._validate(jobData), {
			...DEFAULT_JOB_OPTIONS,
			...configDefaults,
			...options,
		});
	}

	async addBulkJobs(jobs: Array<{ data: TInput; opts?: JobsOptions }>) {
		const queue = this._requireQueue();
		const configDefaults = await resolveQueueDefaults();
		return await queue.addBulk(
			await Promise.all(
				jobs.map(async (job) => ({
					name: this.name,
					data: await this._validate(job.data),
					opts: { ...DEFAULT_JOB_OPTIONS, ...configDefaults, ...job.opts },
				})),
			),
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
		if (!this._worker) {
			throw new Error(
				"Worker is not initialized. Call defineProcessor(...) first.",
			);
		}
		this._worker.on(eventName, callback);
	}

	removeWorkerEventListener<E extends keyof WorkerListener>(
		eventName: E,
		callback: WorkerListener<TInput, TOutput, TName>[E],
	) {
		if (!this._worker) {
			return;
		}
		this._worker.off(eventName, callback);
	}

	/** Gracefully close the worker, queue, and any queue events. */
	async close(): Promise<void> {
		await this._worker?.close();
		await this._queue?.close();
		await this._queueEvents?.close();
	}
}

export type QueueInput<T> =
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	T extends QueueSystem<infer I, any, any, any, any> ? I : never;
export type QueueOutput<T> =
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	T extends QueueSystem<any, infer O, any, any, any> ? O : never;
