import {
	type ConnectionOptions,
	type Job,
	type JobsOptions,
	Queue,
	QueueEvents,
} from "bullmq";
import { resolveQueueDefaults } from "./config.ts";
import { DEFAULT_JOB_OPTIONS } from "./const.ts";
import type { ErrorRegistry, JSONLike, TypedJob } from "./types.ts";

/**
 * A queue-only counterpart to {@link QueueSystem} — adds jobs without starting a
 * worker. Useful for producers (API routes, scripts) that should never process
 * jobs themselves.
 *
 * ```ts
 * const producer = new QueueOnly(NameFactory({ name: "MyCoolQueue", env: "dev" }), {
 *   host: "localhost",
 *   port: 6379,
 * });
 * await producer.addJob({ greeting: "hi" });
 * ```
 */
export class QueueOnly<
	TInput extends Record<string, unknown> = Record<string, unknown>,
	TOutput extends Record<string, unknown> = Record<string, unknown>,
	TName extends string = string,
> {
	readonly name: TName;
	readonly displayName: string;
	queue: Queue<TInput, TOutput, TName, TInput, TOutput, TName>;
	private _connection: ConnectionOptions;
	private _queueEvents: QueueEvents | undefined;

	constructor(
		nameConfig: { name: TName; displayName: string },
		connection: ConnectionOptions,
	) {
		this.name = nameConfig.name;
		this.displayName = nameConfig.displayName;
		this._connection = connection;
		this.queue = new Queue<TInput, TOutput, TName, TInput, TOutput, TName>(
			nameConfig.name,
			{ connection },
		);
	}

	async addJob(jobData: TInput, options?: JobsOptions) {
		const configDefaults = await resolveQueueDefaults();
		return await this.queue.add(this.name, jobData, {
			...DEFAULT_JOB_OPTIONS,
			...configDefaults,
			...options,
		});
	}

	async addBulkJobs(jobs: Array<{ data: TInput; opts?: JobsOptions }>) {
		const configDefaults = await resolveQueueDefaults();
		return await this.queue.addBulk(
			jobs.map((job) => ({
				name: this.name,
				data: job.data,
				opts: { ...DEFAULT_JOB_OPTIONS, ...configDefaults, ...job.opts },
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
		if (!parentJob.id || !parentJob.queueQualifiedName) {
			throw new Error(
				"Parent job must have an id and a queueQualifiedName to add child jobs",
			);
		}
		return await this.addBulkJobs(
			jobs.map((job) => {
				const parentId = parentJob.id;
				if (!parentId) {
					throw new Error("Parent ID is required");
				}
				return {
					data: job.data,
					opts: {
						...job?.opts,
						parent: {
							id: parentId,
							queue: parentJob.queueQualifiedName,
						},
					},
				};
			}),
		);
	}

	get QueueEvents(): QueueEvents {
		if (!this._queueEvents) {
			this._queueEvents = new QueueEvents(this.name, {
				connection: this._connection,
			});
		}
		return this._queueEvents;
	}

	async close(): Promise<void> {
		await this.queue.close();
		await this._queueEvents?.close();
	}
}

export type QueueOnlyInput<T> =
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	T extends QueueOnly<infer I, any, any> ? I : never;
export type QueueOnlyOutput<T> =
	// biome-ignore lint/suspicious/noExplicitAny: <The parameter any replaces is never used in downstream types>
	T extends QueueOnly<any, infer O, any> ? O : never;
