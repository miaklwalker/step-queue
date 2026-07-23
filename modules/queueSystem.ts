import {
	type Job,
	type Processor,
	Queue,
	type QueueEvents,
	Worker,
	type ConnectionOptions,
	MetricsTime,
	type JobsOptions,
} from "bullmq";
import type { JSONLike } from "./types";
import { JOB_DEFAULTS } from "./const";

export class QueueSystem<
	TInput extends JSONLike = JSONLike,
	TOutput extends JSONLike = JSONLike,
	TName extends string = string,
> {
	readonly name: TName;
	readonly displayName: string;
	system: string | undefined;
	description: string | undefined;
	queue: Queue<TInput, TOutput, TName, TInput, TOutput, TName>;
	worker: Worker<TInput, TOutput, TName>;
	queueEvents: QueueEvents | undefined;
	constructor(
		connection: ConnectionOptions,
		nameConfig: { name: TName; displayName: string },
		processor: Processor<TInput, TOutput, TName>,
		concurrency: number = 1,
	) {
		this.name = nameConfig.name;
		this.displayName = nameConfig.displayName;
		this.description = undefined;
		this.system = undefined;
		this.queue = new Queue<TInput, TOutput, TName, TInput, TOutput, TName>(
			nameConfig.name,
			{ connection },
		);
		this.worker = new Worker<TInput, TOutput, TName>(
			nameConfig.name,
			processor,
			{
				connection,
				concurrency,
				metrics: {
					maxDataPoints: MetricsTime.ONE_WEEK * 2,
				},
			},
		);
	}
	async addJob(jobData: TInput, options?: JobsOptions) {
		return await this.queue.add(this.name, jobData, {
			...JOB_DEFAULTS,
			...options,
		});
	}
	async addChildJob(parentJob: Job, jobData: TInput, options?: JobsOptions) {
		if (!parentJob || !parentJob.id) {
			throw new Error("Missing job or job id");
		}
		return await this.addJob(jobData, {
			...JOB_DEFAULTS,
			...options,
			parent: {
				id: parentJob.id,
				queue: parentJob.queueQualifiedName,
			},
		});
	}
}
