import { randomUUID } from 'node:crypto'
import debug from './debug.js'
import { QueueManager } from './queue_manager.js'
import { dispatchChannel } from './tracing_channels.js'
import type { Adapter, PushResult } from './contracts/adapter.js'
import type {
  AdapterSelector,
  DispatchManyResult,
  DispatchResult,
  Duration,
  JobData,
  JobOptions,
} from './types/main.js'
import type { JobDispatchMessage } from './types/tracing_channels.js'
import { parse } from './utils.js'

export type JobDispatchOverrides = {
  queue?: string
  adapter?: AdapterSelector
  priority?: number
  delay?: Duration
  groupId?: string
  dedup?: {
    id: string
    ttl?: number
    extend?: boolean
    replace?: boolean
  }
}

export function resolveAdapterSelector(selector?: AdapterSelector): Adapter {
  return typeof selector === 'function' ? selector() : QueueManager.use(selector)
}

export function resolveJobDispatchTarget(
  jobOptions: JobOptions = {},
  overrides: Pick<JobDispatchOverrides, 'queue' | 'adapter'> = {}
): { queue: string; adapter: Adapter } {
  const queue = overrides.queue ?? jobOptions.queue ?? 'default'
  const adapterSelector =
    overrides.adapter ??
    jobOptions.adapter ??
    QueueManager.getConfigResolver().getQueueAdapter(queue)
  const adapter = resolveAdapterSelector(adapterSelector)

  return { queue, adapter }
}

type JobDispatchBase = {
  name: string
  jobOptions?: JobOptions
  overrides?: JobDispatchOverrides
  scheduleId?: string
}

export type SingleJobDispatchRequest = JobDispatchBase & {
  kind: 'single'
  payload: unknown
}

export type BatchJobDispatchRequest = JobDispatchBase & {
  kind: 'batch'
  payloads: unknown[]
}

export type JobDispatchRequest = SingleJobDispatchRequest | BatchJobDispatchRequest

/**
 * Dispatch Jobs through one interface.
 */
export class JobDispatchRuntime {
  dispatch(request: SingleJobDispatchRequest): Promise<DispatchResult>
  dispatch(request: BatchJobDispatchRequest): Promise<DispatchManyResult>
  async dispatch(request: JobDispatchRequest): Promise<DispatchResult | DispatchManyResult> {
    if (request.kind === 'batch' && request.payloads.length === 0) {
      return { jobIds: [] }
    }

    const { queue, adapter } = resolveJobDispatchTarget(request.jobOptions, request.overrides)
    const wrapInternal = QueueManager.getInternalOperationWrapper()

    if (request.kind === 'batch') {
      debug('dispatching %d jobs of type %s', request.payloads.length, request.name)

      const createdAt = Date.now()
      const jobs = request.payloads.map((payload) =>
        this.#createJobData(request, payload, createdAt)
      )
      const message: JobDispatchMessage = { jobs, queue }

      await dispatchChannel.tracePromise(
        () => wrapInternal(() => adapter.pushManyOn(queue, jobs)),
        message
      )

      return { jobIds: jobs.map((job) => job.id) }
    }

    const job = this.#createJobData(request, request.payload, Date.now())
    debug('dispatching job %s with id %s using payload %s', request.name, job.id, request.payload)
    const delay = request.overrides?.delay ? parse(request.overrides.delay) : undefined
    const message: JobDispatchMessage = { jobs: [job], queue, delay }
    let pushResult: PushResult | undefined

    await dispatchChannel.tracePromise(async () => {
      const result =
        delay === undefined
          ? await wrapInternal(() => adapter.pushOn(queue, job))
          : await wrapInternal(() => adapter.pushLaterOn(queue, job, delay))

      if (result) {
        pushResult = result
        message.dedupOutcome = result.outcome
      }
    }, message)

    if (pushResult && request.overrides?.dedup) {
      return { jobId: pushResult.jobId, deduped: pushResult.outcome }
    }

    return { jobId: job.id }
  }

  #createJobData(request: JobDispatchRequest, payload: unknown, createdAt: number): JobData {
    if (request.kind === 'single' && request.overrides?.dedup) {
      const prefixedLength = request.name.length + 2 + request.overrides.dedup.id.length
      if (prefixedLength > 510) {
        throw new Error(
          `Dedup ID combined with job name exceeds 510 characters ` +
            `(got ${prefixedLength}). Shorten either the job name or the dedup id.`
        )
      }
    }

    return {
      id: randomUUID(),
      name: request.name,
      payload,
      attempts: 0,
      priority: request.overrides?.priority ?? request.jobOptions?.priority,
      groupId: request.overrides?.groupId,
      createdAt,
      ...(request.scheduleId ? { scheduleId: request.scheduleId } : {}),
      ...(request.kind === 'single' && request.overrides?.dedup
        ? {
            dedup: {
              ...request.overrides.dedup,
              id: `${request.name}::${request.overrides.dedup.id}`,
            },
          }
        : {}),
    }
  }
}

export const jobDispatchRuntime = new JobDispatchRuntime()
