import { setTimeout } from 'node:timers/promises'
import debug from './debug.js'
import * as errors from './exceptions.js'
import { parse } from './utils.js'
import { JobPool } from './job_pool.js'
import { Locator } from './locator.js'
import type { SingleJobDispatchRequest } from './job_dispatch_runtime.js'
import type { Adapter, AcquiredJob } from './contracts/adapter.js'
import type { JobExecutionOutcome, JobExecutionRuntime } from './job_runtime.js'
import type { WorkerCycle } from './types/main.js'
import { DEFAULT_ERROR_RETRY_DELAY } from './constants.js'

type StartedCycle = Extract<WorkerCycle, { type: 'started' }>
type SessionMode = 'continuous' | 'manual'

interface PoolFillResult {
  cycles: StartedCycle[]
  errors: unknown[]
}

export type InternalOperationWrapper = <T>(operation: () => Promise<T>) => Promise<T>
export type JobExecutor = Pick<JobExecutionRuntime, 'execute'>
export interface ScheduleDispatcher {
  dispatch(request: SingleJobDispatchRequest): Promise<unknown>
}

export interface WorkerSessionSettings {
  concurrency: number
  idleDelay: number
  stalledInterval: number
  stalledThreshold: number
  maxStalledCount: number
}

export interface WorkerSessionOptions {
  workerId: string
  queues: string[]
  adapter: Adapter
  jobExecutionRuntime: JobExecutor
  scheduleDispatcher: ScheduleDispatcher
  wrapInternal: InternalOperationWrapper
  settings: WorkerSessionSettings
}

/**
 * Owns one continuous period of work for a Worker.
 *
 * A session has immutable queues and reaches quiescence exactly once. It owns
 * every operation started while processing: acquisitions, executions,
 * finalizations, schedule dispatch, stalled recovery, heartbeat renewal, and
 * delays between continuous cycles.
 */
export class WorkerSession {
  readonly #workerId: string
  readonly #queues: readonly string[]
  readonly #adapter: Adapter
  readonly #jobExecutionRuntime: JobExecutor
  readonly #scheduleDispatcher: ScheduleDispatcher
  readonly #wrapInternal: InternalOperationWrapper
  readonly #settings: WorkerSessionSettings

  #mode?: SessionMode
  #running = false
  #stopping = false
  #stopped = false
  #cycleGenerator?: AsyncGenerator<WorkerCycle, void, unknown>
  #cycleGeneratorClose?: Promise<void>
  #startCompletion?: Promise<void>
  #stopOperation?: Promise<void>
  #pool = new JobPool()
  #fillOperation?: Promise<PoolFillResult>
  #lastStalledCheck = 0
  #heartbeatTimer?: NodeJS.Timeout
  #heartbeatRenewal?: Promise<void>
  #delayController?: AbortController

  constructor(options: WorkerSessionOptions) {
    this.#workerId = options.workerId
    this.#queues = Object.freeze([...options.queues])
    this.#adapter = options.adapter
    this.#jobExecutionRuntime = options.jobExecutionRuntime
    this.#scheduleDispatcher = options.scheduleDispatcher
    this.#wrapInternal = options.wrapInternal
    this.#settings = options.settings
  }

  assertQueues(queues: readonly string[]): void {
    const matches =
      this.#queues.length === queues.length &&
      this.#queues.every((queue, index) => queue === queues[index])

    if (!matches) {
      throw new errors.E_CONFIGURATION_ERROR([
        `Cannot change WorkerSession queues from [${this.#queues.join(', ')}] to [${queues.join(', ')}]`,
      ])
    }
  }

  async start(): Promise<void> {
    if (this.#stopped || this.#stopping || this.#startCompletion) {
      return
    }

    const generator = this.process()
    const completion = Promise.withResolvers<void>()
    this.#startCompletion = completion.promise

    try {
      debug('starting worker %s on queues: %O', this.#workerId, this.#queues)

      for await (const cycle of generator) {
        if (cycle.type === 'started' || cycle.type === 'completed') {
          continue
        }

        const delay = parse(cycle.suggestedDelay)

        if (cycle.type === 'error') {
          debug('worker %s encountered an error: %O', this.#workerId, cycle.error)
        } else {
          debug('worker %s is idle, waiting for %dms', this.#workerId, delay)
        }

        await this.#waitBeforeNextCycle(delay)
      }
    } finally {
      completion.resolve()
      if (this.#startCompletion === completion.promise) {
        this.#startCompletion = undefined
      }
    }
  }

  process(): AsyncGenerator<WorkerCycle, void, unknown> {
    this.#claimMode('continuous')

    if (this.#cycleGenerator) {
      throw new errors.E_CONFIGURATION_ERROR(['WorkerSession already has an active cycle consumer'])
    }

    if (!this.#stopped && !this.#stopping) {
      this.#running = true
    }

    return this.#createCycleGenerator()
  }

  async processCycle(): Promise<WorkerCycle | null> {
    if (this.#stopped || this.#stopping) {
      return null
    }

    this.#claimMode('manual')
    this.#running = true

    const generator = this.#cycleGenerator ?? this.#createCycleGenerator()
    const result = await generator.next()

    if (result.done) {
      if (this.#cycleGenerator === generator) {
        this.#cycleGenerator = undefined
      }
      return null
    }

    return result.value
  }

  stop(): Promise<void> {
    if (!this.#stopOperation) {
      this.#stopOperation = this.#stop()
    }

    return this.#stopOperation
  }

  async #stop(): Promise<void> {
    if (this.#stopped) return

    debug('stopping worker session %s', this.#workerId)
    this.#stopping = true
    this.#running = false
    this.#delayController?.abort()

    if (this.#fillOperation) {
      debug('worker %s: waiting for in-flight job acquisitions to complete', this.#workerId)
      await this.#fillOperation.catch(() => {})
    }

    if (!this.#pool.isEmpty()) {
      debug('worker %s: waiting for %d running jobs to complete', this.#workerId, this.#pool.size)
      await this.#pool.drain()
    }

    this.#stopHeartbeat()
    await this.#heartbeatRenewal
    await this.#closeCycleGenerator()

    if (this.#startCompletion) {
      await this.#startCompletion
    }

    this.#stopped = true
    this.#stopping = false
  }

  #claimMode(mode: SessionMode): void {
    if (this.#mode && this.#mode !== mode) {
      throw new errors.E_CONFIGURATION_ERROR([
        `Cannot use ${mode} processing during a ${this.#mode} WorkerSession`,
      ])
    }

    this.#mode = mode
  }

  #createCycleGenerator(): AsyncGenerator<WorkerCycle, void, unknown> {
    const generator = this.#cycles()
    this.#cycleGenerator = generator
    return generator
  }

  async #closeCycleGenerator(): Promise<void> {
    if (!this.#cycleGenerator) {
      await this.#cycleGeneratorClose
      return
    }

    const generator = this.#cycleGenerator
    this.#cycleGenerator = undefined
    const closeOperation = generator.return(undefined).then(() => {})
    this.#cycleGeneratorClose = closeOperation

    try {
      await closeOperation
    } finally {
      if (this.#cycleGeneratorClose === closeOperation) {
        this.#cycleGeneratorClose = undefined
      }
    }
  }

  async #waitBeforeNextCycle(delay: number): Promise<void> {
    if (!this.#running) return

    const controller = new AbortController()
    this.#delayController = controller

    try {
      await setTimeout(delay, undefined, { signal: controller.signal })
    } catch (error) {
      if (!controller.signal.aborted) {
        throw error
      }
    } finally {
      if (this.#delayController === controller) {
        this.#delayController = undefined
      }
    }
  }

  async *#cycles(): AsyncGenerator<WorkerCycle, void, unknown> {
    this.#startHeartbeat()

    try {
      while (this.#running) {
        try {
          await this.#checkStalledJobs()

          if (!this.#running) break

          await this.#dispatchDueSchedules()

          if (!this.#running) break

          const fillOperation = this.#fillPool()
          this.#fillOperation = fillOperation

          try {
            const result = await fillOperation

            for (const cycle of result.cycles) {
              if (!this.#running) break
              yield cycle
            }

            if (result.errors.length > 0 && this.#running) {
              throw result.errors[0]
            }
          } finally {
            if (this.#fillOperation === fillOperation) {
              this.#fillOperation = undefined
            }
          }

          if (!this.#running) break

          if (this.#pool.isEmpty()) {
            yield { type: 'idle', suggestedDelay: this.#settings.idleDelay }
            continue
          }

          const hasCapacity = this.#pool.hasCapacity(this.#settings.concurrency)
          const result = await Promise.race([
            this.#pool
              .waitForNextCompletion()
              .then((completed) => ({ kind: 'completed' as const, completed })),
            ...(hasCapacity
              ? [setTimeout(this.#settings.idleDelay).then(() => ({ kind: 'tick' as const }))]
              : []),
          ])

          if (!this.#running) break
          if (result.kind === 'tick') continue

          yield { type: 'completed', queue: result.completed.queue, job: result.completed.job }
        } catch (error) {
          if (!this.#running) break

          yield {
            type: 'error',
            error: error as Error,
            suggestedDelay: parse(DEFAULT_ERROR_RETRY_DELAY),
          }
        }
      }
    } finally {
      if (!this.#stopping) {
        this.#stopHeartbeat()
      }
    }
  }

  async #fillPool(): Promise<PoolFillResult> {
    const slotsAvailable = this.#settings.concurrency - this.#pool.size

    if (slotsAvailable <= 0) return { cycles: [], errors: [] }

    const cycles: Array<StartedCycle | undefined> = Array.from({ length: slotsAvailable })
    const failures: Array<{ error: unknown } | undefined> = Array.from({ length: slotsAvailable })

    const acquisitions = Array.from({ length: slotsAvailable }, async (_, index) => {
      try {
        const result = await this.#acquireNextJob()
        if (!result) return

        const { job, queue } = result
        const promise = this.#execute(job, queue)
        this.#pool.add(job, queue, promise)
        cycles[index] = { type: 'started', queue, job }
      } catch (error) {
        failures[index] = { error }
      }
    })

    await Promise.all(acquisitions)

    return {
      cycles: cycles.filter((cycle): cycle is StartedCycle => cycle !== undefined),
      errors: failures
        .filter((result): result is { error: unknown } => result !== undefined)
        .map((result) => result.error),
    }
  }

  async #execute(job: AcquiredJob, queue: string): Promise<void> {
    const startTime = performance.now()

    debug('worker %s: executing job %s (%s)', this.#workerId, job.id, job.name)

    const outcome = await this.#jobExecutionRuntime.execute(job, queue)
    await this.#finalizeExecution(job, queue, outcome)

    if (outcome.type === 'completed') {
      debug(
        'worker %s: successfully executed job %s in %dms',
        this.#workerId,
        job.id,
        (performance.now() - startTime).toFixed(2)
      )
    }
  }

  async #finalizeExecution(
    job: AcquiredJob,
    queue: string,
    outcome: JobExecutionOutcome
  ): Promise<void> {
    if (outcome.type === 'completed') {
      await this.#wrapInternal(() =>
        this.#adapter.completeJob(job.id, queue, outcome.removeOnComplete)
      )
      return
    }

    if (outcome.type === 'initialization-failed') {
      debug('worker %s: failed to initialize job %s (%s)', this.#workerId, job.id, job.name)
      await this.#wrapInternal(() =>
        this.#adapter.failJob(job.id, queue, outcome.error, outcome.removeOnFail)
      )
      return
    }

    if (outcome.type === 'failed') {
      await this.#wrapInternal(() =>
        this.#adapter.failJob(job.id, queue, outcome.error, outcome.removeOnFail)
      )

      if (outcome.failedHookError) {
        throw outcome.failedHookError
      }

      return
    }

    if (outcome.retryAt) {
      debug(
        'worker %s: job %s will retry at %s',
        this.#workerId,
        job.id,
        outcome.retryAt.toISOString()
      )
      await this.#wrapInternal(() => this.#adapter.retryJob(job.id, queue, outcome.retryAt))
    } else {
      await this.#wrapInternal(() => this.#adapter.retryJob(job.id, queue))
    }
  }

  async #acquireNextJob(): Promise<{ job: AcquiredJob; queue: string } | null> {
    for (const queue of this.#queues) {
      const job = await this.#wrapInternal(() => this.#adapter.popFrom(queue))

      if (!job) continue

      debug('worker %s: acquired job %s', this.#workerId, job.id)
      return { job, queue }
    }

    return null
  }

  async #checkStalledJobs(): Promise<void> {
    const now = Date.now()

    if (now - this.#lastStalledCheck < this.#settings.stalledInterval) {
      return
    }

    this.#lastStalledCheck = now

    for (const queue of this.#queues) {
      const recovered = await this.#wrapInternal(() =>
        this.#adapter.recoverStalledJobs(
          queue,
          this.#settings.stalledThreshold,
          this.#settings.maxStalledCount
        )
      )

      if (recovered > 0) {
        debug(
          'worker %s: recovered %d stalled jobs from queue %s',
          this.#workerId,
          recovered,
          queue
        )
      }
    }
  }

  #startHeartbeat(): void {
    this.#stopHeartbeat()

    const interval = Math.max(Math.floor(this.#settings.stalledThreshold / 2), 1)

    this.#heartbeatTimer = setInterval(() => {
      if (this.#heartbeatRenewal) return

      const renewal = this.#renewActiveJobs()
      this.#heartbeatRenewal = renewal

      const clearRenewal = () => {
        if (this.#heartbeatRenewal === renewal) {
          this.#heartbeatRenewal = undefined
        }
      }

      void renewal.then(clearRenewal, clearRenewal)
    }, interval)

    this.#heartbeatTimer.unref?.()
  }

  #stopHeartbeat(): void {
    if (this.#heartbeatTimer) {
      clearInterval(this.#heartbeatTimer)
      this.#heartbeatTimer = undefined
    }
  }

  async #renewActiveJobs(): Promise<void> {
    if (this.#pool.isEmpty()) return

    const jobIdsByQueue = this.#pool.activeJobIdsByQueue()

    for (const queue of this.#queues) {
      const jobIds = jobIdsByQueue.get(queue)
      if (!jobIds || jobIds.length === 0) continue

      try {
        await this.#wrapInternal(() => this.#adapter.renewJobs(queue, jobIds))
      } catch (error) {
        debug('worker %s: failed to renew jobs on queue %s: %O', this.#workerId, queue, error)
      }
    }
  }

  async #dispatchDueSchedules(): Promise<void> {
    while (this.#running) {
      const schedule = await this.#wrapInternal(() => this.#adapter.claimDueSchedule())

      if (!schedule) break

      debug(
        'worker %s: dispatching scheduled job %s (schedule: %s, runCount: %d)',
        this.#workerId,
        schedule.name,
        schedule.id,
        schedule.runCount + 1
      )

      const JobClass = await Locator.resolve(schedule.name)

      await this.#scheduleDispatcher.dispatch({
        kind: 'single',
        name: schedule.name,
        payload: schedule.payload,
        jobOptions: JobClass?.options,
        overrides: { adapter: () => this.#adapter },
        scheduleId: schedule.id,
      })
    }
  }
}
