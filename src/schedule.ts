import { QueueManager } from './queue_manager.js'
import { JobDispatcher } from './job_dispatcher.js'
import type { ScheduleData, ScheduleListOptions, ScheduleStatus } from './types/main.js'

/**
 * Represents a persisted job schedule.
 *
 * Use `Schedule.find()` or `Schedule.list()` to retrieve schedules,
 * then use instance methods to manage them.
 *
 * @example
 * ```typescript
 * const schedule = await Schedule.find('cleanup-daily')
 * if (schedule) {
 *   await schedule.pause()
 *   // Later...
 *   await schedule.resume()
 * }
 *
 * // List all active schedules
 * const activeSchedules = await Schedule.list({ status: 'active' })
 * ```
 */
export class Schedule {
  readonly #data: ScheduleData

  constructor(data: ScheduleData) {
    this.#data = data
  }

  get id(): string {
    return this.#data.id
  }

  get name(): string {
    return this.#data.name
  }

  get payload(): unknown {
    return this.#data.payload
  }

  get cronExpression(): string | null {
    return this.#data.cronExpression
  }

  get everyMs(): number | null {
    return this.#data.everyMs
  }

  get timezone(): string {
    return this.#data.timezone
  }

  get from(): Date | null {
    return this.#data.from
  }

  get to(): Date | null {
    return this.#data.to
  }

  get limit(): number | null {
    return this.#data.limit
  }

  get runCount(): number {
    return this.#data.runCount
  }

  get nextRunAt(): Date | null {
    return this.#data.nextRunAt
  }

  get lastRunAt(): Date | null {
    return this.#data.lastRunAt
  }

  get status(): ScheduleStatus {
    return this.#data.status
  }

  get createdAt(): Date {
    return this.#data.createdAt
  }

  /**
   * Find a schedule by ID.
   *
   * @param id - The schedule ID
   * @returns The schedule instance, or null if not found
   */
  static async find(id: string): Promise<Schedule | null> {
    const adapter = QueueManager.use()
    const data = await adapter.getSchedule(id)

    if (!data) return null

    return new Schedule(data)
  }

  /**
   * List all schedules matching the given options.
   *
   * @param options - Optional filters for listing
   * @returns Array of schedule instances
   */
  static async list(options?: ScheduleListOptions): Promise<Schedule[]> {
    const adapter = QueueManager.use()
    const schedules = await adapter.listSchedules(options)

    return schedules.map((data) => new Schedule(data))
  }

  /**
   * Pause this schedule.
   * No jobs will be dispatched while paused.
   */
  async pause(): Promise<void> {
    const adapter = QueueManager.use()
    await adapter.updateSchedule(this.#data.id, { status: 'paused' })
    this.#data.status = 'paused'
  }

  /**
   * Resume this schedule.
   * Jobs will be dispatched according to the schedule.
   */
  async resume(): Promise<void> {
    const adapter = QueueManager.use()
    await adapter.updateSchedule(this.#data.id, { status: 'active' })
    this.#data.status = 'active'
  }

  /**
   * Delete this schedule permanently.
   */
  async delete(): Promise<void> {
    const adapter = QueueManager.use()
    await adapter.deleteSchedule(this.#data.id)
  }

  /**
   * Trigger immediate execution of this schedule's job.
   * Also updates runCount and lastRunAt.
   *
   * If the schedule has reached its limit, the job will not be dispatched.
   *
   * @param payload - Optional custom payload for the job
   */
  async trigger(payload?: any): Promise<void> {
    // Check if limit is reached
    if (this.#data.limit !== null && this.#data.runCount >= this.#data.limit) {
      return
    }

    const adapter = QueueManager.use()

    // Dispatch the job
    const dispatcher = new JobDispatcher(this.#data.name, payload ?? this.#data.payload)
    await dispatcher.run()

    // Update run metadata
    const now = new Date()
    const newRunCount = this.#data.runCount + 1

    await adapter.updateSchedule(this.#data.id, {
      runCount: newRunCount,
      lastRunAt: now,
    })

    this.#data.runCount = newRunCount
    this.#data.lastRunAt = now
  }
}
