import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import { parse as parseDuration } from '@lukeed/ms'
import type { Duration, JobRetention } from './types/main.js'
import * as errors from './exceptions.js'
import {
  FINALIZATION_RETRIES,
  FINALIZATION_RETRY_DELAY,
  PRIORITY_SCORE_MULTIPLIER,
} from './constants.js'

export interface ResolvedRetention {
  keep: boolean
  maxAge: number
  maxCount: number
}

export function resolveRetention(retention?: JobRetention): ResolvedRetention {
  if (retention === undefined || retention === true) {
    return { keep: false, maxAge: 0, maxCount: 0 }
  }

  if (retention === false) {
    return { keep: true, maxAge: 0, maxCount: 0 }
  }

  return {
    keep: true,
    maxAge: retention.age ? parse(retention.age) : 0,
    maxCount: retention.count ?? 0,
  }
}

/**
 * Resolve the payload stored for a schedule. Schedules persist their payload
 * as JSON, which cannot represent `undefined`, so every adapter stores a
 * missing payload as an empty object.
 */
export function resolveSchedulePayload(payload: unknown): unknown {
  return payload === undefined ? {} : payload
}

/**
 * Convert an epoch millisecond value read from a database into a Date.
 * SQL drivers can return `bigint` columns as strings.
 */
export function epochToDate(value: number | string | bigint | null | undefined): Date | null {
  return value === null || value === undefined ? null : new Date(Number(value))
}

export function parse(duration: Duration): number {
  if (typeof duration === 'number') {
    return duration
  }

  const milliseconds = parseDuration(duration)

  if (typeof milliseconds === 'undefined') {
    throw new errors.E_INVALID_DURATION_EXPRESSION([duration])
  }

  return milliseconds
}

/** Longest delay Node timers support: 2^31 - 1 ms, about 24.8 days. */
const MAX_TIMEOUT = 2_147_483_647

/**
 * Parse a job timeout into milliseconds. `undefined` and 0 mean no timeout.
 * Throws for anything `AbortSignal.timeout()` would reject or mishandle: a
 * fraction of a millisecond, a negative timeout, or one longer than Node
 * timers support (which would fire after 1 ms).
 */
export function parseTimeout(timeout: Duration | undefined): number | undefined {
  if (timeout === undefined) return undefined

  const milliseconds = parse(timeout)
  if (milliseconds === 0) return undefined

  if (!Number.isInteger(milliseconds) || milliseconds < 1 || milliseconds > MAX_TIMEOUT) {
    throw new errors.E_INVALID_TIMEOUT([String(timeout)])
  }

  return milliseconds
}

/**
 * Calculate the score for job ordering in the queue.
 * Lower scores are processed first.
 *
 * @param priority - Job priority (1-10, lower = higher priority)
 * @param timestamp - Timestamp in milliseconds
 * @returns Score for queue ordering
 */
export function calculateScore(priority: number, timestamp: number): number {
  return priority * PRIORITY_SCORE_MULTIPLIER + timestamp
}

/**
 * Create the lease token of a new acquisition. It starts with the worker id,
 * so a stored token tells which worker holds the job.
 */
export function createLeaseToken(workerId: string): string {
  return `${workerId}:${randomUUID()}`
}

/**
 * Whether a database error is transient lock contention: the transaction was
 * rolled back and can run again as is. Covers MySQL deadlocks
 * (ER_LOCK_DEADLOCK, 1213) and PostgreSQL deadlocks and serialization
 * failures (40P01, 40001).
 */
export function isDeadlockError(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false

  const { code, errno } = error as { code?: unknown; errno?: unknown }
  return code === 'ER_LOCK_DEADLOCK' || errno === 1213 || code === '40P01' || code === '40001'
}

/**
 * Run a database operation, and run it again, a bounded number of times,
 * when the database rolled it back as a deadlock victim.
 */
export async function retryOnDeadlock<T>(operation: () => Promise<T>): Promise<T> {
  for (let retry = 1; ; retry++) {
    try {
      return await operation()
    } catch (error) {
      if (retry > FINALIZATION_RETRIES || !isDeadlockError(error)) throw error
      await setTimeout(FINALIZATION_RETRY_DELAY * retry)
    }
  }
}
