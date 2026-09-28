import { parse as parseDuration } from '@lukeed/ms'
import type { Duration, JobRetention } from './types/main.js'
import * as errors from './exceptions.js'
import { PRIORITY_SCORE_MULTIPLIER } from './constants.js'

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
