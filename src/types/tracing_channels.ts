/*
 * @boringnode/queue
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

import type { AcquiredJob } from '../contracts/adapter.js'
import type { DedupOutcome, JobData } from './main.js'

/**
 * Tracing data structure for job dispatch events.
 */
export type JobDispatchMessage = {
  /** The jobs being dispatched (single dispatch = array of one) */
  jobs: JobData[]

  /** Target queue name */
  queue: string

  /** Delay in milliseconds before the job becomes available */
  delay?: number

  /**
   * Deduplication outcome when the job used `.dedup()`. Allows OTel/tracing
   * consumers to distinguish added vs skipped/replaced/extended dispatches.
   * Populated by the dispatcher after the push call completes.
   */
  dedupOutcome?: DedupOutcome

  /** Error that caused the dispatch to fail */
  error?: Error
}

/**
 * Tracing data structure for job execution events.
 */
export type JobExecuteMessage = {
  /** The acquired job being executed */
  job: AcquiredJob

  /** Queue the job was acquired from */
  queue: string

  /** Execution outcome (set in asyncEnd) */
  status?: 'completed' | 'failed' | 'retrying'

  /** Execution duration in milliseconds (set in asyncEnd) */
  duration?: number

  /** Error that caused the failure (set in asyncEnd) */
  error?: Error

  /** When the next retry is scheduled (set in asyncEnd for retrying jobs) */
  nextRetryAt?: Date
}
