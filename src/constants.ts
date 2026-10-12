/**
 * Default job priority (1-10 scale, lower = higher priority)
 */
export const DEFAULT_PRIORITY = 5

/**
 * Multiplier used in score calculation: priority * multiplier + timestamp
 *
 * This ensures higher priority jobs are processed first,
 * while preserving FIFO order within the same priority.
 * The value (1e13) leaves room for ~300 years of millisecond timestamps.
 */
export const PRIORITY_SCORE_MULTIPLIER = 1e13

/**
 * Default delay when the worker is idle (no jobs in queue)
 */
export const DEFAULT_IDLE_DELAY = '2s'

/**
 * Default interval between stalled job checks
 */
export const DEFAULT_STALLED_INTERVAL = '30s'

/**
 * Default threshold after which a job is considered stalled
 */
export const DEFAULT_STALLED_THRESHOLD = '30s'

/**
 * Default number of times a job whose class a worker does not know goes back
 * to the queue before it fails
 */
export const DEFAULT_UNKNOWN_JOB_RETRIES = 10

/**
 * Delay before a job whose class a worker does not know runs again, in ms
 */
export const UNKNOWN_JOB_RETRY_DELAY = 30_000

/**
 * Default delay before retrying after an error
 */
export const DEFAULT_ERROR_RETRY_DELAY = '5s'
