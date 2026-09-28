import { createError } from '@poppinss/utils/exception'

export const E_INVALID_DURATION_EXPRESSION = createError(
  'Invalid duration expression: "%s"',
  'E_INVALID_DURATION_EXPRESSION',
  500
)

export const E_INVALID_BASE_DELAY = createError<[reason: string]>(
  'Invalid base delay. Reason: %s',
  'E_INVALID_BASE_DELAY',
  500
)

export const E_INVALID_MAX_DELAY = createError<[reason: string]>(
  'Invalid max delay. Reason: %s',
  'E_INVALID_MAX_DELAY',
  500
)

export const E_INVALID_MULTIPLIER = createError<[reason: string]>(
  'Invalid multiplier. Reason: %s',
  'E_INVALID_MULTIPLIER',
  500
)

export const E_CONFIGURATION_ERROR = createError<[reason: string]>(
  'Configuration error. Reason: %s',
  'E_CONFIGURATION_ERROR',
  500
)

export const E_JOB_NOT_FOUND = createError<[jobName: string]>(
  'Requested job "%s" is not registered',
  'E_JOB_NOT_FOUND'
)

export const E_JOB_MAX_ATTEMPTS_REACHED = createError<[jobName: string]>(
  'The job "%s" has reached the maximum number of retry attempts',
  'E_JOB_MAX_ATTEMPTS_REACHED'
)

export const E_JOB_STALLED = createError<[jobName: string, maxStalledCount: number]>(
  'The job "%s" stalled more than the allowed %d time(s)',
  'E_JOB_STALLED'
)

export const E_JOB_TIMEOUT = createError<[jobName: string, timeout: number]>(
  'The job "%s" has exceeded the timeout of %dms',
  'E_JOB_TIMEOUT'
)

export const E_INVALID_TIMEOUT = createError<[timeout: string]>(
  'Invalid timeout "%s": use 0 for no timeout, or a whole number of milliseconds from 1 to 2147483647 (about 24.8 days)',
  'E_INVALID_TIMEOUT',
  500
)

export const E_QUEUE_NOT_INITIALIZED = createError(
  'QueueManager is not initialized. Call QueueManager.init() before using it.',
  'E_QUEUE_NOT_INITIALIZED',
  500
)

export const E_ADAPTER_INIT_ERROR = createError<[adapterName: string, originalMessage: string]>(
  'Failed to initialize adapter "%s". Reason: %s',
  'E_ADAPTER_INIT_ERROR',
  500
)

export const E_NO_JOBS_FOUND = createError<[patterns: string]>(
  'No jobs found for the specified locations: %s. Verify your glob patterns match your job files.',
  'E_NO_JOBS_FOUND',
  500
)

export const E_INVALID_CRON_EXPRESSION = createError<[expression: string, reason: string]>(
  'Invalid cron expression "%s": %s',
  'E_INVALID_CRON_EXPRESSION',
  500
)

export const E_INVALID_SCHEDULE_CONFIG = createError<[reason: string]>(
  'Invalid schedule configuration: %s',
  'E_INVALID_SCHEDULE_CONFIG',
  500
)
