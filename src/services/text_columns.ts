/**
 * Text columns that hold payloads and error messages. MySQL stores them as
 * LONGTEXT: its TEXT type stops at 64 KB.
 */
export const QUEUE_TEXT_COLUMNS = {
  jobs: ['data', 'error'],
  schedules: ['payload'],
} as const

export interface TextColumnsMigrationOptions {
  /** Name of the jobs table. Defaults to `queue_jobs`. */
  jobsTable?: string
  /** Name of the schedules table. Defaults to `queue_schedules`. */
  schedulesTable?: string
}
