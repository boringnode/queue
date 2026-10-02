/**
 * Schedule columns that decide when a schedule runs. When `upsertSchedule()`
 * leaves them unchanged, an existing schedule keeps its next run.
 */
export const SCHEDULE_TIMING_COLUMNS = [
  'cron_expression',
  'every_ms',
  'timezone',
  'from_date',
  'to_date',
  'run_limit',
] as const

type ScheduleTiming = Partial<Record<(typeof SCHEDULE_TIMING_COLUMNS)[number], unknown>>

/**
 * Compare the timing of a stored schedule with a new one, for the adapters
 * that keep schedules in memory. `null` and `undefined` both mean unset.
 */
export function sameScheduleTiming(stored: ScheduleTiming, next: ScheduleTiming): boolean {
  return SCHEDULE_TIMING_COLUMNS.every(
    (column) => (stored[column] ?? null) === (next[column] ?? null)
  )
}
