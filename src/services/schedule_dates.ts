/**
 * Schedule tables created before 0.8 stored their dates in SQL date columns.
 * These helpers convert the values read back as text into epoch milliseconds.
 */

/** Columns of the schedules table that hold dates. */
export const SCHEDULE_DATE_COLUMNS = [
  'from_date',
  'to_date',
  'next_run_at',
  'last_run_at',
  'created_at',
] as const

export type ScheduleDateColumn = (typeof SCHEDULE_DATE_COLUMNS)[number]

export interface ScheduleDatesMigrationOptions {
  /**
   * IANA time zone the database driver wrote dates in before 0.8. Only used
   * for dates stored without a time zone. It is the time zone of the process,
   * unless the driver was configured otherwise (for example the `timezone`
   * option of mysql2). Defaults to the time zone of the current process.
   */
  timezone?: string

  /**
   * MySQL only. Session time zone of the connections the previous version
   * used, when it differs from the session time zone of the migration
   * connection: MySQL converts stored dates through the session time zone.
   * Accepts any value of the MySQL `time_zone` variable, such as `'+02:00'`.
   */
  databaseTimeZone?: string
}

/** Temporary column holding the converted value of a date column during the migration. */
export function epochColumn(name: ScheduleDateColumn): string {
  return `${name}__epoch`
}

export function isIntegerColumnType(type: string | undefined): boolean {
  return type !== undefined && /int/i.test(type)
}

/**
 * State of a schedules table, from its column types. A migration interrupted
 * on MySQL, which cannot roll back schema changes, can leave any mix of these.
 */
export function scheduleDatesMigrationState(columnTypes: Map<string, string>) {
  const legacy = SCHEDULE_DATE_COLUMNS.filter(
    (name) => columnTypes.has(name) && !isIntegerColumnType(columnTypes.get(name))
  )
  const withEpochColumn = SCHEDULE_DATE_COLUMNS.filter((name) => columnTypes.has(epochColumn(name)))

  return { legacy, withEpochColumn, migrated: legacy.length === 0 && withEpochColumn.length === 0 }
}

/** Throws before any schema change if `timeZone` is not a valid IANA time zone. */
export function assertTimeZone(timeZone: string): void {
  new Intl.DateTimeFormat('en-US', { timeZone })
}

export function scheduleDatesMigrationRequiredMessage(
  tableName: string,
  schemaService: string
): string {
  return (
    `The schedules table "${tableName}" still stores its dates in the format used before 0.8. ` +
    `Stop every process running the previous version, then run ` +
    `${schemaService}.migrateScheduleDates().`
  )
}

const WALL_CLOCK = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$/

/**
 * Convert a legacy schedule date, read as text, into epoch milliseconds.
 *
 * - Digits only: already epoch milliseconds.
 * - A date with `Z` or an offset: an exact instant.
 * - A date without a time zone: a wall-clock time in `wallClockTimeZone`.
 */
export function legacyScheduleDateToEpoch(
  value: string | number | null | undefined,
  wallClockTimeZone: string
): number | null {
  if (value === null || value === undefined || value === '') return null
  if (typeof value === 'number') return value

  const text = value.trim()
  if (/^-?\d+$/.test(text)) return Number(text)

  const wallClock = WALL_CLOCK.exec(text)
  if (wallClock) {
    const [, year, month, day, hour, minute, second, fraction = '0'] = wallClock
    return wallClockToEpoch(
      [year, month, day, hour, minute, second].map(Number) as WallClockParts,
      Math.round(Number(`0.${fraction}`) * 1000),
      wallClockTimeZone
    )
  }

  // PostgreSQL prints offsets as `+00` or `+05:30`; normalize to `+00:00`.
  const normalized = text.replace(' ', 'T').replace(/([+-]\d{2})$/, '$1:00')
  const epoch = Date.parse(normalized)

  if (Number.isNaN(epoch)) {
    throw new Error(`Cannot convert the schedule date "${value}" to epoch milliseconds`)
  }

  return epoch
}

/**
 * The epoch values of the legacy date columns of one schedule row, keyed by
 * epoch column. created_at is required once migrated, so a row without one
 * fails the migration here, before the first schema change.
 */
export function legacyScheduleRowToEpochs(
  row: Record<string, string | number | null>,
  columns: readonly ScheduleDateColumn[],
  wallClockTimeZone: string
): Record<string, number | null> {
  const values: Record<string, number | null> = {}

  for (const name of columns) {
    const epoch = legacyScheduleDateToEpoch(row[name], wallClockTimeZone)

    if (name === 'created_at' && epoch === null) {
      throw new Error(
        `Cannot migrate schedule "${row.id}": its created_at is empty, and the migrated column ` +
          `is required. Set a date, then run the migration again.`
      )
    }

    values[epochColumn(name)] = epoch
  }

  return values
}

type WallClockParts = [
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number,
  second: number,
]

/**
 * Convert a wall-clock time in an IANA time zone into epoch milliseconds.
 *
 * A time repeated by a DST fall-back resolves to its first occurrence. A time
 * skipped by a DST spring-forward is read with the offset in effect before the
 * change, which moves it forward by the size of the gap.
 */
function wallClockToEpoch(parts: WallClockParts, milliseconds: number, timeZone: string): number {
  const [year, month, day, hour, minute, second] = parts
  const asUtc = Date.UTC(year, month - 1, day, hour, minute, second, milliseconds)
  const DAY = 24 * 60 * 60 * 1000

  // The offsets in effect a day before and a day after cover any DST change.
  const offsetBefore = timeZoneOffset(asUtc - DAY, timeZone)
  const offsetAfter = timeZoneOffset(asUtc + DAY, timeZone)

  const valid = [asUtc - offsetBefore, asUtc - offsetAfter].filter(
    (candidate) => asUtc - timeZoneOffset(candidate, timeZone) === candidate
  )

  return valid.length > 0 ? Math.min(...valid) : asUtc - offsetBefore
}

/** Offset of `timeZone` from UTC at `epoch`, in milliseconds. */
function timeZoneOffset(epoch: number, timeZone: string): number {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    hourCycle: 'h23',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).formatToParts(new Date(epoch))

  const value = (type: string) => Number(parts.find((part) => part.type === type)!.value)
  const wallClockAsUtc = Date.UTC(
    value('year'),
    value('month') - 1,
    value('day'),
    value('hour'),
    value('minute'),
    value('second'),
    new Date(epoch).getUTCMilliseconds()
  )

  return wallClockAsUtc - epoch
}

/** Time zone of the current process. */
export function processTimeZone(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone
}
