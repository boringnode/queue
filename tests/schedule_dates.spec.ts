import { test } from '@japa/runner'
import { legacyScheduleDateToEpoch } from '../src/services/schedule_dates.js'

const toIso = (value: number | null) => (value === null ? null : new Date(value).toISOString())

test.group('legacyScheduleDateToEpoch', () => {
  test('reads a wall-clock time in the given time zone', ({ assert }) => {
    assert.equal(
      toIso(legacyScheduleDateToEpoch('2026-07-01 12:00:00.25', 'Europe/Paris')),
      '2026-07-01T10:00:00.250Z'
    )
    assert.equal(
      toIso(legacyScheduleDateToEpoch('2026-07-01 12:00:00', 'America/New_York')),
      '2026-07-01T16:00:00.000Z'
    )
  })

  test('resolves a time repeated by a DST fall-back to its first occurrence', ({ assert }) => {
    assert.equal(
      toIso(legacyScheduleDateToEpoch('2026-10-25 02:30:00', 'Europe/Paris')),
      '2026-10-25T00:30:00.000Z'
    )
    assert.equal(
      toIso(legacyScheduleDateToEpoch('2026-10-25 03:30:00', 'Europe/Paris')),
      '2026-10-25T02:30:00.000Z'
    )
  })

  test('moves a time skipped by a DST spring-forward past the gap', ({ assert }) => {
    assert.equal(
      toIso(legacyScheduleDateToEpoch('2026-03-29 02:30:00', 'Europe/Paris')),
      '2026-03-29T01:30:00.000Z'
    )
  })

  test('keeps exact instants and epoch milliseconds unchanged', ({ assert }) => {
    for (const value of [
      '2026-10-25 00:30:00+00',
      '2026-10-25 06:00:00+05:30',
      '2026-10-25T00:30:00.000Z',
      '1792888200000',
      1792888200000,
    ]) {
      assert.equal(
        toIso(legacyScheduleDateToEpoch(value, 'Asia/Tokyo')),
        '2026-10-25T00:30:00.000Z'
      )
    }
  })

  test('returns null for a missing date and rejects an unreadable one', ({ assert }) => {
    assert.isNull(legacyScheduleDateToEpoch(null, 'UTC'))
    assert.isNull(legacyScheduleDateToEpoch('', 'UTC'))
    assert.throws(() => legacyScheduleDateToEpoch('not a date', 'UTC'), /Cannot convert/)
  })
})
