import { test } from '@japa/runner'
import {
  legacyScheduleDateToEpoch,
  legacyScheduleRowToEpochs,
} from '../src/services/schedule_dates.js'

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

  test('reads the MySQL zero date as no date', ({ assert }) => {
    assert.isNull(legacyScheduleDateToEpoch('0000-00-00 00:00:00', 'Europe/Paris'))
    assert.isNull(legacyScheduleDateToEpoch('0000-00-00 00:00:00.000000', 'UTC'))
  })

  test('keeps the years 0 to 99', ({ assert }) => {
    for (const year of ['0000', '0050']) {
      assert.equal(
        toIso(legacyScheduleDateToEpoch(`${year}-06-01 12:00:00`, 'UTC')),
        `${year}-06-01T12:00:00.000Z`
      )
      // Paris used a local mean time offset of +00:09:21 then.
      assert.equal(
        toIso(legacyScheduleDateToEpoch(`${year}-06-01 12:00:00`, 'Europe/Paris')),
        `${year}-06-01T11:50:39.000Z`
      )
    }
  })

  test('rejects a date or time that does not exist', ({ assert }) => {
    for (const value of [
      '2026-02-31 00:00:00',
      '2026-00-10 00:00:00',
      '2026-13-01 00:00:00',
      '2026-01-00 00:00:00',
      '2026-01-01 24:00:00',
      '2026-01-01 00:60:00',
    ]) {
      assert.throws(() => legacyScheduleDateToEpoch(value, 'UTC'), /Cannot convert/, value)
    }
  })
})

test.group('legacyScheduleRowToEpochs', () => {
  test('converts the legacy columns of a row into its epoch columns', ({ assert }) => {
    const row = { id: 'a', next_run_at: '2026-07-01 12:00:00', last_run_at: null, created_at: 0 }

    assert.deepEqual(
      legacyScheduleRowToEpochs(row, ['next_run_at', 'last_run_at', 'created_at'], 'Europe/Paris'),
      {
        next_run_at__epoch: Date.parse('2026-07-01T10:00:00.000Z'),
        last_run_at__epoch: null,
        created_at__epoch: 0,
      }
    )
  })

  test('rejects a row without created_at, which the migrated table requires', ({ assert }) => {
    for (const createdAt of [null, '0000-00-00 00:00:00']) {
      const row = { id: 'no-created-at', next_run_at: null, created_at: createdAt }

      assert.throws(
        () => legacyScheduleRowToEpochs(row, ['next_run_at', 'created_at'], 'UTC'),
        /Cannot migrate schedule "no-created-at": its created_at is empty/
      )
    }
  })
})
