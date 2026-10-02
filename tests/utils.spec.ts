import { test } from '@japa/runner'
import { parse, parseTimeout, resolveRetention } from '../src/utils.js'
import { E_INVALID_DURATION_EXPRESSION, E_INVALID_TIMEOUT } from '../src/exceptions.js'

test.group('Utils | parse', () => {
  test('parse should return number when input is number', ({ assert, expectTypeOf }) => {
    const result = parse(5000)

    assert.equal(result, 5000)
    expectTypeOf(result).toBeNumber()
  })

  test('parse should parse duration strings', ({ assert }) => {
    assert.equal(parse('1s'), 1000)
    assert.equal(parse('2m'), 120000)
    assert.equal(parse('1h'), 3600000)
    assert.equal(parse('500ms'), 500)
  })

  test('parse should throw error for invalid duration strings', ({ assert }) => {
    assert.plan(1)

    try {
      parse('invalid')
    } catch (error) {
      assert.instanceOf(error, E_INVALID_DURATION_EXPRESSION)
    }
  })
})

test.group('Utils | parseTimeout', () => {
  test('undefined and 0 mean no timeout', ({ assert }) => {
    assert.isUndefined(parseTimeout(undefined))
    assert.isUndefined(parseTimeout(0))
  })

  test('parses timeouts up to 2^31 - 1 ms', ({ assert }) => {
    assert.equal(parseTimeout(1), 1)
    assert.equal(parseTimeout('30s'), 30_000)
    assert.equal(parseTimeout('24d'), 24 * 24 * 60 * 60 * 1000)
    assert.equal(parseTimeout(2_147_483_647), 2_147_483_647)
  })

  test('rejects timeouts Node timers cannot hold', ({ assert }) => {
    const timeouts = [
      -1,
      0.5,
      1.5,
      2_147_483_648,
      '25d',
      '30d',
      Number.POSITIVE_INFINITY,
      Number.NaN,
    ]

    for (const timeout of timeouts) {
      assert.throws(() => parseTimeout(timeout), E_INVALID_TIMEOUT)
    }
  })

  test('explains the valid range', ({ assert }) => {
    assert.throws(
      () => parseTimeout('30d'),
      'Invalid timeout "30d": use 0 for no timeout, or a whole number of milliseconds from 1 to 2147483647 (about 24.8 days)'
    )
  })
})

test.group('Utils | resolveRetention', () => {
  test('undefined retention should return keep: false', ({ assert }) => {
    const result = resolveRetention(undefined)

    assert.deepEqual(result, { keep: false, maxAge: 0, maxCount: 0 })
  })

  test('true retention should return keep: false (remove on complete)', ({ assert }) => {
    const result = resolveRetention(true)

    assert.deepEqual(result, { keep: false, maxAge: 0, maxCount: 0 })
  })

  test('false retention should return keep: true (keep in history)', ({ assert }) => {
    const result = resolveRetention(false)

    assert.deepEqual(result, { keep: true, maxAge: 0, maxCount: 0 })
  })

  test('object with count should return keep: true with maxCount', ({ assert }) => {
    const result = resolveRetention({ count: 100 })

    assert.deepEqual(result, { keep: true, maxAge: 0, maxCount: 100 })
  })

  test('object with age as number should return keep: true with maxAge', ({ assert }) => {
    const result = resolveRetention({ age: 3600000 })

    assert.deepEqual(result, { keep: true, maxAge: 3600000, maxCount: 0 })
  })

  test('object with age as string should parse and return maxAge', ({ assert }) => {
    const result = resolveRetention({ age: '1h' })

    assert.deepEqual(result, { keep: true, maxAge: 3600000, maxCount: 0 })
  })

  test('object with both age and count should return both', ({ assert }) => {
    const result = resolveRetention({ age: '30m', count: 50 })

    assert.deepEqual(result, { keep: true, maxAge: 1800000, maxCount: 50 })
  })
})
