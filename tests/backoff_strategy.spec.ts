import { test } from '@japa/runner'
import {
  BackoffStrategy,
  customBackoff,
  exponentialBackoff,
  fixedBackoff,
  linearBackoff,
} from '../src/strategies/backoff_strategy.js'
import * as errors from '../src/exceptions.js'
import type { JobOptions } from '../src/types/main.js'

test.group('BackoffStrategy', () => {
  test('should validate negative baseDelay', ({ assert }) => {
    assert.plan(2)

    try {
      new BackoffStrategy({ strategy: 'exponential', baseDelay: -1000 })
    } catch (error) {
      assert.instanceOf(error, errors.E_INVALID_BASE_DELAY)
      assert.equal(
        'Invalid base delay. Reason: Base delay must be a positive integer greater than zero',
        error.message
      )
    }
  })

  test('should validate negative maxDelay', ({ assert }) => {
    assert.plan(2)

    try {
      new BackoffStrategy({ strategy: 'exponential', baseDelay: 1000, maxDelay: -5000 })
    } catch (error) {
      assert.instanceOf(error, errors.E_INVALID_MAX_DELAY)
      assert.equal(
        'Invalid max delay. Reason: Max delay must be a positive integer greater than zero',
        error.message
      )
    }
  })

  test('should validate maxDelay less than baseDelay', ({ assert }) => {
    assert.plan(2)

    try {
      new BackoffStrategy({ strategy: 'exponential', baseDelay: 10000, maxDelay: 5000 })
    } catch (error) {
      assert.instanceOf(error, errors.E_INVALID_MAX_DELAY)
      assert.equal(
        'Invalid max delay. Reason: Max delay should be greater than base delay',
        error.message
      )
    }
  })

  test('should validate negative multiplier', ({ assert }) => {
    assert.plan(2)

    try {
      new BackoffStrategy({ strategy: 'exponential', baseDelay: 1000, multiplier: -1 })
    } catch (error) {
      assert.instanceOf(error, errors.E_INVALID_MULTIPLIER)
      assert.equal(
        'Invalid multiplier. Reason: Multiplier must be a positive number greater than zero',
        error.message
      )
    }
  })

  test('should validate exponential multiplier less than 1', ({ assert }) => {
    assert.plan(2)

    try {
      new BackoffStrategy({ strategy: 'exponential', baseDelay: 1000, multiplier: 0.5 })
    } catch (error) {
      assert.instanceOf(error, errors.E_INVALID_MULTIPLIER)
      assert.equal(
        'Invalid multiplier. Reason: Exponential strategy multiplier should be >= 1',
        error.message
      )
    }
  })

  test('should return frozen config copy', ({ assert }) => {
    const config = { strategy: 'fixed' as const, baseDelay: '1s' }
    const strategy = new BackoffStrategy(config)

    const returnedConfig = strategy.getConfig()

    assert.deepEqual(returnedConfig, config)
    assert.throws(() => {
      // @ts-expect-error
      returnedConfig.strategy = 'linear'
    })
  })

  test('should throw error for attempt number less than 1', ({ assert }) => {
    const strategy = new BackoffStrategy({ strategy: 'fixed', baseDelay: '1s' })

    assert.throws(() => strategy.calculateDelay(0), 'Attempt number must be >= 1')
  })

  test('exponentialBackoff factory should return BackoffStrategy instance', ({ assert }) => {
    const factory = exponentialBackoff()
    const strategy = factory()

    assert.instanceOf(strategy, BackoffStrategy)
    assert.equal(strategy.getConfig().strategy, 'exponential')
  })

  test('linearBackoff factory should return BackoffStrategy instance', ({ assert }) => {
    const factory = linearBackoff()
    const strategy = factory()

    assert.instanceOf(strategy, BackoffStrategy)
    assert.equal(strategy.getConfig().strategy, 'linear')
  })

  test('fixedBackoff factory should return BackoffStrategy instance', ({ assert }) => {
    const factory = fixedBackoff()
    const strategy = factory()

    assert.instanceOf(strategy, BackoffStrategy)
    assert.equal(strategy.getConfig().strategy, 'fixed')
  })

  test('customBackoff factory should return BackoffStrategy instance', ({ assert }) => {
    const factory = customBackoff({ strategy: 'exponential', baseDelay: '2s' })
    const strategy = factory()

    assert.instanceOf(strategy, BackoffStrategy)
    assert.equal(strategy.getConfig().strategy, 'exponential')
    assert.equal(strategy.getConfig().baseDelay, '2s')
  })
})

test.group('BackoffStrategy | Exponential', () => {
  test('should calculate exponential backoff delays', ({ assert }) => {
    const strategy = new BackoffStrategy({
      strategy: 'exponential',
      baseDelay: '1s',
      multiplier: 2,
      jitter: false,
    })

    assert.equal(strategy.calculateDelay(1), 1000) // 1s * 2^0
    assert.equal(strategy.calculateDelay(2), 2000) // 1s * 2^1
    assert.equal(strategy.calculateDelay(3), 4000) // 1s * 2^2
    assert.equal(strategy.calculateDelay(4), 8000) // 1s * 2^3
  })

  test('should respect maxDelay in exponential backoff', ({ assert }) => {
    const strategy = new BackoffStrategy({
      strategy: 'exponential',
      baseDelay: '1s',
      maxDelay: '5s',
      multiplier: 2,
      jitter: false,
    })

    assert.equal(strategy.calculateDelay(1), 1000) // 1s
    assert.equal(strategy.calculateDelay(2), 2000) // 2s
    assert.equal(strategy.calculateDelay(3), 4000) // 4s
    assert.equal(strategy.calculateDelay(4), 5000) // capped at 5s
    assert.equal(strategy.calculateDelay(5), 5000) // still capped at 5s
  })

  test('should use default multiplier when not specified', ({ assert }) => {
    const strategy = new BackoffStrategy({
      strategy: 'exponential',
      baseDelay: '1s',
      jitter: false,
    })

    assert.equal(strategy.calculateDelay(1), 1000) // 1s * 2^0
    assert.equal(strategy.calculateDelay(2), 2000) // 1s * 2^1 (default multiplier = 2)
    assert.equal(strategy.calculateDelay(3), 4000) // 1s * 2^2
  })
})

test.group('BackoffStrategy | Linear', () => {
  test('should calculate linear backoff delays', ({ assert }) => {
    const strategy = new BackoffStrategy({
      strategy: 'linear',
      baseDelay: '2s',
      jitter: false,
    })

    assert.equal(strategy.calculateDelay(1), 2000) // 2s * 1
    assert.equal(strategy.calculateDelay(2), 4000) // 2s * 2
    assert.equal(strategy.calculateDelay(3), 6000) // 2s * 3
    assert.equal(strategy.calculateDelay(4), 8000) // 2s * 4
  })
})

test.group('BackoffStrategy | Fixed', () => {
  test('should always return the same delay for fixed backoff', ({ assert }) => {
    const strategy = new BackoffStrategy({
      strategy: 'fixed',
      baseDelay: '3s',
      jitter: false,
    })

    assert.equal(strategy.calculateDelay(1), 3000)
    assert.equal(strategy.calculateDelay(2), 3000)
    assert.equal(strategy.calculateDelay(3), 3000)
    assert.equal(strategy.calculateDelay(10), 3000)
  })

  test('should apply jitter when enabled', ({ assert }) => {
    const strategy = new BackoffStrategy({
      strategy: 'fixed',
      baseDelay: '1s',
      jitter: true,
    })

    const delay1 = strategy.calculateDelay(1)
    const delay2 = strategy.calculateDelay(1)
    const delay3 = strategy.calculateDelay(1)

    // All delays should be around 1000ms but with jitter (±25%)
    assert.notEqual(delay1, 1000)
    assert.notEqual(delay2, 1000)
    assert.notEqual(delay3, 1000)
    assert.isTrue(delay1 >= 750 && delay1 <= 1250)
    assert.isTrue(delay2 >= 750 && delay2 <= 1250)
    assert.isTrue(delay3 >= 750 && delay3 <= 1250)
  })

  test('should return next retry date correctly', ({ assert }) => {
    const strategy = new BackoffStrategy({
      strategy: 'fixed',
      baseDelay: '1s',
      jitter: false,
    })

    const now = Date.now()
    const nextRetry = strategy.getNextRetryAt(1)
    const after = Date.now()

    assert.isTrue(nextRetry.getTime() >= now + 1000)
    assert.isTrue(nextRetry.getTime() <= after + 1000)
  })

  test('README retry examples compile and produce retry dates', ({ assert }) => {
    // Mirrors the "Retry & Backoff" section of the README.
    const options: JobOptions = {
      maxRetries: 5,
      retry: {
        backoff: exponentialBackoff({
          baseDelay: '1s',
          maxDelay: '1m',
          multiplier: 2,
          jitter: true,
        }),
      },
    }

    assert.instanceOf(options.retry!.backoff!().getNextRetryAt(1), Date)
    assert.equal(fixedBackoff('5s')().calculateDelay(3), 5000)
  })
})
