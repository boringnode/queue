import type { BackoffConfig, Duration } from '../types/main.js'
import * as errors from '../exceptions.js'
import { parse } from '../utils.js'
import { RuntimeException } from '@poppinss/utils/exception'
import { assertUnreachable } from '@poppinss/utils/assert'

/**
 * Calculates retry delays using configurable strategies.
 *
 * Supports three built-in strategies:
 * - `exponential`: Delay doubles each attempt (1s, 2s, 4s, 8s, ...)
 * - `linear`: Delay increases linearly (5s, 10s, 15s, 20s, ...)
 * - `fixed`: Same delay every time (10s, 10s, 10s, ...)
 *
 * All strategies support:
 * - `maxDelay`: Cap the maximum delay
 * - `jitter`: Add randomness to prevent thundering herd
 *
 * @example
 * ```typescript
 * const strategy = new BackoffStrategy({
 *   strategy: 'exponential',
 *   baseDelay: '1s',
 *   maxDelay: '5m',
 *   multiplier: 2,
 *   jitter: true,
 * })
 *
 * strategy.calculateDelay(1) // ~1000ms
 * strategy.calculateDelay(2) // ~2000ms
 * strategy.calculateDelay(3) // ~4000ms
 * ```
 */
export class BackoffStrategy {
  readonly #config: BackoffConfig

  /**
   * Create a new backoff strategy.
   *
   * @param config - Backoff configuration
   * @throws {E_INVALID_BASE_DELAY} If baseDelay is not positive
   * @throws {E_INVALID_MAX_DELAY} If maxDelay is invalid
   * @throws {E_INVALID_MULTIPLIER} If multiplier is invalid
   */
  constructor(config: BackoffConfig) {
    this.#config = config
    this.#validateConfig()
  }

  /**
   * Calculate the delay for a given attempt number.
   *
   * @param attempt - The attempt number (1-based)
   * @returns Delay in milliseconds
   * @throws {RuntimeException} If attempt is less than 1
   *
   * @example
   * ```typescript
   * // Exponential: 1s, 2s, 4s, 8s, 16s, ...
   * strategy.calculateDelay(1) // 1000
   * strategy.calculateDelay(2) // 2000
   * strategy.calculateDelay(3) // 4000
   * ```
   */
  calculateDelay(attempt: number): number {
    if (attempt < 1) {
      throw new RuntimeException('Attempt number must be >= 1')
    }

    const baseDelayMs = parse(this.#config.baseDelay)
    const maxDelayMs = this.#config.maxDelay ? parse(this.#config.maxDelay) : Infinity
    const multiplier = this.#config.multiplier ?? 2

    let delay: number

    switch (this.#config.strategy) {
      case 'exponential':
        delay = baseDelayMs * Math.pow(multiplier, attempt - 1)
        break
      case 'linear':
        delay = baseDelayMs * attempt
        break
      case 'fixed':
        delay = baseDelayMs
        break
      default:
        assertUnreachable(this.#config.strategy)
    }

    // Apply max delay limit
    delay = Math.min(delay, maxDelayMs)

    if (this.#config.jitter) {
      delay = this.#applyJitter(delay)
    }

    return Math.floor(delay)
  }

  /**
   * Get the Date when the next retry should occur.
   *
   * @param attempt - The attempt number (1-based)
   * @returns Date for the next retry
   *
   * @example
   * ```typescript
   * const nextRetry = strategy.getNextRetryAt(3)
   * console.log(`Retry at: ${nextRetry.toISOString()}`)
   * ```
   */
  getNextRetryAt(attempt: number): Date {
    const delay = this.calculateDelay(attempt)
    return new Date(Date.now() + delay)
  }

  /**
   * Get a frozen copy of the configuration.
   *
   * @returns Readonly configuration object
   */
  getConfig(): Readonly<BackoffConfig> {
    return Object.freeze({ ...this.#config })
  }

  #validateConfig() {
    const baseDelayMs = parse(this.#config.baseDelay)

    if (baseDelayMs <= 0) {
      throw new errors.E_INVALID_BASE_DELAY([
        'Base delay must be a positive integer greater than zero',
      ])
    }

    if (this.#config.maxDelay) {
      const maxDelayMs = parse(this.#config.maxDelay)

      if (maxDelayMs <= 0) {
        throw new errors.E_INVALID_MAX_DELAY([
          'Max delay must be a positive integer greater than zero',
        ])
      }

      if (maxDelayMs <= baseDelayMs) {
        throw new errors.E_INVALID_MAX_DELAY(['Max delay should be greater than base delay'])
      }
    }

    if (this.#config.multiplier !== undefined) {
      if (this.#config.multiplier <= 0) {
        throw new errors.E_INVALID_MULTIPLIER([
          'Multiplier must be a positive number greater than zero',
        ])
      }

      if (this.#config.strategy === 'exponential' && this.#config.multiplier < 1) {
        throw new errors.E_INVALID_MULTIPLIER(['Exponential strategy multiplier should be >= 1'])
      }
    }
  }

  #applyJitter(delay: number): number {
    const jitterRange = delay * 0.25
    const jitter = (Math.random() - 0.5) * 2 * jitterRange

    return Math.max(0, delay + jitter)
  }
}

/**
 * Create an exponential backoff strategy factory.
 *
 * Delay doubles with each attempt: about 1s → 2s → 4s → 8s → ... The
 * default jitter moves each delay by up to ±25%; pass `jitter: false` for
 * exact delays.
 *
 * Default config:
 * - baseDelay: 1s
 * - maxDelay: 5m
 * - multiplier: 2
 * - jitter: true
 *
 * @param config - Optional overrides for default config
 * @returns Factory function for creating BackoffStrategy instances
 *
 * @example
 * ```typescript
 * const config = {
 *   retry: {
 *     maxRetries: 5,
 *     backoff: exponentialBackoff({ baseDelay: '500ms', maxDelay: '1m' }),
 *   },
 * }
 * ```
 */
export function exponentialBackoff(config?: Partial<Omit<BackoffConfig, 'strategy'>>) {
  return () =>
    new BackoffStrategy({
      strategy: 'exponential',
      baseDelay: '1s',
      maxDelay: '5m',
      multiplier: 2,
      jitter: true,
      ...config,
    })
}

/**
 * Create a linear backoff strategy factory.
 *
 * Delay increases linearly: 5s → 10s → 15s → 20s → ...
 *
 * Default config:
 * - baseDelay: 5s
 * - maxDelay: 2m
 *
 * @param config - Optional overrides for default config
 * @returns Factory function for creating BackoffStrategy instances
 *
 * @example
 * ```typescript
 * const config = {
 *   retry: {
 *     maxRetries: 3,
 *     backoff: linearBackoff({ baseDelay: '10s' }),
 *   },
 * }
 * ```
 */
export function linearBackoff(config?: Partial<Omit<BackoffConfig, 'strategy'>>) {
  return () =>
    new BackoffStrategy({
      strategy: 'linear',
      baseDelay: '5s',
      maxDelay: '2m',
      ...config,
    })
}

/**
 * Create a fixed delay backoff strategy factory.
 *
 * Same delay every time: 10s → 10s → 10s → ...
 *
 * @param delay - The fixed delay (default: '10s')
 * @returns Factory function for creating BackoffStrategy instances
 *
 * @example
 * ```typescript
 * const config = {
 *   retry: {
 *     maxRetries: 3,
 *     backoff: fixedBackoff('30s'),
 *   },
 * }
 * ```
 */
export function fixedBackoff(delay: Duration = '10s') {
  return () =>
    new BackoffStrategy({
      strategy: 'fixed',
      baseDelay: delay,
    })
}

/**
 * Create a custom backoff strategy factory.
 *
 * Use this when you need full control over the configuration.
 *
 * @param config - Complete backoff configuration
 * @returns Factory function for creating BackoffStrategy instances
 *
 * @example
 * ```typescript
 * const config = {
 *   retry: {
 *     maxRetries: 5,
 *     backoff: customBackoff({
 *       strategy: 'exponential',
 *       baseDelay: '100ms',
 *       maxDelay: '30s',
 *       multiplier: 3,
 *       jitter: false,
 *     }),
 *   },
 * }
 * ```
 */
export function customBackoff(config: BackoffConfig) {
  return () => new BackoffStrategy(config)
}
