import { test } from '@japa/runner'
import { exponentialBackoff } from '../src/strategies/backoff_strategy.js'
import { QueueConfigResolver } from '../src/queue_config_resolver.js'

test.group('QueueConfigResolver', () => {
  test('should merge retry configurations correctly (global)', ({ assert }) => {
    const resolver = QueueConfigResolver.from({
      default: 'sync',
      adapters: { sync: () => ({}) as any },
      retry: { maxRetries: 5, backoff: exponentialBackoff() },
    })

    let config = resolver.resolveRetryConfig('default')
    assert.equal(config.maxRetries, 5)
  })

  test('should merge retry configurations correctly (queue)', ({ assert }) => {
    const resolver = QueueConfigResolver.from({
      default: 'sync',
      adapters: { sync: () => ({}) as any },
      retry: { maxRetries: 5, backoff: exponentialBackoff() },
      queues: {
        email: { retry: { maxRetries: 3 } },
      },
    })

    let config = resolver.resolveRetryConfig('email')
    assert.equal(config.maxRetries, 3)
  })

  test('should merge retry configurations correctly (job)', ({ assert }) => {
    const resolver = QueueConfigResolver.from({
      default: 'sync',
      adapters: { sync: () => ({}) as any },
      retry: { maxRetries: 5, backoff: exponentialBackoff() },
      queues: {
        email: { retry: { maxRetries: 3 } },
      },
    })

    let config = resolver.resolveRetryConfig('email', { maxRetries: 2 })
    assert.equal(config.maxRetries, 2)
  })

  test('should respect maxRetries: 0 from job config over global/queue config', ({ assert }) => {
    const resolver = QueueConfigResolver.from({
      default: 'sync',
      adapters: { sync: () => ({}) as any },
      retry: { maxRetries: 5, backoff: exponentialBackoff() },
      queues: {
        email: { retry: { maxRetries: 3 } },
      },
    })

    let config = resolver.resolveRetryConfig('email', { maxRetries: 0 })
    assert.equal(config.maxRetries, 0)
  })

  test('should resolve job retention options with correct precedence', ({ assert }) => {
    const resolver = QueueConfigResolver.from({
      default: 'sync',
      adapters: { sync: () => ({}) as any },
      defaultJobOptions: { removeOnFail: { age: '7d' } },
      queues: {
        email: {
          defaultJobOptions: {
            removeOnFail: { age: '3d' },
            removeOnComplete: { count: 50 },
          },
        },
      },
    })

    const resolved = resolver.resolveJobOptions('email', {
      removeOnComplete: false,
    })

    assert.deepEqual(resolved, {
      removeOnComplete: false,
      removeOnFail: { age: '3d' },
      timeout: undefined,
      failOnTimeout: false,
    })
  })

  test('should resolve the timeout with correct precedence', ({ assert }) => {
    const resolver = QueueConfigResolver.from({
      default: 'sync',
      adapters: { sync: () => ({}) as any },
      worker: { timeout: '1m' },
      defaultJobOptions: { timeout: '30s', failOnTimeout: true },
      queues: {
        email: { defaultJobOptions: { timeout: '10s', failOnTimeout: false } },
      },
    })

    const job = resolver.resolveJobOptions('email', { timeout: '5s', failOnTimeout: true })
    assert.equal(job.timeout, '5s')
    assert.isTrue(job.failOnTimeout)

    const queue = resolver.resolveJobOptions('email', {})
    assert.equal(queue.timeout, '10s')
    assert.isFalse(queue.failOnTimeout)

    const global = resolver.resolveJobOptions('reports', {})
    assert.equal(global.timeout, '30s')
    assert.isTrue(global.failOnTimeout)
  })

  test('should fall back to the worker timeout', ({ assert }) => {
    const resolver = QueueConfigResolver.from({
      default: 'sync',
      adapters: { sync: () => ({}) as any },
      worker: { timeout: '30s' },
    })

    assert.equal(resolver.resolveJobOptions('default').timeout, '30s')
    assert.isFalse(resolver.resolveJobOptions('default').failOnTimeout)
  })
})
