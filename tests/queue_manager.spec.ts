import { test } from '@japa/runner'
import * as errors from '../src/exceptions.js'
import { QueueManager } from '../src/queue_manager.js'
import { sync } from '../src/drivers/sync_adapter.js'
import { MemoryLogger } from './_mocks/memory_logger.js'
import type { Adapter } from '../src/contracts/adapter.js'

test.group('QueueManager', () => {
  test('should validate adapter presence', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: 'sync',
        adapters: {},
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(
        error.message,
        'Configuration error. Reason: At least one adapter must be configured'
      )
    }
  })

  test('should validate default adapter presence', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: '',
        adapters: { sync: sync() },
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(error.message, 'Configuration error. Reason: Default adapter must be specified')
    }
  })

  test('should validate that adapter is a function', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: 'sync',
        adapters: { sync: 'not-a-function' as any },
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(
        error.message,
        'Configuration error. Reason: Adapter "sync" must be a factory function'
      )
    }
  })

  test('should validate default adapter existence in adapters', async ({ assert }) => {
    assert.plan(2)

    try {
      await QueueManager.init({
        default: 'missing',
        adapters: { sync: sync() },
      })
    } catch (error) {
      assert.instanceOf(error, errors.E_CONFIGURATION_ERROR)
      assert.equal(
        error.message,
        'Configuration error. Reason: Default adapter "missing" not found in adapters configuration'
      )
    }
  })

  test('should expose a config resolver after initialization', async ({ assert }) => {
    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
    })

    const resolver = QueueManager.getConfigResolver()

    assert.exists(resolver)
  })

  test('should expose the configured logger', async ({ assert }) => {
    const logger = new MemoryLogger()

    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
      logger,
    })

    assert.strictEqual(QueueManager.getLogger(), logger)
  })

  test('should throw E_QUEUE_NOT_INITIALIZED when use() called before init()', async ({
    assert,
  }) => {
    assert.plan(2)

    await QueueManager.destroy()

    try {
      QueueManager.use()
    } catch (error) {
      assert.instanceOf(error, errors.E_QUEUE_NOT_INITIALIZED)
      assert.equal(
        error.message,
        'QueueManager is not initialized. Call QueueManager.init() before using it.'
      )
    }
  })

  test('should throw E_ADAPTER_INIT_ERROR when adapter factory throws', async ({ assert }) => {
    assert.plan(2)

    await QueueManager.init({
      default: 'broken',
      adapters: {
        broken: () => {
          throw new Error('Connection failed')
        },
      },
    })

    try {
      QueueManager.use()
    } catch (error) {
      assert.instanceOf(error, errors.E_ADAPTER_INIT_ERROR)
      assert.equal(
        error.message,
        'Failed to initialize adapter "broken". Reason: Connection failed'
      )
    }
  })

  test('should log warning when locations match no jobs', async ({ assert }) => {
    const logger = new MemoryLogger()

    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
      locations: ['./non-existent-path/**/*.ts'],
      logger,
    })

    assert.equal(logger.logs.length, 1)
    assert.equal(logger.logs[0].level, 'warn')
    assert.include(logger.logs[0].message, 'No jobs found for locations')
  })

  test('should fake adapters and restore them', async ({ assert }) => {
    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
    })

    const original = QueueManager.use()
    const fakeAdapter = QueueManager.fake()

    assert.strictEqual(QueueManager.use(), fakeAdapter)

    QueueManager.restore()

    assert.strictEqual(QueueManager.use(), original)

    await QueueManager.destroy()
  })

  test('should restore fake using Symbol.dispose', async ({ assert }) => {
    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
    })

    const original = QueueManager.use()

    {
      using fake = QueueManager.fake()
      assert.strictEqual(QueueManager.use(), fake)
    }

    assert.strictEqual(QueueManager.use(), original)

    await QueueManager.destroy()
  })

  test('should destroy existing adapter instances before reinitializing', async ({
    assert,
    cleanup,
  }) => {
    const adapters: Adapter[] = []
    let destroyedCount = 0

    const createAdapter = (): Adapter => ({
      setWorkerId() {},
      pop: async () => null,
      popFrom: async () => null,
      recoverStalledJobs: async () => 0,
      completeJob: async () => {},
      failJob: async () => {},
      retryJob: async () => {},
      getJob: async () => null,
      push: async () => {},
      pushOn: async () => {},
      pushLater: async () => {},
      pushLaterOn: async () => {},
      pushMany: async () => {},
      pushManyOn: async () => {},
      size: async () => 0,
      sizeOf: async () => 0,
      destroy: async () => {
        destroyedCount++
      },
      upsertSchedule: async () => 'schedule-id',
      createSchedule: async () => 'schedule-id',
      getSchedule: async () => null,
      listSchedules: async () => [],
      updateSchedule: async () => {},
      deleteSchedule: async () => {},
      claimDueSchedule: async () => null,
    })

    cleanup(async () => {
      await QueueManager.destroy()
    })

    await QueueManager.init({
      default: 'custom',
      adapters: {
        custom: () => {
          const adapter = createAdapter()
          adapters.push(adapter)
          return adapter
        },
      },
    })

    const firstAdapter = QueueManager.use()

    await QueueManager.init({
      default: 'custom',
      adapters: {
        custom: () => {
          const adapter = createAdapter()
          adapters.push(adapter)
          return adapter
        },
      },
    })

    const secondAdapter = QueueManager.use()

    assert.strictEqual(firstAdapter, adapters[0])
    assert.strictEqual(secondAdapter, adapters[1])
    assert.equal(destroyedCount, 1)
  })

  test('should reset fake state when reinitializing', async ({ assert, cleanup }) => {
    type LabeledAdapter = Adapter & { label: string }

    const createAdapter = (label: string): LabeledAdapter => ({
      label,
      setWorkerId() {},
      pop: async () => null,
      popFrom: async () => null,
      recoverStalledJobs: async () => 0,
      completeJob: async () => {},
      failJob: async () => {},
      retryJob: async () => {},
      getJob: async () => null,
      push: async () => {},
      pushOn: async () => {},
      pushLater: async () => {},
      pushLaterOn: async () => {},
      pushMany: async () => {},
      pushManyOn: async () => {},
      size: async () => 0,
      sizeOf: async () => 0,
      destroy: async () => {},
      upsertSchedule: async () => 'schedule-id',
      createSchedule: async () => 'schedule-id',
      getSchedule: async () => null,
      listSchedules: async () => [],
      updateSchedule: async () => {},
      deleteSchedule: async () => {},
      claimDueSchedule: async () => null,
    })

    cleanup(async () => {
      await QueueManager.destroy()
    })

    await QueueManager.init({
      default: 'custom',
      adapters: {
        custom: () => createAdapter('first'),
      },
    })

    const firstAdapter = QueueManager.use() as LabeledAdapter
    const firstFakeAdapter = QueueManager.fake()

    await QueueManager.init({
      default: 'custom',
      adapters: {
        custom: () => createAdapter('second'),
      },
    })

    const secondFakeAdapter = QueueManager.fake()

    assert.notStrictEqual(secondFakeAdapter, firstFakeAdapter)
    assert.strictEqual(QueueManager.use(), secondFakeAdapter)

    QueueManager.restore()

    const restoredAdapter = QueueManager.use() as LabeledAdapter

    assert.notStrictEqual(restoredAdapter, firstAdapter)
    assert.equal(restoredAdapter.label, 'second')
  })
})
