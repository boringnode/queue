import { test } from '@japa/runner'
import { setTimeout } from 'node:timers/promises'
import { memory } from './_mocks/memory_adapter.js'
import { QueueManager } from '../src/queue_manager.js'
import { JobDispatchRuntime } from '../src/job_dispatch_runtime.js'
import { dispatchChannel } from '../src/tracing_channels.js'
import type { JobDispatchMessage } from '../src/types/tracing_channels.js'

test.group('JobDispatchRuntime', () => {
  test('dispatches one complete Job through its interface', async ({ assert }) => {
    const adapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => adapter },
    })

    const runtime = new JobDispatchRuntime()
    const result = await runtime.dispatch({
      kind: 'single',
      name: 'SendEmailJob',
      payload: { to: 'user@example.com' },
    })

    const job = await adapter.pop()

    assert.isNotNull(job)
    assert.equal(job!.id, result.jobId)
    assert.equal(job!.name, 'SendEmailJob')
    assert.deepEqual(job!.payload, { to: 'user@example.com' })
    assert.equal(job!.attempts, 0)
    assert.isNumber(job!.createdAt)
  })

  test('routes static Job options through the configured queue Adapter', async ({ assert }) => {
    const defaultAdapter = memory()()
    const queueAdapter = memory()()

    await QueueManager.init({
      default: 'default',
      adapters: {
        default: () => defaultAdapter,
        queue: () => queueAdapter,
      },
      queues: {
        emails: { adapter: 'queue' },
      },
    })

    const runtime = new JobDispatchRuntime()
    await runtime.dispatch({
      kind: 'single',
      name: 'SendEmailJob',
      payload: { to: 'user@example.com' },
      jobOptions: { queue: 'emails', priority: 7 },
    })

    const job = await queueAdapter.popFrom('emails')

    assert.isNotNull(job)
    assert.equal(job!.priority, 7)
    assert.isNull(await defaultAdapter.popFrom('emails'))
  })

  test('gives fluent overrides precedence over static Job options', async ({ assert }) => {
    const defaultAdapter = memory()()
    const staticAdapter = memory()()
    const overrideAdapter = memory()()

    await QueueManager.init({
      default: 'default',
      adapters: {
        default: () => defaultAdapter,
        static: () => staticAdapter,
        override: () => overrideAdapter,
      },
    })

    const runtime = new JobDispatchRuntime()
    await runtime.dispatch({
      kind: 'single',
      name: 'SendEmailJob',
      payload: { to: 'user@example.com' },
      jobOptions: { queue: 'static-queue', adapter: 'static', priority: 8 },
      overrides: { queue: 'override-queue', adapter: 'override', priority: 2 },
    })

    const job = await overrideAdapter.popFrom('override-queue')

    assert.isNotNull(job)
    assert.equal(job!.priority, 2)
    assert.isNull(await staticAdapter.popFrom('static-queue'))
    assert.isNull(await defaultAdapter.popFrom('default'))
  })

  test('wraps the Adapter operation as an internal operation', async ({ assert }) => {
    const adapter = memory()()
    let internalOperations = 0

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => adapter },
      internalOperationWrapper: async (run) => {
        internalOperations++
        return run()
      },
    })

    const runtime = new JobDispatchRuntime()
    await runtime.dispatch({ kind: 'single', name: 'WrappedJob', payload: {} })

    assert.equal(internalOperations, 1)
  })

  test('publishes the complete dispatch message', async ({ assert, cleanup }) => {
    const adapter = memory()()
    let traceMessage: JobDispatchMessage | undefined
    const captureTrace = (message: unknown) => {
      traceMessage = { ...(message as JobDispatchMessage) }
    }

    dispatchChannel.asyncEnd.subscribe(captureTrace)
    cleanup(() => dispatchChannel.asyncEnd.unsubscribe(captureTrace))

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => adapter },
    })

    const runtime = new JobDispatchRuntime()
    await runtime.dispatch({
      kind: 'single',
      name: 'TracedJob',
      payload: {},
      jobOptions: { queue: 'traced' },
    })

    assert.equal(traceMessage?.queue, 'traced')
    assert.equal(traceMessage?.jobs[0].name, 'TracedJob')
  })

  test('routes delayed Jobs through the same interface', async ({ assert }) => {
    const adapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => adapter },
    })

    const runtime = new JobDispatchRuntime()
    await runtime.dispatch({
      kind: 'single',
      name: 'DelayedJob',
      payload: {},
      overrides: { delay: '20ms' },
    })

    assert.isNull(await adapter.pop())
    await setTimeout(30)
    assert.equal((await adapter.pop())?.name, 'DelayedJob')
  })

  test('dispatches a batch through the same interface', async ({ assert }) => {
    const adapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => adapter },
    })

    const runtime = new JobDispatchRuntime()
    const result = await runtime.dispatch({
      kind: 'batch',
      name: 'NewsletterJob',
      payloads: [{ userId: 1 }, { userId: 2 }],
      overrides: { queue: 'emails', priority: 4 },
    })

    const first = await adapter.popFrom('emails')
    const second = await adapter.popFrom('emails')

    assert.deepEqual(result.jobIds, [first?.id, second?.id])
    assert.deepEqual(
      [first, second].map((job) => ({
        name: job?.name,
        payload: job?.payload,
        priority: job?.priority,
      })),
      [
        { name: 'NewsletterJob', payload: { userId: 1 }, priority: 4 },
        { name: 'NewsletterJob', payload: { userId: 2 }, priority: 4 },
      ]
    )
  })

  test('returns the Adapter deduplication outcome', async ({ assert }) => {
    const adapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => adapter },
    })

    const runtime = new JobDispatchRuntime()
    const first = await runtime.dispatch({
      kind: 'single',
      name: 'UniqueJob',
      payload: { attempt: 1 },
      overrides: { dedup: { id: 'same' } },
    })
    const duplicate = await runtime.dispatch({
      kind: 'single',
      name: 'UniqueJob',
      payload: { attempt: 2 },
      overrides: { dedup: { id: 'same' } },
    })

    assert.deepEqual(duplicate, { jobId: first.jobId, deduped: 'skipped' })
  })

  test('records Schedule provenance on the dispatched Job', async ({ assert }) => {
    const adapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => adapter },
    })

    const runtime = new JobDispatchRuntime()
    await runtime.dispatch({
      kind: 'single',
      name: 'ScheduledJob',
      payload: {},
      scheduleId: 'daily-cleanup',
    })

    assert.equal((await adapter.pop())?.scheduleId, 'daily-cleanup')
  })
})
