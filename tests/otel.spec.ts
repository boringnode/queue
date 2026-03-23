import { test } from '@japa/runner'
import { trace, SpanStatusCode, SpanKind } from '@opentelemetry/api'
import { setupTracing, resetSpans, getFinishedSpans } from './helpers/setup_tracing.js'
import { QueueInstrumentation } from '../src/otel.js'
import { dispatchChannel, executeChannel } from '../src/tracing_channels.js'
import type { JobExecuteMessage, JobDispatchMessage } from '../src/types/tracing_channels.js'
import type { AcquiredJob } from '../src/contracts/adapter.js'
import type { JobData } from '../src/types/main.js'

function makeJob(overrides: Partial<AcquiredJob> = {}): AcquiredJob {
  return {
    id: 'job-1',
    name: 'TestJob',
    payload: {},
    attempts: 0,
    acquiredAt: Date.now(),
    ...overrides,
  }
}

/**
 * Creates an instrumentation with a fake QueueManager,
 * captures the injected wrappers from the patched init.
 */
async function setupWithWrappers(config: ConstructorParameters<typeof QueueInstrumentation>[0] = {}) {
  const instrumentation = new QueueInstrumentation(config)
  instrumentation.enable()

  let capturedConfig: any
  const fakeManager = {
    init: async (cfg: any) => { capturedConfig = cfg },
  }

  instrumentation.manuallyRegister({ QueueManager: fakeManager })
  await fakeManager.init({ default: 'memory', adapters: {} })

  return {
    instrumentation,
    fakeManager,
    executionWrapper: capturedConfig.executionWrapper as <T>(fn: () => Promise<T>, job: AcquiredJob, queue: string) => Promise<T>,
    internalOperationWrapper: capturedConfig.internalOperationWrapper as <T>(fn: () => Promise<T>) => Promise<T>,
  }
}

test.group('QueueInstrumentation | lifecycle', (group) => {
  group.setup(() => { setupTracing() })
  group.each.setup(() => resetSpans())

  test('enable() is idempotent', ({ assert }) => {
    const instrumentation = new QueueInstrumentation()
    instrumentation.enable()
    instrumentation.enable()
    assert.isTrue(instrumentation.isEnabled())
    instrumentation.disable()
  })

  test('disable() cleans up', ({ assert }) => {
    const instrumentation = new QueueInstrumentation()
    instrumentation.enable()
    instrumentation.disable()
    assert.isFalse(instrumentation.isEnabled())
  })

  test('disable() is idempotent', ({ assert }) => {
    const instrumentation = new QueueInstrumentation()
    instrumentation.enable()
    instrumentation.disable()
    instrumentation.disable()
    assert.isFalse(instrumentation.isEnabled())
  })

  test('can re-enable after disable', ({ assert }) => {
    const instrumentation = new QueueInstrumentation()
    instrumentation.enable()
    instrumentation.disable()
    instrumentation.enable()
    assert.isTrue(instrumentation.isEnabled())
    instrumentation.disable()
  })
})

test.group('QueueInstrumentation | dispatch via DC', (group) => {
  group.setup(() => { setupTracing() })
  group.each.setup(() => resetSpans())

  test('creates PRODUCER span when no active span', async ({ assert }) => {
    const { instrumentation } = await setupWithWrappers()

    const jobData: JobData = { id: 'job-1', name: 'SendEmailJob', payload: {}, attempts: 0 }
    const message: JobDispatchMessage = { jobs: [jobData], queue: 'emails' }
    await dispatchChannel.tracePromise(async () => {}, message)

    const spans = getFinishedSpans()
    assert.lengthOf(spans, 1)
    assert.equal(spans[0].name, 'publish emails')
    assert.equal(spans[0].kind, SpanKind.PRODUCER)
    assert.equal(spans[0].attributes['messaging.system'], 'boringqueue')
    assert.equal(spans[0].attributes['messaging.destination.name'], 'emails')
    assert.equal(spans[0].attributes['messaging.message.id'], 'job-1')
    assert.equal(spans[0].attributes['messaging.job.name'], 'SendEmailJob')

    instrumentation.disable()
  })

  test('creates PRODUCER span as child of active span', async ({ assert }) => {
    const { instrumentation } = await setupWithWrappers()
    const jobData: JobData = { id: 'job-2', name: 'ProcessJob', payload: {}, attempts: 0 }

    const tracer = trace.getTracer('test')
    await tracer.startActiveSpan('http-request', async (parentSpan) => {
      const message: JobDispatchMessage = { jobs: [jobData], queue: 'default' }
      await dispatchChannel.tracePromise(async () => {}, message)
      parentSpan.end()
    })

    const spans = getFinishedSpans()
    const parentSpan = spans.find((span) => span.name === 'http-request')
    const producerSpan = spans.find((span) => span.name === 'publish default')
    assert.lengthOf(spans, 2)
    assert.isDefined(parentSpan)
    assert.isDefined(producerSpan)
    assert.equal(producerSpan!.parentSpanContext?.spanId, parentSpan!.spanContext().spanId)
    assert.equal(jobData.traceContext?.traceparent?.split('-')[2], producerSpan!.spanContext().spanId)

    instrumentation.disable()
  })

  test('injects trace context into jobData', async ({ assert }) => {
    const { instrumentation } = await setupWithWrappers()

    const jobData: JobData = { id: 'job-3', name: 'TestJob', payload: {}, attempts: 0 }
    const message: JobDispatchMessage = { jobs: [jobData], queue: 'default' }
    await dispatchChannel.tracePromise(async () => {}, message)

    assert.isDefined(jobData.traceContext)
    assert.property(jobData.traceContext!, 'traceparent')

    instrumentation.disable()
  })

  test('includes delay_ms and batch_count attributes', async ({ assert }) => {
    const { instrumentation } = await setupWithWrappers()

    const jobData: JobData = { id: 'job-4', name: 'DelayedJob', payload: {}, attempts: 0 }
    const message: JobDispatchMessage = { jobs: [jobData], queue: 'default', delay: 5000 }
    await dispatchChannel.tracePromise(async () => {}, message)

    const spans = getFinishedSpans()
    assert.equal(spans[0].attributes['messaging.job.delay_ms'], 5000)

    instrumentation.disable()
  })

  test('batch dispatch injects trace context into every job', async ({ assert }) => {
    const { instrumentation } = await setupWithWrappers()

    const jobs: JobData[] = [
      { id: 'batch-1', name: 'BatchJob', payload: {}, attempts: 0 },
      { id: 'batch-2', name: 'BatchJob', payload: {}, attempts: 0 },
    ]

    await dispatchChannel.tracePromise(async () => {}, { jobs, queue: 'default' })

    const [span] = getFinishedSpans()
    assert.property(jobs[0].traceContext!, 'traceparent')
    assert.property(jobs[1].traceContext!, 'traceparent')
    assert.equal(jobs[0].traceContext!.traceparent, jobs[1].traceContext!.traceparent)
    assert.notProperty(span.attributes, 'messaging.message.id')
    assert.equal(span.attributes['messaging.batch.message_count'], 2)

    instrumentation.disable()
  })
})

test.group('QueueInstrumentation | execute via executionWrapper', (group) => {
  group.setup(() => { setupTracing() })
  group.each.setup(() => resetSpans())

  test('creates CONSUMER span with semconv attributes', async ({ assert }) => {
    const { instrumentation, executionWrapper } = await setupWithWrappers()

    const job = makeJob({ id: 'attr-1', name: 'WorkerJob' })
    const message: JobExecuteMessage = { job, queue: 'default', status: 'completed' }

    await executeChannel.tracePromise(async () => {
      await executionWrapper(async () => {}, job, 'default')
    }, message)

    const spans = getFinishedSpans()
    const span = spans.find((s) => s.kind === SpanKind.CONSUMER)
    assert.isDefined(span)
    assert.equal(span!.name, 'process default')
    assert.equal(span!.attributes['messaging.system'], 'boringqueue')
    assert.equal(span!.attributes['messaging.operation.name'], 'process')
    assert.equal(span!.attributes['messaging.destination.name'], 'default')
    assert.equal(span!.attributes['messaging.message.id'], 'attr-1')
    assert.equal(span!.attributes['messaging.job.name'], 'WorkerJob')
    assert.equal(span!.attributes['entry_point.type'], 'job')

    instrumentation.disable()
  })

  test('completed job sets OK status', async ({ assert }) => {
    const { instrumentation, executionWrapper } = await setupWithWrappers()

    const job = makeJob({ id: 'ok-1' })
    const message: JobExecuteMessage = { job, queue: 'default', status: 'completed' }

    await executeChannel.tracePromise(async () => {
      await executionWrapper(async () => {}, job, 'default')
    }, message)

    const spans = getFinishedSpans()
    const span = spans.find((s) => s.kind === SpanKind.CONSUMER)
    assert.equal(span!.status.code, SpanStatusCode.OK)
    assert.equal(span!.attributes['messaging.job.status'], 'completed')

    instrumentation.disable()
  })

  test('failed job sets ERROR status', async ({ assert }) => {
    const { instrumentation, executionWrapper } = await setupWithWrappers()

    const job = makeJob({ id: 'fail-1' })
    const error = new Error('Job crashed')
    const message: JobExecuteMessage = { job, queue: 'default', status: 'failed', error }

    await executeChannel.tracePromise(async () => {
      await executionWrapper(async () => {}, job, 'default')
    }, message)

    const spans = getFinishedSpans()
    const span = spans.find((s) => s.kind === SpanKind.CONSUMER)
    assert.equal(span!.status.code, SpanStatusCode.ERROR)

    instrumentation.disable()
  })

  test('retrying job records exception and retry event', async ({ assert }) => {
    const { instrumentation, executionWrapper } = await setupWithWrappers()

    const job = makeJob({ id: 'retry-1' })
    const message: JobExecuteMessage = {
      job,
      queue: 'default',
      status: 'retrying',
      error: new Error('Transient'),
      nextRetryAt: new Date('2025-01-01T00:00:00Z'),
    }

    await executeChannel.tracePromise(async () => {
      await executionWrapper(async () => {}, job, 'default')
    }, message)

    const spans = getFinishedSpans()
    const span = spans.find((s) => s.kind === SpanKind.CONSUMER)
    assert.equal(span!.status.code, SpanStatusCode.OK)
    assert.equal(span!.attributes['messaging.job.status'], 'retrying')

    const retryEvent = span!.events.find((e) => e.name === 'messaging.retry')
    assert.isDefined(retryEvent)

    instrumentation.disable()
  })

  test('child spans are parented to consumer span', async ({ assert }) => {
    const { instrumentation, executionWrapper } = await setupWithWrappers()

    const job = makeJob({ id: 'ctx-1' })
    const message: JobExecuteMessage = { job, queue: 'default', status: 'completed' }

    await executeChannel.tracePromise(async () => {
      await executionWrapper(async () => {
        const tracer = trace.getTracer('test-child')
        const childSpan = tracer.startSpan('child-operation')
        childSpan.end()
      }, job, 'default')
    }, message)

    const spans = getFinishedSpans()
    const consumerSpan = spans.find((s) => s.name === 'process default')
    const childSpan = spans.find((s) => s.name === 'child-operation')

    assert.isDefined(consumerSpan)
    assert.isDefined(childSpan)
    assert.equal(childSpan!.parentSpanContext?.spanId, consumerSpan!.spanContext().spanId)

    instrumentation.disable()
  })
})

test.group('QueueInstrumentation | trace linking', (group) => {
  group.setup(() => { setupTracing() })
  group.each.setup(() => resetSpans())

  test('link mode links consumer to dispatch trace', async ({ assert }) => {
    const { instrumentation, executionWrapper } = await setupWithWrappers()

    const jobData: JobData = { id: 'link-1', name: 'LinkedJob', payload: {}, attempts: 0 }
    await dispatchChannel.tracePromise(async () => {}, { jobs: [jobData], queue: 'default' })

    const dispatchTraceId = getFinishedSpans()[0].spanContext().traceId

    const job = makeJob({ id: 'link-1', name: 'LinkedJob', traceContext: jobData.traceContext })
    const message: JobExecuteMessage = { job, queue: 'default', status: 'completed' }

    await executeChannel.tracePromise(async () => {
      await executionWrapper(async () => {}, job, 'default')
    }, message)

    const consumerSpan = getFinishedSpans().find((s) => s.kind === SpanKind.CONSUMER)
    assert.isNotEmpty(consumerSpan!.links)
    assert.equal(consumerSpan!.links[0].context.traceId, dispatchTraceId)
    assert.notEqual(consumerSpan!.spanContext().traceId, dispatchTraceId)

    instrumentation.disable()
  })

  test('parent mode makes consumer child of dispatch', async ({ assert }) => {
    const { instrumentation, executionWrapper } = await setupWithWrappers({ executionSpanLinkMode: 'parent' })

    const jobData: JobData = { id: 'parent-1', name: 'ParentedJob', payload: {}, attempts: 0 }
    await dispatchChannel.tracePromise(async () => {}, { jobs: [jobData], queue: 'default' })

    const dispatchTraceId = getFinishedSpans()[0].spanContext().traceId

    const job = makeJob({ id: 'parent-1', name: 'ParentedJob', traceContext: jobData.traceContext })
    const message: JobExecuteMessage = { job, queue: 'default', status: 'completed' }

    await executeChannel.tracePromise(async () => {
      await executionWrapper(async () => {}, job, 'default')
    }, message)

    const consumerSpan = getFinishedSpans().find((s) => s.kind === SpanKind.CONSUMER)
    assert.equal(consumerSpan!.spanContext().traceId, dispatchTraceId)
    assert.isEmpty(consumerSpan!.links)

    instrumentation.disable()
  })
})

test.group('QueueInstrumentation | manuallyRegister', (group) => {
  group.setup(() => { setupTracing() })
  group.each.setup(() => resetSpans())

  test('patches init to inject wrappers', async ({ assert }) => {
    const { executionWrapper, internalOperationWrapper } = await setupWithWrappers()

    assert.isFunction(executionWrapper)
    assert.isFunction(internalOperationWrapper)
  })

  test('internalOperationWrapper suppresses tracing', async ({ assert }) => {
    const { instrumentation, internalOperationWrapper } = await setupWithWrappers()

    const tracer = trace.getTracer('test')
    await tracer.startActiveSpan('parent', async (parentSpan) => {
      await internalOperationWrapper(async () => {
        const suppressed = tracer.startSpan('should-be-suppressed')
        suppressed.end()
      })
      parentSpan.end()
    })

    const spans = getFinishedSpans()
    const suppressedSpan = spans.find((s) => s.name === 'should-be-suppressed')
    assert.isUndefined(suppressedSpan)

    instrumentation.disable()
  })
})

test.group('QueueInstrumentation | custom config', (group) => {
  group.setup(() => { setupTracing() })
  group.each.setup(() => resetSpans())

  test('custom messagingSystem attribute', async ({ assert }) => {
    const { instrumentation } = await setupWithWrappers({ messagingSystem: 'my-queue' })

    const jobData: JobData = { id: 'custom-1', name: 'Job', payload: {}, attempts: 0 }
    await dispatchChannel.tracePromise(async () => {}, { jobs: [jobData], queue: 'default' })

    const spans = getFinishedSpans()
    const span = spans.find((s) => s.attributes['messaging.system'] === 'my-queue')
    assert.isDefined(span)

    instrumentation.disable()
  })
})
