import {
  context,
  propagation,
  trace,
  SpanKind,
  SpanStatusCode,
  ROOT_CONTEXT,
  type Span,
  type Link,
} from '@opentelemetry/api'
import type { TracingChannelSubscribers } from 'node:diagnostics_channel'
import { suppressTracing } from '@opentelemetry/core'
import { InstrumentationBase } from '@opentelemetry/instrumentation'
import type { InstrumentationConfig } from '@opentelemetry/instrumentation'
import { dispatchChannel, executeChannel } from './tracing_channels.js'
import type { AcquiredJob } from './contracts/adapter.js'
import type { JobDispatchMessage, JobExecuteMessage } from './types/tracing_channels.js'

export interface QueueInstrumentationConfig extends InstrumentationConfig {
  /**
   * How execution spans relate to the dispatch span.
   *
   * - `'link'` (default): Independent trace, linked to dispatch span
   * - `'parent'`: Child of the dispatch span (same trace)
   */
  executionSpanLinkMode?: 'link' | 'parent'

  /**
   * The messaging system identifier.
   *
   * @default 'boringqueue'
   */
  messagingSystem?: string
}

/**
 * OpenTelemetry instrumentation for @boringnode/queue.
 *
 * Creates PRODUCER spans for job dispatch and CONSUMER spans for
 * job execution, following OTel messaging semantic conventions.
 *
 * Uses `diagnostics_channel` for span lifecycle management and
 * patches `QueueManager.init()` to inject wrappers automatically.
 */
export class QueueInstrumentation extends InstrumentationBase<QueueInstrumentationConfig> {
  protected subscribed = false
  protected executeSpans = new Map<string, Span>()
  protected dispatchSpans = new WeakMap<JobDispatchMessage, Span>()
  protected executeHandlers?: TracingChannelSubscribers<JobExecuteMessage>
  protected dispatchHandlers?: TracingChannelSubscribers<JobDispatchMessage>

  #originalInit?: (...args: any[]) => any
  #patchedManager?: { init: (...args: any[]) => any }

  constructor(config: QueueInstrumentationConfig = {}) {
    super('@boringnode/queue', '0.1.0', config)
  }

  get #messagingSystem(): string {
    return this.getConfig().messagingSystem ?? 'boringqueue'
  }

  get #executionSpanLinkMode(): 'link' | 'parent' {
    return this.getConfig().executionSpanLinkMode ?? 'link'
  }

  /**
   * Required by InstrumentationBase. Returns undefined since we use
   * diagnostics_channel instead of module patching.
   */
  protected init() {
    return undefined
  }

  /**
   * Subscribes to diagnostics_channels for span lifecycle.
   */
  enable() {
    super.enable()
    if (this.subscribed !== undefined) this.#subscribe()
  }

  /**
   * Unsubscribes from diagnostics_channels and restores patched methods.
   */
  disable() {
    if (this.subscribed !== undefined) {
      this.#unsubscribe()
      this.#unpatchInit()
    }

    super.disable()
  }

  /**
   * Patches `QueueManager.init()` to auto-inject OTel wrappers
   * and subscribes to diagnostics_channels.
   */
  manuallyRegister(queueModule: { QueueManager: { init: (...args: any[]) => any } }) {
    this.#patchInit(queueModule.QueueManager)
    this.#subscribe()
  }

  #patchInit(manager: { init: (...args: any[]) => any }) {
    if (this.#originalInit) return

    this.#patchedManager = manager
    this.#originalInit = manager.init.bind(manager)
    const instrumentation = this

    manager.init = async (config: any) => {
      return this.#originalInit!({
        ...config,
        internalOperationWrapper: <T>(fn: () => Promise<T>) => {
          return context.with(suppressTracing(context.active()), fn)
        },
        executionWrapper: <T>(fn: () => Promise<T>, job: AcquiredJob, queue: string) => {
          return instrumentation.#wrapExecution(fn, job, queue)
        },
      })
    }
  }

  #unpatchInit() {
    if (!this.#originalInit || !this.#patchedManager) return

    this.#patchedManager.init = this.#originalInit
    this.#originalInit = undefined
    this.#patchedManager = undefined
  }

  #subscribe() {
    if (this.subscribed) return
    if (!this.isEnabled()) return

    this.subscribed = true

    this.executeHandlers = {
      start: () => {},
      end: () => {},
      asyncStart: () => {},
      asyncEnd: (msg) => this.#handleExecuteAsyncEnd(msg as unknown as JobExecuteMessage),
      error: () => {},
    }

    this.dispatchHandlers = {
      start: (msg) => this.#handleDispatchStart(msg as unknown as JobDispatchMessage),
      end: () => {},
      asyncStart: () => {},
      asyncEnd: (msg) => this.#handleDispatchAsyncEnd(msg as unknown as JobDispatchMessage),
      error: () => {},
    }

    executeChannel.subscribe(this.executeHandlers as any)
    dispatchChannel.subscribe(this.dispatchHandlers as any)
  }

  #unsubscribe() {
    if (!this.subscribed) return

    if (this.executeHandlers) executeChannel.unsubscribe(this.executeHandlers as any)
    if (this.dispatchHandlers) dispatchChannel.unsubscribe(this.dispatchHandlers as any)

    this.subscribed = false
    this.executeHandlers = undefined
    this.dispatchHandlers = undefined
    this.executeSpans.clear()
    this.dispatchSpans = new WeakMap()
  }

  /**
   * Called on dispatchChannel `start` — injects trace context into jobData
   * and creates/enriches a PRODUCER span.
   */
  #handleDispatchStart(message: JobDispatchMessage) {
    const attributes = this.#buildDispatchAttributes(message)
    const span = this.tracer.startSpan(`publish ${message.queue}`, {
      kind: SpanKind.PRODUCER,
      attributes,
    })

    const dispatchContext = trace.setSpan(context.active(), span)
    for (const job of message.jobs) {
      if (!job.traceContext) job.traceContext = {}
      propagation.inject(dispatchContext, job.traceContext)
    }

    this.dispatchSpans.set(message, span)
  }

  #handleDispatchAsyncEnd(message: JobDispatchMessage) {
    const span = this.dispatchSpans.get(message)
    if (!span) return

    if (message.error) {
      span.recordException(message.error)
      span.setStatus({ code: SpanStatusCode.ERROR, message: message.error.message })
    }

    span.end()
    this.dispatchSpans.delete(message)
  }

  /**
   * Called by `executionWrapper` config — creates CONSUMER span and wraps
   * execution in OTel context for proper child span parenting.
   */
  #wrapExecution<T>(fn: () => Promise<T>, job: AcquiredJob, queue: string): Promise<T> {
    const extractedContext = this.#extractParentContext(job.traceContext)
    const parentSpanContext = trace.getSpanContext(extractedContext)

    let baseContext: typeof extractedContext
    let links: Link[]

    if (this.#executionSpanLinkMode === 'parent' && parentSpanContext) {
      baseContext = extractedContext
      links = []
    } else {
      links = parentSpanContext ? [{ context: parentSpanContext }] : []
      baseContext = ROOT_CONTEXT
    }

    const span = this.tracer.startSpan(
      `process ${queue}`,
      {
        kind: SpanKind.CONSUMER,
        attributes: this.#buildExecuteAttributes(job, queue),
        links,
      },
      baseContext
    )

    this.executeSpans.set(job.id, span)
    const executionContext = trace.setSpan(baseContext, span)

    return context.with(executionContext, fn)
  }

  #handleExecuteAsyncEnd(message: JobExecuteMessage) {
    const span = this.executeSpans.get(message.job.id)
    if (!span) return

    if (message.status) span.setAttribute('messaging.job.status', message.status)
    if (message.error) span.recordException(message.error)

    if (message.status === 'retrying' && message.nextRetryAt) {
      span.addEvent('messaging.retry', {
        'messaging.message.retry.count': message.job.attempts + 1,
        'messaging.job.retry_at': message.nextRetryAt.toISOString(),
      })
    }

    if (message.status === 'failed') {
      span.setStatus({ code: SpanStatusCode.ERROR, message: message.error?.message })
    } else {
      span.setStatus({ code: SpanStatusCode.OK })
    }

    span.end()
    this.executeSpans.delete(message.job.id)
  }

  #extractParentContext(traceContext?: Record<string, string>) {
    if (!traceContext || Object.keys(traceContext).length === 0) return ROOT_CONTEXT
    return propagation.extract(ROOT_CONTEXT, traceContext)
  }

  #buildDispatchAttributes(message: JobDispatchMessage) {
    const firstJob = message.jobs[0]
    const attributes: Record<string, string | number | boolean> = {
      'messaging.system': this.#messagingSystem,
      'messaging.operation.name': 'publish',
      'messaging.operation.type': 'send',
      'messaging.destination.name': message.queue,
      'messaging.job.name': firstJob.name,
    }

    if (message.jobs.length === 1) attributes['messaging.message.id'] = firstJob.id
    if (message.jobs.length > 1) attributes['messaging.batch.message_count'] = message.jobs.length
    if (firstJob.groupId) attributes['messaging.job.group_id'] = firstJob.groupId
    if (firstJob.priority !== undefined) attributes['messaging.job.priority'] = firstJob.priority
    if (message.delay !== undefined) attributes['messaging.job.delay_ms'] = message.delay

    return attributes
  }

  #buildExecuteAttributes(job: AcquiredJob, queue: string) {
    const attributes: Record<string, string | number | boolean> = {
      'entry_point.type': 'job',
      'messaging.system': this.#messagingSystem,
      'messaging.operation.name': 'process',
      'messaging.operation.type': 'process',
      'messaging.destination.name': queue,
      'messaging.message.id': job.id,
      'messaging.message.retry.count': job.attempts,
      'messaging.job.name': job.name,
    }

    if (job.groupId) attributes['messaging.job.group_id'] = job.groupId
    if (job.priority !== undefined) attributes['messaging.job.priority'] = job.priority

    return attributes
  }
}
