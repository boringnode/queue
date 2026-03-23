import { trace, context, propagation } from '@opentelemetry/api'
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base'
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks'
import { W3CTraceContextPropagator } from '@opentelemetry/core'

let initialized = false
let exporter: InMemorySpanExporter
let provider: BasicTracerProvider

export function setupTracing() {
  if (!initialized) {
    exporter = new InMemorySpanExporter()
    const contextManager = new AsyncLocalStorageContextManager()

    context.setGlobalContextManager(contextManager.enable())
    propagation.setGlobalPropagator(new W3CTraceContextPropagator())
    provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] })

    trace.setGlobalTracerProvider(provider)
    initialized = true
  }

  return { exporter, provider }
}

export function resetSpans() {
  exporter?.reset()
}

export function getFinishedSpans() {
  return exporter?.getFinishedSpans() ?? []
}
