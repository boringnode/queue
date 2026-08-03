import { MemoryAdapter } from './memory_adapter.js'

/** Controls when calls to one adapter operation may continue or fail, using 1-based call numbers. */
export class OperationController {
  #callCount = 0
  #gates = new Map<number, PromiseWithResolvers<void>>()
  #failures = new Map<number, unknown>()
  #startedWaiters = new Map<number, PromiseWithResolvers<void>>()
  #settledCalls = new Set<number>()
  #settledWaiters = new Map<number, PromiseWithResolvers<void>>()

  get calls(): number {
    return this.#callCount
  }

  block(...calls: number[]): this {
    for (const call of calls) {
      if (!this.#gates.has(call)) {
        this.#gates.set(call, Promise.withResolvers<void>())
      }
    }

    return this
  }

  fail(call: number, error: unknown): this {
    this.#failures.set(call, error)
    return this
  }

  waitForStarted(count = 1): Promise<void> {
    if (this.#callCount >= count) {
      return Promise.resolve()
    }

    let waiter = this.#startedWaiters.get(count)
    if (!waiter) {
      waiter = Promise.withResolvers<void>()
      this.#startedWaiters.set(count, waiter)
    }

    return waiter.promise
  }

  waitForSettled(call: number): Promise<void> {
    if (this.#settledCalls.has(call)) {
      return Promise.resolve()
    }

    let waiter = this.#settledWaiters.get(call)
    if (!waiter) {
      waiter = Promise.withResolvers<void>()
      this.#settledWaiters.set(call, waiter)
    }

    return waiter.promise
  }

  release(...calls: number[]): void {
    for (const call of calls) {
      this.#gates.get(call)?.resolve()
      this.#gates.delete(call)
    }
  }

  releaseAll(): void {
    this.release(...this.#gates.keys())
  }

  async run<T>(operation: () => T | Promise<T>): Promise<T> {
    const call = ++this.#callCount

    for (const [count, waiter] of this.#startedWaiters) {
      if (call >= count) {
        waiter.resolve()
        this.#startedWaiters.delete(count)
      }
    }

    try {
      await this.#gates.get(call)?.promise

      if (this.#failures.has(call)) {
        throw this.#failures.get(call)
      }

      return await operation()
    } finally {
      this.#settledCalls.add(call)
      this.#settledWaiters.get(call)?.resolve()
      this.#settledWaiters.delete(call)
    }
  }
}

/** Memory adapter with deterministic controls for worker lifecycle tests. */
export class ControllableAdapter extends MemoryAdapter {
  readonly acquisitions = new OperationController()
  readonly finalizations = new OperationController()
  readonly stalledChecks = new OperationController()
  readonly destruction = new OperationController()
  readonly polledQueues: string[] = []

  override async popFrom(queue: string) {
    this.polledQueues.push(queue)
    return this.acquisitions.run(() => super.popFrom(queue))
  }

  override async completeJob(...args: Parameters<MemoryAdapter['completeJob']>): Promise<void> {
    return this.finalizations.run(() => super.completeJob(...args))
  }

  override async failJob(...args: Parameters<MemoryAdapter['failJob']>): Promise<void> {
    return this.finalizations.run(() => super.failJob(...args))
  }

  override async retryJob(...args: Parameters<MemoryAdapter['retryJob']>): Promise<void> {
    return this.finalizations.run(() => super.retryJob(...args))
  }

  override async recoverStalledJobs(
    ...args: Parameters<MemoryAdapter['recoverStalledJobs']>
  ): Promise<number> {
    return this.stalledChecks.run(() => super.recoverStalledJobs(...args))
  }

  override async destroy(): Promise<void> {
    return this.destruction.run(() => super.destroy())
  }

  releaseAll(): void {
    this.acquisitions.releaseAll()
    this.finalizations.releaseAll()
    this.stalledChecks.releaseAll()
    this.destruction.releaseAll()
  }
}
