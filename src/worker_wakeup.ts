/**
 * Lets an idle worker sleep until an adapter signals that work may be due.
 *
 * Signals for now wake the current wait, or the next one when they arrive
 * while the worker is looking for work. Signals for later are kept as timers,
 * so a delayed job, a retry, or a schedule wakes the worker when it is due.
 *
 * ```
 * cycle:  reset() ─► stalled check, schedules, pop ─► wait(maxDelay)
 *                     ▲ notify(now) here sets `#requested`: the wait returns at once
 * ```
 */
export class WorkerWakeup {
  /** Bounds the timers kept: past it, the latest are dropped and found by polling. */
  static readonly MAX_TIMERS = 1024

  #requested = false
  /** Future due times, sorted ascending, without duplicates. */
  #timers: number[] = []
  /** Re-evaluates the current wait. */
  #interrupt?: () => void

  notify(at: number): void {
    if (at <= Date.now()) {
      this.#requested = true
    } else if (!this.#insert(at)) {
      return
    }

    this.#interrupt?.()
  }

  /**
   * Call before looking for work: the signals received until then are
   * covered by this look, those received from now on wake the next wait.
   */
  reset(): void {
    this.#requested = false
    const now = Date.now()
    const due = this.#timers.findIndex((at) => at > now)
    this.#timers.splice(0, due === -1 ? this.#timers.length : due)
  }

  /**
   * Resolve on a signal for now, when the next timer is due, after
   * `maxDelay`, or when `signal` aborts.
   */
  async wait(maxDelay: number, signal: AbortSignal): Promise<void> {
    const deadline = Date.now() + maxDelay

    while (!signal.aborted && !this.#requested) {
      const now = Date.now()
      const until = Math.min(deadline, this.#timers[0] ?? Infinity)
      if (until <= now) return

      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer)
          signal.removeEventListener('abort', done)
          this.#interrupt = undefined
          resolve()
        }
        const timer = setTimeout(done, until - now)
        signal.addEventListener('abort', done, { once: true })
        this.#interrupt = done
      })
    }
  }

  /** Returns false when `at` was already known or past the bound. */
  #insert(at: number): boolean {
    let index = this.#timers.findIndex((timer) => timer >= at)
    if (index === -1) index = this.#timers.length
    if (this.#timers[index] === at) return false
    if (index >= WorkerWakeup.MAX_TIMERS) return false

    this.#timers.splice(index, 0, at)
    if (this.#timers.length > WorkerWakeup.MAX_TIMERS) this.#timers.pop()
    return true
  }
}
