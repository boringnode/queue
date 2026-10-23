/**
 * Receives the time (epoch ms) at which work may become due: now for a job
 * ready to run, later for a delayed job, a retry, or a schedule.
 */
export type WakeListener = (at: number) => void

/**
 * Message published on the LISTEN channel of the Knex adapter.
 *
 * - `{ ready, due }`: jobs of the `ready` queues are ready now, and a job of
 *   each `due` queue becomes due at its time (epoch ms). Without a time, a
 *   writer whose clock runs ahead cannot delay the watcher.
 * - `{ schedule }`: the next schedule run after a schedule changed, read by
 *   the writer so watchers do not each read it.
 * - `{ probe }`: sent by a watcher to check that its connection receives
 *   notifications. Other watchers ignore it.
 */
export type WakeMessage =
  | { ready?: string[]; due?: Record<string, number> }
  | { schedule: number }
  | { probe: string }

export function encodeWakeMessage(message: WakeMessage): string {
  return JSON.stringify(message)
}

/**
 * Route a raw message of the wake channel: jobs of the watched queues to
 * `onWake`, the next schedule run to `onSchedule`. Others are ignored.
 */
export function dispatchWakeMessage(
  raw: string,
  queues: readonly string[],
  onWake: WakeListener,
  onSchedule: WakeListener
): void {
  let message: Partial<{ ready: unknown; due: unknown; schedule: unknown }> | null

  try {
    message = JSON.parse(raw)
  } catch {
    return
  }
  // Any role allowed to connect can notify the channel, with any payload.
  if (!message || typeof message !== 'object') return

  if (typeof message.schedule === 'number') {
    onSchedule(message.schedule)
    return
  }

  if (Array.isArray(message.ready) && message.ready.some((queue) => queues.includes(queue))) {
    onWake(Date.now())
  }
  if (message.due && typeof message.due === 'object') {
    for (const [queue, at] of Object.entries(message.due)) {
      if (typeof at === 'number' && queues.includes(queue)) onWake(at)
    }
  }
}

/**
 * How often a watcher checks its connection, and how long the connection has
 * to answer: a PING on the LISTEN connection, or the timeout of BZPOPMIN on
 * Redis. Tests shorten them.
 */
export const subscriptionHeartbeat = { interval: 10_000, timeout: 5_000 }

/**
 * Ping a subscription connection on an interval. A connection can stop
 * delivering without closing, behind a stuck proxy or a half-open TCP
 * connection: `onSilent` is called when a ping gets no answer in time, to
 * reconnect. `ping` returns undefined to skip a beat, while reconnecting.
 *
 * Returns a function that stops the heartbeat.
 */
export function startHeartbeat(
  ping: () => Promise<unknown> | undefined,
  onSilent: () => void
): () => void {
  let timeout: NodeJS.Timeout | undefined

  const interval = setInterval(() => {
    if (timeout) return
    const answer = ping()
    if (!answer) return

    const timer = setTimeout(() => {
      timeout = undefined
      onSilent()
    }, subscriptionHeartbeat.timeout)
    timeout = timer
    const answered = () => {
      clearTimeout(timer)
      if (timeout === timer) timeout = undefined
    }
    answer.then(answered, answered)
  }, subscriptionHeartbeat.interval)
  interval.unref()

  return () => {
    clearInterval(interval)
    clearTimeout(timeout)
  }
}
