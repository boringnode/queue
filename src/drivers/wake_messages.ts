/**
 * Receives the time (epoch ms) at which work may become due: now for a job
 * ready to run, later for a delayed job, a retry, or a schedule.
 */
export type WakeListener = (at: number) => void

/**
 * Message published on the LISTEN channel of the Knex adapter.
 *
 * - `{ queue, at }`: a job of `queue` becomes due at `at` (epoch ms).
 * - `{ queue }`: a job of `queue` is ready now. Without a timestamp, a
 *   writer whose clock runs ahead cannot delay the watcher.
 * - `{ schedule }`: the next schedule run after a schedule changed, read by
 *   the writer so watchers do not each read it.
 * - `{ probe }`: sent by a watcher to check that its connection receives
 *   notifications. Other watchers ignore it.
 */
export type WakeMessage = { queue: string; at?: number } | { schedule: number } | { probe: string }

export function encodeWakeMessage(message: WakeMessage): string {
  return JSON.stringify(message)
}

/**
 * Route a raw message of the wake channel: a job of a watched queue to
 * `onWake`, the next schedule run to `onSchedule`. Others are ignored.
 */
export function dispatchWakeMessage(
  raw: string,
  queues: readonly string[],
  onWake: WakeListener,
  onSchedule: WakeListener
): void {
  let message: Partial<{ queue: unknown; at: unknown; schedule: unknown }>

  try {
    message = JSON.parse(raw)
  } catch {
    return
  }

  if (typeof message.schedule === 'number') {
    onSchedule(message.schedule)
  } else if (typeof message.queue === 'string' && queues.includes(message.queue)) {
    onWake(typeof message.at === 'number' ? message.at : Date.now())
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
