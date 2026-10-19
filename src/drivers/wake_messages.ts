import type { WakeListener } from '../contracts/adapter.js'

/**
 * Message adapters publish on their wake channel.
 *
 * - `{ queue, at }`: a job of `queue` becomes due at `at` (epoch ms).
 * - `{ queue }`: a job of `queue` is ready now. Without a timestamp, a
 *   writer whose clock runs ahead cannot delay the subscriber.
 * - `{ schedules: true }`: schedules changed. A schedule is not tied to a
 *   queue and its next run is not always known by the writer, so subscribers
 *   read the next due schedule from storage.
 * - `{ probe }`: sent by a subscriber to check that its connection receives
 *   notifications. Other subscribers ignore it.
 */
export type WakeMessage = { queue: string; at?: number } | { schedules: true } | { probe: string }

export function encodeWakeMessage(message: WakeMessage): string {
  return JSON.stringify(message)
}

/**
 * Route a raw message of the wake channel to `onWake`. Messages for other
 * queues are ignored; schedule changes call `readNextSchedule`.
 */
export async function dispatchWakeMessage(
  raw: string,
  queues: readonly string[],
  onWake: WakeListener,
  readNextSchedule: () => Promise<number | null>
): Promise<void> {
  let message: Partial<{ queue: unknown; at: unknown; schedules: unknown }>

  try {
    message = JSON.parse(raw)
  } catch {
    return
  }

  if (message.schedules === true) {
    const at = await readNextSchedule()
    if (at !== null) onWake(at)
    return
  }

  if (typeof message.queue === 'string' && queues.includes(message.queue)) {
    onWake(typeof message.at === 'number' ? message.at : Date.now())
  }
}

/**
 * How often a subscription connection is pinged, and how long it has to
 * answer. Tests shorten them.
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
