import type { WakeListener } from '../contracts/adapter.js'

/**
 * Message adapters publish on their wake channel.
 *
 * - `{ queue, at }`: a job of `queue` becomes due at `at` (epoch ms).
 * - `{ schedules: true }`: schedules changed. A schedule is not tied to a
 *   queue and its next run is not always known by the writer, so subscribers
 *   read the next due schedule from storage.
 */
export type WakeMessage = { queue: string; at: number } | { schedules: true }

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

  if (typeof message.queue === 'string' && typeof message.at === 'number') {
    if (queues.includes(message.queue)) onWake(message.at)
  }
}
