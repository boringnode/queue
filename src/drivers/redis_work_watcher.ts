import { setTimeout as sleep } from 'node:timers/promises'
import type { Redis } from 'ioredis'
import type { WorkWatcher } from '../contracts/adapter.js'
import type { Logger } from '../logger.js'
import { ADVANCE_MARKER_SCRIPT, RELEASE_MARKER_SCRIPT } from './redis_scripts.js'
import { subscriptionHeartbeat } from './wake_messages.js'

/**
 * A marker key, and the sorted set whose due times it announces: the
 * delayed jobs of a queue, or the schedules.
 */
export interface RedisMarker {
  key: string
  source: string
}

interface PoppedMarker {
  key: string
  member: string
  score: number
}

const ERROR_RETRY_DELAY_MS = 1_000

/**
 * Waits for the markers that writers add for the queues and schedules (see
 * MARKER_LUA). BZPOPMIN pops each marker for one waiting worker only, so a
 * job wakes one worker, not all of them: `cooldown` does not apply.
 *
 * A popped due time is held while the worker waits for it, and put back
 * when the worker stops waiting first, so that another idle worker gets it.
 * A marker popped once the worker stopped waiting is put back as well.
 */
export class RedisWorkWatcher implements WorkWatcher {
  readonly #connection: Redis
  readonly #blocking: Redis
  readonly #markers: readonly RedisMarker[]
  readonly #logger: Logger
  /** The key prefix of the connection: replies of BZPOPMIN include it. */
  readonly #prefix: string
  /** Due times popped by the current wait, by marker key. */
  readonly #held = new Map<string, number>()
  /** The BZPOPMIN in flight. It may outlive the wait that sent it. */
  #reading?: Promise<void>
  /** Receives the result of the BZPOPMIN in flight while a wait reads it. */
  #deliver?: (marker: PoppedMarker | null) => void
  #closed = false

  constructor(connection: Redis, blocking: Redis, markers: readonly RedisMarker[], logger: Logger) {
    this.#connection = connection
    this.#blocking = blocking
    this.#markers = markers
    this.#logger = logger
    this.#prefix = blocking.options?.keyPrefix ?? ''
  }

  reset(): void {}

  async wait(maxDelay: number, signal: AbortSignal): Promise<boolean> {
    const deadline = Date.now() + maxDelay

    try {
      while (!signal.aborted && !this.#closed) {
        const now = Date.now()
        const [heldKey, heldAt] = this.#nextHeld()
        if (heldKey !== undefined && heldAt <= now) {
          this.#held.delete(heldKey)
          await this.#advance(heldKey, now)
          return true
        }
        if (deadline <= now) return false

        const marker = await this.#read(Math.min(deadline, heldAt), signal)
        if (!marker) continue
        if (marker.member === '0') return true
        if (marker.score <= Date.now()) {
          await this.#advance(marker.key, Date.now())
          return true
        }

        const held = this.#held.get(marker.key)
        if (held === undefined || marker.score < held) this.#held.set(marker.key, marker.score)
      }
      return false
    } finally {
      for (const [key, at] of this.#held) void this.#release({ key, member: '1', score: at })
      this.#held.clear()
    }
  }

  async close(): Promise<void> {
    this.#closed = true
    this.#deliver?.(null)
    this.#blocking.disconnect()
  }

  #nextHeld(): [string | undefined, number] {
    let next: [string | undefined, number] = [undefined, Infinity]
    for (const [key, at] of this.#held) {
      if (at < next[1]) next = [key, at]
    }
    return next
  }

  /**
   * Resolve with the next popped marker, or null at `until`, on abort, or
   * when the BZPOPMIN in flight times out.
   */
  #read(until: number, signal: AbortSignal): Promise<PoppedMarker | null> {
    if (!this.#reading) this.#reading = this.#pop(until)

    return new Promise((resolve) => {
      const done = (marker: PoppedMarker | null) => {
        clearTimeout(timer)
        signal.removeEventListener('abort', abort)
        if (this.#deliver === done) this.#deliver = undefined
        resolve(marker)
      }
      const abort = () => done(null)
      const timer = setTimeout(abort, until - Date.now())
      signal.addEventListener('abort', abort, { once: true })
      this.#deliver = done
    })
  }

  async #pop(until: number): Promise<void> {
    // A command blocked longer may not survive a reconnection, as in BullMQ.
    // It also bounds how long a silent connection goes unnoticed.
    const timeout = Math.min(Math.max(until - Date.now(), 10), subscriptionHeartbeat.interval)
    let watchdog: NodeJS.Timeout | undefined
    let marker: PoppedMarker | null = null

    try {
      const command = this.#blocking.bzpopmin(
        ...this.#markers.map((marker) => marker.key),
        timeout / 1000
      )
      // ioredis may never settle a command on a connection that went silent,
      // nor one it dropped on reconnection: past the timeout, give up on it.
      command.catch(() => {})
      const stuck = new Promise<'stuck'>((resolve) => {
        watchdog = setTimeout(() => resolve('stuck'), timeout + subscriptionHeartbeat.timeout)
      })
      const reply = await Promise.race([command, stuck])
      if (reply === 'stuck') {
        this.#logger.error(
          { err: new Error('BZPOPMIN did not return in time') },
          'Redis blocking connection is silent, reconnecting'
        )
        this.#blocking.disconnect(true)
        marker = this.#mayBeLost()
      } else if (reply) {
        marker = this.#parse(reply)
      }
    } catch (error) {
      if (!this.#closed) {
        this.#logger.error({ err: error as Error }, 'Failed to wait for work on Redis')
        await sleep(ERROR_RETRY_DELAY_MS)
        marker = this.#mayBeLost()
      }
    } finally {
      clearTimeout(watchdog)
      this.#reading = undefined
    }

    if (this.#deliver) {
      this.#deliver(marker)
    } else if (marker) {
      await this.#release(marker)
    }
  }

  /**
   * Redis may have popped a marker whose reply was lost with the connection:
   * look for work rather than wait for the next poll.
   */
  #mayBeLost(): PoppedMarker {
    return { key: this.#markers[0].key, member: '0', score: 0 }
  }

  #parse([prefixedKey, member, score]: [string, string, string]): PoppedMarker | null {
    const key = prefixedKey.slice(this.#prefix.length)
    if (!this.#markers.some((marker) => marker.key === key)) return null
    return { key, member, score: Number(score) }
  }

  /** Put back a marker this worker does not act on, for another worker. */
  async #release(marker: PoppedMarker): Promise<void> {
    await this.#connection
      .eval(RELEASE_MARKER_SCRIPT, 1, marker.key, marker.member, marker.score.toString())
      .catch((error) => this.#logger.error({ err: error }, 'Failed to put back a Redis marker'))
  }

  /** Mark the next due time of a marker whose due time this worker runs. */
  async #advance(key: string, now: number): Promise<void> {
    const source = this.#markers.find((marker) => marker.key === key)!.source
    await this.#connection
      .eval(ADVANCE_MARKER_SCRIPT, 2, source, key, now.toString())
      .catch((error) => this.#logger.error({ err: error }, 'Failed to advance a Redis marker'))
  }
}
