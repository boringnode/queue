import { randomUUID } from 'node:crypto'
import { Redis, type RedisOptions } from 'ioredis'
import { DEFAULT_PRIORITY } from '../constants.js'
import { calculateScore } from '../utils.js'
import type { Adapter, AcquiredJob, PushResult } from '../contracts/adapter.js'
import type { DedupOutcome } from '../types/main.js'
import type {
  JobData,
  JobRecord,
  JobRetention,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../types/main.js'
import { resolveRetention } from '../utils.js'

const redisKey = 'jobs'
const schedulesKey = 'schedules'
const schedulesIndexKey = 'schedules::index'
type RedisConfig = Redis | RedisOptions

/**
 * Lua script for pushing a job to the queue.
 * Stores job data in the central hash and adds jobId to pending ZSET.
 */
const PUSH_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local pending_key = KEYS[2]
  local job_id = ARGV[1]
  local job_data = ARGV[2]
  local score = tonumber(ARGV[3])

  redis.call('HSET', data_key, job_id, job_data)
  redis.call('ZADD', pending_key, score, job_id)

  return 1
`

/**
 * Lua script for pushing a dedup job.
 *
 * Behavior:
 * - If dedup key exists AND job still exists AND within TTL: apply replace/extend, skip insert.
 * - If dedup key exists but job data missing (orphan): proceed to insert new.
 * - If TTL expired or no prior entry: insert new job, record dedup key with TTL.
 *
 * Replace only applies to jobs in pending or delayed state. Active and
 * retained completed/failed jobs are left untouched (returns 'skipped').
 * Replace swaps the payload only — priority/queue/delay/groupId/dedup
 * options of the existing job are preserved.
 *
 * Extend uses the ORIGINAL ttl recorded on the existing job (stored in
 * its dedup field), not the ttl arg of the current dispatch. Matches
 * Knex/Fake behavior: extend resets the clock but never changes the
 * window length.
 *
 * Returns {outcome, job_id}: outcome ∈ 'added' | 'skipped' | 'replaced' | 'extended'.
 */
const PUSH_DEDUP_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local pending_key = KEYS[2]
  local dedup_key = KEYS[3]
  local delayed_key = KEYS[4]
  local job_id = ARGV[1]
  local job_data = ARGV[2]
  local score = tonumber(ARGV[3])
  local ttl = tonumber(ARGV[4])
  local extend = tonumber(ARGV[5])
  local replace = tonumber(ARGV[6])

  local existing = redis.call('GET', dedup_key)
  if existing then
    local existing_data = redis.call('HGET', data_key, existing)
    if existing_data then
      local in_pending = redis.call('ZSCORE', pending_key, existing)
      local in_delayed = redis.call('ZSCORE', delayed_key, existing)
      local replaceable = in_pending or in_delayed
      local ok_e, existing_job = pcall(cjson.decode, existing_data)
      local original_ttl = nil
      if ok_e and type(existing_job) == 'table' and existing_job.dedup then
        original_ttl = tonumber(existing_job.dedup.ttl)
      end
      if replace == 1 and replaceable then
        local ok_n, new_job = pcall(cjson.decode, job_data)
        if ok_e and ok_n and existing_job and new_job then
          existing_job.payload = new_job.payload
          redis.call('HSET', data_key, existing, cjson.encode(existing_job))
          if extend == 1 and original_ttl and original_ttl > 0 then
            redis.call('PEXPIRE', dedup_key, original_ttl)
          end
          return {'replaced', existing}
        end
        return {'skipped', existing}
      end
      if extend == 1 and original_ttl and original_ttl > 0 then
        redis.call('PEXPIRE', dedup_key, original_ttl)
        return {'extended', existing}
      end
      return {'skipped', existing}
    end
  end

  redis.call('HSET', data_key, job_id, job_data)
  redis.call('ZADD', pending_key, score, job_id)
  redis.call('SET', dedup_key, job_id)
  if ttl > 0 then
    redis.call('PEXPIRE', dedup_key, ttl)
  end
  return {'added', job_id}
`

/**
 * Lua script for pushing a delayed job.
 * Stores job data in the central hash and adds jobId to delayed ZSET.
 */
const PUSH_DELAYED_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local delayed_key = KEYS[2]
  local job_id = ARGV[1]
  local job_data = ARGV[2]
  local execute_at = tonumber(ARGV[3])

  redis.call('HSET', data_key, job_id, job_data)
  redis.call('ZADD', delayed_key, execute_at, job_id)

  return 1
`

/**
 * Lua script for pushing a dedup delayed job.
 * Same semantics as PUSH_DEDUP_JOB_SCRIPT but adds to delayed ZSET.
 */
const PUSH_DEDUP_DELAYED_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local delayed_key = KEYS[2]
  local dedup_key = KEYS[3]
  local pending_key = KEYS[4]
  local job_id = ARGV[1]
  local job_data = ARGV[2]
  local execute_at = tonumber(ARGV[3])
  local ttl = tonumber(ARGV[4])
  local extend = tonumber(ARGV[5])
  local replace = tonumber(ARGV[6])

  local existing = redis.call('GET', dedup_key)
  if existing then
    local existing_data = redis.call('HGET', data_key, existing)
    if existing_data then
      local in_pending = redis.call('ZSCORE', pending_key, existing)
      local in_delayed = redis.call('ZSCORE', delayed_key, existing)
      local replaceable = in_pending or in_delayed
      local ok_e, existing_job = pcall(cjson.decode, existing_data)
      local original_ttl = nil
      if ok_e and type(existing_job) == 'table' and existing_job.dedup then
        original_ttl = tonumber(existing_job.dedup.ttl)
      end
      if replace == 1 and replaceable then
        local ok_n, new_job = pcall(cjson.decode, job_data)
        if ok_e and ok_n and existing_job and new_job then
          existing_job.payload = new_job.payload
          redis.call('HSET', data_key, existing, cjson.encode(existing_job))
          if extend == 1 and original_ttl and original_ttl > 0 then
            redis.call('PEXPIRE', dedup_key, original_ttl)
          end
          return {'replaced', existing}
        end
        return {'skipped', existing}
      end
      if extend == 1 and original_ttl and original_ttl > 0 then
        redis.call('PEXPIRE', dedup_key, original_ttl)
        return {'extended', existing}
      end
      return {'skipped', existing}
    end
  end

  redis.call('HSET', data_key, job_id, job_data)
  redis.call('ZADD', delayed_key, execute_at, job_id)
  redis.call('SET', dedup_key, job_id)
  if ttl > 0 then
    redis.call('PEXPIRE', dedup_key, ttl)
  end
  return {'added', job_id}
`

/**
 * Lua script for atomic job acquisition.
 * 1. Check and process delayed jobs
 * 2. Pop from pending queue
 * 3. Add to active hash with worker info
 * 4. Return job data
 */
const ACQUIRE_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local pending_key = KEYS[2]
  local active_key = KEYS[3]
  local delayed_key = KEYS[4]
  local worker_id = ARGV[1]
  local now = tonumber(ARGV[2])

  -- Process delayed jobs: move ready jobs to pending
  local ready_job_ids = redis.call('ZRANGEBYSCORE', delayed_key, 0, now)
  if #ready_job_ids > 0 then
    for i = 1, #ready_job_ids do
      local job_id = ready_job_ids[i]
      local job_data = redis.call('HGET', data_key, job_id)
      if job_data then
        local job = cjson.decode(job_data)
        local priority = job.priority or 5
        local score = priority * 10000000000000 + now
        redis.call('ZADD', pending_key, score, job_id)
        redis.call('ZREM', delayed_key, job_id)
      end
    end
  end

  -- Pop highest priority job (lowest score)
  local result = redis.call('ZPOPMIN', pending_key)
  if not result or #result == 0 then
    return nil
  end

  local job_id = result[1]
  local job_data = redis.call('HGET', data_key, job_id)
  if not job_data then
    return nil
  end

  -- Store in active hash (without data, it's in data_key)
  local active_data = cjson.encode({
    workerId = worker_id,
    acquiredAt = now
  })
  redis.call('HSET', active_key, job_id, active_data)

  -- Return job with acquiredAt
  local job = cjson.decode(job_data)
  job.acquiredAt = now
  return cjson.encode(job)
`

/**
 * Lua script for removing a job completely (no history).
 * Also cleans up the dedup key if the job had dedup metadata.
 */
const REMOVE_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local job_id = ARGV[1]
  local dedup_prefix = ARGV[2]

  if redis.call('HEXISTS', active_key, job_id) == 0 then
    return 0
  end

  -- Read job data to extract dedup.id before deleting
  local job_data = redis.call('HGET', data_key, job_id)
  if job_data then
    local ok, job = pcall(cjson.decode, job_data)
    if ok and job and job.dedup and job.dedup.id then
      local dkey = dedup_prefix .. job.dedup.id
      if redis.call('GET', dkey) == job_id then
        redis.call('DEL', dkey)
      end
    end
  end

  redis.call('HDEL', active_key, job_id)
  redis.call('HDEL', data_key, job_id)

  return 1
`

/**
 * Lua script for finalizing a job in history.
 * Removes from active, stores finalization info, and prunes old records.
 * When pruning removes job data, also deletes the associated dedup key.
 */
const FINALIZE_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local history_key = KEYS[3]
  local index_key = KEYS[4]
  local job_id = ARGV[1]
  local now = tonumber(ARGV[2])
  local max_age = tonumber(ARGV[3])
  local max_count = tonumber(ARGV[4])
  local error_message = ARGV[5]
  local dedup_prefix = ARGV[6]

  -- Verify job is active
  if redis.call('HEXISTS', active_key, job_id) == 0 then
    return 0
  end

  -- Remove from active
  redis.call('HDEL', active_key, job_id)

  -- Store finalization info (data stays in data_key)
  local record = {
    finishedAt = now
  }
  if error_message and error_message ~= '' then
    record.error = error_message
  end
  redis.call('HSET', history_key, job_id, cjson.encode(record))
  redis.call('ZADD', index_key, now, job_id)

  local function delete_dedup_for(ids)
    for i = 1, #ids do
      local id = ids[i]
      local d = redis.call('HGET', data_key, id)
      if d then
        local ok, job = pcall(cjson.decode, d)
        if ok and job and job.dedup and job.dedup.id then
          local dkey = dedup_prefix .. job.dedup.id
          if redis.call('GET', dkey) == id then
            redis.call('DEL', dkey)
          end
        end
      end
    end
  end

  -- Prune by age
  if max_age and max_age > 0 then
    local cutoff = now - max_age
    local expired = redis.call('ZRANGEBYSCORE', index_key, 0, cutoff)
    if #expired > 0 then
      delete_dedup_for(expired)
      redis.call('ZREM', index_key, unpack(expired))
      redis.call('HDEL', history_key, unpack(expired))
      redis.call('HDEL', data_key, unpack(expired))
    end
  end

  -- Prune by count
  if max_count and max_count > 0 then
    local size = tonumber(redis.call('ZCARD', index_key))
    if size > max_count then
      local excess = size - max_count
      local stale = redis.call('ZRANGE', index_key, 0, excess - 1)
      if #stale > 0 then
        delete_dedup_for(stale)
        redis.call('ZREM', index_key, unpack(stale))
        redis.call('HDEL', history_key, unpack(stale))
        redis.call('HDEL', data_key, unpack(stale))
      end
    end
  end

  return 1
`

/**
 * Lua script for retrying a job.
 * 1. Verify job is active
 * 2. Remove from active hash
 * 3. Increment attempts in data
 * 4. Add back to pending (or delayed if retryAt is set)
 */
const RETRY_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local pending_key = KEYS[3]
  local delayed_key = KEYS[4]
  local job_id = ARGV[1]
  local retry_at = tonumber(ARGV[2])
  local now = tonumber(ARGV[3])

  -- Verify job is active
  if redis.call('HEXISTS', active_key, job_id) == 0 then
    return 0
  end

  -- Get job data
  local job_data = redis.call('HGET', data_key, job_id)
  if not job_data then
    return 0
  end

  -- Remove from active
  redis.call('HDEL', active_key, job_id)

  -- Increment attempts and update data
  local job = cjson.decode(job_data)
  job.attempts = (job.attempts or 0) + 1
  redis.call('HSET', data_key, job_id, cjson.encode(job))

  -- Add back to pending or delayed
  if retry_at and retry_at > now then
    redis.call('ZADD', delayed_key, retry_at, job_id)
  else
    -- Score = priority * 1e13 + timestamp
    -- Lower score = higher priority, FIFO within same priority
    local priority = job.priority or 5
    local score = priority * 10000000000000 + now
    redis.call('ZADD', pending_key, score, job_id)
  end

  return 1
`

/**
 * Lua script for recovering stalled jobs.
 * Scans the active hash for jobs that have been active too long.
 * - Jobs within maxStalledCount: move back to pending with incremented stalledCount
 * - Jobs exceeding maxStalledCount: remove permanently (fail)
 * Returns the number of recovered jobs (not including failed ones).
 */
const RECOVER_STALLED_JOBS_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local pending_key = KEYS[3]
  local now = tonumber(ARGV[1])
  local stalled_threshold = tonumber(ARGV[2])
  local max_stalled_count = tonumber(ARGV[3])
  local dedup_prefix = ARGV[4]

  local recovered = 0
  local stalled_cutoff = now - stalled_threshold

  -- Get all active jobs
  local active_jobs = redis.call('HGETALL', active_key)

  -- HGETALL returns [field1, value1, field2, value2, ...]
  for i = 1, #active_jobs, 2 do
    local job_id = active_jobs[i]
    local active_data = active_jobs[i + 1]
    local active = cjson.decode(active_data)

    -- Check if job is stalled
    if active.acquiredAt < stalled_cutoff then
      local job_data = redis.call('HGET', data_key, job_id)
      if job_data then
        local job = cjson.decode(job_data)
        local current_stalled_count = job.stalledCount or 0

        -- Remove from active hash
        redis.call('HDEL', active_key, job_id)

        -- Check if job has exceeded max stalled count
        if current_stalled_count >= max_stalled_count then
          -- Job failed permanently, remove data + dedup key (only if pointer still ours)
          if job.dedup and job.dedup.id then
            local dkey = dedup_prefix .. job.dedup.id
            if redis.call('GET', dkey) == job_id then
              redis.call('DEL', dkey)
            end
          end
          redis.call('HDEL', data_key, job_id)
        else
          -- Recover: increment stalledCount and put back in pending
          job.stalledCount = current_stalled_count + 1
          redis.call('HSET', data_key, job_id, cjson.encode(job))
          -- Score = priority * 1e13 + timestamp
          local priority = job.priority or 5
          local score = priority * 10000000000000 + now
          redis.call('ZADD', pending_key, score, job_id)
          recovered = recovered + 1
        end
      end
    end
  end

  return recovered
`

/**
 * Lua script for getting a job record with its status.
 */
const GET_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local pending_key = KEYS[2]
  local delayed_key = KEYS[3]
  local active_key = KEYS[4]
  local completed_key = KEYS[5]
  local failed_key = KEYS[6]
  local job_id = ARGV[1]

  local job_data = redis.call('HGET', data_key, job_id)
  if not job_data then
    return nil
  end

  local status = nil
  local finished_at = nil
  local error_msg = nil

  -- Check status in order
  if redis.call('HEXISTS', active_key, job_id) == 1 then
    status = 'active'
  elseif redis.call('ZSCORE', pending_key, job_id) then
    status = 'pending'
  elseif redis.call('ZSCORE', delayed_key, job_id) then
    status = 'delayed'
  else
    local completed_data = redis.call('HGET', completed_key, job_id)
    if completed_data then
      status = 'completed'
      local record = cjson.decode(completed_data)
      finished_at = record.finishedAt
    else
      local failed_data = redis.call('HGET', failed_key, job_id)
      if failed_data then
        status = 'failed'
        local record = cjson.decode(failed_data)
        finished_at = record.finishedAt
        error_msg = record.error
      end
    end
  end

  if not status then
    return nil
  end

  return cjson.encode({
    status = status,
    data = cjson.decode(job_data),
    finishedAt = finished_at,
    error = error_msg
  })
`

/**
 * Lua script for atomically claiming a due schedule.
 * Iterates the schedule index server-side and claims the first due schedule.
 * Returns the schedule data if claimed, nil otherwise.
 */
const CLAIM_SCHEDULE_SCRIPT = `
  local schedules_index_key = KEYS[1]
  local schedule_key_prefix = KEYS[2]
  local now = tonumber(ARGV[1])

  local ids = redis.call('SMEMBERS', schedules_index_key)

  for i = 1, #ids do
    local schedule_key = schedule_key_prefix .. ids[i]

    -- Get schedule data
    local data = redis.call('HGETALL', schedule_key)
    if #data > 0 then
      -- Convert HGETALL result to table
      local schedule = {}
      for j = 1, #data, 2 do
        schedule[data[j]] = data[j + 1]
      end

      -- Check if schedule is due
      if schedule.status == 'active' then
        local next_run_at = tonumber(schedule.next_run_at)

        if next_run_at and next_run_at <= now then
          local run_count = tonumber(schedule.run_count or '0')
          local run_limit = schedule.run_limit and tonumber(schedule.run_limit) or nil
          local to_date = schedule.to_date and tonumber(schedule.to_date) or nil

          -- Check limits
          if not (run_limit and run_count >= run_limit) and not (to_date and now > to_date) then
            -- This schedule is claimable - atomically update it
            local new_run_count = run_count + 1

            -- Calculate new next_run_at (simple interval-based for now)
            -- Complex cron calculation happens in the caller
            local new_next_run_at = ''
            local every_ms = schedule.every_ms and tonumber(schedule.every_ms) or nil
            if every_ms then
              new_next_run_at = tostring(now + every_ms)
            end

            -- Check if we've hit the limit after this run
            if run_limit and new_run_count >= run_limit then
              new_next_run_at = ''
            end

            -- Check if past end date
            if to_date and new_next_run_at ~= '' and tonumber(new_next_run_at) > to_date then
              new_next_run_at = ''
            end

            -- Update the schedule atomically
            redis.call('HSET', schedule_key,
              'next_run_at', new_next_run_at,
              'last_run_at', tostring(now),
              'run_count', tostring(new_run_count))

            -- Return the schedule data (before update) as JSON
            return cjson.encode(schedule)
          end
        end
      end
    end
  end

  return nil
`

/**
 * Create a new Redis adapter factory.
 * Accepts either a Redis instance or Redis options.
 *
 * When passing options, the adapter will create and manage
 * the connection lifecycle (closing it on destroy).
 *
 * When passing a Redis instance, the caller is responsible for
 * managing the connection lifecycle.
 */
export function redis(config?: RedisConfig) {
  return () => {
    if (config instanceof Redis) {
      return new RedisAdapter(config, false)
    }

    const options: RedisOptions = {
      host: 'localhost',
      port: 6379,
      keyPrefix: 'boringnode::queue::',
      db: 0,
      ...config,
    }

    const connection = new Redis(options)
    return new RedisAdapter(connection, true)
  }
}

export class RedisAdapter implements Adapter {
  readonly #connection: Redis
  readonly #ownsConnection: boolean
  #workerId: string = ''

  constructor(connection: Redis, ownsConnection: boolean = false) {
    this.#connection = connection
    this.#ownsConnection = ownsConnection
  }

  #getKeys(queue: string) {
    return {
      data: `${redisKey}::${queue}::data`,
      pending: `${redisKey}::${queue}::pending`,
      delayed: `${redisKey}::${queue}::delayed`,
      active: `${redisKey}::${queue}::active`,
      completed: `${redisKey}::${queue}::completed`,
      completedIndex: `${redisKey}::${queue}::completed::index`,
      failed: `${redisKey}::${queue}::failed`,
      failedIndex: `${redisKey}::${queue}::failed::index`,
    }
  }

  #getDedupKey(queue: string, dedupId: string): string {
    return `${this.#getDedupPrefix(queue)}${dedupId}`
  }

  #getDedupPrefix(queue: string): string {
    return `${redisKey}::${queue}::dedup::`
  }

  setWorkerId(workerId: string): void {
    this.#workerId = workerId
  }

  async destroy(): Promise<void> {
    if (this.#ownsConnection) {
      await this.#connection.quit()
    }
  }

  pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    const keys = this.#getKeys(queue)
    const now = Date.now()

    const result = await this.#connection.eval(
      ACQUIRE_JOB_SCRIPT,
      4,
      keys.data,
      keys.pending,
      keys.active,
      keys.delayed,
      this.#workerId,
      now.toString()
    )

    if (!result) {
      return null
    }

    return JSON.parse(result as string)
  }

  async completeJob(jobId: string, queue: string, removeOnComplete?: JobRetention): Promise<void> {
    const keys = this.#getKeys(queue)
    const dedupPrefix = this.#getDedupPrefix(queue)
    const { keep, maxAge, maxCount } = resolveRetention(removeOnComplete)

    if (!keep) {
      await this.#connection.eval(REMOVE_JOB_SCRIPT, 2, keys.data, keys.active, jobId, dedupPrefix)
      return
    }

    await this.#connection.eval(
      FINALIZE_JOB_SCRIPT,
      4,
      keys.data,
      keys.active,
      keys.completed,
      keys.completedIndex,
      jobId,
      Date.now().toString(),
      maxAge.toString(),
      maxCount.toString(),
      '',
      dedupPrefix
    )
  }

  async failJob(
    jobId: string,
    queue: string,
    error?: Error,
    removeOnFail?: JobRetention
  ): Promise<void> {
    const keys = this.#getKeys(queue)
    const dedupPrefix = this.#getDedupPrefix(queue)
    const { keep, maxAge, maxCount } = resolveRetention(removeOnFail)

    if (!keep) {
      await this.#connection.eval(REMOVE_JOB_SCRIPT, 2, keys.data, keys.active, jobId, dedupPrefix)
      return
    }

    await this.#connection.eval(
      FINALIZE_JOB_SCRIPT,
      4,
      keys.data,
      keys.active,
      keys.failed,
      keys.failedIndex,
      jobId,
      Date.now().toString(),
      maxAge.toString(),
      maxCount.toString(),
      error?.message || '',
      dedupPrefix
    )
  }

  async retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void> {
    const keys = this.#getKeys(queue)
    const now = Date.now()

    await this.#connection.eval(
      RETRY_JOB_SCRIPT,
      4,
      keys.data,
      keys.active,
      keys.pending,
      keys.delayed,
      jobId,
      retryAt ? retryAt.getTime().toString() : '0',
      now.toString()
    )
  }

  async getJob(jobId: string, queue: string): Promise<JobRecord | null> {
    const keys = this.#getKeys(queue)

    const result = await this.#connection.eval(
      GET_JOB_SCRIPT,
      6,
      keys.data,
      keys.pending,
      keys.delayed,
      keys.active,
      keys.completed,
      keys.failed,
      jobId
    )

    if (!result) {
      return null
    }

    return JSON.parse(result as string)
  }

  push(jobData: JobData): Promise<PushResult | void> {
    return this.pushOn('default', jobData)
  }

  pushLater(jobData: JobData, delay: number): Promise<PushResult | void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  async pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<PushResult | void> {
    const keys = this.#getKeys(queue)
    const executeAt = Date.now() + delay

    if (jobData.dedup) {
      const dedupKey = this.#getDedupKey(queue, jobData.dedup.id)
      const result = (await this.#connection.eval(
        PUSH_DEDUP_DELAYED_JOB_SCRIPT,
        4,
        keys.data,
        keys.delayed,
        dedupKey,
        keys.pending,
        jobData.id,
        JSON.stringify(jobData),
        executeAt.toString(),
        (jobData.dedup.ttl ?? 0).toString(),
        jobData.dedup.extend ? '1' : '0',
        jobData.dedup.replace ? '1' : '0'
      )) as [string, string]
      return { outcome: result[0] as DedupOutcome, jobId: result[1] }
    }

    await this.#connection.eval(
      PUSH_DELAYED_JOB_SCRIPT,
      2,
      keys.data,
      keys.delayed,
      jobData.id,
      JSON.stringify(jobData),
      executeAt.toString()
    )
  }

  async pushOn(queue: string, jobData: JobData): Promise<PushResult | void> {
    const keys = this.#getKeys(queue)
    const priority = jobData.priority ?? DEFAULT_PRIORITY
    const timestamp = Date.now()
    const score = calculateScore(priority, timestamp)

    if (jobData.dedup) {
      const dedupKey = this.#getDedupKey(queue, jobData.dedup.id)
      const result = (await this.#connection.eval(
        PUSH_DEDUP_JOB_SCRIPT,
        4,
        keys.data,
        keys.pending,
        dedupKey,
        keys.delayed,
        jobData.id,
        JSON.stringify(jobData),
        score.toString(),
        (jobData.dedup.ttl ?? 0).toString(),
        jobData.dedup.extend ? '1' : '0',
        jobData.dedup.replace ? '1' : '0'
      )) as [string, string]
      return { outcome: result[0] as DedupOutcome, jobId: result[1] }
    }

    await this.#connection.eval(
      PUSH_JOB_SCRIPT,
      2,
      keys.data,
      keys.pending,
      jobData.id,
      JSON.stringify(jobData),
      score.toString()
    )
  }

  pushMany(jobs: JobData[]): Promise<void> {
    return this.pushManyOn('default', jobs)
  }

  async pushManyOn(queue: string, jobs: JobData[]): Promise<void> {
    if (jobs.length === 0) return

    if (jobs.some((j) => j.dedup)) {
      throw new Error('dedup is not supported in batch dispatch; use single dispatch')
    }

    const keys = this.#getKeys(queue)
    const now = Date.now()
    const multi = this.#connection.multi()

    for (const job of jobs) {
      const priority = job.priority ?? DEFAULT_PRIORITY
      const score = calculateScore(priority, now)
      multi.hset(keys.data, job.id, JSON.stringify(job))
      multi.zadd(keys.pending, score, job.id)
    }

    await multi.exec()
  }

  size(): Promise<number> {
    return this.sizeOf('default')
  }

  sizeOf(queue: string): Promise<number> {
    const keys = this.#getKeys(queue)
    return this.#connection.zcard(keys.pending)
  }

  async recoverStalledJobs(
    queue: string,
    stalledThreshold: number,
    maxStalledCount: number
  ): Promise<number> {
    const keys = this.#getKeys(queue)
    const now = Date.now()

    const recovered = await this.#connection.eval(
      RECOVER_STALLED_JOBS_SCRIPT,
      3,
      keys.data,
      keys.active,
      keys.pending,
      now.toString(),
      stalledThreshold.toString(),
      maxStalledCount.toString(),
      this.#getDedupPrefix(queue)
    )

    return recovered as number
  }

  async upsertSchedule(config: ScheduleConfig): Promise<string> {
    const id = config.id ?? randomUUID()
    const now = Date.now()
    const scheduleKey = `${schedulesKey}::${id}`
    const [existingRunCount, existingCreatedAt] = await this.#connection.hmget(
      scheduleKey,
      'run_count',
      'created_at'
    )

    const scheduleData: Record<string, string> = {
      id,
      name: config.name,
      payload: JSON.stringify(config.payload),
      timezone: config.timezone,
      status: 'active',
      run_count: existingRunCount ?? '0',
      created_at: existingCreatedAt ?? now.toString(),
    }

    if (config.cronExpression !== undefined) scheduleData.cron_expression = config.cronExpression
    if (config.everyMs !== undefined) scheduleData.every_ms = config.everyMs.toString()
    if (config.from !== undefined) scheduleData.from_date = config.from.getTime().toString()
    if (config.to !== undefined) scheduleData.to_date = config.to.getTime().toString()
    if (config.limit !== undefined) scheduleData.run_limit = config.limit.toString()

    // Upsert schedule and clear stale optional fields from previous config.
    await this.#connection
      .multi()
      .hdel(scheduleKey, 'cron_expression', 'every_ms', 'from_date', 'to_date', 'run_limit')
      .hset(scheduleKey, scheduleData)
      .sadd(schedulesIndexKey, id)
      .exec()

    return id
  }

  /**
   * @deprecated Use `upsertSchedule` instead.
   */
  createSchedule(config: ScheduleConfig): Promise<string> {
    return this.upsertSchedule(config)
  }

  async getSchedule(id: string): Promise<ScheduleData | null> {
    const scheduleKey = `${schedulesKey}::${id}`
    const data = await this.#connection.hgetall(scheduleKey)

    if (!data || Object.keys(data).length === 0) {
      return null
    }

    return this.#hashToScheduleData(data)
  }

  async listSchedules(options?: ScheduleListOptions): Promise<ScheduleData[]> {
    const ids = await this.#connection.smembers(schedulesIndexKey)
    if (ids.length === 0) {
      return []
    }

    const pipeline = this.#connection.pipeline()

    for (const id of ids) {
      pipeline.hgetall(`${schedulesKey}::${id}`)
    }

    const results = await pipeline.exec()
    if (!results) {
      return []
    }

    const schedules: ScheduleData[] = []

    for (const [, data] of results) {
      if (!data || Object.keys(data).length === 0) {
        continue
      }

      const schedule = this.#hashToScheduleData(data as Record<string, string>)

      // Filter by status if provided
      if (options?.status && schedule.status !== options.status) {
        continue
      }

      schedules.push(schedule)
    }

    return schedules
  }

  async updateSchedule(
    id: string,
    updates: Partial<Pick<ScheduleData, 'status' | 'nextRunAt' | 'lastRunAt' | 'runCount'>>
  ): Promise<void> {
    const scheduleKey = `${schedulesKey}::${id}`
    const data: Record<string, string> = {}

    if (updates.status !== undefined) data.status = updates.status
    if (updates.nextRunAt !== undefined) {
      data.next_run_at = updates.nextRunAt ? updates.nextRunAt.getTime().toString() : ''
    }
    if (updates.lastRunAt !== undefined) {
      data.last_run_at = updates.lastRunAt ? updates.lastRunAt.getTime().toString() : ''
    }
    if (updates.runCount !== undefined) data.run_count = updates.runCount.toString()

    if (Object.keys(data).length > 0) {
      await this.#connection.hset(scheduleKey, data)
    }
  }

  async deleteSchedule(id: string): Promise<void> {
    const scheduleKey = `${schedulesKey}::${id}`
    await this.#connection.multi().del(scheduleKey).srem(schedulesIndexKey, id).exec()
  }

  async claimDueSchedule(): Promise<ScheduleData | null> {
    const now = Date.now()
    const result = await this.#connection.eval(
      CLAIM_SCHEDULE_SCRIPT,
      2,
      schedulesIndexKey,
      `${schedulesKey}::`,
      now.toString()
    )

    if (!result) {
      return null
    }

    const data = JSON.parse(result as string) as Record<string, string>

    // If cron expression, we need to recalculate next_run_at properly.
    // The Lua script only handles simple interval; cron needs JS cron-parser.
    // This is safe because the schedule is already claimed (run_count incremented).
    if (data.cron_expression) {
      const { CronExpressionParser } = await import('cron-parser')
      const cron = CronExpressionParser.parse(data.cron_expression, {
        currentDate: new Date(now),
        tz: data.timezone || 'UTC',
      })
      const nextRun = cron.next().toDate().getTime()

      // Check limits before updating
      const runCount = Number.parseInt(data.run_count || '0', 10) + 1
      const runLimit = data.run_limit ? Number.parseInt(data.run_limit, 10) : null
      const toDate = data.to_date ? Number.parseInt(data.to_date, 10) : null

      let newNextRunAt: number | string = nextRun

      if (runLimit !== null && runCount >= runLimit) {
        newNextRunAt = ''
      } else if (toDate && nextRun > toDate) {
        newNextRunAt = ''
      }

      await this.#connection.hset(
        `${schedulesKey}::${data.id}`,
        'next_run_at',
        newNextRunAt.toString()
      )
    }

    return this.#hashToScheduleData(data)
  }

  #hashToScheduleData(data: Record<string, string>): ScheduleData {
    return {
      id: data.id,
      name: data.name,
      payload: JSON.parse(data.payload || '{}'),
      cronExpression: data.cron_expression || null,
      everyMs: data.every_ms ? Number.parseInt(data.every_ms, 10) : null,
      timezone: data.timezone || 'UTC',
      from: data.from_date ? new Date(Number.parseInt(data.from_date, 10)) : null,
      to: data.to_date ? new Date(Number.parseInt(data.to_date, 10)) : null,
      limit: data.run_limit ? Number.parseInt(data.run_limit, 10) : null,
      runCount: Number.parseInt(data.run_count || '0', 10),
      nextRunAt: data.next_run_at ? new Date(Number.parseInt(data.next_run_at, 10)) : null,
      lastRunAt: data.last_run_at ? new Date(Number.parseInt(data.last_run_at, 10)) : null,
      status: (data.status as 'active' | 'paused') || 'active',
      createdAt: data.created_at ? new Date(Number.parseInt(data.created_at, 10)) : new Date(),
    }
  }
}
