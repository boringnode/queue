import { REDIS_DEDUP_LUA, REDIS_JOB_STORAGE_LUA } from './redis_job_storage.js'

/**
 * Markers wake idle workers, as in BullMQ. A marker key is a sorted set that
 * workers wait on with BZPOPMIN: each marker wakes one worker only, and a
 * marker written while every worker is busy waits for the first idle one.
 *
 * - Member '0', score 0: a job is ready. Many pushes leave one marker. The
 *   worker that pops a job writes it again while jobs remain, to wake the
 *   next worker.
 * - Member '1': the earliest known time at which a delayed job or a schedule
 *   becomes due. A worker holds it until then.
 *
 * The marker key of a script is its last key.
 */
const MARKER_LUA = `
  local marker_key = KEYS[#KEYS]

  local function mark_ready()
    redis.call('ZADD', marker_key, 0, '0')
  end

  -- A later time would hide the earlier one.
  local function mark_due(at)
    local current = redis.call('ZSCORE', marker_key, '1')
    if not current or tonumber(current) > at then
      redis.call('ZADD', marker_key, at, '1')
    end
  end

  -- Exactly the first score of a sorted set, once the previous one is handled.
  local function mark_first_of(key)
    local first = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    if #first > 0 then
      redis.call('ZADD', marker_key, first[2], '1')
    else
      redis.call('ZREM', marker_key, '1')
    end
  end
`

/**
 * Writes the markers of work stored before a worker waits for it, or by an
 * older version of this package. KEYS = per queue its pending, delayed and
 * marker keys, then the schedules due index and its marker.
 */
export const SEED_MARKERS_SCRIPT = `
  local function seed(source_key, marker_key, ready)
    local first = redis.call('ZRANGE', source_key, 0, 0, 'WITHSCORES')
    if #first == 0 then
      return
    end
    if ready then
      redis.call('ZADD', marker_key, 0, '0')
      return
    end
    local current = redis.call('ZSCORE', marker_key, '1')
    if not current or tonumber(current) > tonumber(first[2]) then
      redis.call('ZADD', marker_key, first[2], '1')
    end
  end

  for i = 1, #KEYS - 2, 3 do
    seed(KEYS[i], KEYS[i + 2], true)
    seed(KEYS[i + 1], KEYS[i + 2], false)
  end
  seed(KEYS[#KEYS - 1], KEYS[#KEYS], false)
`

/**
 * Marks the first entry of a sorted set due after now, ARGV[1]. KEYS = the
 * sorted set (delayed jobs or schedules), its marker. A worker woken by a
 * due marker runs it: what is due now is its own work, and the marker of
 * the next entry is not lost if its cycle writes none.
 */
export const ADVANCE_MARKER_SCRIPT = `
  local next = redis.call('ZRANGEBYSCORE', KEYS[1], '(' .. ARGV[1], '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
  if #next == 0 then
    return
  end
  local current = redis.call('ZSCORE', KEYS[2], '1')
  if not current or tonumber(current) > tonumber(next[2]) then
    redis.call('ZADD', KEYS[2], next[2], '1')
  end
`

/**
 * Puts back a marker a worker popped but does not act on: ARGV = member,
 * score. A due time only replaces a later one.
 */
export const RELEASE_MARKER_SCRIPT = `
  local current = redis.call('ZSCORE', KEYS[1], ARGV[1])
  if not current or tonumber(current) > tonumber(ARGV[2]) then
    redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
  end
`

/**
 * Lua script for pushing a job to the queue.
 * Stores job data in the central hash and adds jobId to pending ZSET.
 */
export const PUSH_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local pending_key = KEYS[2]
  local overlay_key = KEYS[3]
  local job_id = ARGV[1]
  local job_data = ARGV[2]
  local score = tonumber(ARGV[3])

${REDIS_JOB_STORAGE_LUA}
${MARKER_LUA}

  store_job_data(data_key, overlay_key, job_id, job_data)
  redis.call('ZADD', pending_key, score, job_id)
  mark_ready()

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
export const PUSH_DEDUP_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local target_key = KEYS[2]
  local dedup_key = KEYS[3]
  local other_state_key = KEYS[4]
  local overlay_key = KEYS[5]
  local job_id = ARGV[1]
  local job_data = ARGV[2]
  local score = tonumber(ARGV[3])
  local ttl = tonumber(ARGV[4])
  local extend = tonumber(ARGV[5])
  local replace = tonumber(ARGV[6])
  local payload_data = ARGV[7]
  local payload_is_undefined = tonumber(ARGV[8])
  local delayed = ARGV[9] == '1'

${REDIS_JOB_STORAGE_LUA}
${REDIS_DEDUP_LUA}
${MARKER_LUA}

  -- The job may be skipped: a spurious marker only costs one worker a look.
  if delayed then
    mark_due(score)
  else
    mark_ready()
  end

  local existing_result = resolve_dedup_existing_job(
    data_key,
    target_key,
    other_state_key,
    overlay_key,
    dedup_key,
    extend,
    replace,
    payload_data,
    payload_is_undefined
  )
  if existing_result then
    return existing_result
  end

  store_job_data(data_key, overlay_key, job_id, job_data)
  redis.call('ZADD', target_key, score, job_id)
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
export const PUSH_DELAYED_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local delayed_key = KEYS[2]
  local overlay_key = KEYS[3]
  local job_id = ARGV[1]
  local job_data = ARGV[2]
  local execute_at = tonumber(ARGV[3])

${REDIS_JOB_STORAGE_LUA}
${MARKER_LUA}

  store_job_data(data_key, overlay_key, job_id, job_data)
  redis.call('ZADD', delayed_key, execute_at, job_id)
  mark_due(execute_at)

  return 1
`

/**
 * Lua script for atomic job acquisition.
 * 1. Check and process delayed jobs
 * 2. Pop from pending queue
 * 3. Add to active hash with worker info
 * 4. Return job data
 */
export const ACQUIRE_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local pending_key = KEYS[2]
  local active_key = KEYS[3]
  local delayed_key = KEYS[4]
  local overlay_key = KEYS[5]
  local worker_id = ARGV[1]
  local now = tonumber(ARGV[2])
  local lease_token = ARGV[3]

${REDIS_JOB_STORAGE_LUA}
${MARKER_LUA}

  -- Process delayed jobs: move ready jobs to pending
  local ready_job_ids = redis.call('ZRANGEBYSCORE', delayed_key, 0, now)
  if #ready_job_ids > 0 then
    for i = 1, #ready_job_ids do
      local job_id = ready_job_ids[i]
      local job_data = redis.call('HGET', data_key, job_id)
      if job_data then
        local priority = read_job_fields(job_data).priority or 5
        local score = priority * 10000000000000 + now
        redis.call('ZADD', pending_key, score, job_id)
        redis.call('ZREM', delayed_key, job_id)
      end
    end

    mark_first_of(delayed_key)
  end

  -- Pop highest priority job (lowest score)
  local result = redis.call('ZPOPMIN', pending_key)
  if not result or #result == 0 then
    return nil
  end

  -- Wake the next idle worker while jobs remain.
  if redis.call('ZCARD', pending_key) > 0 then
    mark_ready()
  end

  local job_id = result[1]
  local job_data = redis.call('HGET', data_key, job_id)
  if not job_data then
    return nil
  end

  -- Store in active hash (without data, it's in data_key)
  local active_data = cjson.encode({
    workerId = worker_id,
    acquiredAt = now,
    leaseToken = lease_token
  })
  redis.call('HSET', active_key, job_id, active_data)

  return encode_job_result(job_data, overlay_key, job_id, {
    acquiredAt = now
  })
`

/**
 * True if the active hash holds the job under the given lease token.
 */
const REDIS_LEASE_LUA = `
  local function holds_lease(active_key, job_id, lease_token)
    local active_data = redis.call('HGET', active_key, job_id)
    if not active_data then
      return false
    end

    return cjson.decode(active_data).leaseToken == lease_token
  end
`

/**
 * Lua script for removing a job completely (no history).
 * Also cleans up the dedup key if the job had dedup metadata.
 */
export const REMOVE_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local overlay_key = KEYS[3]
  local dedup_prefix = KEYS[4]
  local job_id = ARGV[1]
  local lease_token = ARGV[2]

${REDIS_JOB_STORAGE_LUA}
${REDIS_LEASE_LUA}

  if not holds_lease(active_key, job_id, lease_token) then
    return 0
  end

  -- Read job data to extract dedup.id before deleting
  local job_data = redis.call('HGET', data_key, job_id)
  if job_data then
    local job = read_job_fields(job_data)
    if job.dedup and job.dedup.id then
      local dkey = dedup_prefix .. job.dedup.id
      if redis.call('GET', dkey) == job_id then
        redis.call('DEL', dkey)
      end
    end
  end

  redis.call('HDEL', active_key, job_id)
  delete_job_data(data_key, overlay_key, job_id)

  return 1
`

/**
 * Lua script for finalizing a job in history.
 * Removes from active, stores finalization info, and prunes old records.
 * When pruning removes job data, also deletes the associated dedup key.
 */
export const FINALIZE_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local history_key = KEYS[3]
  local index_key = KEYS[4]
  local overlay_key = KEYS[5]
  local dedup_prefix = KEYS[6]
  local job_id = ARGV[1]
  local now = tonumber(ARGV[2])
  local max_age = tonumber(ARGV[3])
  local max_count = tonumber(ARGV[4])
  local error_message = ARGV[5]
  local lease_token = ARGV[6]

${REDIS_JOB_STORAGE_LUA}
${REDIS_LEASE_LUA}

  -- Verify the caller still holds the job
  if not holds_lease(active_key, job_id, lease_token) then
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
        local job = read_job_fields(d)
        if job.dedup and job.dedup.id then
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
      delete_jobs_data(data_key, overlay_key, expired)
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
        delete_jobs_data(data_key, overlay_key, stale)
      end
    end
  end

  return 1
`

/**
 * Lua script for retrying a job.
 * 1. Verify the caller still holds the job
 * 2. Remove from active hash
 * 3. Increment attempts in data
 * 4. Add back to pending (or delayed if retryAt is set)
 */
export const RETRY_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local pending_key = KEYS[3]
  local delayed_key = KEYS[4]
  local overlay_key = KEYS[5]
  local job_id = ARGV[1]
  local retry_at = tonumber(ARGV[2])
  local now = tonumber(ARGV[3])
  local lease_token = ARGV[4]

${REDIS_JOB_STORAGE_LUA}
${REDIS_LEASE_LUA}
${MARKER_LUA}

  -- Verify the caller still holds the job
  if not holds_lease(active_key, job_id, lease_token) then
    return 0
  end

  -- Get job data
  local job_data = redis.call('HGET', data_key, job_id)
  if not job_data then
    return 0
  end

  -- Read everything before the first write: an error past it would leave
  -- the job in no state, since a script does not roll back its writes.
  local job = read_job_fields(job_data)
  local overlay = read_job_overlay(overlay_key, job_id)

  -- Remove from active
  redis.call('HDEL', active_key, job_id)

  -- Increment attempts without rewriting opaque job JSON.
  overlay.attempts = (overlay.attempts or job.attempts or 0) + 1
  write_job_overlay(overlay_key, job_id, overlay)

  -- Add back to pending or delayed
  if retry_at and retry_at > now then
    redis.call('ZADD', delayed_key, retry_at, job_id)
    mark_due(retry_at)
  else
    -- Score = priority * 1e13 + timestamp
    -- Lower score = higher priority, FIFO within same priority
    local priority = job.priority or 5
    local score = priority * 10000000000000 + now
    redis.call('ZADD', pending_key, score, job_id)
    mark_ready()
  end

  return 1
`

/**
 * Lua script for recovering stalled jobs.
 * Scans the active hash for jobs that have been active too long.
 * - Jobs within maxStalledCount: move back to pending with incremented stalledCount
 * - Jobs exceeding maxStalledCount: up to max_exceeded of them stay active,
 *   reacquired by the calling worker, and are returned so the worker fails
 *   them through the regular path. The others are left stalled.
 * Returns {recovered, exceeded job 1, exceeded job 2, ...}.
 */
export const RECOVER_STALLED_JOBS_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local pending_key = KEYS[3]
  local overlay_key = KEYS[4]
  local now = tonumber(ARGV[1])
  local stalled_threshold = tonumber(ARGV[2])
  local max_stalled_count = tonumber(ARGV[3])
  local worker_id = ARGV[4]
  local max_exceeded = tonumber(ARGV[5])
  local lease_token_prefix = ARGV[6]

${REDIS_JOB_STORAGE_LUA}
${MARKER_LUA}

  local recovered = 0
  -- The first element is replaced by the recovered count before returning.
  local result = { 0 }
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
        local job = read_job_fields(job_data)
        local overlay = read_job_overlay(overlay_key, job_id)
        local current_stalled_count = overlay.stalledCount or job.stalledCount or 0

        -- Check if job has exceeded max stalled count
        if current_stalled_count >= max_stalled_count then
          -- Reacquire for the calling worker, which fails it through the regular
          -- path. Past max_exceeded, the job stays stalled for a later recovery.
          if #result - 1 < max_exceeded then
            local lease_token = lease_token_prefix .. ':' .. job_id
            redis.call('HSET', active_key, job_id, cjson.encode({
              workerId = worker_id,
              acquiredAt = now,
              leaseToken = lease_token
            }))
            result[#result + 1] = encode_job_result(job_data, overlay_key, job_id, {
              acquiredAt = now,
              leaseToken = lease_token
            })
          end
        else
          redis.call('HDEL', active_key, job_id)
          -- Recover: increment stalledCount without rewriting opaque job JSON.
          overlay.stalledCount = current_stalled_count + 1
          write_job_overlay(overlay_key, job_id, overlay)
          -- Score = priority * 1e13 + timestamp
          local priority = job.priority or 5
          local score = priority * 10000000000000 + now
          redis.call('ZADD', pending_key, score, job_id)
          recovered = recovered + 1
        end
      end
    end
  end

  result[1] = recovered
  if recovered > 0 then
    mark_ready()
  end
  return result
`

/**
 * Lua script for renewing the acquired timestamp of in-flight jobs (heartbeat).
 * Only entries still present in the active hash under the given lease token
 * are renewed, so a job that was already recovered, finalized, or acquired
 * again is never resurrected by a late heartbeat.
 * Preserves the existing worker info, updating only acquiredAt.
 * ARGV = now, then job id and lease token pairs.
 * Returns the number of jobs renewed.
 */
export const RENEW_JOBS_SCRIPT = `
  local active_key = KEYS[1]
  local now = tonumber(ARGV[1])

  local renewed = 0
  for i = 2, #ARGV, 2 do
    local job_id = ARGV[i]
    local lease_token = ARGV[i + 1]
    local active_data = redis.call('HGET', active_key, job_id)
    if active_data then
      local active = cjson.decode(active_data)
      -- Only the current holder of the lease may renew it.
      if active.leaseToken == lease_token then
        active.acquiredAt = now
        redis.call('HSET', active_key, job_id, cjson.encode(active))
        renewed = renewed + 1
      end
    end
  end

  return renewed
`

/**
 * Lua script for getting a job record with its status.
 */
export const GET_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local pending_key = KEYS[2]
  local delayed_key = KEYS[3]
  local active_key = KEYS[4]
  local completed_key = KEYS[5]
  local failed_key = KEYS[6]
  local overlay_key = KEYS[7]
  local job_id = ARGV[1]

${REDIS_JOB_STORAGE_LUA}

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

  return encode_job_result(job_data, overlay_key, job_id, {
    status = status,
    finishedAt = finished_at,
    error = error_msg
  })
`

/**
 * Time a worker has to finalize its cron claim. A cron claim not finalized
 * stays in the due index until then, so a claimer finds it once its worker
 * died and finalizes it. A claimer finalizing a claim whose worker is only
 * slow writes the same next run, and only one of them applies.
 */
const CRON_CLAIM_LEASE_MS = 10_000

const SCHEDULE_DUE_INDEX_LUA = `
  local cron_claim_lease_ms = ${CRON_CLAIM_LEASE_MS}

  -- Returns the end of the lease of a cron claim not finalized yet, or nil.
  local function cron_claim_expires_at(next_run_at, claim_token, claimed_at)
    if (next_run_at or '') ~= '' or (claim_token or '') == '' then
      return nil
    end
    local claimed_at_ms = tonumber(claimed_at or '')
    return claimed_at_ms and (claimed_at_ms + cron_claim_lease_ms) or nil
  end

  local function sync_schedule_due_index(schedule_key, due_key, id)
    local status = redis.call('HGET', schedule_key, 'status')
    local fields = redis.call('HMGET', schedule_key, 'next_run_at', 'claim_token', 'claimed_at')
    local score = tonumber(fields[1] or '')
      or cron_claim_expires_at(fields[1], fields[2], fields[3])

    if status == 'active' and score then
      redis.call('ZADD', due_key, score, id)
    else
      redis.call('ZREM', due_key, id)
    end
  end
`

/**
 * Atomically upserts schedule configuration while preserving runtime fields
 * and synchronizing the derived due index from the resulting hash.
 *
 * A new schedule is created active with the given next run. An existing one
 * keeps its status, run metadata, and next run, unless its timing changes:
 * then it takes the given next run, and a cron claim in flight is
 * invalidated so its finalization cannot overwrite it. With the same timing,
 * the claim stays valid, since the next run it computes is still right.
 *
 * Fails without changes while the schedule is still stored at its 0.7 key:
 * migrate() keeps a hash already at the new key and drops the 0.7 one, so a
 * schedule defined before the migration would lose its status and run count.
 *
 * Returns the claim token, config revision, and claim time of a cron claim
 * not finalized yet, so the caller can finalize it in case the process that
 * claimed it died. Returns nil otherwise.
 */
export const UPSERT_SCHEDULE_SCRIPT = `
  local schedule_key = KEYS[1]
  local schedules_index_key = KEYS[2]
  local due_key = KEYS[3]
  local legacy_schedule_key = KEYS[4]
  local id = ARGV[1]
  local now = ARGV[2]
  local schedule = cjson.decode(ARGV[3])
  local next_run_at = ARGV[4]

${SCHEDULE_DUE_INDEX_LUA}
${MARKER_LUA}

  -- The same test as migrate(): an indexed id with a 0.7 hash at its old key.
  if redis.call('SISMEMBER', schedules_index_key, id) == 1
    and redis.call('TYPE', legacy_schedule_key).ok == 'hash'
    and redis.call('HGET', legacy_schedule_key, 'id') == id then
    return redis.error_reply(
      'Schedule "' .. id .. '" is still stored in the format used before 0.8. ' ..
      'Stop every process running the previous version, then run migrate() on the Redis adapter.'
    )
  end

  local exists = redis.call('EXISTS', schedule_key) == 1
  local timing_changed = not exists
  local timing_fields = { 'cron_expression', 'every_ms', 'timezone', 'from_date', 'to_date', 'run_limit' }
  for _, field in ipairs(timing_fields) do
    if (redis.call('HGET', schedule_key, field) or '') ~= (schedule[field] or '') then
      timing_changed = true
    end
  end

  local run_count = redis.call('HGET', schedule_key, 'run_count') or '0'
  local created_at = redis.call('HGET', schedule_key, 'created_at') or now
  local config_revision = tonumber(redis.call('HGET', schedule_key, 'config_revision') or '0')

  redis.call('HDEL', schedule_key, 'cron_expression', 'every_ms', 'from_date', 'to_date', 'run_limit')

  for field, value in pairs(schedule) do
    redis.call('HSET', schedule_key, field, value)
  end

  if not exists then
    redis.call('HSET', schedule_key, 'status', 'active')
  end

  if timing_changed then
    config_revision = config_revision + 1
    redis.call('HDEL', schedule_key, 'claim_token', 'claimed_at')
    redis.call('HSET', schedule_key, 'next_run_at', next_run_at)
  end

  redis.call(
    'HSET',
    schedule_key,
    'run_count',
    run_count,
    'created_at',
    created_at,
    'config_revision',
    tostring(config_revision)
  )
  redis.call('SADD', schedules_index_key, id)
  sync_schedule_due_index(schedule_key, due_key, id)
  mark_first_of(due_key)

  -- A finished schedule never keeps a claim token, so an empty next run with
  -- one is a cron claim not finalized yet.
  local claim_token = redis.call('HGET', schedule_key, 'claim_token') or ''
  if claim_token ~= '' and redis.call('HGET', schedule_key, 'next_run_at') == '' then
    return {
      claim_token,
      tostring(config_revision),
      redis.call('HGET', schedule_key, 'claimed_at') or ''
    }
  end

  return false
`

/**
 * Atomically updates schedule runtime fields and synchronizes the derived due
 * index from the resulting canonical hash.
 */
export const UPDATE_SCHEDULE_SCRIPT = `
  local schedule_key = KEYS[1]
  local due_key = KEYS[2]
  local id = ARGV[1]
  local updates = cjson.decode(ARGV[2])

${SCHEDULE_DUE_INDEX_LUA}
${MARKER_LUA}

  if redis.call('EXISTS', schedule_key) == 0 then
    redis.call('ZREM', due_key, id)
    mark_first_of(due_key)
    return 0
  end

  for field, value in pairs(updates) do
    redis.call('HSET', schedule_key, field, value)
  end

  -- An explicit next run supersedes an in-flight cron claim, so the claim's
  -- finalization must not overwrite it.
  if updates.next_run_at ~= nil then
    redis.call('HDEL', schedule_key, 'claim_token', 'claimed_at')
  end

  sync_schedule_due_index(schedule_key, due_key, id)
  mark_first_of(due_key)

  return 1
`

/**
 * Finalizes the JS-calculated next run for a cron claim only when the same
 * claimed occurrence still owns the canonical hash.
 */
export const FINALIZE_CRON_SCHEDULE_SCRIPT = `
  local schedule_key = KEYS[1]
  local due_key = KEYS[2]
  local id = ARGV[1]
  local expected_cron_expression = ARGV[2]
  local expected_config_revision = ARGV[3]
  local expected_claim_token = ARGV[4]
  local next_run_at = ARGV[5]

${SCHEDULE_DUE_INDEX_LUA}
${MARKER_LUA}

  if redis.call('EXISTS', schedule_key) == 0 then
    redis.call('ZREM', due_key, id)
    mark_first_of(due_key)
    return 0
  end

  local current_next_run_at = redis.call('HGET', schedule_key, 'next_run_at')
  local cron_expression = redis.call('HGET', schedule_key, 'cron_expression')
  local config_revision = redis.call('HGET', schedule_key, 'config_revision') or ''
  local claim_token = redis.call('HGET', schedule_key, 'claim_token') or ''

  -- Preserve the next run while paused; index synchronization excludes it until resume.
  if current_next_run_at ~= ''
    or cron_expression ~= expected_cron_expression
    or config_revision ~= expected_config_revision
    or claim_token ~= expected_claim_token then
    if claim_token == expected_claim_token then
      redis.call('HDEL', schedule_key, 'claim_token', 'claimed_at')
    end
    sync_schedule_due_index(schedule_key, due_key, id)
    mark_first_of(due_key)
    return 0
  end

  local run_count = tonumber(redis.call('HGET', schedule_key, 'run_count') or '0')
  local run_limit = tonumber(redis.call('HGET', schedule_key, 'run_limit') or '')
  local to_date = tonumber(redis.call('HGET', schedule_key, 'to_date') or '')
  local next_score = tonumber(next_run_at)

  if (run_limit and run_count >= run_limit)
    or (to_date and next_score and next_score > to_date) then
    next_run_at = ''
  end

  redis.call('HSET', schedule_key, 'next_run_at', next_run_at)
  redis.call('HDEL', schedule_key, 'claim_token', 'claimed_at')
  sync_schedule_due_index(schedule_key, due_key, id)
  mark_first_of(due_key)

  return 1
`

/**
 * Atomically migrates schedule storage to the current layout.
 *
 * 1. Moves schedule hashes from `schedules::<id>` (0.7 and earlier) to
 *    `schedules::data::<id>`. A hash already stored at the new key is kept and
 *    the legacy copy is dropped.
 * 2. Rebuilds the derived due index from the canonical schedule hashes.
 *
 * Legacy keys can overlap new ones: the 0.7 hash of the id `data::x` is stored
 * at the new key of the id `x`. A key is treated as legacy only when its `id`
 * field matches, and every legacy hash is removed before any is written back.
 * The move also runs before the index rebuild because a 0.7 schedule with the
 * id `due` is stored at the due index key.
 *
 * Every destination is validated before anything changes. A destination that
 * holds anything other than this schedule, such as the `next_run_at`-only hash
 * a 0.7 cron finalization recreates after a delete, aborts the migration.
 *
 * This is an explicit O(N) migration and blocks concurrent Redis commands
 * until the complete index reflects one consistent point in time.
 * Returns the number of indexed schedules.
 */
export const MIGRATE_SCHEDULES_SCRIPT = `
  local schedules_index_key = KEYS[1]
  local due_key = KEYS[2]
  local schedule_key_prefix = KEYS[3]
  local legacy_schedule_key_prefix = KEYS[4]
  local ids = redis.call('SMEMBERS', schedules_index_key)
  local count = 0

${SCHEDULE_DUE_INDEX_LUA}

  local legacy_schedules = {}
  local legacy_keys = {}

  for i = 1, #ids do
    local id = ids[i]
    local legacy_key = legacy_schedule_key_prefix .. id

    if redis.call('TYPE', legacy_key).ok == 'hash'
      and redis.call('HGET', legacy_key, 'id') == id then
      legacy_schedules[#legacy_schedules + 1] = {
        id = id,
        legacy_key = legacy_key,
        schedule_key = schedule_key_prefix .. id,
        fields = redis.call('HGETALL', legacy_key),
      }
      legacy_keys[legacy_key] = true
    end
  end

  for i = 1, #legacy_schedules do
    local schedule = legacy_schedules[i]
    local schedule_key = schedule.schedule_key

    -- A destination freed by this migration is not a conflict.
    if not legacy_keys[schedule_key] and redis.call('EXISTS', schedule_key) == 1 then
      if redis.call('TYPE', schedule_key).ok ~= 'hash'
        or redis.call('HGET', schedule_key, 'id') ~= schedule.id then
        return redis.error_reply(
          'Cannot migrate schedule "' .. schedule.id .. '": ' .. schedule_key ..
          ' already exists and does not belong to it. Remove or rename that key, then run migrate() again.'
        )
      end
      schedule.already_migrated = true
    end
  end

  for i = 1, #legacy_schedules do
    redis.call('DEL', legacy_schedules[i].legacy_key)
  end

  for i = 1, #legacy_schedules do
    local schedule = legacy_schedules[i]
    if not schedule.already_migrated then
      redis.call('HSET', schedule.schedule_key, unpack(schedule.fields))
    end
  end

  redis.call('DEL', due_key)

  for i = 1, #ids do
    local id = ids[i]
    sync_schedule_due_index(schedule_key_prefix .. id, due_key, id)
    if redis.call('ZSCORE', due_key, id) then
      count = count + 1
    end
  end

  return count
`

/**
 * Lua script for atomically claiming a due schedule using a sorted set index.
 *
 * Uses ZRANGEBYSCORE on schedules::due (scored by next_run_at) for O(log N)
 * lookup instead of scanning all schedule hashes via SMEMBERS.
 *
 * Stale entries (paused, exhausted, deleted) are cleaned from the ZSET on
 * sight so subsequent calls skip them.
 *
 * A cron claim stays indexed until its lease ends. A claim its worker did not
 * finalize by then is returned as `unfinalized` instead of claiming anything,
 * so the caller can finalize it as its worker would have.
 *
 * KEYS[1] = schedules::due (the ZSET)
 * KEYS[2] = schedule key prefix (e.g. "schedules::data::")
 * ARGV[1] = now (epoch milliseconds)
 * ARGV[2] = claim token, kept by the schedule when it is a cron
 * KEYS[3] = schedules marker, written when the due index changes
 *
 * Returns nil, or `{ 'claimed' | 'unfinalized', <schedule hash as JSON> }`.
 */
export const CLAIM_SCHEDULE_SCRIPT = `
  local due_key = KEYS[1]
  local prefix = KEYS[2]
  local now = tonumber(ARGV[1])
  local claim_token = ARGV[2]

${SCHEDULE_DUE_INDEX_LUA}
${MARKER_LUA}

  local skipped = false

  while true do
    local candidates = redis.call('ZRANGEBYSCORE', due_key, '-inf', tostring(now), 'LIMIT', 0, 1)

    if #candidates == 0 then
      if skipped then
        mark_first_of(due_key)
      end
      return nil
    end

    skipped = true

    local id = candidates[1]
    local schedule_key = prefix .. id

    -- Get schedule data
    local data = redis.call('HGETALL', schedule_key)

    -- Deleted schedule still in ZSET
    if #data == 0 then
      redis.call('ZREM', due_key, id)
    else
      -- Convert HGETALL result to table
      local schedule = {}
      for j = 1, #data, 2 do
        schedule[data[j]] = data[j + 1]
      end

      -- Check if schedule is active
      if schedule.status ~= 'active' then
        redis.call('ZREM', due_key, id)
      else
        -- Hash is the source of truth for next_run_at.
        -- If the ZSET score is stale, repair it and skip this candidate.
        local hash_nra = schedule.next_run_at
        local hash_score = hash_nra and tonumber(hash_nra) or nil
        local claim_expires_at =
          cron_claim_expires_at(hash_nra, schedule.claim_token, schedule.claimed_at)
        if claim_expires_at and claim_expires_at <= now then
          -- Its worker did not finalize it in time. Hide it for another lease, so
          -- concurrent claimers skip it and a failing finalization does not block
          -- other schedules.
          redis.call('ZADD', due_key, now + cron_claim_lease_ms, id)
          return { 'unfinalized', cjson.encode(schedule) }
        elseif claim_expires_at then
          redis.call('ZADD', due_key, claim_expires_at, id)
        elseif not hash_score then
          redis.call('ZREM', due_key, id)
        elseif hash_score > now then
          redis.call('ZADD', due_key, hash_score, id)
        else
          local run_count = tonumber(schedule.run_count or '0')
          local run_limit = schedule.run_limit and tonumber(schedule.run_limit) or nil
          local to_date = schedule.to_date and tonumber(schedule.to_date) or nil

          -- Check limits
          if (run_limit and run_count >= run_limit) or (to_date and now > to_date) then
            redis.call('ZREM', due_key, id)
          else
            -- This schedule is claimable - atomically update it
            local new_run_count = run_count + 1
            local new_claim_token = ''
            local new_claimed_at = ''

            -- Calculate new next_run_at (simple interval-based for now)
            -- Complex cron calculation happens in the caller
            local new_next_run_at = ''
            local every_ms = schedule.every_ms and tonumber(schedule.every_ms) or nil
            if every_ms then
              new_next_run_at = tostring(now + every_ms)
            elseif schedule.cron_expression then
              new_claim_token = claim_token
              new_claimed_at = tostring(now)
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
              'run_count', tostring(new_run_count),
              'claim_token', new_claim_token,
              'claimed_at', new_claimed_at)

            sync_schedule_due_index(schedule_key, due_key, id)
            mark_first_of(due_key)

            -- Return the schedule data (before update) as JSON
            return { 'claimed', cjson.encode(schedule) }
          end
        end
      end
    end
  end
`
