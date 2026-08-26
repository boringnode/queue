import { REDIS_DEDUP_LUA, REDIS_JOB_STORAGE_LUA } from './redis_job_storage.js'

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

  store_job_data(data_key, overlay_key, job_id, job_data)
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

${REDIS_JOB_STORAGE_LUA}
${REDIS_DEDUP_LUA}

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

  store_job_data(data_key, overlay_key, job_id, job_data)
  redis.call('ZADD', delayed_key, execute_at, job_id)

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

${REDIS_JOB_STORAGE_LUA}

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

  return encode_job_result(job_data, overlay_key, job_id, {
    acquiredAt = now
  })
`

/**
 * Lua script for removing a job completely (no history).
 * Also cleans up the dedup key if the job had dedup metadata.
 */
export const REMOVE_JOB_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local overlay_key = KEYS[3]
  local job_id = ARGV[1]
  local dedup_prefix = ARGV[2]

${REDIS_JOB_STORAGE_LUA}

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
  local job_id = ARGV[1]
  local now = tonumber(ARGV[2])
  local max_age = tonumber(ARGV[3])
  local max_count = tonumber(ARGV[4])
  local error_message = ARGV[5]
  local dedup_prefix = ARGV[6]

${REDIS_JOB_STORAGE_LUA}

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
 * 1. Verify job is active
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

${REDIS_JOB_STORAGE_LUA}

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

  -- Increment attempts without rewriting opaque job JSON.
  local job = cjson.decode(job_data)
  local overlay = read_job_overlay(overlay_key, job_id)
  overlay.attempts = (overlay.attempts or job.attempts or 0) + 1
  write_job_overlay(overlay_key, job_id, overlay)

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
export const RECOVER_STALLED_JOBS_SCRIPT = `
  local data_key = KEYS[1]
  local active_key = KEYS[2]
  local pending_key = KEYS[3]
  local overlay_key = KEYS[4]
  local now = tonumber(ARGV[1])
  local stalled_threshold = tonumber(ARGV[2])
  local max_stalled_count = tonumber(ARGV[3])
  local dedup_prefix = ARGV[4]

${REDIS_JOB_STORAGE_LUA}

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
        local overlay = read_job_overlay(overlay_key, job_id)
        local current_stalled_count = overlay.stalledCount or job.stalledCount or 0

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
          delete_job_data(data_key, overlay_key, job_id)
        else
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

  return recovered
`

/**
 * Lua script for renewing the acquired timestamp of in-flight jobs (heartbeat).
 * Only entries still present in the active hash AND still owned by the calling
 * worker are renewed, so a job that was already recovered, finalized, or
 * re-acquired by another worker is never resurrected by a late heartbeat.
 * Preserves the existing worker info, updating only acquiredAt.
 * Returns the number of jobs renewed.
 */
export const RENEW_JOBS_SCRIPT = `
  local active_key = KEYS[1]
  local now = tonumber(ARGV[1])
  local worker_id = ARGV[2]

  local renewed = 0
  for i = 3, #ARGV do
    local job_id = ARGV[i]
    local active_data = redis.call('HGET', active_key, job_id)
    if active_data then
      local active = cjson.decode(active_data)
      -- Only the worker that currently owns the lease may renew it.
      if active.workerId == worker_id then
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

const SCHEDULE_DUE_INDEX_LUA = `
  local function sync_schedule_due_index(schedule_key, due_key, id)
    local status = redis.call('HGET', schedule_key, 'status')
    local next_run_at = redis.call('HGET', schedule_key, 'next_run_at')
    local score = next_run_at and tonumber(next_run_at) or nil

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
 */
export const UPSERT_SCHEDULE_SCRIPT = `
  local schedule_key = KEYS[1]
  local schedules_index_key = KEYS[2]
  local due_key = KEYS[3]
  local id = ARGV[1]
  local now = ARGV[2]
  local schedule = cjson.decode(ARGV[3])

${SCHEDULE_DUE_INDEX_LUA}

  local run_count = redis.call('HGET', schedule_key, 'run_count') or '0'
  local created_at = redis.call('HGET', schedule_key, 'created_at') or now

  redis.call(
    'HDEL',
    schedule_key,
    'cron_expression',
    'every_ms',
    'from_date',
    'to_date',
    'run_limit'
  )

  for field, value in pairs(schedule) do
    redis.call('HSET', schedule_key, field, value)
  end

  redis.call('HSET', schedule_key, 'run_count', run_count, 'created_at', created_at)
  redis.call('SADD', schedules_index_key, id)
  sync_schedule_due_index(schedule_key, due_key, id)

  return id
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

  for field, value in pairs(updates) do
    redis.call('HSET', schedule_key, field, value)
  end

  sync_schedule_due_index(schedule_key, due_key, id)

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
  local expected_run_count = ARGV[2]
  local expected_last_run_at = ARGV[3]
  local expected_cron_expression = ARGV[4]
  local next_run_at = ARGV[5]

${SCHEDULE_DUE_INDEX_LUA}

  if redis.call('EXISTS', schedule_key) == 0 then
    redis.call('ZREM', due_key, id)
    return 0
  end

  local status = redis.call('HGET', schedule_key, 'status')
  local run_count = redis.call('HGET', schedule_key, 'run_count')
  local last_run_at = redis.call('HGET', schedule_key, 'last_run_at')
  local current_next_run_at = redis.call('HGET', schedule_key, 'next_run_at')
  local cron_expression = redis.call('HGET', schedule_key, 'cron_expression')

  if status ~= 'active'
    or run_count ~= expected_run_count
    or last_run_at ~= expected_last_run_at
    or current_next_run_at ~= ''
    or cron_expression ~= expected_cron_expression then
    sync_schedule_due_index(schedule_key, due_key, id)
    return 0
  end

  redis.call('HSET', schedule_key, 'next_run_at', next_run_at)
  sync_schedule_due_index(schedule_key, due_key, id)

  return 1
`

/**
 * Atomically rebuilds the derived due index from canonical schedule hashes.
 * This is an explicit O(N) migration and blocks concurrent Redis commands
 * until the complete index reflects one consistent point in time.
 */
export const BACKFILL_SCHEDULE_DUE_INDEX_SCRIPT = `
  local schedules_index_key = KEYS[1]
  local due_key = KEYS[2]
  local schedule_key_prefix = KEYS[3]
  local ids = redis.call('SMEMBERS', schedules_index_key)
  local count = 0

  redis.call('DEL', due_key)

  for i = 1, #ids do
    local id = ids[i]
    local schedule_key = schedule_key_prefix .. id
    local status = redis.call('HGET', schedule_key, 'status')
    local next_run_at = redis.call('HGET', schedule_key, 'next_run_at')
    local score = next_run_at and tonumber(next_run_at) or nil

    if status == 'active' and score then
      redis.call('ZADD', due_key, score, id)
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
 * KEYS[1] = schedules::due (the ZSET)
 * KEYS[2] = schedule key prefix (e.g. "schedules::")
 * ARGV[1] = now (epoch milliseconds)
 */
export const CLAIM_SCHEDULE_SCRIPT = `
  local due_key = KEYS[1]
  local prefix = KEYS[2]
  local now = tonumber(ARGV[1])

  while true do
    local candidates = redis.call('ZRANGEBYSCORE', due_key, '-inf', tostring(now), 'LIMIT', 0, 1)

    if #candidates == 0 then
      return nil
    end

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
        if not hash_score then
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

          -- Update or remove from ZSET
          if new_next_run_at ~= '' then
            redis.call('ZADD', due_key, tonumber(new_next_run_at), id)
          else
            redis.call('ZREM', due_key, id)
          end

          -- Return the schedule data (before update) as JSON
          return cjson.encode(schedule)
        end
        end
      end
    end
  end
`
