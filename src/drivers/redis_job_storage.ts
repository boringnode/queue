import type { JobData } from '../types/main.js'

/**
 * Redis stores the original job JSON as an opaque payload in the data hash.
 * Lua scripts write only mutable Redis-side overrides in this overlay so
 * legacy jobs without overlays keep decoding from their original data.
 */
export type RedisJobOverlay = Partial<Pick<JobData, 'attempts' | 'stalledCount'>> & {
  payload?: string
  payloadUndefined?: boolean
}

export function hydrateRedisJob(data: string, overlay?: string | null | false): JobData {
  const jobData = JSON.parse(data) as JobData
  const parsedOverlay = overlay ? (JSON.parse(overlay) as RedisJobOverlay) : undefined

  if (parsedOverlay?.payloadUndefined) {
    jobData.payload = undefined
  } else if (parsedOverlay?.payload !== undefined) {
    jobData.payload = JSON.parse(parsedOverlay.payload)
  }

  return {
    ...jobData,
    ...(parsedOverlay?.attempts !== undefined ? { attempts: parsedOverlay.attempts } : {}),
    ...(parsedOverlay?.stalledCount !== undefined
      ? { stalledCount: parsedOverlay.stalledCount }
      : {}),
  }
}

export function encodeRedisJobPayloadOverlay(payload: JobData['payload']): [string, string] {
  const encoded = JSON.stringify(payload)

  return encoded === undefined ? ['', '1'] : [encoded, '0']
}

export const REDIS_JOB_STORAGE_LUA = `
  local function read_job_overlay(overlay_key, job_id)
    local overlay_json = redis.call('HGET', overlay_key, job_id)
    if overlay_json then
      return cjson.decode(overlay_json)
    end
    return {}
  end

  local function write_job_overlay(overlay_key, job_id, overlay)
    local has_overlay = false
    for _ in pairs(overlay) do
      has_overlay = true
      break
    end

    if has_overlay then
      redis.call('HSET', overlay_key, job_id, cjson.encode(overlay))
    else
      redis.call('HDEL', overlay_key, job_id)
    end
  end

  local function store_job_data(data_key, overlay_key, job_id, job_data)
    redis.call('HDEL', overlay_key, job_id)
    redis.call('HSET', data_key, job_id, job_data)
  end

  local function delete_job_data(data_key, overlay_key, job_id)
    redis.call('HDEL', data_key, job_id)
    redis.call('HDEL', overlay_key, job_id)
  end

  local function delete_jobs_data(data_key, overlay_key, job_ids)
    if #job_ids > 0 then
      redis.call('HDEL', data_key, unpack(job_ids))
      redis.call('HDEL', overlay_key, unpack(job_ids))
    end
  end

  local function encode_job_result(job_data, overlay_key, job_id, result)
    local encoded = result or {}
    encoded.data = job_data

    local overlay_json = redis.call('HGET', overlay_key, job_id)
    if overlay_json then
      encoded.overlay = overlay_json
    end

    return cjson.encode(encoded)
  end

  local function apply_payload_overlay(overlay, payload_data, payload_is_undefined)
    if payload_is_undefined == 1 then
      overlay.payload = nil
      overlay.payloadUndefined = true
    else
      overlay.payload = payload_data
      overlay.payloadUndefined = nil
    end
  end
`

export const REDIS_DEDUP_LUA = `
  local function resolve_dedup_existing_job(
    data_key,
    state_key_a,
    state_key_b,
    overlay_key,
    dedup_key,
    extend,
    replace,
    payload_data,
    payload_is_undefined
  )
    local existing = redis.call('GET', dedup_key)
    if not existing then
      return nil
    end

    local existing_data = redis.call('HGET', data_key, existing)
    if not existing_data then
      return nil
    end

    local in_state_a = redis.call('ZSCORE', state_key_a, existing)
    local in_state_b = redis.call('ZSCORE', state_key_b, existing)
    local replaceable = in_state_a or in_state_b
    local ok, existing_job = pcall(cjson.decode, existing_data)
    local original_ttl = nil

    if ok and type(existing_job) == 'table' and existing_job.dedup then
      original_ttl = tonumber(existing_job.dedup.ttl)
    end

    if replace == 1 and replaceable then
      if ok and existing_job then
        local overlay = read_job_overlay(overlay_key, existing)
        apply_payload_overlay(overlay, payload_data, payload_is_undefined)
        write_job_overlay(overlay_key, existing, overlay)

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
`
