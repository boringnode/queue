# Redis Empty Array Payloads

## Bug Fix

The Redis adapter now preserves empty arrays in job payloads across all Redis-side job lifecycle operations.

Previously, some Lua scripts decoded and re-encoded stored job JSON through Redis `cjson`. Redis represents Lua tables ambiguously, so empty arrays could be serialized back as empty objects. This could affect jobs when they were acquired, inspected, retried, recovered after stalling, or updated through dedup replacement.

The Redis adapter now keeps stored job JSON opaque to Lua and stores Redis-side mutable fields separately, so payload values such as `[]` remain unchanged.

## Upgrade Notes

Existing Redis jobs continue to work after upgrading. Jobs that do not yet have the new Redis metadata are read from their existing stored JSON, and retry/stalled counters continue from their previous values.

Jobs that were already corrupted before upgrading cannot be automatically repaired, because an already-stored `{}` cannot be distinguished from an original `[]`.
