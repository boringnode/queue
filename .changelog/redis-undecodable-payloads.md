# Redis Queues Survive Payloads Lua Cannot Decode

## Bug Fix

Redis Lua scripts decode job JSON with cjson, which rejects some JSON that `JSON.parse` accepts: a
lone surrogate such as `'\ud800'`, or more than 1000 levels of nesting. One such payload poisoned
its queue:

- `popFrom()` failed every time once the job was in the queue, or due as a delayed job, so no job of
  that queue ran;
- `retryJob()` removed the job from the active jobs before failing, so it ended in no state and
  never ran again;
- `recoverStalledJobs()` failed, so stalled jobs of that queue were never recovered.

The scripts no longer need to decode the payload: they read the priority, attempts, and stalled
count with a guarded decode, and fall back to the defaults (priority 5) for such a job. Scripts
read everything they need before their first write.
