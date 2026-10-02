# Lighter Worker Polling and Faster Shutdown

## Bug Fixes

- An idle Worker now polls each queue once per cycle, whatever its concurrency. It used to start one
  acquisition per free slot, and each acquisition polled every queue: a Worker with a concurrency of
  50 on 3 queues made 150 empty `popFrom` calls per cycle, and now makes 3. When the first poll finds
  a Job, the remaining slots are filled in parallel as before.
- Waiting for a Job to complete no longer leaks memory. Each idle tick of a Worker with spare
  capacity used to attach a new handler to every running Job, and these handlers were only released
  when the Jobs finished. Memory grew for as long as long-running Jobs ran. Completions are now
  queued with a single handler per Job, and one wait is reused across idle ticks.
- `worker.stop()` now interrupts the wait for a running Job to complete. With a Job running and a
  free slot, it could previously wait up to `idleDelay` before noticing the stop request.

No configuration changes are required.
