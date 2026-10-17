# Retries From `defaultJobOptions` Apply

## Bug Fix

`maxRetries` and `retry` were typed and accepted in `defaultJobOptions`, globally and per queue, but
ignored: only the job's own options and the dedicated `retry` configs were applied. A job expected
to retry through these defaults failed at its first error.

`maxRetries` and `backoff` now each come from the first place that sets them: the job's options,
then the queue's `defaultJobOptions`, then the queue's `retry`, then the global
`defaultJobOptions`, then the global `retry`. A queue setting wins over a global one.

## Breaking Changes

If your config sets `maxRetries` or `retry` in `defaultJobOptions`, failed jobs now retry according
to it. Remove these settings to keep the previous behavior.
