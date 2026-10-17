# Job Files That Fail to Load Go to the Logger

## Bug Fix

When a file matched by `locations` failed to import, for example because of a syntax error, the
Locator wrote it with `console.warn`, bypassing the configured logger. QueueManager then also warned
`No jobs found for locations ... Verify your glob patterns match your job files.`, although the
patterns did match.

A file that fails to load is now logged at the `error` level through the configured logger, with
the error in `err` and the path in `file`. The `No jobs found` warning is only logged when no file
failed to load. `Locator.registerFromGlob()` accepts a `logger` option, which defaults to the
console logger, and now logs the `No jobs found` warning itself.
