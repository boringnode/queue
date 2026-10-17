# Sync Adapter Cancels Delayed Jobs on Destroy

## Bug Fix

The Sync adapter ran a delayed job (`.in()`) in an untracked timer, which `destroy()` left running:
the job still ran after `QueueManager.destroy()`, against a torn down QueueManager, and the timer
kept the process alive until it fired.

`destroy()` now cancels the delayed jobs that have not run yet, as the Fake adapter does.
