# Defining a Schedule Again Keeps Its Status and Next Run

## Bug Fix

Applications define their schedules at boot, so every deploy ran `Job.schedule()...` again. Each
run reset the schedule:

- it forced `status: 'active'`, so a schedule paused by an operator resumed at the next deploy;
- it recalculated the next run from now, so an `every('1h')` schedule never ran if you deployed
  more often than once an hour;
- it wrote the definition and the next run in two separate writes, so concurrent boots could mix
  them.

Defining an existing schedule now updates it without resetting it. It keeps its status, run count,
and last run. It keeps its next run too, unless its timing changes: cron expression, interval,
timezone, `from`, `to`, or `limit`. A payload change alone does not move the next run. A new
schedule is created active, as before. Workers that boot together and define the same schedule
end with one definition and the next run that goes with it.

A next run that passed while the application was stopped is kept too, so the schedule runs once
when a worker starts, provided it is active and has not reached its run limit or end date. 0.7
skipped to the first occurrence after the restart. On Redis, this includes a cron schedule whose
claim a crash interrupted: it runs the occurrence after the interrupted one, under the same
conditions.

## Breaking Changes

`schedule.resume()` is now the only way to resume a paused schedule. Code that relied on defining
the schedule again to resume it must call `resume()`.

Custom adapters must implement the new `upsertSchedule()` contract: keep the status, run count,
last run, and creation date of an existing schedule, and its next run unless the timing changes.
`ScheduleConfig` has a new `nextRunAt` field, the next run of a new schedule or of one whose timing
changes. `ScheduleBuilder.run()` no longer calls `updateSchedule()` after `upsertSchedule()`.
