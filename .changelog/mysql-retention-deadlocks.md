# Count and Age Retention No Longer Lose Completions on MySQL

## Bug Fix

With `removeOnComplete` or `removeOnFail` set to a `count` or an `age`, every finalization pruned
the history of its queue with one `DELETE` that scanned the finished jobs. On MySQL, that scan
locked a range of the status index, which the finalizations of other workers also write to.
Under load, InnoDB resolved the resulting deadlocks by aborting the update that marked a job
completed, often more times than the worker retries: the job stayed active, the stalled check ran
it again, and the queue processed duplicates.

The Knex and Kysely adapters now prune in two steps: a plain read of the jobs to remove, then a
`DELETE` of those jobs by primary key. It locks only the deleted rows, so concurrent finalizations
no longer deadlock on the history. One pruning removes at most 500 jobs; a larger backlog, for
example after lowering a `count`, shrinks over the next finalizations.
