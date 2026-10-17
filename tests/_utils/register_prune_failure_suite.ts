import { test as JapaTest } from '@japa/runner'
import type { Adapter, AcquiredJob } from '../../src/contracts/adapter.js'
import type { Logger } from '../../src/logger.js'
import type { JobRetention } from '../../src/types/main.js'
import { MemoryLogger } from '../_mocks/memory_logger.js'

interface PruneFailureSuiteOptions {
  test: typeof JapaTest
  createAdapter: (logger: Logger) => Adapter | Promise<Adapter>
  /** Make the next `count` row deletions on the jobs table raise `sqlstate`. */
  failDeletes: (sqlstate: string, count: number) => Promise<void>
}

const finalizers = {
  completeJob: (adapter: Adapter, job: AcquiredJob, retention: JobRetention) =>
    adapter.completeJob(job, 'prune-failure', retention),
  failJob: (adapter: Adapter, job: AcquiredJob, retention: JobRetention) =>
    adapter.failJob(job, 'prune-failure', new Error('boom'), retention),
}

/**
 * The finalization of a kept job commits before its retention pruning: a
 * failed pruning must neither undo it nor go unnoticed.
 */
export function registerPruneFailureSuite(options: PruneFailureSuiteOptions) {
  for (const [name, finalize] of Object.entries(finalizers)) {
    const setup = async () => {
      const logger = new MemoryLogger()
      const adapter = await options.createAdapter(logger)
      adapter.setWorkerId('worker-1')

      for (const id of ['old-job', 'new-job']) {
        await adapter.pushOn('prune-failure', { id, name: 'TestJob', payload: {}, attempts: 0 })
      }
      const oldJob = (await adapter.popFrom('prune-failure'))!
      await finalize(adapter, oldJob, false)
      const newJob = (await adapter.popFrom('prune-failure'))!

      return { adapter, logger, newJob }
    }

    options.test(`${name} retries a retention pruning that lost a deadlock`, async ({ assert }) => {
      const { adapter, logger, newJob } = await setup()
      await options.failDeletes('40001', 1)

      assert.isTrue(await finalize(adapter, newJob, { count: 1 }))

      assert.isNull(await adapter.getJob('old-job', 'prune-failure'))
      assert.isNotNull(await adapter.getJob('new-job', 'prune-failure'))
      assert.deepEqual(logger.logs, [])
    })

    options.test(`${name} logs a retention pruning that fails`, async ({ assert }) => {
      const { adapter, logger, newJob } = await setup()
      await options.failDeletes('P0001', 100)

      // The job is finalized: the next finalization prunes the history again.
      assert.isTrue(await finalize(adapter, newJob, { count: 1 }))

      assert.isNotNull(await adapter.getJob('old-job', 'prune-failure'))
      const finalized = await adapter.getJob('new-job', 'prune-failure')
      assert.notEqual(finalized!.status, 'active')
      assert.lengthOf(logger.logs, 1)
      assert.equal(logger.logs[0].level, 'error')
      assert.equal(logger.logs[0].obj?.queue, 'prune-failure')
      const error = logger.logs[0].obj!.err as Error & { code?: string }
      assert.equal(error.code, 'P0001')
    })
  }
}

/** SQL creating a PostgreSQL trigger that fails the next `count` row deletions of `table`. */
export function pgDeleteFailureSql(table: string, sqlstate: string, count: number): string[] {
  const name = `${table}_delete_failure`
  return [
    `drop sequence if exists ${name}_seq`,
    `create sequence ${name}_seq`,
    `create or replace function ${name}() returns trigger language plpgsql as $$
     begin
       if nextval('${name}_seq') <= ${count} then
         raise exception 'simulated pruning failure' using errcode = '${sqlstate}';
       end if;
       return old;
     end $$`,
    `drop trigger if exists ${name} on ${table}`,
    `create trigger ${name} before delete on ${table} for each row execute function ${name}()`,
  ]
}
