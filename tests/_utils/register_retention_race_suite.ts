import { setTimeout } from 'node:timers/promises'
import { test as JapaTest } from '@japa/runner'
import type { AcquiredJob, Adapter } from '../../src/contracts/adapter.js'

interface RetentionRaceSuiteOptions {
  test: typeof JapaTest
  /**
   * Two adapters on separate connections to the same jobs table. The first one
   * awaits `beforeDelete` before it sends each DELETE.
   */
  createAdapters: (
    beforeDelete: () => Promise<void>,
    cleanup: (handler: () => Promise<void>) => void
  ) => Promise<[Adapter, Adapter]>
}

const finalizers = {
  completeJob: (adapter: Adapter, job: AcquiredJob) =>
    adapter.completeJob(job, 'retention-race', { count: 1 }),
  failJob: (adapter: Adapter, job: AcquiredJob) =>
    adapter.failJob(job, 'retention-race', new Error('boom'), { count: 1 }),
}

export function registerRetentionRaceSuite(options: RetentionRaceSuiteOptions) {
  for (const [name, finalize] of Object.entries(finalizers)) {
    options
      .test(
        `${name} keeps the latest job when another worker prunes meanwhile`,
        async ({ assert, cleanup }) => {
          let beforeDelete: (() => Promise<void>) | undefined
          const [first, second] = await options.createAdapters(
            async () => beforeDelete?.(),
            cleanup
          )
          first.setWorkerId('worker-1')
          second.setWorkerId('worker-2')

          for (const id of ['job-a', 'job-b']) {
            await first.pushOn('retention-race', { id, name: 'TestJob', payload: {}, attempts: 0 })
          }
          const jobA = (await first.popFrom('retention-race'))!
          const jobB = (await second.popFrom('retention-race'))!

          // A finalizes its job and stops before the DELETE of its pruning.
          const reached = Promise.withResolvers<void>()
          const resumed = Promise.withResolvers<void>()
          beforeDelete = async () => {
            beforeDelete = undefined
            reached.resolve()
            await resumed.promise
          }
          const finalizingA = finalize(first, jobA)
          await reached.promise

          // B finalizes a more recent job and prunes the job of A.
          await setTimeout(5)
          await finalize(second, jobB)

          resumed.resolve()
          await finalizingA

          assert.isNull(await second.getJob(jobA.id, 'retention-race'))
          assert.isNotNull(await second.getJob(jobB.id, 'retention-race'))
        }
      )
      .timeout(10_000)
  }
}
