import { setTimeout } from 'node:timers/promises'
import { test as JapaTest } from '@japa/runner'
import type { AcquiredJob, Adapter } from '../../src/contracts/adapter.js'
import { isDeadlockError, retryOnDeadlock } from '../../src/utils.js'

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
  /**
   * Whether the adapters sustain twenty concurrent consumers. SQLite, with
   * one writer at a time, runs out of its busy timeout.
   * @default false
   */
  manyConsumers?: boolean
}

const finalizers = {
  completeJob: (adapter: Adapter, job: AcquiredJob, count = 1) =>
    adapter.completeJob(job, 'retention-race', { count }),
  failJob: (adapter: Adapter, job: AcquiredJob, count = 1) =>
    adapter.failJob(job, 'retention-race', new Error('boom'), { count }),
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

          for (const id of ['job-0', 'job-a', 'job-b']) {
            await first.pushOn('retention-race', { id, name: 'TestJob', payload: {}, attempts: 0 })
          }
          // An older finished job, so that the pruning of A has one to delete.
          await finalize(second, (await second.popFrom('retention-race'))!)
          await setTimeout(5)
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

    if (!options.manyConsumers) continue

    options
      .test(
        `concurrent ${name} calls with count retention all take effect`,
        async ({ assert, cleanup }) => {
          const adapters = [
            ...(await options.createAdapters(async () => {}, cleanup)),
            ...(await options.createAdapters(async () => {}, cleanup)),
          ]
          const ids = Array.from({ length: 2_000 }, (_, index) => `retained-${index}`)
          await adapters[0].pushManyOn('retention-race', [
            ...ids.map((id) => ({ id, name: 'TestJob', payload: {}, attempts: 0 })),
          ])

          // Ten consumers per adapter, retrying deadlocks as WorkerSession does:
          // a failed acquisition is only a failed cycle, while a failed
          // finalization leaves the job active until the stalled check reruns it.
          const failures: unknown[] = []
          let finalized = 0
          const acquire = async (adapter: Adapter): Promise<AcquiredJob | null> => {
            try {
              return await adapter.popFrom('retention-race')
            } catch (error) {
              if (!isDeadlockError(error)) throw error
              return acquire(adapter)
            }
          }
          const consume = async (adapter: Adapter, worker: string) => {
            adapter.setWorkerId(worker)
            for (let job = await acquire(adapter); job; job = await acquire(adapter)) {
              const current = job
              await retryOnDeadlock(() => finalize(adapter, current, 100)).then(
                (done) => done && finalized++,
                (error) => failures.push(error)
              )
            }
          }
          await Promise.all(
            adapters.flatMap((adapter, index) =>
              Array.from({ length: 10 }, () => consume(adapter, `worker-${index}`))
            )
          )

          assert.deepEqual(failures, [])
          assert.equal(finalized, ids.length)
          for (const id of ids) {
            const record = await adapters[0].getJob(id, 'retention-race')
            assert.notEqual(record?.status, 'active', id)
          }
        }
      )
      .timeout(60_000)
  }
}
