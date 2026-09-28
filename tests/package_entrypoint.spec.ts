import { spawnSync } from 'node:child_process'
import { test } from '@japa/runner'
import type { DedupOutcome, PushResult } from '../src/types/index.js'
import type { redis, RedisConfig } from '../src/drivers/redis_adapter.js'
import type { knex, KnexConfig } from '../src/drivers/knex_adapter.js'

test('package root does not load optional adapter peers', ({ assert }) => {
  const result = spawnSync(
    process.execPath,
    [
      '--import=@poppinss/ts-exec',
      '--eval',
      `
        import { registerHooks } from 'node:module'

        const optionalPeers = new Set(['ioredis', 'knex', 'kysely'])
        registerHooks({
          resolve(specifier, context, nextResolve) {
            if (optionalPeers.has(specifier)) {
              throw new Error(\`Package root loaded optional peer "\${specifier}"\`)
            }
            return nextResolve(specifier, context)
          },
        })

        await import('./index.ts')
      `,
    ],
    { cwd: process.cwd(), encoding: 'utf8' }
  )

  assert.equal(result.status, 0, result.stderr)
})

test('package root exports the dispatcher returned by Job.dispatch()', async ({ assert }) => {
  const { Job, JobDispatcher } = await import('../index.js')

  class ExportedDispatcherJob extends Job {
    async execute() {}
  }

  assert.instanceOf(ExportedDispatcherJob.dispatch({}), JobDispatcher)
})

test('public types cover adapter results and adapter configs', ({ expectTypeOf }) => {
  expectTypeOf<PushResult['outcome']>().toEqualTypeOf<DedupOutcome>()
  expectTypeOf<Parameters<typeof redis>[0]>().toEqualTypeOf<RedisConfig | undefined>()
  expectTypeOf<Parameters<typeof knex>[0]>().toEqualTypeOf<KnexConfig>()
})
