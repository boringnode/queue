import { spawnSync } from 'node:child_process'
import { test } from '@japa/runner'

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
