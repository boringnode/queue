import { test } from '@japa/runner'
import { fork, type ChildProcess } from 'node:child_process'
import { realpath } from 'node:fs/promises'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { Job } from '../src/job.js'
import { Locator } from '../src/locator.js'
import * as errors from '../src/exceptions.js'
import SendEmailJob from '../examples/jobs/send_email_job.js'

type ChildMessage =
  | { type: 'resolved'; version: string }
  | { type: 'hot-hook:invalidated'; paths: string[] }

function waitForMessage(
  child: ChildProcess,
  predicate: (message: ChildMessage) => boolean,
  getStderr: () => string
): Promise<ChildMessage> {
  return new Promise((resolveMessage, reject) => {
    const timeout = setTimeout(() => {
      cleanup()
      reject(new Error(`Timed out waiting for child process message.\n${getStderr()}`))
    }, 5_000)

    const onMessage = (message: ChildMessage) => {
      if (!predicate(message)) {
        return
      }

      cleanup()
      resolveMessage(message)
    }

    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup()
      reject(
        new Error(
          `Child process exited before sending the expected message (code=${code}, signal=${signal}).\n${getStderr()}`
        )
      )
    }

    const cleanup = () => {
      clearTimeout(timeout)
      child.off('message', onMessage)
      child.off('exit', onExit)
    }

    child.on('message', onMessage)
    child.once('exit', onExit)
  })
}

function hotReloadJobSource(version: string): string {
  const jobUrl = pathToFileURL(resolve('src/job.ts')).href

  return `
    import { Job } from ${JSON.stringify(jobUrl)}

    export default class HotReloadJob extends Job {
      static version = ${JSON.stringify(version)}

      execute() {
        return Promise.resolve()
      }
    }
  `
}

class TestJob extends Job<{ message: string }> {
  execute(): Promise<void> {
    return Promise.resolve()
  }

  rescue(_error: Error): Promise<void> {
    return Promise.resolve()
  }
}

class AnotherTestJob extends Job<{ value: number }> {
  execute(): Promise<void> {
    return Promise.resolve()
  }

  rescue(_error: Error): Promise<void> {
    return Promise.resolve()
  }
}

class JobWithCustomName extends Job<{ data: string }> {
  static options = {
    name: 'CustomNamedJob',
  }

  execute(): Promise<void> {
    return Promise.resolve()
  }
}

test.group('Locator', (group) => {
  group.each.setup(() => {
    Locator.clear()
  })

  test('should register a job class', ({ assert }) => {
    Locator.register('TestJob', TestJob)

    const job = Locator.get('TestJob')
    assert.equal(job, TestJob)
  })

  test('should register a job class from glob pattern', async ({ assert }) => {
    await Locator.registerFromGlob(['./examples/jobs/*.ts'])

    assert.equal(Locator.get('SendEmailJob'), SendEmailJob)
  })

  test('should return undefined for non-existent job', ({ assert }) => {
    const job = Locator.get('NonExistentJob')
    assert.isUndefined(job)
  })

  test('should clear all registered jobs', ({ assert }) => {
    Locator.register('TestJob', TestJob)
    Locator.register('AnotherTestJob', AnotherTestJob)

    assert.equal(Locator.get('TestJob'), TestJob)
    assert.equal(Locator.get('AnotherTestJob'), AnotherTestJob)

    Locator.clear()

    assert.isUndefined(Locator.get('TestJob'))
    assert.isUndefined(Locator.get('AnotherTestJob'))
  })

  test('should overwrite existing job registration', ({ assert }) => {
    Locator.register('TestJob', TestJob)
    assert.equal(Locator.get('TestJob'), TestJob)

    Locator.register('TestJob', AnotherTestJob)
    assert.equal(Locator.get('TestJob'), AnotherTestJob)
  })

  test('should register multiple jobs and retrieve them correctly', ({ assert }) => {
    Locator.register('TestJob', TestJob)
    Locator.register('AnotherTestJob', AnotherTestJob)

    assert.equal(Locator.get('TestJob'), TestJob)
    assert.equal(Locator.get('AnotherTestJob'), AnotherTestJob)
    assert.isUndefined(Locator.get('Job3'))
  })

  test('should getOrThrow should return the job class if it exists', ({ assert }) => {
    Locator.register('TestJob', TestJob)

    const job = Locator.getOrThrow('TestJob')

    assert.equal(job, TestJob)
  })

  test('should resolve a manually registered job class', async ({ assert }) => {
    Locator.register('TestJob', TestJob)

    const job = await Locator.resolve('TestJob')

    assert.equal(job, TestJob)
  })

  test('should resolve a hot reloadable job class registered from glob', async ({ assert }) => {
    await Locator.registerFromGlob(['./examples/jobs/*.ts'], { hotReload: true })

    const job = await Locator.resolve('SendEmailJob')

    assert.equal(job, SendEmailJob)
  })

  test('should resolve the latest job class after Hot Hook invalidates its module', async ({
    assert,
    cleanup,
    fs,
  }) => {
    const locatorUrl = pathToFileURL(resolve('src/locator.ts')).href
    const jobPath = resolve(fs.basePath, 'hot_reload_job.ts')
    const runnerPath = resolve(fs.basePath, 'hot_reload_runner.ts')

    await fs.create('hot_reload_job.ts', hotReloadJobSource('v1'))
    const realJobPath = await realpath(jobPath)
    await fs.create(
      'hot_reload_runner.ts',
      `
        import { Locator } from ${JSON.stringify(locatorUrl)}

        await Locator.registerFromGlob([${JSON.stringify(jobPath)}], { hotReload: true })

        async function resolveJob() {
          const JobClass = await Locator.resolve('HotReloadJob')
          process.send?.({ type: 'resolved', version: JobClass?.version })
        }

        setInterval(() => {}, 1_000)
        await resolveJob()
        process.on('message', (message) => {
          if (message === 'resolve') {
            void resolveJob()
          }
        })
      `
    )

    let stderr = ''
    const child = fork(runnerPath, [], {
      cwd: process.cwd(),
      execArgv: ['--import=@poppinss/ts-exec', '--import=hot-hook/register'],
      env: { ...process.env, HOT_HOOK_WATCH: 'false' },
      silent: true,
    })
    child.stderr?.on('data', (chunk) => {
      stderr += chunk.toString()
    })
    cleanup(() => {
      child.kill()
    })

    const firstResolution = await waitForMessage(
      child,
      (message) => message.type === 'resolved',
      () => stderr
    )
    assert.deepEqual(firstResolution, { type: 'resolved', version: 'v1' })

    const invalidation = waitForMessage(
      child,
      (message) => message.type === 'hot-hook:invalidated' && message.paths.includes(realJobPath),
      () => stderr
    )
    await fs.create('hot_reload_job.ts', hotReloadJobSource('v2'))
    child.send({ type: 'hot-hook:file-changed', path: jobPath })
    await invalidation

    const secondResolution = waitForMessage(
      child,
      (message) => message.type === 'resolved',
      () => stderr
    )
    child.send('resolve')

    assert.deepEqual(await secondResolution, { type: 'resolved', version: 'v2' })
  }).timeout(10_000)

  test('should getOrThrow should throw for non-existent job', ({ assert }) => {
    try {
      Locator.getOrThrow('NonExistentJob')
    } catch (error) {
      assert.instanceOf(error, errors.E_JOB_NOT_FOUND)
      assert.equal(error.message, 'Requested job "NonExistentJob" is not registered')
    }
  })

  test('should resolveOrThrow should throw for non-existent job', async ({ assert }) => {
    try {
      await Locator.resolveOrThrow('NonExistentJob')
    } catch (error) {
      assert.instanceOf(error, errors.E_JOB_NOT_FOUND)
      assert.equal(error.message, 'Requested job "NonExistentJob" is not registered')
    }
  })

  test('should use constructor.name as default job name when registering from glob', async ({
    assert,
  }) => {
    await Locator.registerFromGlob(['./examples/jobs/*.ts'])

    // SendEmailJob has no options.name, so it should use the class name
    assert.equal(Locator.get('SendEmailJob'), SendEmailJob)
  })

  test('should use options.name when provided', async ({ assert }) => {
    Locator.register(JobWithCustomName.options.name!, JobWithCustomName)

    assert.equal(Locator.get('CustomNamedJob'), JobWithCustomName)
    assert.isUndefined(Locator.get('JobWithCustomName'))
  })
})
