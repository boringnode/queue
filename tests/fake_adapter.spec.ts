import { test } from '@japa/runner'
import { Job } from '../src/job.js'
import { fake } from '../src/drivers/fake_adapter.js'

test.group('FakeAdapter', () => {
  test('should record pushes and support assertions', async ({ assert }) => {
    const adapter = fake()()

    adapter.assertNothingPushed()

    await adapter.pushOn('default', {
      id: 'job-1',
      name: 'SendEmailJob',
      payload: { to: 'user@example.com' },
      attempts: 0,
    })

    await adapter.pushLaterOn(
      'emails',
      {
        id: 'job-2',
        name: 'SendEmailJob',
        payload: { to: 'admin@example.com' },
        attempts: 0,
      },
      500
    )

    adapter.assertPushed('SendEmailJob')
    adapter.assertPushed('SendEmailJob', {
      queue: 'default',
      payload: { to: 'user@example.com' },
    })
    adapter.assertPushed('SendEmailJob', { queue: 'emails', delay: 500 })
    adapter.assertPushedCount(2)
    adapter.assertPushedCount(1, { queue: 'emails' })

    assert.throws(() => adapter.assertNotPushed('SendEmailJob', { queue: 'default' }))
    adapter.assertNotPushed('MissingJob')

    adapter.clearPushedJobs()
    adapter.assertNothingPushed()

    await adapter.destroy()
  })

  test('should support matcher functions and getters', async ({ assert }) => {
    const adapter = fake()()

    await adapter.pushOn('default', {
      id: 'job-1',
      name: 'SendEmailJob',
      payload: { to: 'user@example.com' },
      attempts: 0,
    })

    await adapter.pushLaterOn(
      'emails',
      {
        id: 'job-2',
        name: 'SendEmailJob',
        payload: { to: 'admin@example.com' },
        attempts: 0,
      },
      250
    )

    assert.equal(adapter.getPushedJobs().length, 2)
    assert.equal(adapter.getPushedJobsOn('emails').length, 1)

    const matcherRecord = adapter.findPushed(
      (job) => job.name === 'SendEmailJob' && job.payload?.to === 'user@example.com',
      { queue: 'default' }
    )
    assert.isDefined(matcherRecord)

    const payloadRecord = adapter.findPushed('SendEmailJob', {
      payload: (payload) =>
        typeof payload === 'object' &&
        payload !== null &&
        'to' in payload &&
        payload.to === 'admin@example.com',
    })
    assert.isDefined(payloadRecord)

    const delayRecord = adapter.findPushed('SendEmailJob', {
      delay: (delay) => (delay ?? 0) >= 250,
    })
    assert.isDefined(delayRecord)

    await adapter.destroy()
  })

  test('should skip duplicate pushOn when dedup is set', async ({ assert }) => {
    const adapter = fake()()

    await adapter.pushOn('default', {
      id: 'TestJob::order-1',
      name: 'TestJob',
      payload: { attempt: 1 },
      attempts: 0,
      dedup: { id: 'order-1' },
    })

    await adapter.pushOn('default', {
      id: 'TestJob::order-1',
      name: 'TestJob',
      payload: { attempt: 2 },
      attempts: 0,
      dedup: { id: 'order-1' },
    })

    const size = await adapter.size()
    assert.equal(size, 1)
    adapter.assertPushedCount(1)

    await adapter.destroy()
  })

  test('should skip duplicate pushLaterOn when dedup is set', async () => {
    const adapter = fake()()

    await adapter.pushLaterOn(
      'default',
      {
        id: 'TestJob::delayed-1',
        name: 'TestJob',
        payload: { attempt: 1 },
        attempts: 0,
        dedup: { id: 'delayed-1' },
      },
      5000
    )

    await adapter.pushLaterOn(
      'default',
      {
        id: 'TestJob::delayed-1',
        name: 'TestJob',
        payload: { attempt: 2 },
        attempts: 0,
        dedup: { id: 'delayed-1' },
      },
      5000
    )

    adapter.assertPushedCount(1)

    await adapter.destroy()
  })

  test('should support job class matchers', async ({ assert }) => {
    const adapter = fake()()

    class SendEmailJob extends Job<{ to: string }> {
      async execute() {}
    }

    class CustomNamedJob extends Job {
      static options = { name: 'CustomJob' }
      async execute() {}
    }

    await adapter.pushOn('default', {
      id: 'job-1',
      name: 'SendEmailJob',
      payload: { to: 'user@example.com' },
      attempts: 0,
    })

    await adapter.pushOn('default', {
      id: 'job-2',
      name: 'CustomJob',
      payload: null,
      attempts: 0,
    })

    adapter.assertPushed(SendEmailJob)
    adapter.assertPushed(CustomNamedJob)

    class MissingJob extends Job {
      async execute() {}
    }

    assert.throws(() => adapter.assertPushed(MissingJob))

    await adapter.destroy()
  })
})
