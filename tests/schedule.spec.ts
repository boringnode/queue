import { test } from '@japa/runner'
import { memory } from './_mocks/memory_adapter.js'
import { QueueManager } from '../src/queue_manager.js'
import { ScheduleBuilder } from '../src/schedule_builder.js'
import { Schedule } from '../src/schedule.js'
import { Job } from '../src/job.js'
import { Locator } from '../src/locator.js'
import * as errors from '../src/exceptions.js'

test.group('ScheduleBuilder', (group) => {
  group.each.setup(async () => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    return () => QueueManager.destroy()
  })

  test('should create a schedule with cron expression', async ({ assert }) => {
    const builder = new ScheduleBuilder('CleanupJob', { days: 30 })

    const { scheduleId } = await builder.cron('0 0 * * *').run()

    assert.isString(scheduleId)

    const schedule = await Schedule.find(scheduleId)
    assert.isNotNull(schedule)
    assert.equal(schedule!.name, 'CleanupJob')
    assert.deepEqual(schedule!.payload, { days: 30 })
    assert.equal(schedule!.cronExpression, '0 0 * * *')
    assert.isNull(schedule!.everyMs)
    assert.equal(schedule!.timezone, 'UTC')
    assert.equal(schedule!.status, 'active')
  })

  test('should create a schedule with interval using every()', async ({ assert }) => {
    const builder = new ScheduleBuilder('SyncJob', { source: 'api' })

    const { scheduleId } = await builder.every('5m').run()

    assert.isString(scheduleId)

    const schedule = await Schedule.find(scheduleId)
    assert.isNotNull(schedule)
    assert.equal(schedule!.name, 'SyncJob')
    assert.equal(schedule!.everyMs, 5 * 60 * 1000)
    assert.isNull(schedule!.cronExpression)
  })

  test('should use job name as default schedule id', async ({ assert }) => {
    const builder = new ScheduleBuilder('DefaultIdJob', { test: true })

    const { scheduleId } = await builder.every('1h').run()

    assert.equal(scheduleId, 'DefaultIdJob')

    const schedule = await Schedule.find('DefaultIdJob')
    assert.isNotNull(schedule)
    assert.equal(schedule!.id, 'DefaultIdJob')
  })

  test('should set custom schedule id for upsert behavior', async ({ assert }) => {
    const builder = new ScheduleBuilder('CleanupJob', { days: 30 })

    const { scheduleId } = await builder.id('cleanup-daily').cron('0 0 * * *').run()

    assert.equal(scheduleId, 'cleanup-daily')

    const schedule = await Schedule.find('cleanup-daily')
    assert.isNotNull(schedule)
    assert.equal(schedule!.id, 'cleanup-daily')
  })

  test('should upsert schedule when id already exists', async ({ assert }) => {
    const builder1 = new ScheduleBuilder('CleanupJob', { days: 30 })
    await builder1.id('cleanup-daily').cron('0 0 * * *').run()

    // Update with new payload and cron
    const builder2 = new ScheduleBuilder('CleanupJob', { days: 7 })
    const { scheduleId } = await builder2.id('cleanup-daily').cron('0 12 * * *').run()

    assert.equal(scheduleId, 'cleanup-daily')

    const schedule = await Schedule.find('cleanup-daily')
    assert.deepEqual(schedule!.payload, { days: 7 })
    assert.equal(schedule!.cronExpression, '0 12 * * *')
  })

  test('should set timezone', async ({ assert }) => {
    const builder = new ScheduleBuilder('ReportJob', {})

    const { scheduleId } = await builder.cron('0 9 * * *').timezone('Europe/Paris').run()

    const schedule = await Schedule.find(scheduleId)
    assert.equal(schedule!.timezone, 'Europe/Paris')
  })

  test('should set start boundary with from()', async ({ assert }) => {
    const fromDate = new Date('2025-01-01T00:00:00Z')
    const builder = new ScheduleBuilder('CampaignJob', {})

    const { scheduleId } = await builder.cron('0 0 * * *').from(fromDate).run()

    const schedule = await Schedule.find(scheduleId)
    assert.deepEqual(schedule!.from, fromDate)
  })

  test('should set end boundary with to()', async ({ assert }) => {
    const toDate = new Date('2025-12-31T23:59:59Z')
    const builder = new ScheduleBuilder('CampaignJob', {})

    const { scheduleId } = await builder.cron('0 0 * * *').to(toDate).run()

    const schedule = await Schedule.find(scheduleId)
    assert.deepEqual(schedule!.to, toDate)
  })

  test('should set both boundaries with between()', async ({ assert }) => {
    const fromDate = new Date('2025-01-01T00:00:00Z')
    const toDate = new Date('2025-12-31T23:59:59Z')
    const builder = new ScheduleBuilder('CampaignJob', {})

    const { scheduleId } = await builder.cron('0 0 * * *').between(fromDate, toDate).run()

    const schedule = await Schedule.find(scheduleId)
    assert.deepEqual(schedule!.from, fromDate)
    assert.deepEqual(schedule!.to, toDate)
  })

  test('should set run limit', async ({ assert }) => {
    const builder = new ScheduleBuilder('LimitedJob', {})

    const { scheduleId } = await builder.every('1h').limit(100).run()

    const schedule = await Schedule.find(scheduleId)
    assert.equal(schedule!.limit, 100)
  })

  test('should calculate nextRunAt on creation', async ({ assert }) => {
    const builder = new ScheduleBuilder('SyncJob', {})

    const { scheduleId } = await builder.every('5m').run()

    const schedule = await Schedule.find(scheduleId)
    assert.isNotNull(schedule!.nextRunAt)
    // nextRunAt should be approximately 5 minutes from now
    const expectedNextRun = Date.now() + 5 * 60 * 1000
    const actualNextRun = schedule!.nextRunAt!.getTime()
    // Allow 1 second tolerance
    assert.isTrue(Math.abs(actualNextRun - expectedNextRun) < 1000)
  })

  test('should calculate nextRunAt from cron expression', async ({ assert }) => {
    const builder = new ScheduleBuilder('CronJob', {})

    const { scheduleId } = await builder.cron('0 0 * * *').run()

    const schedule = await Schedule.find(scheduleId)
    assert.isNotNull(schedule!.nextRunAt)
    // nextRunAt should be in the future
    assert.isTrue(schedule!.nextRunAt!.getTime() > Date.now())
  })

  test('should throw when neither cron nor every is set', async ({ assert }) => {
    assert.plan(1)
    const builder = new ScheduleBuilder('InvalidJob', {})

    try {
      await builder.run()
    } catch (error) {
      assert.instanceOf(error, errors.E_INVALID_SCHEDULE_CONFIG)
    }
  })

  test('should throw when both cron and every are set', async ({ assert }) => {
    assert.plan(1)
    const builder = new ScheduleBuilder('InvalidJob', {})

    try {
      await builder.cron('0 0 * * *').every('5m').run()
    } catch (error) {
      assert.instanceOf(error, errors.E_INVALID_SCHEDULE_CONFIG)
    }
  })

  test('should throw for invalid cron expression', async ({ assert }) => {
    assert.plan(1)
    const builder = new ScheduleBuilder('InvalidJob', {})

    try {
      await builder.cron('invalid cron').run()
    } catch (error) {
      assert.instanceOf(error, errors.E_INVALID_CRON_EXPRESSION)
    }
  })

  test('should be thenable for auto-run', async ({ assert }) => {
    const builder = new ScheduleBuilder('SyncJob', { source: 'api' })

    // Using await directly on builder chain
    const { scheduleId } = await builder.every('5m')

    assert.isString(scheduleId)
  })

  test('should respect from() for nextRunAt calculation', async ({ assert }) => {
    const futureDate = new Date(Date.now() + 24 * 60 * 60 * 1000) // tomorrow
    const builder = new ScheduleBuilder('FutureJob', {})

    const { scheduleId } = await builder.every('1h').from(futureDate).run()

    const schedule = await Schedule.find(scheduleId)
    // nextRunAt should be at or after from date
    assert.isTrue(schedule!.nextRunAt!.getTime() >= futureDate.getTime())
  })
})

test.group('Schedule', (group) => {
  let sharedAdapter: ReturnType<ReturnType<typeof memory>>

  group.each.setup(async () => {
    sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    return () => QueueManager.destroy()
  })

  test('Schedule.find() should return null for non-existent schedule', async ({ assert }) => {
    const schedule = await Schedule.find('non-existent')
    assert.isNull(schedule)
  })

  test('Schedule.find() should return schedule data', async ({ assert }) => {
    const builder = new ScheduleBuilder('TestJob', { data: 'test' })
    const { scheduleId } = await builder.id('test-schedule').every('5m').run()

    const schedule = await Schedule.find(scheduleId)

    assert.isNotNull(schedule)
    assert.instanceOf(schedule, Schedule)
    assert.equal(schedule!.id, 'test-schedule')
    assert.equal(schedule!.name, 'TestJob')
    assert.deepEqual(schedule!.payload, { data: 'test' })
  })

  test('Schedule.list() should return all schedules', async ({ assert }) => {
    await new ScheduleBuilder('Job1', {}).id('schedule-1').every('5m').run()
    await new ScheduleBuilder('Job2', {}).id('schedule-2').every('10m').run()
    await new ScheduleBuilder('Job3', {}).id('schedule-3').cron('0 0 * * *').run()

    const schedules = await Schedule.list()

    assert.lengthOf(schedules, 3)
    assert.isTrue(schedules.every((s) => s instanceof Schedule))
  })

  test('Schedule.list() should filter by status', async ({ assert }) => {
    await new ScheduleBuilder('Job1', {}).id('active-schedule').every('5m').run()

    const pausedSchedule = await Schedule.find('active-schedule')
    await pausedSchedule!.pause()

    await new ScheduleBuilder('Job2', {}).id('still-active').every('10m').run()

    const activeSchedules = await Schedule.list({ status: 'active' })
    const pausedSchedules = await Schedule.list({ status: 'paused' })

    assert.lengthOf(activeSchedules, 1)
    assert.equal(activeSchedules[0].id, 'still-active')

    assert.lengthOf(pausedSchedules, 1)
    assert.equal(pausedSchedules[0].id, 'active-schedule')
  })

  test('schedule.pause() should set status to paused', async ({ assert }) => {
    await new ScheduleBuilder('TestJob', {}).id('pausable').every('5m').run()

    const schedule = await Schedule.find('pausable')
    await schedule!.pause()

    const updated = await Schedule.find('pausable')
    assert.equal(updated!.status, 'paused')
  })

  test('schedule.resume() should set status to active', async ({ assert }) => {
    await new ScheduleBuilder('TestJob', {}).id('resumable').every('5m').run()

    const schedule = await Schedule.find('resumable')
    await schedule!.pause()
    await schedule!.resume()

    const updated = await Schedule.find('resumable')
    assert.equal(updated!.status, 'active')
  })

  test('schedule.delete() should remove the schedule', async ({ assert }) => {
    await new ScheduleBuilder('TestJob', {}).id('deletable').every('5m').run()

    const schedule = await Schedule.find('deletable')
    await schedule!.delete()

    const deleted = await Schedule.find('deletable')
    assert.isNull(deleted)
  })

  test('schedule.trigger() should dispatch job immediately', async ({ assert }) => {
    await new ScheduleBuilder('TriggerJob', { immediate: true }).id('triggerable').every('1h').run()

    const schedule = await Schedule.find('triggerable')
    await schedule!.trigger()

    // Job should be in the queue
    const job = await sharedAdapter.pop()
    assert.isNotNull(job)
    assert.equal(job!.name, 'TriggerJob')
    assert.deepEqual(job!.payload, { immediate: true })
  })

  test('schedule.trigger() should update lastRunAt and runCount', async ({ assert }) => {
    await new ScheduleBuilder('TriggerJob', {}).id('trigger-updates').every('1h').run()

    const schedule = await Schedule.find('trigger-updates')
    const beforeRunCount = schedule!.runCount

    await schedule!.trigger()

    const updated = await Schedule.find('trigger-updates')
    assert.equal(updated!.runCount, beforeRunCount + 1)
    assert.isNotNull(updated!.lastRunAt)
  })

  test('schedule.trigger() should not run if limit is reached', async ({ assert }) => {
    await new ScheduleBuilder('LimitedJob', {}).id('limited').every('1h').limit(1).run()

    const schedule = await Schedule.find('limited')
    await schedule!.trigger() // First run, should work

    const updatedSchedule = await Schedule.find('limited')
    await updatedSchedule!.trigger() // Second run, should be skipped

    // Only one job should be in the queue
    const job1 = await sharedAdapter.pop()
    const job2 = await sharedAdapter.pop()

    assert.isNotNull(job1)
    assert.isNull(job2)
  })

  test('schedule.trigger(payload) should dispatch job with custom payload', async ({ assert }) => {
    await new ScheduleBuilder('TriggerJob', { immediate: true, custom: false })
      .id('triggerable')
      .every('1h')
      .run()

    const schedule = await Schedule.find('triggerable')
    await schedule!.trigger({ immediate: true, custom: true })

    // Job should be in the queue
    const job = await sharedAdapter.pop()
    assert.isNotNull(job)
    assert.equal(job!.name, 'TriggerJob')
    assert.deepEqual(job!.payload, { immediate: true, custom: true })
  })

  test('schedule properties should reflect data', async ({ assert }) => {
    const fromDate = new Date('2025-01-01')
    const toDate = new Date('2025-12-31')

    await new ScheduleBuilder('FullJob', { key: 'value' })
      .id('full-schedule')
      .cron('0 9 * * 1-5')
      .timezone('America/New_York')
      .between(fromDate, toDate)
      .limit(50)
      .run()

    const schedule = await Schedule.find('full-schedule')

    assert.equal(schedule!.id, 'full-schedule')
    assert.equal(schedule!.name, 'FullJob')
    assert.deepEqual(schedule!.payload, { key: 'value' })
    assert.equal(schedule!.cronExpression, '0 9 * * 1-5')
    assert.isNull(schedule!.everyMs)
    assert.equal(schedule!.timezone, 'America/New_York')
    assert.deepEqual(schedule!.from, fromDate)
    assert.deepEqual(schedule!.to, toDate)
    assert.equal(schedule!.limit, 50)
    assert.equal(schedule!.runCount, 0)
    assert.equal(schedule!.status, 'active')
    assert.isNotNull(schedule!.createdAt)
  })
})

test.group('Job.schedule()', (group) => {
  class TestScheduleJob extends Job<{ value: string }> {
    async execute() {}
  }

  group.each.setup(async () => {
    const sharedAdapter = memory()()

    // Register the job
    Locator.register(TestScheduleJob.name, TestScheduleJob)

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    return () => {
      Locator.clear()
      return QueueManager.destroy()
    }
  })

  test('Job.schedule() should return a ScheduleBuilder', async ({ assert }) => {
    const builder = TestScheduleJob.schedule({ value: 'test' })

    assert.instanceOf(builder, ScheduleBuilder)
  })

  test('Job.schedule() should create a schedule with job name', async ({ assert }) => {
    const { scheduleId } = await TestScheduleJob.schedule({ value: 'hello' })
      .id('test-job-schedule')
      .every('10m')
      .run()

    const schedule = await Schedule.find(scheduleId)

    assert.isNotNull(schedule)
    assert.equal(schedule!.name, 'TestScheduleJob')
    assert.deepEqual(schedule!.payload, { value: 'hello' })
  })

  test('Job.schedule() should work with cron expression', async ({ assert }) => {
    const { scheduleId } = await TestScheduleJob.schedule({ value: 'cron-test' })
      .cron('0 */2 * * *')
      .timezone('Europe/Paris')
      .run()

    const schedule = await Schedule.find(scheduleId)

    assert.isNotNull(schedule)
    assert.equal(schedule!.cronExpression, '0 */2 * * *')
    assert.equal(schedule!.timezone, 'Europe/Paris')
  })

  test('Job.schedule() should be thenable', async ({ assert }) => {
    const { scheduleId } = await TestScheduleJob.schedule({ value: 'thenable' }).every('1h')

    assert.isString(scheduleId)

    const schedule = await Schedule.find(scheduleId)
    assert.equal(schedule!.everyMs, 60 * 60 * 1000)
  })
})

test.group('claimDueSchedule', (group) => {
  let sharedAdapter: ReturnType<ReturnType<typeof memory>>

  group.each.setup(async () => {
    sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    return () => QueueManager.destroy()
  })

  test('should return null when no schedules are due', async ({ assert }) => {
    // Create a schedule with nextRunAt in the future
    await new ScheduleBuilder('FutureJob', {}).id('future').every('1h').run()

    const claimed = await sharedAdapter.claimDueSchedule()

    assert.isNull(claimed)
  })

  test('should claim a due schedule', async ({ assert }) => {
    await new ScheduleBuilder('DueJob', { key: 'value' }).id('due-schedule').every('5m').run()

    // Manually set nextRunAt to the past to make it due
    await sharedAdapter.updateSchedule('due-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    const claimed = await sharedAdapter.claimDueSchedule()

    assert.isNotNull(claimed)
    assert.equal(claimed!.id, 'due-schedule')
    assert.equal(claimed!.name, 'DueJob')
    assert.deepEqual(claimed!.payload, { key: 'value' })
  })

  test('should update nextRunAt after claiming', async ({ assert }) => {
    await new ScheduleBuilder('IntervalJob', {}).id('interval-schedule').every('10m').run()

    // Make it due
    const pastDate = new Date(Date.now() - 1000)
    await sharedAdapter.updateSchedule('interval-schedule', { nextRunAt: pastDate })

    const beforeClaim = await sharedAdapter.getSchedule('interval-schedule')
    assert.deepEqual(beforeClaim!.nextRunAt, pastDate)

    await sharedAdapter.claimDueSchedule()

    const afterClaim = await sharedAdapter.getSchedule('interval-schedule')
    // nextRunAt should be ~10 minutes in the future
    assert.isNotNull(afterClaim!.nextRunAt)
    assert.isTrue(afterClaim!.nextRunAt!.getTime() > Date.now())
  })

  test('should increment runCount after claiming', async ({ assert }) => {
    await new ScheduleBuilder('CountJob', {}).id('count-schedule').every('5m').run()

    await sharedAdapter.updateSchedule('count-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    const before = await sharedAdapter.getSchedule('count-schedule')
    assert.equal(before!.runCount, 0)

    await sharedAdapter.claimDueSchedule()

    const after = await sharedAdapter.getSchedule('count-schedule')
    assert.equal(after!.runCount, 1)
  })

  test('should set lastRunAt after claiming', async ({ assert }) => {
    await new ScheduleBuilder('LastRunJob', {}).id('lastrun-schedule').every('5m').run()

    await sharedAdapter.updateSchedule('lastrun-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    const before = await sharedAdapter.getSchedule('lastrun-schedule')
    assert.isNull(before!.lastRunAt)

    const now = Date.now()
    await sharedAdapter.claimDueSchedule()

    const after = await sharedAdapter.getSchedule('lastrun-schedule')
    assert.isNotNull(after!.lastRunAt)
    // Should be approximately now
    assert.isTrue(Math.abs(after!.lastRunAt!.getTime() - now) < 1000)
  })

  test('should not claim paused schedules', async ({ assert }) => {
    await new ScheduleBuilder('PausedJob', {}).id('paused-schedule').every('5m').run()

    await sharedAdapter.updateSchedule('paused-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
      status: 'paused',
    })

    const claimed = await sharedAdapter.claimDueSchedule()

    assert.isNull(claimed)
  })

  test('should not claim schedule when limit is reached', async ({ assert }) => {
    await new ScheduleBuilder('LimitedJob', {}).id('limited-schedule').every('5m').limit(3).run()

    await sharedAdapter.updateSchedule('limited-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
      runCount: 3, // Already at limit
    })

    const claimed = await sharedAdapter.claimDueSchedule()

    assert.isNull(claimed)
  })

  test('should set nextRunAt to null when limit will be reached', async ({ assert }) => {
    await new ScheduleBuilder('LastRunLimitJob', {}).id('lastrun-limit').every('5m').limit(1).run()

    await sharedAdapter.updateSchedule('lastrun-limit', {
      nextRunAt: new Date(Date.now() - 1000),
      runCount: 0,
    })

    await sharedAdapter.claimDueSchedule()

    const after = await sharedAdapter.getSchedule('lastrun-limit')
    assert.isNull(after!.nextRunAt) // No more runs
    assert.equal(after!.runCount, 1)
  })

  test('should not claim schedule past end date (to)', async ({ assert }) => {
    const pastDate = new Date(Date.now() - 24 * 60 * 60 * 1000) // yesterday

    await new ScheduleBuilder('ExpiredJob', {})
      .id('expired-schedule')
      .every('5m')
      .to(pastDate)
      .run()

    await sharedAdapter.updateSchedule('expired-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    const claimed = await sharedAdapter.claimDueSchedule()

    assert.isNull(claimed)
  })

  test('should only claim one schedule at a time', async ({ assert }) => {
    // Create multiple due schedules
    await new ScheduleBuilder('Job1', {}).id('schedule-1').every('5m').run()
    await new ScheduleBuilder('Job2', {}).id('schedule-2').every('5m').run()
    await new ScheduleBuilder('Job3', {}).id('schedule-3').every('5m').run()

    const pastDate = new Date(Date.now() - 1000)
    await sharedAdapter.updateSchedule('schedule-1', { nextRunAt: pastDate })
    await sharedAdapter.updateSchedule('schedule-2', { nextRunAt: pastDate })
    await sharedAdapter.updateSchedule('schedule-3', { nextRunAt: pastDate })

    const claimed1 = await sharedAdapter.claimDueSchedule()
    const claimed2 = await sharedAdapter.claimDueSchedule()
    const claimed3 = await sharedAdapter.claimDueSchedule()
    const claimed4 = await sharedAdapter.claimDueSchedule()

    assert.isNotNull(claimed1)
    assert.isNotNull(claimed2)
    assert.isNotNull(claimed3)
    assert.isNull(claimed4) // No more due schedules

    // All should be different
    const ids = [claimed1!.id, claimed2!.id, claimed3!.id]
    assert.lengthOf(new Set(ids), 3)
  })
})
