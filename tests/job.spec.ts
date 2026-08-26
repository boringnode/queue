import { test } from '@japa/runner'
import { Job } from '../src/job.js'
import { JobDispatcher } from '../src/job_dispatcher.js'
import { JobBatchDispatcher } from '../src/job_batch_dispatcher.js'
import { ScheduleBuilder } from '../src/schedule_builder.js'
import { Schedule } from '../src/schedule.js'
import { QueueManager } from '../src/queue_manager.js'
import type { JobOptions } from '../src/types/main.js'
import { memory } from './_mocks/memory_adapter.js'

class PaymentService {}

class ProcessPaymentJob extends Job<{ paymentId: string }> {
  constructor(protected paymentService: PaymentService) {
    super()
  }

  async execute() {
    void this.paymentService
  }
}

test.group('Job static methods', () => {
  test('should support subclasses with typed constructor injection', ({ assert, expectTypeOf }) => {
    const dispatcher = ProcessPaymentJob.dispatch({ paymentId: 'pay_123' })
    const batchDispatcher = ProcessPaymentJob.dispatchMany([{ paymentId: 'pay_123' }])
    const schedule = ProcessPaymentJob.schedule({ paymentId: 'pay_123' })

    assert.instanceOf(dispatcher, JobDispatcher)
    assert.instanceOf(batchDispatcher, JobBatchDispatcher)
    assert.instanceOf(schedule, ScheduleBuilder)

    expectTypeOf(dispatcher).toEqualTypeOf<JobDispatcher<{ paymentId: string }>>()
    expectTypeOf(batchDispatcher).toEqualTypeOf<JobBatchDispatcher<{ paymentId: string }>>()
    expectTypeOf(schedule).toEqualTypeOf<ScheduleBuilder<{ paymentId: string }>>()
  })

  test('should resolve static options when the fluent dispatch runs', async ({
    assert,
    cleanup,
  }) => {
    const adapter = memory()()

    class MutableOptionsJob extends Job {
      static options: JobOptions = { name: 'BeforeName', queue: 'before', priority: 8 }

      async execute() {}
    }

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => adapter },
    })
    cleanup(() => QueueManager.destroy())

    const dispatcher = MutableOptionsJob.dispatch({})
    const batchDispatcher = MutableOptionsJob.dispatchMany([{}])
    const scheduleBuilder = MutableOptionsJob.schedule({})
    MutableOptionsJob.options = { queue: 'after', priority: 2 }
    await dispatcher.run()
    await batchDispatcher.run()
    await scheduleBuilder.id('mutable-options').every('1h').run()

    const jobs = [await adapter.popFrom('after'), await adapter.popFrom('after')]
    const schedule = await Schedule.find('mutable-options')
    assert.deepEqual(
      jobs.map((job) => ({ name: job?.name, priority: job?.priority })),
      [
        { name: 'MutableOptionsJob', priority: 2 },
        { name: 'MutableOptionsJob', priority: 2 },
      ]
    )
    assert.equal(schedule?.name, 'MutableOptionsJob')
    assert.isNull(await adapter.popFrom('before'))
  })
})
