import { Worker } from '../../src/worker.js'
import { Locator } from '../../src/locator.js'
import { QueueManager } from '../../src/queue_manager.js'
import type { Job } from '../../src/job.js'
import type { JobClass, JobData, QueueManagerConfig } from '../../src/types/main.js'
import { ControllableAdapter } from '../_mocks/controllable_adapter.js'

interface WorkerFixtureOptions {
  adapter?: ControllableAdapter
  worker?: NonNullable<QueueManagerConfig['worker']>
  config?: Omit<QueueManagerConfig, 'default' | 'adapters' | 'worker'>
}

interface PushOptions<Payload> {
  id: string
  payload?: Payload
  queue?: string
}

export function createWorkerFixture(options: WorkerFixtureOptions = {}) {
  const adapter = options.adapter ?? new ControllableAdapter()
  const worker = new Worker({
    default: 'test',
    adapters: { test: () => adapter },
    autoLoadJobs: false,
    ...options.config,
    worker: {
      gracefulShutdown: false,
      ...options.worker,
    },
  })

  function register<T extends Job>(JobClass: JobClass<T>): string {
    const name = JobClass.name
    Locator.register(name, JobClass)
    return name
  }

  async function push<Payload, T extends Job<Payload>>(
    JobClass: JobClass<T>,
    pushOptions: PushOptions<Payload>
  ): Promise<void> {
    const name = register(JobClass)
    const job: JobData = {
      id: pushOptions.id,
      name,
      payload: pushOptions.payload ?? {},
      attempts: 0,
    }

    await adapter.pushOn(pushOptions.queue ?? 'default', job)
  }

  return {
    adapter,
    worker,
    register,
    push,

    start(queues: string[] = ['default']): Promise<void> {
      return worker.start(queues)
    },

    async cleanup(): Promise<void> {
      adapter.releaseAll()
      try {
        await worker.stop()
      } finally {
        Locator.clear()
        await QueueManager.destroy()
      }
    },
  }
}
