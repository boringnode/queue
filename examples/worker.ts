import { Worker } from '../src/worker.js'
import { QueueManager } from '../src/queue_manager.js'
import { config } from './config.js'

const worker = new Worker(config)

// Resolves once SIGINT/SIGTERM stopped the Worker and its running jobs finished.
await worker.start(['default', 'email', 'reports', 'metrics'])

// The Adapters keep their connections open: close them so the process exits.
await QueueManager.destroy()
