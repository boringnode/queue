import { createRequire } from 'node:module'
import { Worker } from 'node:worker_threads'

const contenderSource = `
const { workerData, parentPort } = require('node:worker_threads')
const Database = require(workerData.betterSqlite3)
const db = new Database(workerData.filename, { timeout: 5000 })
db.exec('create table if not exists write_contender (id integer primary key)')
const stop = new Int32Array(workerData.stop)
const sleep = (ms) => Atomics.wait(stop, 0, 0, ms)
parentPort.postMessage('ready')
while (Atomics.load(stop, 0) === 0) {
  db.exec('begin immediate')
  db.exec('insert into write_contender default values')
  sleep(2)
  db.exec('commit')
  sleep(6)
}
db.close()
`

/**
 * Write to a SQLite file from another thread, as another process would: a
 * loop of short write transactions that keep the write lock a quarter of the time.
 * Stop it before closing the file.
 */
export async function startSqliteWriteContender(filename: string) {
  const stop = new SharedArrayBuffer(4)
  const worker = new Worker(contenderSource, {
    eval: true,
    workerData: {
      filename,
      stop,
      betterSqlite3: createRequire(import.meta.url).resolve('better-sqlite3'),
    },
  })

  await new Promise<void>((resolve, reject) => {
    worker.once('message', () => resolve())
    worker.once('error', reject)
  })

  return async () => {
    Atomics.store(new Int32Array(stop), 0, 1)
    Atomics.notify(new Int32Array(stop), 0)
    await new Promise((resolve) => worker.once('exit', resolve))
  }
}
