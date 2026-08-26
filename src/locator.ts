import { Job } from './job.js'
import * as errors from './exceptions.js'
import type { JobClass } from './types/main.js'
import debug from './debug.js'
import { glob } from 'node:fs/promises'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

type HotImportMeta = ImportMeta & {
  hot?: {
    boundary?: ImportCallOptions
  }
}

type LocatorEntry = {
  JobClass: JobClass
  path?: string
  hotReload: boolean
}

type RegisterFromGlobOptions = {
  hotReload?: boolean
}

/**
 * Job class registry.
 *
 * The Locator maintains a mapping of job names to their classes,
 * allowing the Worker to instantiate jobs by name when processing.
 *
 * Jobs are typically registered automatically via `QueueManager.init()`
 * using the `locations` config option, but can also be registered manually.
 *
 * @example
 * ```typescript
 * import { Locator } from '@boringnode/queue'
 * import SendEmailJob from './jobs/send_email_job.js'
 *
 * // Manual registration
 * Locator.register('SendEmailJob', SendEmailJob)
 *
 * // Auto-registration via glob (used by QueueManager.init)
 * await Locator.registerFromGlob(['./jobs/**\/*.js'])
 *
 * // Retrieve a job class
 * const JobClass = Locator.getOrThrow('SendEmailJob')
 * ```
 */
class LocatorSingleton {
  #registry = new Map<string, LocatorEntry>()

  /**
   * Register a job class with a given name.
   *
   * @param name - The job name (usually the class name)
   * @param JobClass - The job class constructor
   *
   * @example
   * ```typescript
   * Locator.register('SendEmailJob', SendEmailJob)
   * ```
   */
  register<T extends Job>(name: string, JobClass: JobClass<T>) {
    debug('registering job: %s', name)

    this.#registry.set(name, { JobClass, hotReload: false })
  }

  /**
   * Auto-register job classes from files matching glob patterns.
   *
   * Each file should have a default export that is a Job class.
   * The class name is used as the registration name.
   *
   * @param patterns - Glob patterns to match job files
   * @returns Number of jobs successfully registered
   *
   * @example
   * ```typescript
   * const count = await Locator.registerFromGlob([
   *   './jobs/**\/*.js',
   *   './app/jobs/**\/*.ts'
   * ])
   * console.log(`Registered ${count} jobs`)
   * ```
   */
  async registerFromGlob(
    patterns: string[],
    options: RegisterFromGlobOptions = {}
  ): Promise<number> {
    let registered = 0

    for (const pattern of patterns) {
      debug('registering jobs from glob pattern: %s', pattern)
      for await (const file of glob(pattern)) {
        debug('found job file: %s', file)

        try {
          const absolutePath = resolve(file)
          const module = await this.#import(absolutePath, options.hotReload ?? false)
          const JobClass = module.default as JobClass

          if (JobClass && typeof JobClass === 'function') {
            const jobName = JobClass.options?.name || JobClass.name
            this.#registry.set(jobName, {
              JobClass,
              path: absolutePath,
              hotReload: options.hotReload ?? false,
            })
            registered++
          }
        } catch (error) {
          console.warn(`Failed to load job from ${file}:`, error)
        }
      }
    }

    return registered
  }

  /**
   * Get a job class by name.
   *
   * @param name - The job name to look up
   * @returns The job class, or undefined if not found
   *
   * @example
   * ```typescript
   * const JobClass = Locator.get('SendEmailJob')
   * if (JobClass) {
   *   const instance = new JobClass(payload)
   * }
   * ```
   */
  get<T extends Job = Job>(name: string): JobClass<T> | undefined {
    return this.#registry.get(name)?.JobClass as JobClass<T> | undefined
  }

  /**
   * Get a job class by name, throwing if not found.
   *
   * @param name - The job name to look up
   * @returns The job class
   * @throws {E_JOB_NOT_FOUND} If the job is not registered
   *
   * @example
   * ```typescript
   * const JobClass = Locator.getOrThrow('SendEmailJob')
   * const instance = new JobClass(payload)
   * ```
   */
  getOrThrow<T extends Job = Job>(name: string): JobClass<T> {
    const JobClass = this.get<T>(name)

    if (!JobClass) {
      throw new errors.E_JOB_NOT_FOUND([name])
    }

    return JobClass
  }

  /**
   * Resolve a job class by name.
   *
   * Jobs registered from glob patterns with `hotReload: true` are imported
   * again on every resolution. When Hot Hook is active, the dynamic import is
   * enough for it to return the latest version of the job module.
   *
   * @param name - The job name to look up
   * @returns The job class, or undefined if not found
   */
  async resolve<T extends Job = Job>(name: string): Promise<JobClass<T> | undefined> {
    const entry = this.#registry.get(name)

    if (!entry) {
      return undefined
    }

    if (!entry.hotReload || !entry.path) {
      return entry.JobClass as JobClass<T>
    }

    const module = await this.#import(entry.path, true)
    const JobClass = module.default as JobClass<T>

    if (JobClass && typeof JobClass === 'function') {
      entry.JobClass = JobClass
    }

    return entry.JobClass as JobClass<T>
  }

  /**
   * Resolve a job class by name, throwing if not found.
   *
   * @param name - The job name to look up
   * @returns The job class
   * @throws {E_JOB_NOT_FOUND} If the job is not registered
   */
  async resolveOrThrow<T extends Job = Job>(name: string): Promise<JobClass<T>> {
    const JobClass = await this.resolve<T>(name)

    if (!JobClass) {
      throw new errors.E_JOB_NOT_FOUND([name])
    }

    return JobClass
  }

  /**
   * Remove all registered jobs.
   *
   * Primarily useful for testing.
   */
  clear(): void {
    this.#registry.clear()
  }

  async #import(path: string, hotReload: boolean): Promise<{ default?: JobClass }> {
    const url = pathToFileURL(path).href

    if (!hotReload) {
      return import(url)
    }

    const boundary = (import.meta as HotImportMeta).hot?.boundary
    return boundary ? import(url, { with: { hot: 'true' } }) : import(url)
  }
}

/** Global job class registry singleton */
export const Locator = new LocatorSingleton()
