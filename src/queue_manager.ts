import * as errors from './exceptions.js'
import debug from './debug.js'
import { Locator } from './locator.js'
import { consoleLogger, type Logger } from './logger.js'
import { FakeAdapter } from './drivers/fake_adapter.js'
import { QueueConfigResolver } from './queue_config_resolver.js'
import type { Adapter } from './contracts/adapter.js'
import type { AdapterFactory, JobFactory, QueueManagerConfig } from './types/main.js'

const noopInternalOperationWrapper: NonNullable<QueueManagerConfig['internalOperationWrapper']> = async (fn) => fn()
const noopExecutionWrapper: NonNullable<QueueManagerConfig['executionWrapper']> = async (fn) => fn()

type QueueManagerFakeState = {
  defaultAdapter: string
  adapters: Record<string, AdapterFactory>
  adapterInstances: Map<string, Adapter>
  logger: Logger
  jobFactory?: JobFactory
  internalOperationWrapper?: QueueManagerConfig['internalOperationWrapper']
  executionWrapper?: QueueManagerConfig['executionWrapper']
  configResolver: QueueConfigResolver
  fakeAdapter: FakeAdapter
}

/**
 * Central configuration and adapter management for the queue system.
 *
 * The QueueManager is responsible for:
 * - Initializing adapters and job registration
 * - Providing adapter instances to workers and dispatchers
 * - Managing retry configuration across global, queue, and job levels
 *
 * @example
 * ```typescript
 * import { QueueManager, redis } from '@boringnode/queue'
 *
 * await QueueManager.init({
 *   default: 'redis',
 *   adapters: {
 *     redis: redis({ host: 'localhost' }),
 *   },
 *   locations: ['./jobs/**\/*.js'],
 *   retry: {
 *     maxRetries: 3,
 *     backoff: exponentialBackoff(),
 *   },
 * })
 *
 * // Get the default adapter
 * const adapter = QueueManager.use()
 *
 * // Clean up when done
 * await QueueManager.destroy()
 * ```
 */
class QueueManagerSingleton {
  #initialized = false
  #defaultAdapter!: string
  #adapters: Record<string, AdapterFactory> = {}
  #adapterInstances: Map<string, Adapter> = new Map()
  #logger: Logger = consoleLogger
  #jobFactory?: JobFactory
  #internalOperationWrapper?: QueueManagerConfig['internalOperationWrapper']
  #executionWrapper?: QueueManagerConfig['executionWrapper']
  #configResolver: QueueConfigResolver = new QueueConfigResolver({})
  #fakeState?: QueueManagerFakeState

  /**
   * Initialize the queue system with the given configuration.
   *
   * This must be called before using the queue system. It:
   * - Validates the configuration
   * - Registers adapters
   * - Auto-discovers and registers job classes from `locations`
   *
   * @param config - The queue configuration
   * @returns This instance for chaining
   * @throws {E_CONFIGURATION_ERROR} If the configuration is invalid
   *
   * @example
   * ```typescript
   * await QueueManager.init({
   *   default: 'redis',
   *   adapters: {
   *     redis: redis(),
   *     postgres: knex(pgConfig),
   *   },
   *   locations: ['./jobs/**\/*.js'],
   * })
   * ```
   */
  async init(config: QueueManagerConfig) {
    debug('initializing queue manager with config: %O', config)

    this.#validateConfig(config)

    await this.#cleanupBeforeReinitialization()

    this.#defaultAdapter = config.default
    this.#adapters = config.adapters
    this.#logger = config.logger ?? consoleLogger
    this.#jobFactory = config.jobFactory
    this.#internalOperationWrapper = config.internalOperationWrapper
    this.#executionWrapper = config.executionWrapper
    this.#configResolver = QueueConfigResolver.from(config)

    if (config.locations && config.locations.length > 0) {
      const registered = await Locator.registerFromGlob(config.locations)

      if (registered === 0) {
        this.#logger.warn(
          `No jobs found for locations: ${config.locations.join(', ')}. ` +
            'Verify your glob patterns match your job files.'
        )
      }
    }

    this.#initialized = true

    return this
  }

  /**
   * Destroy any materialized adapters from the current configuration before
   * replacing it with a new one.
   */
  async #cleanupBeforeReinitialization() {
    const destroyedAdapters = new Set<Adapter>()

    await this.#destroyAdapters(this.#adapterInstances, destroyedAdapters)

    if (this.#fakeState) {
      await this.#destroyAdapter('fake', this.#fakeState.fakeAdapter, destroyedAdapters)
      await this.#destroyAdapters(this.#fakeState.adapterInstances, destroyedAdapters)
      this.#fakeState = undefined
    }

    this.#adapterInstances.clear()
  }

  /**
   * Destroy a collection of adapters while avoiding double-destroying the same
   * instance through multiple references.
   */
  async #destroyAdapters(adapters: Iterable<[string, Adapter]>, destroyedAdapters: Set<Adapter>) {
    for (const [name, adapter] of adapters) {
      await this.#destroyAdapter(name, adapter, destroyedAdapters)
    }
  }

  /**
   * Destroy a single adapter once for the current cleanup pass.
   */
  async #destroyAdapter(name: string, adapter: Adapter, destroyedAdapters: Set<Adapter>) {
    if (destroyedAdapters.has(adapter)) {
      return
    }

    destroyedAdapters.add(adapter)
    debug('destroying adapter "%s" before reinitialization', name)
    await adapter.destroy()
  }

  /**
   * Get an adapter instance by name.
   *
   * Adapter instances are cached and reused. If no name is provided,
   * the default adapter is returned.
   *
   * @param adapter - Adapter name (optional, defaults to the default adapter)
   * @returns The adapter instance
   * @throws {E_QUEUE_NOT_INITIALIZED} If `init()` hasn't been called
   * @throws {E_CONFIGURATION_ERROR} If the adapter is not registered
   * @throws {E_ADAPTER_INIT_ERROR} If the adapter factory throws
   *
   * @example
   * ```typescript
   * // Get default adapter
   * const adapter = QueueManager.use()
   *
   * // Get specific adapter
   * const redisAdapter = QueueManager.use('redis')
   * ```
   */
  use(adapter?: string): Adapter {
    if (!this.#initialized) {
      throw new errors.E_QUEUE_NOT_INITIALIZED()
    }

    if (!adapter) {
      adapter = this.#defaultAdapter
    }

    // Return cached instance if exists
    const cached = this.#adapterInstances.get(adapter)
    if (cached) {
      return cached
    }

    const adapterFactory = this.#adapters[adapter]

    if (!adapterFactory) {
      throw new errors.E_CONFIGURATION_ERROR([`Adapter "${adapter}" is not registered`])
    }

    debug('using adapter "%s"', adapter)

    try {
      const instance = adapterFactory()
      this.#adapterInstances.set(adapter, instance)
      return instance
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      throw new errors.E_ADAPTER_INIT_ERROR([adapter, message], { cause: error })
    }
  }

  /**
   * Replace all adapters with a fake adapter for testing.
   *
   * The fake adapter records pushed jobs and exposes assertion helpers.
   * Call `restore()` to return to the previous configuration.
   *
   * @returns The fake adapter instance for assertions
   * @throws {E_QUEUE_NOT_INITIALIZED} If `init()` hasn't been called
   *
   * @example
   * ```typescript
   * const fake = QueueManager.fake()
   *
   * await SendEmailJob.dispatch({ to: 'user@example.com' })
   *
   * fake.assertPushed(SendEmailJob)
   * QueueManager.restore()
   * ```
   */
  fake(): FakeAdapter {
    if (!this.#initialized) {
      throw new errors.E_QUEUE_NOT_INITIALIZED()
    }

    if (this.#fakeState) {
      return this.#fakeState.fakeAdapter
    }

    const fakeAdapter = new FakeAdapter()

    this.#fakeState = {
      defaultAdapter: this.#defaultAdapter,
      adapters: this.#adapters,
      adapterInstances: this.#adapterInstances,
      logger: this.#logger,
      jobFactory: this.#jobFactory,
      internalOperationWrapper: this.#internalOperationWrapper,
      executionWrapper: this.#executionWrapper,
      configResolver: this.#configResolver,
      fakeAdapter,
    }

    const fakeFactory = () => fakeAdapter
    const nextAdapters: Record<string, AdapterFactory> = {}

    for (const name of Object.keys(this.#fakeState.adapters)) {
      nextAdapters[name] = fakeFactory
    }

    this.#adapters = nextAdapters
    this.#adapterInstances = new Map()

    return fakeAdapter
  }

  /**
   * Restore adapters after calling `fake()`.
   */
  restore(): void {
    if (!this.#fakeState) {
      return
    }

    void this.#fakeState.fakeAdapter.destroy()

    for (const adapter of this.#adapterInstances.values()) {
      void adapter.destroy()
    }

    const state = this.#fakeState
    this.#fakeState = undefined

    this.#defaultAdapter = state.defaultAdapter
    this.#adapters = state.adapters
    this.#adapterInstances = state.adapterInstances
    this.#logger = state.logger
    this.#jobFactory = state.jobFactory
    this.#internalOperationWrapper = state.internalOperationWrapper
    this.#executionWrapper = state.executionWrapper
    this.#configResolver = state.configResolver
  }

  /**
   * Get the configured job factory for custom instantiation.
   *
   * @returns The job factory function, or undefined if not configured
   */
  getJobFactory(): JobFactory | undefined {
    return this.#jobFactory
  }

  /**
   * Whether the queue manager has been initialized.
   */
  isInitialized(): boolean {
    return this.#initialized
  }

  /**
   * Get the configured logger used by the queue runtime.
   */
  getLogger(): Logger {
    return this.#logger
  }

  /**
   * Get the configured internal operation wrapper.
   */
  getInternalOperationWrapper() {
    return this.#internalOperationWrapper ?? noopInternalOperationWrapper
  }

  /**
   * Get the configured execution wrapper.
   */
  getExecutionWrapper() {
    return this.#executionWrapper ?? noopExecutionWrapper
  }

  /**
   * Get the resolver responsible for effective queue/job runtime config.
   */
  getConfigResolver(): QueueConfigResolver {
    if (!this.#initialized) {
      throw new errors.E_QUEUE_NOT_INITIALIZED()
    }

    return this.#configResolver
  }

  #validateConfig(config: QueueManagerConfig): void {
    if (!config.adapters || Object.keys(config.adapters).length === 0) {
      throw new errors.E_CONFIGURATION_ERROR(['At least one adapter must be configured'])
    }

    if (!config.default) {
      throw new errors.E_CONFIGURATION_ERROR(['Default adapter must be specified'])
    }

    if (!config.adapters[config.default]) {
      throw new errors.E_CONFIGURATION_ERROR([
        `Default adapter "${config.default}" not found in adapters configuration`,
      ])
    }

    for (const [name, factory] of Object.entries(config.adapters)) {
      if (typeof factory !== 'function') {
        throw new errors.E_CONFIGURATION_ERROR([`Adapter "${name}" must be a factory function`])
      }
    }
  }

  /**
   * Clean up all adapter instances and reset state.
   *
   * Call this when shutting down the application or when
   * you need to reinitialize with a new configuration.
   *
   * @example
   * ```typescript
   * // On application shutdown
   * await QueueManager.destroy()
   * ```
   */
  async destroy() {
    for (const [name, adapter] of this.#adapterInstances) {
      debug('destroying adapter "%s"', name)
      await adapter.destroy()
    }

    if (this.#fakeState) {
      await this.#fakeState.fakeAdapter.destroy()

      for (const [name, adapter] of this.#fakeState.adapterInstances) {
        debug('destroying adapter "%s"', name)
        await adapter.destroy()
      }
    }

    this.#adapterInstances.clear()
    this.#initialized = false
    this.#internalOperationWrapper = undefined
    this.#executionWrapper = undefined
    this.#configResolver = new QueueConfigResolver({})
    this.#fakeState = undefined
  }
}

/** Global queue manager singleton */
export const QueueManager = new QueueManagerSingleton()
