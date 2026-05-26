/**
 * Queue manager.
 *
 * Manages multiple named queue connections and dispatches each one to
 * the appropriate connector based on its `driver` discriminator.
 *
 * Drivers are registered through `MultipleInstanceManager.extend(driver,
 * creator)`-style logic — but because every driver requires async
 * setup, the registry is a `Map<driverName, IQueueConnector>` instead
 * of a sync factory. `QueueModule.forRoot()` registers the built-in
 * drivers; `QueueModule.forFeature(driver, ConnectorClass)` registers
 * additional drivers without touching the manager source.
 *
 * Lifecycle:
 *
 * - `OnModuleInit`     — eagerly warms the default connection so config
 *   errors surface at boot.
 * - `OnModuleDestroy`  — closes every active connection so process
 *   shutdown isn't blocked by open IndexedDB handles, timers, or HTTP
 *   keepalives.
 *
 * @module @stackra/ts-queue/services/queue-manager
 */

import { Inject, Injectable, type OnModuleDestroy, type OnModuleInit } from '@stackra/ts-container';
import { MultipleInstanceManager } from '@stackra/ts-support';
import { Logger } from '@stackra/ts-logger';

import {
  QUEUE_CONFIG,
  type IQueueConnection,
  type IQueueConnector,
  type IQueueModuleOptions,
  type IQueueService,
  type IWorkerOptions,
  type QueueConnectionConfig,
} from '@stackra/contracts';

import { QueueDriverError } from '@/errors/queue-driver.error';
import { QueueHandle } from './queue-handle.service';

/**
 * Queue manager — the single entry point for queue access.
 *
 * Concrete implementation of `IQueueService`. Extends
 * `MultipleInstanceManager<IQueueConnection>` for lazy resolution +
 * caching + introspection, identical to `RedisManager`/`CacheManager`.
 */
@Injectable()
export class QueueManager
  extends MultipleInstanceManager<IQueueConnection>
  implements IQueueService, OnModuleInit, OnModuleDestroy
{
  /** Scoped logger. */
  private readonly logger = new Logger(QueueManager.name);

  /**
   * Registered connectors keyed by driver name.
   *
   * Populated by `QueueModule.forRoot()` (built-ins) and
   * `QueueModule.forFeature()` (extensions).
   */
  private readonly connectors: Map<string, IQueueConnector> = new Map();

  /**
   * Cached `(connection, queue)` handles. Handles are light, but
   * keeping the cache means the same instance is returned for the same
   * pair — which keeps tests stable when they patch handle methods.
   */
  private readonly handles: Map<string, QueueHandle> = new Map();

  /**
   * @param config - Queue module configuration.
   */
  public constructor(@Inject(QUEUE_CONFIG) private readonly config: IQueueModuleOptions) {
    super();
  }

  // ────────────────────────────────────────────────────────────────────
  // Lifecycle
  // ────────────────────────────────────────────────────────────────────

  /**
   * Validate the default connection name and warm it eagerly so config
   * errors surface at bootstrap rather than at the first dispatch.
   */
  public async onModuleInit(): Promise<void> {
    if (!this.config?.default) {
      this.logger.warn("[QueueManager] Config is missing 'default'; skipping warm-up.");
      return;
    }

    const defaultName = this.config.default;
    if (!this.config.connections[defaultName]) {
      this.logger.warn(
        `[QueueManager] Default connection "${defaultName}" is not declared in connections; skipping warm-up.`
      );
      return;
    }

    try {
      await this.connection();
    } catch (err: Error | any) {
      this.logger.warn(
        `[QueueManager] Failed to warm default connection '${defaultName}': ${(err as Error).message}`
      );
    }
  }

  /**
   * Close every active connection on shutdown so the process can exit
   * cleanly. Errors per-connection are swallowed — partial cleanup
   * shouldn't block the rest.
   */
  public async onModuleDestroy(): Promise<void> {
    await this.disconnectAll();
    this.handles.clear();
  }

  // ────────────────────────────────────────────────────────────────────
  // Connector registry
  // ────────────────────────────────────────────────────────────────────

  /**
   * Register a connector for a given driver name.
   *
   * Called by the module's `forRoot()` (built-in drivers) and
   * `forFeature()` (extensions). Subsequent calls with the same driver
   * name overwrite the prior registration.
   *
   * @param driver    - Driver name (matches `QueueConnectionConfig.driver`).
   * @param connector - Connector instance for the driver.
   */
  public registerConnector(driver: string, connector: IQueueConnector): void {
    this.connectors.set(driver, connector);
  }

  /**
   * List the driver names that have a registered connector.
   *
   * @returns Driver name array (e.g. `["memory", "indexeddb"]`).
   */
  public getRegisteredDrivers(): string[] {
    return Array.from(this.connectors.keys());
  }

  // ────────────────────────────────────────────────────────────────────
  // MultipleInstanceManager contract
  // ────────────────────────────────────────────────────────────────────

  /** @inheritdoc */
  public getDefaultInstance(): string {
    return this.config.default;
  }

  /** @inheritdoc */
  public setDefaultInstance(name: string): void {
    (this.config as { default: string }).default = name;
  }

  /** @inheritdoc */
  public getInstanceConfig(name: string): Record<string, unknown> | undefined {
    const raw = this.config.connections[name];
    if (!raw) return undefined;
    // Tag the config with its connection name so connectors can name
    // the connection instance correctly without re-passing the name.
    return { ...(raw as unknown as Record<string, unknown>), name } as Record<string, unknown>;
  }

  /**
   * Sync driver creation is not supported — every queue driver requires
   * async setup (IndexedDB open, BroadcastChannel handshake, ...).
   *
   * @inheritdoc
   */
  protected createDriver(_driver: string, _config: Record<string, unknown>): IQueueConnection {
    throw new QueueDriverError('QueueManager: connections are async; call connection() instead.');
  }

  /**
   * Resolve a connector by driver name and delegate to it.
   *
   * @inheritdoc
   */
  protected async createDriverAsync(
    driver: string,
    config: Record<string, unknown>
  ): Promise<IQueueConnection> {
    const connector = this.connectors.get(driver);
    if (!connector) {
      throw new QueueDriverError(
        `Queue driver "${driver}" is not registered. ` +
          `Registered drivers: ${this.getRegisteredDrivers().join(', ') || '(none)'}.`
      );
    }

    return connector.connect(config as unknown as QueueConnectionConfig);
  }

  // ────────────────────────────────────────────────────────────────────
  // Public API — connections & queues
  // ────────────────────────────────────────────────────────────────────

  /** @inheritdoc */
  public async connection(name?: string): Promise<IQueueConnection> {
    return this.instanceAsync(name);
  }

  /**
   * Get a queue handle bound to a specific `(connection, queue)`.
   *
   * Handles are cached by the `connection:queue` pair so consumers get
   * the same instance for the same pair — which is essential for
   * tests that patch handle methods.
   *
   * @param queue      - Queue tube name (defaults to `"default"`).
   * @param connection - Connection name (defaults to the module default).
   * @returns A `QueueHandle` ready to push/pop the named tube.
   */
  public async queue(queue: string = 'default', connection?: string): Promise<QueueHandle> {
    const connName = connection ?? this.config.default;
    const key = `${connName}:${queue}`;

    const cached = this.handles.get(key);
    if (cached) return cached;

    const conn = await this.connection(connName);
    const handle = new QueueHandle(conn, queue);
    this.handles.set(key, handle);
    return handle;
  }

  /** @inheritdoc */
  public async disconnect(name?: string): Promise<void> {
    const connectionName = name ?? this.config.default;
    if (!this.hasInstance(connectionName)) return;

    const conn = this.instance(connectionName);
    try {
      await conn.close();
    } catch (err: Error | any) {
      this.logger.warn(
        `[QueueManager] Failed to close connection '${connectionName}': ${(err as Error).message}`
      );
    } finally {
      this.forgetInstance(connectionName);

      // Drop cached handles bound to the closed connection.
      const prefix = `${connectionName}:`;
      for (const key of Array.from(this.handles.keys())) {
        if (key.startsWith(prefix)) this.handles.delete(key);
      }
    }
  }

  /** @inheritdoc */
  public async disconnectAll(): Promise<void> {
    const names = this.getResolvedInstances();
    await Promise.all(names.map((n) => this.disconnect(n)));
    this.purge();
  }

  // ────────────────────────────────────────────────────────────────────
  // Introspection
  // ────────────────────────────────────────────────────────────────────

  /** @inheritdoc */
  public getConnectionNames(): string[] {
    return Object.keys(this.config.connections);
  }

  /** @inheritdoc */
  public getDefaultConnectionName(): string {
    return this.config.default;
  }

  /** @inheritdoc */
  public isConnectionActive(name?: string): boolean {
    return this.hasInstance(name ?? this.config.default);
  }

  /**
   * Whether the named connection is configured.
   *
   * @param name - Connection name.
   * @returns `true` when the name appears in module config.
   */
  public hasConnection(name: string): boolean {
    return name in this.config.connections;
  }

  /**
   * Currently active (resolved) connection names.
   *
   * @returns Array of connection names that have been resolved at
   *   least once and are currently cached.
   */
  public getActiveConnectionNames(): string[] {
    return this.getResolvedInstances();
  }

  /**
   * Shared worker options resolved with sensible defaults.
   *
   * Workers consult this for poll interval, retry policy, timeouts.
   * Defaults mirror Laravel's worker defaults so apps familiar with
   * the Laravel queue feel at home.
   *
   * @returns Fully resolved worker options.
   */
  public getWorkerOptions(): Required<IWorkerOptions> {
    const w = this.config.worker ?? {};
    return {
      tries: w.tries ?? 1,
      backoffMs: w.backoffMs ?? 1000,
      maxBackoffMs: w.maxBackoffMs ?? 30_000,
      timeoutMs: w.timeoutMs ?? 30_000,
      pollIntervalMs: w.pollIntervalMs ?? 500,
      autoStart: w.autoStart ?? true,
      failOnTimeout: w.failOnTimeout ?? true,
    };
  }
}
