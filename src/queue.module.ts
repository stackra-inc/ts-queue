/**
 * Queue module.
 *
 * Wires `QueueManager`, the built-in connectors, the per-connection
 * tokens, and the bootstrap loader for `@Processor` discovery.
 *
 * Three registration entry points:
 *
 * - `forRoot(config)` — static configuration. Registers all built-in
 *   connectors (memory/sync/null/local-storage/indexeddb/broadcast-channel/qstash)
 *   plus the manager and the per-connection tokens.
 * - `forRootAsync(options)` — async/factory variant for DI-resolved
 *   configuration.
 * - `forFeature(driver, ConnectorClass)` — register a custom connector
 *   so consumers can plug in any extra driver without touching the
 *   manager source.
 *
 * Mirrors `RedisModule`/`RealtimeModule`. The pattern keeps the
 * manager open to extension and the module surface narrow.
 *
 * @module @stackra/ts-queue/queue.module
 */

import { Global, Module, type DynamicModule, type Type } from '@stackra/ts-container';
import { Logger } from '@stackra/ts-logger';

import {
  DEFAULT_QUEUE_CONNECTION_TOKEN,
  QUEUE_CONFIG,
  QUEUE_MANAGER,
  type IQueueConnector,
  type IQueueModuleAsyncOptions,
  type IQueueModuleOptions,
} from '@stackra/contracts';

import { QueueManager } from '@/services/queue-manager.service';
import { QueueEventBus } from '@/services/event-bus.service';
import { ProcessorMetadataAccessor } from '@/accessors/processor-metadata.accessor';
import { ProcessorSubscribersLoader } from '@/loaders/processor-subscribers.loader';
import {
  BroadcastChannelConnector,
  IndexedDBConnector,
  LocalStorageConnector,
  MemoryConnector,
  NullConnector,
  QStashConnector,
  SyncConnector,
} from '@/connectors';
import { getQueueConnectionToken, getQueueToken } from '@/utils';
import { QueueModuleOptionsError } from '@/errors';

/**
 * Built-in connector registration entry.
 */
interface BuiltInConnector {
  /** Driver name (matches `QueueConnectionConfig.driver`). */
  driver: string;
  /** Connector class to instantiate via DI. */
  type: Type<IQueueConnector>;
}

/**
 * Built-in queue connectors registered automatically by `forRoot()`.
 *
 * Adding a new built-in driver: write the connector class, append its
 * entry here, the manager picks it up automatically. External drivers
 * that should NOT be in the default bundle stay out of this list and
 * are registered through `forFeature(driver, ConnectorClass)`.
 */
const BUILT_IN_CONNECTORS: ReadonlyArray<BuiltInConnector> = Object.freeze([
  { driver: 'memory', type: MemoryConnector },
  { driver: 'sync', type: SyncConnector },
  { driver: 'null', type: NullConnector },
  { driver: 'local-storage', type: LocalStorageConnector },
  { driver: 'indexeddb', type: IndexedDBConnector },
  { driver: 'broadcast-channel', type: BroadcastChannelConnector },
  { driver: 'qstash', type: QStashConnector },
]);

/**
 * Queue DI module.
 */
@Global()
@Module({})
export class QueueModule {
  /** Scoped logger for module-level diagnostics. */
  private static readonly logger = new Logger(QueueModule.name);

  // ────────────────────────────────────────────────────────────────────
  // forRoot
  // ────────────────────────────────────────────────────────────────────

  /**
   * Configure the queue module statically.
   *
   * Registers:
   *
   * - `QUEUE_CONFIG` — the raw `IQueueModuleOptions` value.
   * - `QueueManager` and `QUEUE_MANAGER` alias.
   * - Every built-in connector listed in {@link BUILT_IN_CONNECTORS}.
   * - `DEFAULT_QUEUE_CONNECTION_TOKEN` factory.
   * - One `IQueueConnection` provider per configured connection.
   * - One default `(connection, "default")` queue handle per connection.
   * - Bootstrap services for `@Processor` discovery.
   *
   * @param config - Queue configuration.
   * @returns A dynamic module with everything wired.
   *
   * @example
   * ```typescript
   * @Module({
   *   imports: [
   *     QueueModule.forRoot({
   *       default: 'indexeddb',
   *       connections: {
   *         indexeddb: { driver: 'indexeddb', dbName: 'app-queue' },
   *         qstash:    { driver: 'qstash', mode: 'proxy', proxyUrl: '/api/q' },
   *       },
   *       worker: { tries: 3, backoffMs: 1000, timeoutMs: 30_000 },
   *     }),
   *   ],
   * })
   * export class AppModule {}
   * ```
   */
  public static forRoot(config: IQueueModuleOptions): DynamicModule {
    QueueModule.validate(config);

    const connectorRegistrations = QueueModule.buildConnectorRegistrations(BUILT_IN_CONNECTORS);

    const connectionProviders = Object.keys(config.connections).map((connectionName) => ({
      provide: getQueueConnectionToken(connectionName),
      useFactory: async (manager: QueueManager) => manager.connection(connectionName),
      inject: [QueueManager],
    }));

    const defaultConnectionProvider = {
      provide: DEFAULT_QUEUE_CONNECTION_TOKEN,
      useFactory: async (manager: QueueManager) => manager.connection(config.default),
      inject: [QueueManager],
    };

    const defaultHandleProviders = Object.keys(config.connections).map((connectionName) => ({
      provide: getQueueToken('default', connectionName),
      useFactory: async (manager: QueueManager) => manager.queue('default', connectionName),
      inject: [QueueManager],
    }));

    const defaultQueueHandleProvider = {
      provide: getQueueToken(),
      useFactory: async (manager: QueueManager) => manager.queue(),
      inject: [QueueManager],
    };

    return {
      module: QueueModule,
      global: true,
      providers: [
        // Config
        { provide: QUEUE_CONFIG, useValue: config },

        // Manager
        QueueManager,
        { provide: QUEUE_MANAGER, useExisting: QueueManager },

        // Built-in connectors and their auto-registration side-effects.
        ...connectorRegistrations.providers,

        // Connection-level providers
        defaultConnectionProvider,
        ...connectionProviders,

        // Default queue handle providers
        defaultQueueHandleProvider,
        ...defaultHandleProviders,

        // Bootstrap infrastructure
        QueueEventBus,
        ProcessorMetadataAccessor,
        ProcessorSubscribersLoader,
      ],
      exports: [
        QUEUE_CONFIG,
        QueueManager,
        QUEUE_MANAGER,
        DEFAULT_QUEUE_CONNECTION_TOKEN,
        ...connectionProviders.map((p) => p.provide),
        ...defaultHandleProviders.map((p) => p.provide),
        getQueueToken(),
        QueueEventBus,
      ],
    };
  }

  // ────────────────────────────────────────────────────────────────────
  // forRootAsync
  // ────────────────────────────────────────────────────────────────────

  /**
   * Configure the queue module asynchronously.
   *
   * Useful when configuration depends on a DI-resolved dependency (a
   * config service, a feature flag service, etc.). Built-in connectors
   * are still registered so consumers can use any of the supported
   * drivers without an additional `forFeature()` call.
   *
   * Note: per-connection tokens are NOT registered here because the
   * connection list is unknown at module-build time. Use
   * `@InjectQueueConnection()` (no name) or `QueueManager.connection()`
   * directly when configuring async.
   *
   * @param options - Async options carrying `useFactory` / `inject`.
   */
  public static forRootAsync(options: IQueueModuleAsyncOptions): DynamicModule {
    if (!options.useFactory) {
      QueueModule.logger.warn('[QueueModule] forRootAsync requires useFactory.');
      return { module: QueueModule, providers: [], exports: [] };
    }

    const connectorRegistrations = QueueModule.buildConnectorRegistrations(BUILT_IN_CONNECTORS);

    return {
      module: QueueModule,
      global: true,
      imports: (options.imports ?? []) as never[],
      providers: [
        {
          provide: QUEUE_CONFIG,
          useFactory: options.useFactory,
          inject: (options.inject ?? []) as never[],
        },

        QueueManager,
        { provide: QUEUE_MANAGER, useExisting: QueueManager },

        ...connectorRegistrations.providers,

        QueueEventBus,
        ProcessorMetadataAccessor,
        ProcessorSubscribersLoader,
      ],
      exports: [QUEUE_CONFIG, QueueManager, QUEUE_MANAGER, QueueEventBus],
    };
  }

  // ────────────────────────────────────────────────────────────────────
  // forFeature — additional drivers
  // ────────────────────────────────────────────────────────────────────

  /**
   * Register an additional connector for a custom driver.
   *
   * Mirrors `RealtimeModule.forFeature()` and `RedisModule.forFeature()`.
   *
   * @param driver        - Driver name (e.g. `"sqs"`).
   * @param connectorType - Connector class implementing `IQueueConnector`.
   * @returns A dynamic module that registers the connector at boot.
   *
   * @example
   * ```typescript
   * @Module({
   *   imports: [
   *     QueueModule.forRoot(queueConfig),
   *     QueueModule.forFeature("sqs", SqsConnector),
   *   ],
   * })
   * export class AppModule {}
   * ```
   */
  public static forFeature(driver: string, connectorType: Type<IQueueConnector>): DynamicModule {
    const registrationToken = Symbol.for(`QUEUE_CONNECTOR_REGISTRATION:${driver}`);

    return {
      module: QueueModule,
      providers: [
        connectorType,
        {
          provide: registrationToken,
          useFactory: (manager: QueueManager, connector: IQueueConnector) => {
            manager.registerConnector(driver, connector);
            return null;
          },
          inject: [QueueManager, connectorType],
        },
      ],
      exports: [connectorType],
    };
  }

  // ────────────────────────────────────────────────────────────────────
  // forFeatureQueues — additional queue handle bindings
  // ────────────────────────────────────────────────────────────────────

  /**
   * Register additional queue handles for specific `(queue, connection)`
   * pairs that need to be injected by token.
   *
   * Handles created on demand via `manager.queue(...)` are always
   * available — but to use `@InjectQueue('scans', 'indexeddb')` you
   * need a registered provider. Call this per feature module.
   *
   * @param queues - Array of `{ queue, connection? }` pairs to register.
   * @returns A dynamic module exposing the requested handle tokens.
   *
   * @example
   * ```typescript
   * @Module({
   *   imports: [
   *     QueueModule.forFeatureQueues([
   *       { queue: 'scans', connection: 'indexeddb' },
   *       { queue: 'receipts', connection: 'indexeddb' },
   *     ]),
   *   ],
   * })
   * export class ScannerModule {}
   * ```
   */
  public static forFeatureQueues(
    queues: Array<{ queue: string; connection?: string }>
  ): DynamicModule {
    const providers = queues.map(({ queue, connection }) => ({
      provide: getQueueToken(queue, connection ?? 'default'),
      useFactory: async (manager: QueueManager) => manager.queue(queue, connection),
      inject: [QueueManager],
    }));

    return {
      module: QueueModule,
      providers,
      exports: providers.map((p) => p.provide),
    };
  }

  // ────────────────────────────────────────────────────────────────────
  // Internal
  // ────────────────────────────────────────────────────────────────────

  /**
   * Build provider entries for an array of built-in connectors.
   *
   * Each connector is registered as itself plus a side-effect provider
   * that calls `manager.registerConnector(driver, connector)` so the
   * manager knows the driver name → connector mapping.
   *
   * Typed `any[]` because the DI container's `Provider` union is
   * recursive and not easily expressible without `any`. The
   * `RealtimeModule` and `RedisModule` use the same pattern.
   *
   * @param connectors - Built-in connector entries to register.
   * @returns Provider list.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private static buildConnectorRegistrations(connectors: ReadonlyArray<BuiltInConnector>): {
    providers: any[];
  } {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const providers: any[] = [];

    for (const { driver, type } of connectors) {
      const registrationToken = Symbol.for(`QUEUE_CONNECTOR_REGISTRATION:${driver}`);

      providers.push(type);
      providers.push({
        provide: registrationToken,
        useFactory: (manager: QueueManager, connector: IQueueConnector) => {
          manager.registerConnector(driver, connector);
          return null;
        },
        inject: [QueueManager, type],
      });
    }

    return { providers };
  }

  /**
   * Validate static configuration so misconfiguration surfaces as a
   * clear error at bootstrap rather than as a confusing runtime
   * failure on first use.
   *
   * @param config - The configuration to validate.
   */
  private static validate(config: IQueueModuleOptions): void {
    if (!config) {
      throw new QueueModuleOptionsError('[QueueModule] forRoot() requires a configuration object.');
    }

    if (!config.default) {
      throw new QueueModuleOptionsError('[QueueModule] config.default is required.');
    }

    if (!config.connections || Object.keys(config.connections).length === 0) {
      throw new QueueModuleOptionsError(
        '[QueueModule] config.connections must define at least one entry.'
      );
    }

    if (!config.connections[config.default]) {
      throw new QueueModuleOptionsError(
        `[QueueModule] config.default "${config.default}" is not present in config.connections.`
      );
    }

    for (const [name, conn] of Object.entries(config.connections)) {
      if (!conn.driver) {
        throw new QueueModuleOptionsError(
          `[QueueModule] connection "${name}" is missing the "driver" field.`
        );
      }
    }
  }
}
