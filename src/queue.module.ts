/**
 * @fileoverview QueueModule — the DI module that wires everything together.
 *
 * Consumers call `QueueModule.forRoot({...})` once at the app level.
 * The module registers the config, the {@link QueueManager}, per-connection
 * providers for `@InjectQueueConnection(name)`, and the bootstrap loader
 * that discovers `@Processor` classes.
 *
 * Processor classes are picked up by the decorator discovery plugin at
 * build time (see the side-effect registration at the bottom of
 * `src/index.ts`). No `forFeature()` is required for them — they only
 * need to be `@Injectable()` and in the DI scan path.
 *
 * @module queue.module
 * @category Module
 */

import { Module, type IDynamicModule } from "@stackra/ts-container";

import { DEFAULT_QUEUE_CONNECTION_TOKEN, QUEUE_CONFIG, QUEUE_MANAGER } from "@stackra/contracts";
import { getQueueConnectionToken, getQueueToken } from "@/constants";
import type { QueueModuleOptions } from "@/interfaces/queue-module-options.interface";
import { QueueManager } from "@/services/queue-manager.service";
import { QueueEventBus } from "@/services/event-bus.service";
import { ProcessorMetadataAccessor } from "@/accessors/processor-metadata.accessor";
import { ProcessorSubscribersLoader } from "@/loaders/processor-subscribers.loader";
/**
 * Queue DI module.
 *
 * @example
 * ```typescript
 * @Module({
 *   imports: [
 *     QueueModule.forRoot({
 *       default: 'indexeddb',
 *       connections: {
 *         indexeddb: { driver: 'indexeddb', dbName: 'app-queue' },
 *         qstash: {
 *           driver: 'qstash',
 *           mode: 'proxy',
 *           proxyUrl: '/api/queue/publish',
 *           defaultDestination: 'https://api.example.com/webhooks/queue',
 *         },
 *       },
 *       worker: { tries: 3, backoffMs: 1000, timeoutMs: 30_000 },
 *     }),
 *   ],
 * })
 * export class AppModule {}
 * ```
 */
@Module({})
// biome-ignore lint/complexity/noStaticOnlyClass: Module pattern requires static methods
export class QueueModule {
  /**
   * Configure the queue module globally.
   *
   * Registers:
   * - {@link QUEUE_CONFIG} — the raw {@link QueueModuleOptions} object.
   * - {@link QueueManager} and {@link QUEUE_MANAGER} alias token.
   * - {@link DEFAULT_QUEUE_CONNECTION_TOKEN} factory.
   * - One provider per named connection (`getQueueConnectionToken(name)`).
   * - One provider for the `(connection, 'default')` handle per named
   *   connection (`getQueueToken('default', name)`) — powers the
   *   `@InjectQueue()` shorthand.
   * - The bootstrap services: {@link ProcessorSubscribersLoader},
   *   {@link ProcessorMetadataAccessor}, {@link QueueEventBus}.
   *
   * The returned module is marked `global: true` so every provider is
   * available app-wide without re-importing.
   *
   * @param config - The queue configuration.
   * @returns A dynamic module.
   */
  public static forRoot(config: QueueModuleOptions): IDynamicModule {
    // can access it by name — keeps parity with other packages.
    // One provider per configured connection — powers
    // `@InjectQueueConnection('name')` and the default-connection token.
    const connectionProviders: any[] = Object.keys(config.connections).map((name) => ({
      provide: getQueueConnectionToken(name),
      useFactory: (manager: QueueManager) => manager.connection(name),
      inject: [QueueManager],
    }));

    // Default connection sentinel so `@InjectQueueConnection()` without a
    // name works even before the user has read the config.
    const defaultConnectionProvider: any = {
      provide: DEFAULT_QUEUE_CONNECTION_TOKEN,
      useFactory: (manager: QueueManager) => manager.connection(config.default),
      inject: [QueueManager],
    };

    // One `(connection, 'default')` handle per connection — lets
    // `@InjectQueue()` (no args) resolve to a sensible default handle
    // without requiring `forFeature`. Additional `(connection, queue)`
    // handles are built on demand by `manager.queue(...)`.
    const defaultHandleProviders: any[] = Object.keys(config.connections).map((name) => ({
      provide: getQueueToken("default", name),
      useFactory: (manager: QueueManager) => manager.queue("default", name),
      inject: [QueueManager],
    }));

    // Convenience: `@InjectQueue()` with no args resolves to the
    // `(default-connection, 'default')` handle.
    const defaultQueueHandleProvider: any = {
      provide: getQueueToken(),
      useFactory: (manager: QueueManager) => manager.queue(),
      inject: [QueueManager],
    };

    return {
      module: QueueModule,
      global: true,
      providers: [
        // Config
        { provide: QUEUE_CONFIG, useValue: config },

        // Manager
        { provide: QueueManager, useClass: QueueManager },
        { provide: QUEUE_MANAGER, useExisting: QueueManager },

        // Connection-level providers
        defaultConnectionProvider,
        ...connectionProviders,

        // Queue handle providers
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
        ...connectionProviders.map((p) => (p as { provide: symbol }).provide),
        ...defaultHandleProviders.map((p) => (p as { provide: symbol }).provide),
        getQueueToken(),
        QueueEventBus,
      ],
    };
  }

  /**
   * Register additional queue handles for specific `(queue, connection)`
   * pairs that need to be injected by token.
   *
   * Handles created on-demand via `manager.queue(...)` are always
   * available — but to `@InjectQueue('scans', 'indexeddb')` you need a
   * registered provider. Call `forFeature` per feature module.
   *
   * @example
   * ```typescript
   * @Module({
   *   imports: [
   *     QueueModule.forFeature([
   *       { queue: 'scans', connection: 'indexeddb' },
   *       { queue: 'receipts', connection: 'indexeddb' },
   *     ]),
   *   ],
   * })
   * export class ScannerModule {}
   * ```
   */
  public static forFeature(queues: Array<{ queue: string; connection?: string }>): IDynamicModule {
    const providers: any[] = queues.map(({ queue, connection }) => ({
      provide: getQueueToken(queue, connection ?? "default"),
      useFactory: (manager: QueueManager) => manager.queue(queue, connection),
      inject: [QueueManager],
    }));

    return {
      module: QueueModule,
      providers,
      exports: providers.map((p) => (p as { provide: symbol }).provide),
    };
  }
}
