import { DynamicModule, Type, OnModuleInit, OnModuleDestroy, OnApplicationBootstrap, OnApplicationShutdown } from '@stackra/ts-container';
import { IQueueModuleOptions, IQueueModuleAsyncOptions, IQueueConnector, IQueueConnection, IJobOptions, IQueueService, IWorkerOptions, IEventEmitter, QueueEventName, IQueuedJob, IProcessorMetadata, QueueConnectionConfig, IQStashQueueConnectionConfig, IJobMetadata } from '@stackra/contracts';
import { MultipleInstanceManager } from '@stackra/ts-support';
import { TabCoordinator } from '@stackra/ts-coordinator';

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

/**
 * Queue DI module.
 */
declare class QueueModule {
    /** Scoped logger for module-level diagnostics. */
    private static readonly logger;
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
    static forRoot(config: IQueueModuleOptions): DynamicModule;
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
    static forRootAsync(options: IQueueModuleAsyncOptions): DynamicModule;
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
    static forFeature(driver: string, connectorType: Type<IQueueConnector>): DynamicModule;
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
    static forFeatureQueues(queues: Array<{
        queue: string;
        connection?: string;
    }>): DynamicModule;
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
    private static buildConnectorRegistrations;
    /**
     * Validate static configuration so misconfiguration surfaces as a
     * clear error at bootstrap rather than as a confusing runtime
     * failure on first use.
     *
     * @param config - The configuration to validate.
     */
    private static validate;
}

/**
 * Queue handle.
 *
 * A typed handle bound to one `(connection, queue)` pair. Forwards
 * `push`/`later`/`pop`/sizing/control to the underlying connection
 * with this handle's `queue` pre-applied so callers don't have to
 * repeat the queue name at every dispatch site.
 *
 * Kept as a class (not a proxy object) so `instanceof` works and the
 * API surface shows up in IDE autocomplete.
 *
 * @module @stackra/ts-queue/services/queue-handle
 */

/**
 * Bound handle scoping a queue tube on a single connection.
 */
declare class QueueHandle {
    private readonly connection;
    readonly queue: string;
    /**
     * @param connection - Underlying queue connection.
     * @param queue      - Queue tube name this handle is bound to.
     */
    constructor(connection: IQueueConnection, queue: string);
    /**
     * Dispatch a job onto the bound queue.
     *
     * @typeParam T - Payload type.
     * @param name    - Application-level job name.
     * @param data    - Job payload.
     * @param options - Optional dispatch options. `queue` is overridden
     *   by the handle.
     * @returns Driver-assigned job id.
     */
    push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * Dispatch a job with a delay.
     *
     * @param delayMs - Delay in milliseconds before the job becomes eligible.
     */
    later<T = unknown>(delayMs: number, name: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * Bulk dispatch.
     *
     * @typeParam T - Payload type for every job.
     * @param jobs - Array of `{ name, data, options }` tuples.
     * @returns Array of dispatched job ids.
     */
    bulk<T = unknown>(jobs: Array<{
        name: string;
        data: T;
        options?: IJobOptions;
    }>): Promise<string[]>;
    /**
     * Total in-flight job count for the bound queue.
     *
     * @returns The number of in-flight jobs.
     */
    size(): Promise<number>;
    /**
     * Pending-only count.
     *
     * @returns The number of jobs in the Pending state.
     */
    pendingSize(): Promise<number>;
    /**
     * Delayed-only count.
     *
     * @returns The number of jobs in the Delayed state.
     */
    delayedSize(): Promise<number>;
    /**
     * Reserved-only count.
     *
     * @returns The number of jobs in the Reserved state.
     */
    reservedSize(): Promise<number>;
    /**
     * Wipe every job on the bound queue tube.
     */
    clear(): Promise<void>;
    /**
     * Pause processing on the bound queue tube.
     */
    pause(): Promise<void>;
    /**
     * Resume processing on the bound queue tube.
     */
    resume(): Promise<void>;
    /**
     * Whether the bound queue tube is currently paused.
     *
     * @returns `true` when paused, `false` otherwise.
     */
    isPaused(): Promise<boolean>;
}

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

/**
 * Queue manager — the single entry point for queue access.
 *
 * Concrete implementation of `IQueueService`. Extends
 * `MultipleInstanceManager<IQueueConnection>` for lazy resolution +
 * caching + introspection, identical to `RedisManager`/`CacheManager`.
 */
declare class QueueManager extends MultipleInstanceManager<IQueueConnection> implements IQueueService, OnModuleInit, OnModuleDestroy {
    private readonly config;
    /** Scoped logger. */
    private readonly logger;
    /**
     * Registered connectors keyed by driver name.
     *
     * Populated by `QueueModule.forRoot()` (built-ins) and
     * `QueueModule.forFeature()` (extensions).
     */
    private readonly connectors;
    /**
     * Cached `(connection, queue)` handles. Handles are light, but
     * keeping the cache means the same instance is returned for the same
     * pair — which keeps tests stable when they patch handle methods.
     */
    private readonly handles;
    /**
     * @param config - Queue module configuration.
     */
    constructor(config: IQueueModuleOptions);
    /**
     * Validate the default connection name and warm it eagerly so config
     * errors surface at bootstrap rather than at the first dispatch.
     */
    onModuleInit(): Promise<void>;
    /**
     * Close every active connection on shutdown so the process can exit
     * cleanly. Errors per-connection are swallowed — partial cleanup
     * shouldn't block the rest.
     */
    onModuleDestroy(): Promise<void>;
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
    registerConnector(driver: string, connector: IQueueConnector): void;
    /**
     * List the driver names that have a registered connector.
     *
     * @returns Driver name array (e.g. `["memory", "indexeddb"]`).
     */
    getRegisteredDrivers(): string[];
    /** @inheritdoc */
    getDefaultInstance(): string;
    /** @inheritdoc */
    setDefaultInstance(name: string): void;
    /** @inheritdoc */
    getInstanceConfig(name: string): Record<string, unknown> | undefined;
    /**
     * Sync driver creation is not supported — every queue driver requires
     * async setup (IndexedDB open, BroadcastChannel handshake, ...).
     *
     * @inheritdoc
     */
    protected createDriver(_driver: string, _config: Record<string, unknown>): IQueueConnection;
    /**
     * Resolve a connector by driver name and delegate to it.
     *
     * @inheritdoc
     */
    protected createDriverAsync(driver: string, config: Record<string, unknown>): Promise<IQueueConnection>;
    /** @inheritdoc */
    connection(name?: string): Promise<IQueueConnection>;
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
    queue(queue?: string, connection?: string): Promise<QueueHandle>;
    /** @inheritdoc */
    disconnect(name?: string): Promise<void>;
    /** @inheritdoc */
    disconnectAll(): Promise<void>;
    /** @inheritdoc */
    getConnectionNames(): string[];
    /** @inheritdoc */
    getDefaultConnectionName(): string;
    /** @inheritdoc */
    isConnectionActive(name?: string): boolean;
    /**
     * Whether the named connection is configured.
     *
     * @param name - Connection name.
     * @returns `true` when the name appears in module config.
     */
    hasConnection(name: string): boolean;
    /**
     * Currently active (resolved) connection names.
     *
     * @returns Array of connection names that have been resolved at
     *   least once and are currently cached.
     */
    getActiveConnectionNames(): string[];
    /**
     * Shared worker options resolved with sensible defaults.
     *
     * Workers consult this for poll interval, retry policy, timeouts.
     * Defaults mirror Laravel's worker defaults so apps familiar with
     * the Laravel queue feel at home.
     *
     * @returns Fully resolved worker options.
     */
    getWorkerOptions(): Required<IWorkerOptions>;
}

/**
 * @fileoverview Thin wrapper around `@stackra/ts-events` for queue lifecycle.
 *
 * The queue package publishes every lifecycle transition through the
 * event bus so instrumentation code can subscribe without coupling to
 * queue internals. This service isolates that dependency — if
 * `@stackra/ts-events` isn't registered, all emits become no-ops.
 *
 * @module services/event-bus
 * @category Services
 */

/**
 * Publishes queue lifecycle events on the default event connection.
 *
 * The `EventManager` dep is `@Optional()` — consumers that don't install
 * `@stackra/ts-events` still get a working queue; they just don't get
 * lifecycle events. Use the facade or event manager directly if you
 * want to subscribe.
 */
declare class QueueEventBus {
    private readonly events?;
    constructor(events?: IEventEmitter | undefined);
    /**
     * Emit a queue lifecycle event.
     *
     * @param event   - One of the {@link QUEUE_EVENTS} constants from
     *                  `@stackra/contracts`.
     * @param payload - Event payload (`{ job, error?, … }`).
     */
    emit(event: QueueEventName, payload: unknown): void;
}

/**
 * @fileoverview Abstract base class for `@Processor`-decorated classes.
 *
 * Mirrors NestJS Bull's `WorkerHost`. Processor classes extend it and
 * implement `process(job)` — the bootstrap loader uses an `instanceof`
 * check to catch misuse at runtime.
 *
 * @module hosts/worker-host
 * @category Hosts
 */

/**
 * Base class every `@Processor`-decorated class must extend.
 *
 * Subclasses implement `process()` — called once per popped job. The
 * method may be sync or async; async implementations allow awaiting I/O
 * (API calls, database writes) naturally.
 *
 * @typeParam T - Payload type for the queue this processor consumes.
 *
 * @example
 * ```typescript
 * @Processor('tracking')
 * export class PixelProcessor extends WorkerHost<PixelPayload> {
 *   async process(job: IQueuedJob<PixelPayload>): Promise<void> {
 *     await this.pixels.fireEvent(job.data.eventName, job.data.params);
 *   }
 * }
 * ```
 */
declare abstract class WorkerHost<T = unknown> {
    /**
     * Handle a single queued job.
     *
     * Implementations should throw on failure — the worker catches the
     * throw and decides whether to retry (attempts remaining) or mark the
     * job permanently failed. Return normally to indicate success.
     *
     * @param job - The job as rehydrated from the driver.
     */
    abstract process(job: IQueuedJob<T>): Promise<void> | void;
}

/**
 * Internal worker configuration.
 *
 * Carries everything a `Worker` instance needs to drive a single
 * `(connection, queue)` polling loop. Lives in this package because
 * no other package needs to construct a `Worker` directly.
 *
 * @module @stackra/ts-queue/interfaces/worker-config
 */

/**
 * Per-worker configuration assembled by the loader.
 */
interface IWorkerConfig {
    /** Driver this worker pulls from. */
    connection: IQueueConnection;
    /** Queue tube name within the connection. */
    queue: string;
    /** Processor host providing `process(job)`. */
    host: WorkerHost;
    /** Shared worker policy resolved with defaults. */
    options: Required<IWorkerOptions>;
    /** Event bus for lifecycle notifications. */
    eventBus: QueueEventBus;
}

/**
 * @fileoverview Per-processor worker loop.
 *
 * Each `@Processor` class gets its own {@link Worker} instance at
 * bootstrap. The worker polls the connection for jobs, enforces
 * timeouts, applies the retry/backoff policy, and emits lifecycle
 * events via {@link QueueEventBus}.
 *
 * Workers run on a simple `setTimeout` loop instead of `setInterval` so
 * stopping the worker is deterministic — we clear the pending timeout
 * and no further invocations happen.
 *
 * @module services/worker
 * @category Services
 */

/**
 * One polling loop per (connection, queue, host).
 *
 * The design is intentionally "one worker = one loop." Concurrency is
 * achieved by instantiating multiple workers for the same queue — the
 * bootstrap loader reads `ProcessorMetadata.concurrency` and creates N
 * workers to fan out processing.
 */
declare class Worker {
    private readonly config;
    /**
     * Logger instance scoped to the Worker context.
     */
    private readonly logger;
    /** Whether the worker has been started and is currently polling. */
    private running;
    /** The scheduled next-tick handle, cleared on stop(). */
    private pendingTick;
    constructor(config: IWorkerConfig);
    /**
     * Begin polling for jobs.
     *
     * Emits {@link QUEUE_EVENTS.WORKER_STARTING} once on first call.
     * Subsequent calls are no-ops — safe to call after reloads.
     */
    start(): void;
    /**
     * Stop polling.
     *
     * Clears the next-tick timer. In-flight processors are not
     * interrupted — they complete naturally and the worker is marked
     * stopped. Emits {@link QUEUE_EVENTS.WORKER_STOPPING}.
     */
    stop(): void;
    /**
     * Schedule the next polling tick.
     *
     * Wraps `setTimeout` so we can cancel the next tick during `stop()`.
     * Errors thrown by the tick handler are caught and logged so the
     * polling loop never crashes silently.
     *
     * @param delayMs - Delay in milliseconds before the next tick fires.
     */
    private scheduleNext;
    /**
     * One polling iteration: pop a job, run it, schedule the next tick.
     *
     * Keeps the next-poll delay short when a job was processed (so the
     * worker drains the queue quickly) and longer when the queue was
     * empty.
     */
    private tick;
    /**
     * Execute a single job under timeout and retry policy.
     *
     * The try/catch/finally choreography mirrors Laravel's Worker:
     *
     * 1. Emit `JOB_PROCESSING`.
     * 2. Race `host.process(job)` vs. the timeout timer.
     * 3. On success: mark complete, emit `JOB_PROCESSED`.
     * 4. On timeout/failure: apply the retry policy, release or fail,
     *    emit `JOB_FAILED` + `JOB_RELEASED` accordingly.
     * 5. Always emit `JOB_ATTEMPTED` for instrumentation.
     */
    private handle;
    /**
     * Race the processor against a timeout timer.
     *
     * The timer rejects with a {@link TimeoutExceededError}, which the
     * caller differentiates from a regular throw for reporting purposes.
     */
    private runWithTimeout;
    /**
     * Apply the retry/backoff/fail policy after a processor throws.
     *
     * Flow:
     * - If the job has retries left, release it for another attempt with
     *   exponential backoff.
     * - Otherwise, mark it permanently failed and emit `JOB_FAILED` with a
     *   {@link MaxAttemptsExceededError}.
     * - If `failOnTimeout` is true, a timeout short-circuits to permanent
     *   failure even when retries remain.
     */
    private handleFailure;
    /**
     * Safely emit a queue event.
     *
     * Wraps `eventBus.emit` so a misbehaving subscriber can't crash the
     * worker loop. Subscriber errors are logged at warning level.
     *
     * @param event   - Lifecycle event name.
     * @param payload - Event payload (shape depends on the event).
     */
    private emit;
}

/**
 * `@OnJobEvent` decorator metadata shape.
 *
 * Internal — used only by the `@OnJobEvent` decorator and the
 * processor-subscribers loader. Not promoted to contracts because
 * no other package needs to type-check against it.
 *
 * @module @stackra/ts-queue/interfaces/on-job-event-metadata
 */

/**
 * Metadata stored on processor methods by the `@OnJobEvent` decorator.
 *
 * Multiple decorators on the same method accumulate into an array
 * under the metadata key, so the loader inspects the array shape and
 * subscribes each entry independently.
 */
interface IOnJobEventMetadata {
    /** Lifecycle event name. */
    event: QueueEventName;
    /**
     * Restrict the listener to events for a specific connection. When
     * omitted, the handler fires for jobs on any connection.
     */
    connection?: string;
}

/**
 * @fileoverview Reads `@Processor` and `@OnJobEvent` metadata.
 *
 * Tiny helper separated from the loader so each metadata lookup is
 * testable in isolation (and the loader stays readable).
 *
 * @module services/processor-metadata-accessor
 * @category Services
 */

/**
 * Reads queue decorator metadata from classes and methods.
 */
declare class ProcessorMetadataAccessor {
    /**
     * Return the `@Processor` metadata for a class, or `undefined` if the
     * class isn't decorated.
     */
    getProcessorMetadata(target: unknown): IProcessorMetadata | undefined;
    /**
     * Return the `@OnJobEvent` metadata array for a method, or `undefined`
     * if the method isn't decorated. Always returns an array — decorators
     * can stack, so a method may carry more than one listener entry.
     */
    getOnJobEventMetadata(target: unknown): IOnJobEventMetadata[] | undefined;
}

/**
 * @fileoverview Bootstrap loader.
 *
 * Mirrors the NestJS Bull `BullExplorer` + `BullRegistrar` pattern:
 *
 * 1. On `onApplicationBootstrap`, walk every DI provider.
 * 2. For each `@Processor`-decorated class, create one or more
 *    {@link Worker} instances (according to `concurrency`) and start them.
 * 3. For each `@OnJobEvent`-decorated method, subscribe it to the
 *    matching lifecycle event on the `EventManager` (via the event bus
 *    indirection so `@stackra/ts-events` stays optional).
 * 4. On `onApplicationShutdown`, stop every worker and remove every
 *    event listener.
 *
 * The loader uses the same "walk the container modules" trick
 * `EventSubscribersLoader` uses — it accesses the global application
 * exposed by `@stackra/ts-container` and iterates provider instances.
 *
 * @module services/processor-subscribers-loader
 * @category Services
 */

/** Minimal event manager contract — see `event-bus.service.ts`. */
interface EventManagerLike {
    on(event: string, listener: (...args: unknown[]) => void): unknown;
    off(event: string, listener: (...args: unknown[]) => void): unknown;
}
/**
 * Discovers `@Processor` classes + `@OnJobEvent` methods and wires them
 * to the running {@link QueueManager}.
 */
declare class ProcessorSubscribersLoader implements OnApplicationBootstrap, OnApplicationShutdown {
    private readonly manager;
    private readonly metadataAccessor;
    private readonly eventBus;
    private readonly events?;
    /** Every worker started by the loader — stopped on shutdown. */
    private readonly workers;
    /**
     * Logger instance scoped to the ProcessorSubscribersLoader context.
     */
    private readonly logger;
    /**
     * Event manager listeners the loader registered — removed on shutdown.
     * Stored as `[event, listener]` tuples so we can call `.off(...)`
     * symmetrically with the `.on(...)` we did at bootstrap.
     */
    private readonly listeners;
    constructor(manager: QueueManager, metadataAccessor: ProcessorMetadataAccessor, eventBus: QueueEventBus, events?: EventManagerLike | undefined);
    /**
     * Walk every DI provider, wire workers and event listeners.
     *
     * Called once at application bootstrap. The implementation:
     * 1. Resolves the global application via `getGlobalApplication()`.
     * 2. Iterates every container module's providers.
     * 3. For each `@Processor`-decorated instance, spawns workers and
     *    subscribes its `@OnJobEvent` methods to the lifecycle bus.
     *
     * This mirrors NestJS Bull's `BullExplorer` and stays opt-in — apps
     * that don't decorate any classes simply do nothing here.
     */
    onApplicationBootstrap(): Promise<void>;
    /**
     * Stop every worker and unsubscribe every event listener.
     *
     * Idempotent — safe to call multiple times during shutdown. Errors
     * inside individual stops are swallowed to ensure a partial failure
     * doesn't prevent the rest of the cleanup from running.
     */
    onApplicationShutdown(): Promise<void>;
    /**
     * Grab every provider instance from the running application.
     *
     * We don't want a hard dependency on the exact shape of the global app
     * — just the contract that it exposes `getContainer().getModules()`
     * returning a `Map<string, { providers: Map<string, { instance }> }>`.
     */
    private collectProviderInstances;
    /**
     * Create worker instances for a processor and start them.
     *
     * Throws if the class doesn't extend {@link WorkerHost} — this catches
     * the common misuse of decorating an arbitrary class.
     */
    private registerProcessor;
    /**
     * Subscribe every `@OnJobEvent` method on a processor to the bus.
     *
     * Walks the prototype's own property names looking for methods
     * annotated with `@OnJobEvent`, then binds each to the matching
     * lifecycle event name on the {@link EventManagerLike}.
     *
     * No-op when no `EventManager` is wired (the dependency is optional
     * so apps without `@stackra/ts-events` still bootstrap).
     *
     * @param instance - The processor instance to inspect.
     */
    private registerEventListeners;
    /**
     * Bind one method to one lifecycle event.
     *
     * Wraps the method invocation in a closure that:
     * - Filters payloads by `meta.connection` when set.
     * - Logs but does not rethrow exceptions thrown by the handler.
     * - Catches Promise rejections from async handlers.
     *
     * @param instance  - The processor instance owning the method.
     * @param methodKey - Property name of the method on the prototype.
     * @param meta      - Decorator metadata describing the event binding.
     */
    private subscribe;
}

/**
 * Memory connector.
 *
 * Resolves a `IMemoryQueueConnectionConfig` into a live
 * `MemoryConnection`. Used by `QueueModule.forRoot()` as one of the
 * default built-in connectors.
 *
 * @module @stackra/ts-queue/connectors/memory
 */

/**
 * Memory connector — wraps `MemoryConnection`.
 */
declare class MemoryConnector implements IQueueConnector {
    /**
     * Build a `MemoryConnection` from the supplied configuration.
     *
     * @param config - Driver-specific connection configuration.
     * @returns A ready-to-use memory connection.
     */
    connect(config: QueueConnectionConfig): Promise<IQueueConnection>;
}

/**
 * Sync connector.
 *
 * Resolves a `ISyncQueueConnectionConfig` into a live `SyncConnection`.
 *
 * @module @stackra/ts-queue/connectors/sync
 */

/**
 * Sync connector — wraps `SyncConnection`.
 */
declare class SyncConnector implements IQueueConnector {
    /**
     * Build a `SyncConnection` from the supplied configuration.
     *
     * @param config - Driver-specific connection configuration.
     * @returns A ready-to-use sync connection.
     */
    connect(config: QueueConnectionConfig): Promise<IQueueConnection>;
}

/**
 * Null connector.
 *
 * Resolves a `INullQueueConnectionConfig` into a live `NullConnection`.
 *
 * @module @stackra/ts-queue/connectors/null
 */

/**
 * Null connector — wraps `NullConnection`.
 */
declare class NullConnector implements IQueueConnector {
    /**
     * Build a `NullConnection` from the supplied configuration.
     *
     * @param config - Driver-specific connection configuration.
     * @returns A ready-to-use null connection.
     */
    connect(config: QueueConnectionConfig): Promise<IQueueConnection>;
}

/**
 * Local-storage connector.
 *
 * Resolves a `ILocalStorageQueueConnectionConfig` into a live
 * `LocalStorageConnection`.
 *
 * @module @stackra/ts-queue/connectors/local-storage
 */

/**
 * Local-storage connector — wraps `LocalStorageConnection`.
 */
declare class LocalStorageConnector implements IQueueConnector {
    /**
     * Build a `LocalStorageConnection` from the supplied configuration.
     *
     * @param config - Driver-specific connection configuration.
     * @returns A ready-to-use local-storage connection.
     */
    connect(config: QueueConnectionConfig): Promise<IQueueConnection>;
}

/**
 * IndexedDB connector.
 *
 * Resolves an `IIndexedDBQueueConnectionConfig` into a live
 * `IndexedDBConnection`.
 *
 * @module @stackra/ts-queue/connectors/indexeddb
 */

/**
 * IndexedDB connector — wraps `IndexedDBConnection`.
 */
declare class IndexedDBConnector implements IQueueConnector {
    /**
     * Build an `IndexedDBConnection` from the supplied configuration.
     *
     * @param config - Driver-specific connection configuration.
     * @returns A ready-to-use IndexedDB connection.
     */
    connect(config: QueueConnectionConfig): Promise<IQueueConnection>;
}

/**
 * Broadcast-channel connector.
 *
 * Resolves an `IBroadcastChannelQueueConnectionConfig` into a live
 * `BroadcastChannelConnection`. The optional `TabCoordinator` is
 * injected when `@stackra/ts-coordinator` is present in the host
 * application — when missing the connection runs as always-leader.
 *
 * @module @stackra/ts-queue/connectors/broadcast-channel
 */

/**
 * Broadcast-channel connector — wraps `BroadcastChannelConnection`.
 */
declare class BroadcastChannelConnector implements IQueueConnector {
    private readonly coordinator?;
    /**
     * @param coordinator - Optional shared `TabCoordinator` instance.
     *   When unavailable, the resulting connection acts as always-leader.
     */
    constructor(coordinator?: TabCoordinator | undefined);
    /**
     * Build a `BroadcastChannelConnection` from the supplied configuration.
     *
     * @param config - Driver-specific connection configuration.
     * @returns A ready-to-use broadcast-channel connection.
     */
    connect(config: QueueConnectionConfig): Promise<IQueueConnection>;
}

/**
 * QStash connector.
 *
 * Resolves an `IQStashQueueConnectionConfig` into a live
 * `QStashConnection`.
 *
 * @module @stackra/ts-queue/connectors/qstash
 */

/**
 * QStash connector — wraps `QStashConnection`.
 */
declare class QStashConnector implements IQueueConnector {
    /**
     * Build a `QStashConnection` from the supplied configuration.
     *
     * @param config - Driver-specific connection configuration.
     * @returns A ready-to-use QStash connection.
     */
    connect(config: QueueConnectionConfig): Promise<IQueueConnection>;
}

/**
 * Shared abstract base class for every driver.
 *
 * Reduces boilerplate in the individual drivers by implementing the
 * methods that always look the same: `later()` forwards to `push()`
 * with a delay, `bulk()` falls back to a loop, pause state lives in a
 * `Set`. Drivers still own the real work — persistence, pop semantics,
 * and anything storage-specific — but they no longer re-implement the
 * cosmetic wrappers.
 *
 * @module @stackra/ts-queue/connections/base
 */

/**
 * Abstract base every built-in driver extends.
 *
 * Subclasses must implement {@link push}, {@link pop}, {@link size},
 * {@link pendingSize}, {@link delayedSize}, {@link reservedSize},
 * {@link remove}, {@link release}, {@link fail}, {@link clear} and
 * {@link close}.
 */
declare abstract class BaseConnection implements IQueueConnection {
    readonly name: string;
    /**
     * Set of paused queue names.
     *
     * Drivers that need cross-tab pause coordination override
     * {@link pause}/{@link resume}/{@link isPaused} and persist the
     * state themselves (see the broadcast-channel driver).
     */
    protected readonly pausedQueues: Set<string>;
    /**
     * @param name - Connection name from module config.
     */
    constructor(name: string);
    abstract push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string>;
    abstract pop(queue?: string): Promise<IQueuedJob | null>;
    abstract size(queue?: string): Promise<number>;
    abstract pendingSize(queue?: string): Promise<number>;
    abstract delayedSize(queue?: string): Promise<number>;
    abstract reservedSize(queue?: string): Promise<number>;
    abstract remove(jobId: string): Promise<void>;
    abstract release(jobId: string, delayMs?: number): Promise<void>;
    abstract fail(jobId: string, reason: string): Promise<void>;
    abstract clear(queue?: string): Promise<void>;
    abstract close(): Promise<void>;
    /**
     * Delayed dispatch — forwards to `push()` with an additional `delayMs`.
     *
     * Drivers that persist delayed jobs differently from immediate ones
     * override this method.
     */
    later<T = unknown>(delayMs: number, name: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * Bulk dispatch — loops over `push()`.
     *
     * Drivers with native batch support (IndexedDB transactions, QStash
     * batch publish) override this for efficiency.
     */
    bulk<T = unknown>(jobs: Array<{
        name: string;
        data: T;
        options?: IJobOptions;
    }>): Promise<string[]>;
    /**
     * Pause a named queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     */
    pause(queue?: string): Promise<void>;
    /**
     * Resume a previously paused queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     */
    resume(queue?: string): Promise<void>;
    /**
     * Whether the named queue is currently paused.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns `true` when paused, `false` otherwise.
     */
    isPaused(queue?: string): Promise<boolean>;
}

/**
 * @fileoverview In-memory driver.
 *
 * Jobs live in a `Map<jobId, IQueuedJob>`. `pop()` scans for the oldest
 * pending job whose `availableAt <= now` and whose `queue` matches the
 * requested tube. Nothing is persisted — reloading the page wipes every
 * job. Use this driver for tests and for truly ephemeral work.
 *
 * @module connections/memory
 * @category Connections
 */

/**
 * In-memory queue driver.
 *
 * @example
 * ```typescript
 * const conn = new MemoryConnection('memory');
 * const id = await conn.push('send-email', { to: 'x@y.com' });
 * const job = await conn.pop();   // reserves job
 * await conn.remove(job!.id);     // marks complete
 * ```
 */
declare class MemoryConnection extends BaseConnection {
    /** Every job lives here — pending, reserved, delayed, or failed. */
    private readonly jobs;
    /**
     * Enqueue a new job onto the in-memory map.
     *
     * Honours `uniqueFor` semantics by scanning existing jobs for a
     * matching `uniqueId` that is still in-flight (Pending, Delayed,
     * Reserved, or Processing). When a duplicate is found the existing
     * job's id is returned so callers can treat the dedup outcome as a
     * "queued" success rather than as a hard error.
     *
     * @typeParam T - Type of the job payload.
     * @param name    - Application-level job name.
     * @param data    - The job payload.
     * @param options - Optional dispatch options (queue, delay, retries, …).
     * @returns The id of the enqueued (or deduplicated) job.
     */
    push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * Reserve and return the oldest available job on the given queue.
     *
     * Scans the map for jobs whose status is Pending or Delayed and whose
     * `availableAt` timestamp has elapsed. The oldest match (by
     * `createdAt`) is marked Reserved and returned to the caller — the
     * worker is then responsible for invoking the processor.
     *
     * Paused queues always return `null` so workers idle without errors.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The reserved job, or `null` if nothing is eligible.
     */
    pop(queue?: string): Promise<IQueuedJob | null>;
    /**
     * Total in-flight job count for a queue.
     *
     * Counts every job whose status is not terminal (i.e. neither
     * Completed nor Failed). Useful for "is the queue drained?" checks.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The number of in-flight jobs.
     */
    size(queue?: string): Promise<number>;
    /**
     * Number of jobs currently in the Pending state on a queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The pending job count.
     */
    pendingSize(queue?: string): Promise<number>;
    /**
     * Number of jobs currently in the Delayed state on a queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The delayed job count.
     */
    delayedSize(queue?: string): Promise<number>;
    /**
     * Number of jobs currently in the Reserved state on a queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The reserved job count.
     */
    reservedSize(queue?: string): Promise<number>;
    /**
     * Remove a single job from the in-memory map.
     *
     * Called by the worker after a job is processed successfully. No-op
     * when the id is unknown — the worker may have already removed it.
     *
     * @param jobId - The id of the job to remove.
     */
    remove(jobId: string): Promise<void>;
    /**
     * Release a reserved job back to the queue.
     *
     * Called by the worker after a job throws so a retry can be attempted.
     * The job's status is reset to Pending (or Delayed when `delayMs > 0`)
     * and `availableAt` is pushed forward by the requested delay.
     *
     * @param jobId   - The id of the job to release.
     * @param delayMs - Delay in milliseconds before the job becomes
     *   eligible again. `0` releases it immediately.
     */
    release(jobId: string, delayMs?: number): Promise<void>;
    /**
     * Mark a job permanently failed.
     *
     * Stores the failure reason and sets status to Failed. The job
     * remains in the map for inspection (consumers can list failures via
     * a custom predicate); test teardown should call `clear()` or
     * `close()` to drop them.
     *
     * @param jobId  - The id of the job to fail.
     * @param reason - Human-readable failure message.
     */
    fail(jobId: string, reason: string): Promise<void>;
    /**
     * Wipe every job belonging to a queue tube.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     */
    clear(queue?: string): Promise<void>;
    /**
     * Drop the entire job map and forget any paused queues.
     *
     * Used by tests and by `OnModuleDestroy` so the driver doesn't leak
     * state between runs or hot reloads.
     */
    close(): Promise<void>;
    /**
     * Count jobs on a queue that match the given status.
     *
     * @param queue  - Queue tube name to filter by.
     * @param status - The status to count.
     * @returns The number of matching jobs.
     */
    private countByStatus;
}

/**
 * SyncJobHandler — Type.
 *
 * @module @stackra/queue/types
 */
/**
 * Function that handles a single popped job.
 *
 * The sync driver calls this right after building the {@link IQueuedJob}.
 * When no resolver is registered, the driver discards the job and returns
 * — identical to the null driver.
 */
type SyncJobHandler = (job: IQueuedJob) => void | Promise<void>;

/**
 * @fileoverview Sync driver.
 *
 * Executes the processor inline during `push()` — no queue, no worker,
 * no delay. The Laravel `SyncQueue` equivalent.
 *
 * The sync driver relies on the caller wiring up a processor resolver at
 * construction time (typically the {@link QueueManager} during module
 * init). When a job is dispatched, the driver looks up the processor by
 * queue + name and invokes it synchronously. Errors propagate to the
 * caller of `push()` so tests can assert on them directly.
 *
 * @module connections/sync
 * @category Connections
 */

/**
 * In-process driver that runs processors inline.
 *
 * @example
 * ```typescript
 * const conn = new SyncConnection('sync');
 * conn.setHandler(async (job) => { logger.info('handled', job); });
 * await conn.push('ping', { x: 1 });
 * ```
 */
declare class SyncConnection extends BaseConnection {
    /** The synchronous processor resolver, if one has been registered. */
    private handler?;
    /**
     * Register the processor resolver.
     *
     * Called by the {@link QueueManager} during bootstrap with a function
     * that routes a job to its `@Processor`-decorated class instance. When
     * no handler is registered, `push()` silently discards jobs (mirroring
     * the null driver) so tests can run without wiring real processors.
     *
     * @param handler - The function that processes a single job.
     *
     * @example
     * ```typescript
     * conn.setHandler(async (job) => {
     *   logger.info('processing', job.name, job.data);
     * });
     * ```
     */
    setHandler(handler: SyncJobHandler): void;
    /**
     * Build a job and invoke the handler immediately.
     *
     * The sync driver bypasses any queueing — it constructs a {@link IQueuedJob}
     * and forwards it to the registered handler in the same call frame.
     * Errors thrown by the handler propagate to the caller, which is the
     * point: tests can assert on processor failures synchronously.
     *
     * If the queue is paused or no handler is registered, the job id is
     * returned without invoking the handler — preserving the same return
     * type contract as other drivers.
     *
     * @typeParam T - Type of the job payload.
     * @param name    - Application-level job name (e.g. `'send-email'`).
     * @param data    - The job payload.
     * @param options - Optional dispatch options (queue, delay, retries, …).
     * @returns The id of the dispatched job.
     *
     * @example
     * ```typescript
     * const id = await conn.push('ping', { x: 1 });
     * ```
     */
    push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * No-op pop.
     *
     * The sync driver runs jobs in `push()` — there is never anything to
     * pop. Always resolves to `null`.
     *
     * @returns Always `null`.
     */
    pop(): Promise<IQueuedJob | null>;
    /**
     * Always-zero job count.
     *
     * @returns `0`.
     */
    size(): Promise<number>;
    /**
     * Always-zero pending count.
     *
     * @returns `0`.
     */
    pendingSize(): Promise<number>;
    /**
     * Always-zero delayed count.
     *
     * @returns `0`.
     */
    delayedSize(): Promise<number>;
    /**
     * Always-zero reserved count.
     *
     * @returns `0`.
     */
    reservedSize(): Promise<number>;
    /**
     * No-op remove.
     *
     * Sync jobs never enter a queue, so removal is meaningless.
     */
    remove(): Promise<void>;
    /**
     * No-op release.
     *
     * Sync jobs never enter a queue, so release is meaningless.
     */
    release(): Promise<void>;
    /**
     * No-op fail.
     *
     * Failures propagate from the handler in `push()` — there is nothing
     * to record after the fact.
     */
    fail(): Promise<void>;
    /**
     * No-op clear.
     *
     * Sync jobs never enter a queue, so clearing is meaningless.
     */
    clear(): Promise<void>;
    /**
     * Tear down the driver.
     *
     * Drops the registered handler so the next `push()` degrades to the
     * null-driver behaviour. Used in tests to reset between cases.
     */
    close(): Promise<void>;
}

/**
 * @fileoverview Null driver.
 *
 * Silently discards every dispatched job. Useful as an SSR-safe fallback
 * or as an off-switch that keeps call sites working unchanged.
 *
 * @module connections/null
 * @category Connections
 */

/**
 * No-op driver. Every method is a cheap success.
 *
 * @example
 * ```typescript
 * QueueModule.forRoot({
 *   default: 'silent',
 *   connections: { silent: { driver: 'null' } },
 * });
 * ```
 */
declare class NullConnection extends BaseConnection {
    /**
     * Discard the job and return a freshly generated id.
     *
     * The null driver intentionally drops every payload — useful as an
     * SSR-safe fallback or as a "queue disabled" toggle that keeps call
     * sites working unchanged.
     *
     * @typeParam T - Type of the job payload (ignored).
     * @param _name    - Application-level job name (ignored).
     * @param _data    - The job payload (ignored).
     * @param _options - Optional dispatch options (ignored).
     * @returns A freshly generated job id.
     */
    push<T = unknown>(_name: string, _data: T, _options?: IJobOptions): Promise<string>;
    /**
     * Always-empty pop.
     *
     * The null driver never stores jobs, so there is never anything to
     * pop. Always resolves to `null`.
     *
     * @param _queue - Queue tube name (ignored).
     * @returns Always `null`.
     */
    pop(_queue?: string): Promise<IQueuedJob | null>;
    /**
     * Always-zero size.
     *
     * @returns `0`.
     */
    size(): Promise<number>;
    /**
     * Always-zero pending count.
     *
     * @returns `0`.
     */
    pendingSize(): Promise<number>;
    /**
     * Always-zero delayed count.
     *
     * @returns `0`.
     */
    delayedSize(): Promise<number>;
    /**
     * Always-zero reserved count.
     *
     * @returns `0`.
     */
    reservedSize(): Promise<number>;
    /**
     * No-op remove.
     *
     * Null jobs never exist in storage.
     */
    remove(): Promise<void>;
    /**
     * No-op release.
     *
     * Null jobs never exist in storage.
     */
    release(): Promise<void>;
    /**
     * No-op fail.
     *
     * Null jobs never exist in storage.
     */
    fail(): Promise<void>;
    /**
     * No-op clear.
     *
     * There is no storage to clear.
     */
    clear(): Promise<void>;
    /**
     * No-op close.
     *
     * The null driver holds no resources.
     */
    close(): Promise<void>;
}

/**
 * @fileoverview LocalStorage driver.
 *
 * Persists the job map for each queue tube in a single JSON blob under
 * `${prefix}queue:${connection}:${queue}`. Synchronous storage is fine
 * for small payloads — typically tracking pixels, autosave debouncers,
 * UI preferences.
 *
 * Per-queue isolation keeps the serialisation cost linear in the queue
 * being read/written, not the entire app's queue traffic.
 *
 * @module connections/local-storage
 * @category Connections
 */

/**
 * localStorage-backed queue driver.
 *
 * @example
 * ```typescript
 * const conn = new LocalStorageConnection('ls', { prefix: 'app:' });
 * await conn.push('track', { event: 'click' });
 * ```
 */
declare class LocalStorageConnection extends BaseConnection {
    private readonly prefix;
    /**
     * @param name   - Connection name from module config.
     * @param prefix - Optional key prefix. Keys become
     *   `${prefix}queue:${name}:${queueName}`.
     */
    constructor(name: string, prefix?: string);
    /**
     * Persist a job onto the queue's storage blob.
     *
     * Honours `uniqueFor` semantics by scanning the blob for matching
     * `uniqueId` jobs that are still in-flight. The whole blob is
     * rewritten via `JSON.stringify` — fine for small payloads, painful
     * for large ones (use the IndexedDB driver instead at scale).
     *
     * @typeParam T - Type of the job payload.
     * @param jobName - Application-level job name.
     * @param data    - The job payload.
     * @param options - Optional dispatch options (queue, delay, retries, …).
     * @returns The id of the persisted (or deduplicated) job.
     */
    push<T = unknown>(jobName: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * Reserve and return the oldest available job on the given queue.
     *
     * Reads the queue's blob, scans for jobs in Pending/Delayed state
     * whose `availableAt` has elapsed, picks the oldest by `createdAt`,
     * marks it Reserved, and rewrites the blob.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The reserved job, or `null` if nothing is eligible.
     */
    pop(queue?: string): Promise<IQueuedJob | null>;
    /**
     * Total in-flight job count for a queue.
     *
     * Counts jobs whose status is not terminal.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The number of in-flight jobs.
     */
    size(queue?: string): Promise<number>;
    /**
     * Number of jobs currently in the Pending state on a queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The pending job count.
     */
    pendingSize(queue?: string): Promise<number>;
    /**
     * Number of jobs currently in the Delayed state on a queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The delayed job count.
     */
    delayedSize(queue?: string): Promise<number>;
    /**
     * Number of jobs currently in the Reserved state on a queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The reserved job count.
     */
    reservedSize(queue?: string): Promise<number>;
    /**
     * Permanently remove a job from storage.
     *
     * Iterates every queue tube the prefix owns to find the job, since
     * the localStorage layout is per-queue rather than per-job.
     *
     * @param jobId - The id of the job to remove.
     */
    remove(jobId: string): Promise<void>;
    /**
     * Release a reserved job back to the queue.
     *
     * @param jobId   - The id of the job to release.
     * @param delayMs - Delay in milliseconds before the job becomes
     *   eligible again. `0` releases it immediately.
     */
    release(jobId: string, delayMs?: number): Promise<void>;
    /**
     * Mark a job permanently failed.
     *
     * @param jobId  - The id of the job to fail.
     * @param reason - Human-readable failure message.
     */
    fail(jobId: string, reason: string): Promise<void>;
    /**
     * Wipe every job belonging to a queue tube.
     *
     * Implemented as a single `localStorage.removeItem` call since the
     * blob is per-queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     */
    clear(queue?: string): Promise<void>;
    /**
     * Persist the paused flag for a queue tube.
     *
     * Overrides the in-memory base implementation so the pause survives
     * reloads.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     */
    pause(queue?: string): Promise<void>;
    /**
     * Clear the paused flag for a queue tube.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     */
    resume(queue?: string): Promise<void>;
    /**
     * Whether the queue tube is currently paused.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns `true` when paused, `false` otherwise.
     */
    isPaused(queue?: string): Promise<boolean>;
    /**
     * No-op close.
     *
     * localStorage doesn't expose an explicit handle to release.
     */
    close(): Promise<void>;
    /**
     * Build the storage key for a given queue tube.
     *
     * @param queue - Queue tube name.
     * @returns The fully prefixed localStorage key.
     */
    private keyFor;
    /**
     * Read the storage blob for a queue.
     *
     * Returns an empty blob when the key doesn't exist or the stored
     * JSON is corrupt — graceful degradation matters more here than
     * throwing because the user could have cleared their storage.
     *
     * @param queue - Queue tube name to read.
     * @returns The decoded storage blob (never `null`).
     */
    private read;
    /**
     * Persist a blob back to storage.
     *
     * Silently ignores quota errors. Callers can detect quota issues via
     * `size()` if they care to handle it; today none do.
     *
     * @param queue - Queue tube name to write.
     * @param blob  - Blob to persist.
     */
    private write;
    /**
     * Discover every queue tube this connection owns by prefix scan.
     *
     * Used by `remove()`, `release()`, and `fail()` since the
     * localStorage layout is per-queue and we don't know which tube a
     * given job id lives on.
     *
     * @returns Array of queue tube names found in storage.
     */
    private listQueues;
    /**
     * Count jobs on a queue tube that match the given status.
     *
     * @param queue  - Queue tube name to filter by.
     * @param status - The status to count.
     * @returns The number of matching jobs.
     */
    private countByStatus;
}

/**
 * @fileoverview IndexedDB driver.
 *
 * The browser default for offline-first queues. Uses one object store per
 * connection (`queue:${connection}`) with indexes on `queue`,
 * `availableAt`, and `status` so `pop()` and delayed-job scans stay fast
 * at tens of thousands of jobs.
 *
 * Pause state is stored in a second object store (`meta:${connection}`).
 * All operations go through transactions to keep the on-disk state
 * consistent even when multiple tabs touch the same database (combined
 * with the broadcast-channel driver for leader election).
 *
 * @module connections/indexeddb
 * @category Connections
 */

/**
 * IndexedDB-backed queue driver.
 *
 * @example
 * ```typescript
 * const conn = new IndexedDBConnection('idx', {
 *   dbName: 'app-queue',
 *   dbVersion: 1,
 *   prefix: 'app:',
 * });
 * await conn.push('sale.sync', saleData, { tries: 5 });
 * ```
 */
declare class IndexedDBConnection extends BaseConnection {
    private readonly dbName;
    private readonly version;
    private readonly prefix;
    /** Cached database handle, opened lazily on first operation. */
    private dbPromise;
    /**
     * @param name    - Connection name from module config.
     * @param dbName  - IndexedDB database name.
     * @param version - Schema version (bumped when the driver changes schema).
     * @param prefix  - Object store name prefix.
     */
    constructor(name: string, dbName?: string, version?: number, prefix?: string);
    /**
     * Persist a job to IndexedDB and return its id.
     *
     * Uniqueness check + insert run inside a single readwrite transaction
     * so concurrent pushes never both succeed when they shouldn't. The
     * `uniqueId` index defined in `onupgradeneeded` is used to discover
     * in-flight duplicates.
     *
     * @typeParam T - Type of the job payload.
     * @param jobName - Application-level job name.
     * @param data    - The job payload.
     * @param options - Optional dispatch options (queue, delay, retries, …).
     * @returns The id of the persisted (or deduplicated) job.
     */
    push<T = unknown>(jobName: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * Reserve and return the oldest available job on the given queue.
     *
     * Iterates over the `queue` index with a cursor, filters for eligible
     * jobs (Pending/Delayed with `availableAt <= now`), and selects the
     * oldest by `createdAt`. The chosen job is then marked Reserved in a
     * follow-up transaction so concurrent workers don't double-dispatch.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The reserved job, or `null` if nothing is eligible.
     */
    pop(queue?: string): Promise<IQueuedJob | null>;
    /**
     * Total in-flight job count for a queue.
     *
     * Counts jobs whose status is not terminal (i.e. neither Completed
     * nor Failed).
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The number of in-flight jobs.
     */
    size(queue?: string): Promise<number>;
    /**
     * Number of jobs currently in the Pending state on a queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The pending job count.
     */
    pendingSize(queue?: string): Promise<number>;
    /**
     * Number of jobs currently in the Delayed state on a queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The delayed job count.
     */
    delayedSize(queue?: string): Promise<number>;
    /**
     * Number of jobs currently in the Reserved state on a queue.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns The reserved job count.
     */
    reservedSize(queue?: string): Promise<number>;
    /**
     * Permanently remove a job from the object store.
     *
     * Called by the worker after successful processing. No-op when the
     * id is unknown.
     *
     * @param jobId - The id of the job to remove.
     */
    remove(jobId: string): Promise<void>;
    /**
     * Release a reserved job back to the queue.
     *
     * Resets the status to Pending (or Delayed when `delayMs > 0`) and
     * pushes `availableAt` forward so the worker won't pop it again
     * until the delay elapses.
     *
     * @param jobId   - The id of the job to release.
     * @param delayMs - Delay in milliseconds before the job becomes
     *   eligible again. `0` releases it immediately.
     */
    release(jobId: string, delayMs?: number): Promise<void>;
    /**
     * Mark a job permanently failed.
     *
     * Stores the failure reason and sets status to Failed. The record is
     * kept in the object store for inspection — call `clear()` to drop
     * failures explicitly.
     *
     * @param jobId  - The id of the job to fail.
     * @param reason - Human-readable failure message.
     */
    fail(jobId: string, reason: string): Promise<void>;
    /**
     * Wipe every job belonging to a queue tube.
     *
     * Iterates the `queue` index with a key cursor and deletes each
     * matching record in one readwrite transaction.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     */
    clear(queue?: string): Promise<void>;
    /**
     * Persist the paused flag for a queue tube.
     *
     * Overrides the in-memory base implementation so the pause survives
     * reloads and is visible to other tabs sharing this database.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     */
    pause(queue?: string): Promise<void>;
    /**
     * Clear the paused flag for a queue tube.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     */
    resume(queue?: string): Promise<void>;
    /**
     * Whether the queue tube is currently paused.
     *
     * Reads the persisted meta record. Defaults to `false` when no record
     * exists yet.
     *
     * @param queue - Queue tube name (defaults to `"default"`).
     * @returns `true` when paused, `false` otherwise.
     */
    isPaused(queue?: string): Promise<boolean>;
    /**
     * Close the IndexedDB handle and forget the lazy promise.
     *
     * Required during teardown so tests don't leak open databases and
     * hot-reload doesn't accumulate handles.
     */
    close(): Promise<void>;
    /**
     * Build the object store name for job records.
     *
     * @returns The fully qualified store name.
     */
    private jobsStore;
    /**
     * Build the object store name for per-queue metadata (pause flag).
     *
     * @returns The fully qualified store name.
     */
    private metaStore;
    /**
     * Open (or create) the IndexedDB database.
     *
     * Runs the schema migration in `onupgradeneeded` — which fires on a
     * fresh install or a version bump. We add the stores and indexes for
     * this connection if they don't already exist so multiple connections
     * can share the same database without stepping on each other.
     */
    private openDb;
    /**
     * Iterate jobs on the given queue and count those matching a predicate.
     */
    private countMatching;
    /**
     * Load a single job, apply the mutator, write it back. Runs in a single
     * readwrite transaction.
     */
    private mutate;
    /**
     * Read the meta record for a queue tube (pause state).
     *
     * @param queue - Queue tube name to read metadata for.
     * @returns The stored meta record, or `null` when no record exists.
     */
    private readMeta;
    /**
     * Write the meta record for a queue tube.
     *
     * Used by `pause()`/`resume()` to persist the paused flag.
     *
     * @param record - The meta record to persist.
     */
    private writeMeta;
}

/**
 * @fileoverview BroadcastChannel driver.
 *
 * Wraps the IndexedDB driver with cross-tab coordination via
 * `@stackra/ts-coordinator`. Only the leader tab (as determined by
 * the shared `TabCoordinator`) drains the queue — followers always
 * see an empty queue from `pop()`.
 *
 * This is the right default when multiple tabs may be open against the
 * same origin (POS multi-terminal, admin dashboards) and you want a
 * single source of truth for processing order.
 *
 * @module connections/broadcast-channel
 * @category Connections
 */

/**
 * Cross-tab queue driver. Inherits storage semantics from
 * {@link IndexedDBConnection} and delegates leader election to
 * `@stackra/ts-coordinator`'s {@link TabCoordinator}.
 *
 * Only the leader tab calls `pop()` into a real result — followers
 * always resolve `null` so their worker loops sit idle.
 *
 * @example
 * ```typescript
 * const conn = new BroadcastChannelConnection('bc', coordinator, {
 *   dbName: 'stackra-queue',
 * });
 * const job = await conn.pop(); // only returns a job when this tab is leader
 * ```
 */
declare class BroadcastChannelConnection extends IndexedDBConnection {
    /**
     * Reference to the shared TabCoordinator from `@stackra/ts-coordinator`.
     *
     * Used to determine if this tab is the leader and should drain the queue.
     * When `null` (SSR/non-browser), the connection behaves as always-leader.
     */
    private readonly coordinator;
    /**
     * @param name        - Connection name.
     * @param coordinator - The shared TabCoordinator instance (null for SSR).
     * @param dbName      - IndexedDB database name.
     * @param dbVersion   - IndexedDB schema version.
     * @param prefix      - Object store prefix.
     */
    constructor(name: string, coordinator: TabCoordinator | null, dbName?: string, dbVersion?: number, prefix?: string);
    /**
     * Push is leader-agnostic — any tab may enqueue. Storage is shared so
     * the leader sees it on its next `pop()`.
     */
    push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * Only the leader returns jobs from `pop()` — followers always see an
     * empty queue. This keeps work serialised without requiring locks on
     * the underlying IndexedDB transactions.
     *
     * Leadership is determined by the shared `TabCoordinator` from
     * `@stackra/ts-coordinator`.
     */
    pop(queue?: string): Promise<IQueuedJob | null>;
    /**
     * Teardown — release the IndexedDB handle.
     *
     * Leadership resignation is handled by the TabCoordinator's own
     * lifecycle — no need to manage it here.
     */
    close(): Promise<void>;
}

/**
 * @fileoverview Upstash QStash driver.
 *
 * Browser-producer, server-consumer. Publishes messages to QStash which
 * durably stores them and delivers to an HTTP destination you control.
 *
 * ## Two modes
 *
 * - **proxy** (default, recommended for public apps): POST to your own
 *   backend endpoint, which then calls the QStash API server-side using
 *   the real `QSTASH_TOKEN`. This keeps the token out of browser bundles.
 *   Use this for POS/ticketing/anywhere anonymous users can reach the app.
 *
 * - **direct**: Use `@upstash/qstash` from the browser with the token
 *   baked into the client. Only safe for internal tools behind SSO.
 *
 * ## Consumer-side methods are no-ops
 *
 * QStash is a producer-only broker from the browser's perspective — it
 * delivers to server endpoints, not back to the browser. The worker-side
 * methods (`pop`, `size`, `remove`, …) throw/no-op because they have no
 * meaning here. The backend that receives QStash callbacks is the
 * consumer; it uses its own persistence (database, Redis, etc.) rather
 * than this driver.
 *
 * @module connections/qstash
 * @category Connections
 */

/**
 * QStash-backed producer driver.
 *
 * @example
 * Proxy mode — publish via your own server.
 * ```typescript
 * const conn = new QStashConnection('qstash', {
 *   driver: 'qstash',
 *   mode: 'proxy',
 *   proxyUrl: '/api/queue/publish',
 *   defaultDestination: 'https://api.example.com/webhooks/queue',
 * });
 * await conn.push('send-reminder', { ticketId: '42' }, { delayMs: 3600_000 });
 * ```
 */
declare class QStashConnection extends BaseConnection {
    private readonly config;
    /**
     * Lazy-loaded QStash client for `mode: 'direct'`.
     *
     * `null` until the first `pushDirect()` call. The dynamic import of
     * `@upstash/qstash` is deferred so apps using proxy mode never ship
     * the QStash SDK.
     */
    private directClient;
    constructor(name: string, config: IQStashQueueConnectionConfig);
    /**
     * Publish a job to QStash.
     *
     * Routes to either {@link pushProxy} (recommended for public apps,
     * keeps the QStash token server-side) or {@link pushDirect} (for
     * trusted internal tools where the token can ship to browsers).
     *
     * @typeParam T - Type of the job payload.
     * @param jobName - Application-level job name.
     * @param data    - The job payload.
     * @param options - Optional dispatch options. `driverOptions.destination`
     *   may override the default destination at call time.
     * @returns The QStash messageId on success.
     * @throws {QueueDriverError} When the destination cannot be resolved or
     *   the HTTP/SDK call fails.
     */
    push<T = unknown>(jobName: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * Delayed dispatch — forwards to push() with the delay option set.
     *
     * QStash natively supports delayed delivery via the `Upstash-Delay`
     * header (proxy mode) or the `delay` option (direct mode); the driver
     * translates `delayMs` into the correct unit for each path.
     */
    later<T = unknown>(delayMs: number, jobName: string, data: T, options?: IJobOptions): Promise<string>;
    /**
     * No-op pop.
     *
     * QStash delivers to HTTP endpoints; the browser never polls it. The
     * server-side consumer uses its own persistence and is the actual
     * worker for these jobs.
     *
     * @param _queue - Queue tube name (ignored).
     * @returns Always `null`.
     */
    pop(_queue?: string): Promise<IQueuedJob | null>;
    /**
     * Always-zero size.
     *
     * Producer-only driver — the browser does not track in-flight jobs.
     *
     * @returns `0`.
     */
    size(): Promise<number>;
    /**
     * Always-zero pending count.
     *
     * @returns `0`.
     */
    pendingSize(): Promise<number>;
    /**
     * Always-zero delayed count.
     *
     * @returns `0`.
     */
    delayedSize(): Promise<number>;
    /**
     * Always-zero reserved count.
     *
     * @returns `0`.
     */
    reservedSize(): Promise<number>;
    /**
     * No-op remove.
     *
     * Lifecycle is owned by the server-side consumer that receives the
     * QStash callback.
     *
     * @param _jobId - Job id (ignored).
     */
    remove(_jobId: string): Promise<void>;
    /**
     * No-op release.
     *
     * Lifecycle is owned by the server-side consumer.
     *
     * @param _jobId - Job id (ignored).
     */
    release(_jobId: string): Promise<void>;
    /**
     * No-op fail.
     *
     * Lifecycle is owned by the server-side consumer.
     *
     * @param _jobId - Job id (ignored).
     */
    fail(_jobId: string): Promise<void>;
    /**
     * Unsupported clear.
     *
     * QStash doesn't expose a "clear queue" primitive over its HTTP API
     * for browsers — use the QStash console or the API server-side.
     *
     * @throws {QueueDriverError} Always.
     */
    clear(): Promise<void>;
    /**
     * Drop the lazy QStash client reference.
     *
     * Called on `OnModuleDestroy` so subsequent calls re-import the SDK
     * cleanly during hot reloads.
     */
    close(): Promise<void>;
    /**
     * Publish via the server proxy endpoint.
     *
     * The request body is intentionally declarative — the backend stays
     * in control of which QStash options are honoured and which are
     * ignored, so a compromised browser cannot request, say, unbounded
     * retry counts.
     */
    private pushProxy;
    /**
     * Publish directly from the browser using @upstash/qstash.
     *
     * Only safe in trusted contexts. The client is lazy-loaded so apps
     * using proxy mode don't ship the QStash SDK at all.
     */
    private pushDirect;
    /**
     * Lazy-load the QStash client. Throws a clear error if the peer dep
     * isn't installed so the failure mode is obvious.
     */
    private getDirectClient;
}

/**
 * @fileoverview `@Processor(queue, options?)` class decorator.
 *
 * Marks a class as the handler for a named queue tube. Stores metadata
 * that the bootstrap loader reads at `onApplicationBootstrap` to wire
 * `instance.process(job)` as the worker for `(connection, queue)`.
 *
 * The decorator composes `@Injectable()` internally by way of the
 * metadata key — the loader discovers the class through the decorator
 * discovery plugin (build-time) and resolves its instance through the DI
 * container at bootstrap.
 *
 * @module decorators/processor
 * @category Decorators
 */

/**
 * Mark a class as a queue processor.
 *
 * Accepts either a queue name string (shorthand) or an options object.
 * The class must extend {@link WorkerHost} — the loader uses the
 * `instanceof` check to catch misuse early.
 *
 * @example
 * Shorthand — queue name only.
 * ```typescript
 * @Processor('tracking')
 * class PixelProcessor extends WorkerHost {
 *   async process(job: QueuedJob) { ... }
 * }
 * ```
 *
 * @example
 * Full options.
 * ```typescript
 * @Processor({ queue: 'scans', connection: 'indexeddb', concurrency: 2 })
 * class ScanProcessor extends WorkerHost { ... }
 * ```
 */
declare function Processor(queue: string): ClassDecorator;
declare function Processor(options: IProcessorMetadata): ClassDecorator;

/**
 * @fileoverview `@OnJobEvent(event, connection?)` method decorator.
 *
 * Marks a method as a lifecycle event listener on a processor class.
 * Accumulates metadata using `updateMetadata` so a single method can
 * carry multiple `@OnJobEvent` decorators (rare, but supported — e.g.
 * one method handling both `processed` and `failed`).
 *
 * @module decorators/on-job-event
 * @category Decorators
 */

/**
 * Mark a method as a queue lifecycle event listener.
 *
 * @param event      - Queue event name from {@link QUEUE_EVENTS}.
 * @param connection - Optional connection name to scope the listener to.
 *
 * @example
 * ```typescript
 * import { QUEUE_EVENTS } from '@stackra/contracts';
 *
 * @Processor('tracking')
 * class PixelProcessor extends WorkerHost {
 *   async process(job: QueuedJob) { ... }
 *
 *   @OnJobEvent(QUEUE_EVENTS.JOB_FAILED)
 *   onFailed(payload: { job: QueuedJob; error: Error }) {
 *     logger.warn(`Pixel job ${payload.job.id} failed`, payload.error);
 *   }
 * }
 * ```
 */
declare function OnJobEvent(event: QueueEventName, connection?: string): MethodDecorator;

/**
 * @fileoverview `@Job({...})` class decorator.
 *
 * Attaches default {@link JobOptions} to a job class. When the class is
 * dispatched via `JobDispatcher.dispatch(JobClass, data)`, the dispatcher
 * merges these defaults with per-dispatch overrides before enqueuing.
 *
 * Mirrors Laravel's PHP 8 attribute pattern (`#[Tries(3)]`, `#[Queue]`,
 * `#[Backoff]`) but consolidated into a single decorator since
 * TypeScript doesn't have a native concept of multi-attribute decoration
 * the way PHP does.
 *
 * Composes `@Injectable()` internally so consumers don't need to apply
 * both decorators — the job can be resolved from the DI container with
 * its declared dependencies.
 *
 * @module decorators/job
 * @category Decorators
 */

/**
 * Configure class-level defaults for a job.
 *
 * Marks the class as `@Injectable()` and attaches the provided
 * {@link IJobMetadata} via the `JOB_METADATA` key.
 *
 * @param options - Default dispatch options for every instance of this job.
 * @returns A class decorator that wires both `@Injectable()` and the
 *   job-metadata side-effect onto the target.
 *
 * @example
 * ```typescript
 * @Job({
 *   name: 'send-receipt',
 *   queue: 'emails',
 *   tries: 3,
 *   backoffMs: 2000,
 *   uniqueFor: 60_000,
 * })
 * class SendReceiptJob {
 *   constructor(
 *     @Inject(MAILER) private readonly mailer: Mailer,
 *     public readonly saleId: string,
 *   ) {}
 * }
 *
 * dispatcher.dispatch(SendReceiptJob, { saleId: '123' });
 * ```
 */
declare function Job(options: IJobMetadata): ClassDecorator;

/**
 * @fileoverview `@InjectQueue` parameter decorator.
 *
 * Resolves a queue **handle** bound to a specific `(connection, queue)`
 * pair. Mirrors NestJS Bull's `@InjectQueue(name)` — the caller gets a
 * thin typed wrapper around the underlying connection, pre-scoped to a
 * single queue tube.
 *
 * @module decorators/inject-queue
 * @category Decorators
 */
/**
 * Inject a queue handle scoped to `(connection, queue)`.
 *
 * @param queue      - Queue tube name (defaults to `"default"`).
 * @param connection - Connection name (defaults to `"default"`).
 *
 * @example
 * ```typescript
 * @Injectable()
 * class TrackingService {
 *   constructor(@InjectQueue('tracking') private readonly queue: QueueHandle) {}
 *
 *   fireEvent(data: unknown) {
 *     return this.queue.push('pixel.fireEvent', data, { uniqueFor: 60_000 });
 *   }
 * }
 * ```
 */
declare function InjectQueue(queue?: string, connection?: string): ParameterDecorator & PropertyDecorator;

/**
 * @fileoverview `@InjectQueueConnection` parameter decorator.
 *
 * Resolves a {@link QueueConnection} for a named connection from the DI
 * container. When called without arguments, resolves the default
 * connection via {@link DEFAULT_QUEUE_CONNECTION_TOKEN}.
 *
 * @module decorators/inject-queue-connection
 * @category Decorators
 */
/**
 * Inject the {@link QueueConnection} for the named driver.
 *
 * @param name - Connection name from module config. Defaults to the
 *   configured default when omitted.
 *
 * @example
 * ```typescript
 * @Injectable()
 * class OrderService {
 *   constructor(
 *     @InjectQueueConnection('indexeddb') private readonly queue: QueueConnection,
 *   ) {}
 * }
 * ```
 */
declare function InjectQueueConnection(name?: string): ParameterDecorator & PropertyDecorator;

/**
 * @fileoverview Decorator metadata keys.
 *
 * Centralised registry of every metadata key used by `@stackra/ts-queue`
 * decorators. DI tokens (`Symbol.for(...)`) live in `@stackra/contracts`,
 * and the per-connection / per-queue token factories live in
 * `@/utils/get-queue-connection-token.util` and
 * `@/utils/get-queue-token.util` — runtime helpers belong in `utils/`,
 * not `constants/`.
 *
 * @module constants/tokens
 * @category Constants
 */
/**
 * Metadata key for `@Processor(queueName, options?)` class decorators.
 *
 * Stored on the processor class via `@vivtel/metadata`. The
 * {@link ProcessorSubscribersLoader} reads this at bootstrap to bind the
 * class instance's `process()` method as a consumer of the named queue.
 */
declare const PROCESSOR_METADATA = "QUEUE_PROCESSOR_METADATA";
/**
 * Metadata key for `@OnJobEvent(eventName)` method decorators.
 *
 * Stored on the method function via `@vivtel/metadata`. The
 * {@link ProcessorSubscribersLoader} reads this at bootstrap and wires the
 * method to the appropriate lifecycle event on the `EventManager`.
 */
declare const ON_JOB_EVENT_METADATA = "QUEUE_ON_JOB_EVENT_METADATA";
/**
 * Metadata key for `@Job({ tries, backoff, timeout, ... })` class decorators.
 *
 * Stored on the job class. {@link JobDispatcher} merges this with the
 * per-push options when enqueueing so every dispatch inherits the class
 * defaults without callers repeating them.
 */
declare const JOB_METADATA = "QUEUE_JOB_METADATA";

/**
 * @fileoverview Base error class for the queue package.
 *
 * Every error thrown by `@stackra/ts-queue` extends {@link QueueError}.
 * Having one shared base makes `instanceof` checks easy for consumers:
 * `err instanceof QueueError` will be `true` for any error raised inside
 * the package regardless of the specific subclass.
 *
 * @module errors/queue
 * @category Errors
 */
/**
 * Base error class for all errors thrown by the queue package.
 *
 * Provides a typed `code` property for programmatic handling and captures
 * the stack trace at construction time so the thrower frame is visible in
 * logs instead of the base-class frame.
 *
 * @example
 * ```typescript
 * try {
 *   await queue.push('sale.sync', payload);
 * } catch (error: Error | any) {
 *   if (error instanceof QueueError) {
 *     logger.error('Queue error:', error.code, error.message);
 *   }
 * }
 * ```
 */
declare class QueueError extends Error {
    /** Error name for identification in logs and stack traces. */
    readonly name: string;
    /** Machine-readable error code for programmatic handling. */
    readonly code: string;
    /** Optional underlying cause that triggered this error. */
    readonly cause?: Error;
    /**
     * Create a new QueueError.
     *
     * @param message - Human-readable error message.
     * @param cause   - Optional underlying error that caused this failure.
     */
    constructor(message: string, cause?: Error);
}

/**
 * @fileoverview Queue driver error.
 *
 * Thrown whenever {@link QueueManager} cannot instantiate or resolve a
 * driver — unknown `driver` name, missing peer dependency, or invalid
 * per-connection configuration.
 *
 * @module errors/queue-driver
 * @category Errors
 */

/**
 * Error thrown when a queue driver cannot be resolved.
 *
 * Typical causes:
 * - The `driver` string in `QueueModuleOptions.connections` is not
 *   registered (typo, or `manager.extend()` was not called).
 * - A driver peer dependency is missing — for example, the `qstash`
 *   driver requires `@upstash/qstash` to be installed.
 * - Invalid driver-specific configuration — missing token, unreachable
 *   URL, unsupported option combination.
 *
 * @example
 * ```typescript
 * try {
 *   manager.connection('mysterious');
 * } catch (error: Error | any) {
 *   if (error instanceof QueueDriverError) {
 *     // Handle driver resolution failure
 *   }
 * }
 * ```
 */
declare class QueueDriverError extends QueueError {
    readonly name: string;
    readonly code: string;
}

/**
 * @fileoverview Max attempts exceeded error.
 *
 * Raised by the worker when a job has failed more times than its
 * configured `tries` budget allows. The worker marks the job as
 * {@link JobStatus.Failed} and emits {@link QUEUE_EVENTS.JOB_FAILED};
 * this error is the cause attached to that event.
 *
 * @module errors/max-attempts-exceeded
 * @category Errors
 */

/**
 * Error thrown when a job exceeds its retry budget.
 *
 * Mirrors Laravel's `MaxAttemptsExceededException`. Carries the job id
 * and attempt count so telemetry can differentiate retry-exhaustion from
 * first-attempt failures.
 *
 * @example
 * ```typescript
 * import { QUEUE_EVENTS } from '@stackra/contracts';
 *
 * @OnEvent(QUEUE_EVENTS.JOB_FAILED)
 * onFailed(payload: { job: QueuedJob; error: Error }): void {
 *   if (payload.error instanceof MaxAttemptsExceededError) {
 *     alerting.notifyOncall(payload.job.id);
 *   }
 * }
 * ```
 */
declare class MaxAttemptsExceededError extends QueueError {
    readonly jobId: string;
    readonly attempts: number;
    readonly name: string;
    readonly code: string;
    /**
     * Create a new MaxAttemptsExceededError.
     *
     * @param jobId    - The job identifier.
     * @param attempts - Total attempts that were made before giving up.
     * @param cause    - The underlying exception from the last attempt.
     */
    constructor(jobId: string, attempts: number, cause?: Error);
}

/**
 * @fileoverview Timeout exceeded error.
 *
 * Raised by the worker when a processor runs longer than the job's
 * `timeout` setting. What happens next depends on `failOnTimeout`: if
 * true, the job is marked {@link JobStatus.Failed}; otherwise it is
 * released back to the queue for retry.
 *
 * @module errors/timeout-exceeded
 * @category Errors
 */

/**
 * Error thrown when a job processor exceeds its configured timeout.
 *
 * Mirrors Laravel's `TimeoutExceededException`. Includes the job id and
 * elapsed milliseconds so instrumentation can track slow processors.
 *
 * @example
 * ```typescript
 * import { QUEUE_EVENTS } from '@stackra/contracts';
 *
 * @OnEvent(QUEUE_EVENTS.JOB_TIMED_OUT)
 * onTimedOut(payload: { job: QueuedJob; error: TimeoutExceededError }) {
 *   metrics.increment('queue.timeouts', { name: payload.job.name });
 * }
 * ```
 */
declare class TimeoutExceededError extends QueueError {
    readonly jobId: string;
    readonly elapsedMs: number;
    readonly timeoutMs: number;
    readonly name: string;
    readonly code: string;
    /**
     * Create a new TimeoutExceededError.
     *
     * @param jobId       - The job identifier.
     * @param elapsedMs   - How long the processor ran before being killed.
     * @param timeoutMs   - The configured timeout in milliseconds.
     */
    constructor(jobId: string, elapsedMs: number, timeoutMs: number);
}

/**
 * Module-options validation error.
 *
 * Thrown by `QueueModule.forRoot()` when the supplied configuration is
 * malformed (missing `default`, empty `connections`, mismatched
 * default key, ...). Surfaces immediately at bootstrap so the
 * developer sees the problem rather than a confusing runtime failure
 * on first dispatch.
 *
 * @module @stackra/ts-queue/errors/queue-module-options
 */

/**
 * Configuration validation error for the queue module.
 */
declare class QueueModuleOptionsError extends QueueError {
    readonly name: string;
    readonly code: string;
}

/**
 * @fileoverview Unique job id generator.
 *
 * Uses `crypto.randomUUID()` when available (modern browsers, Node 19+).
 * Falls back to a timestamp + random suffix for environments where the
 * Web Crypto API is not yet exposed. Both paths are sufficiently unique
 * for queue ids — collisions are vanishingly unlikely.
 *
 * @module utils/generate-job-id
 * @category Utils
 */
/**
 * Generate a unique identifier suitable for a {@link QueuedJob.id}.
 *
 * @returns A globally unique string (UUID v4 when available).
 *
 * @example
 * ```typescript
 * const id = generateJobId();
 * // "8f3c5e62-7f4a-4b09-9e1c-24eeab5f3c72"
 * ```
 */
declare function generateJobId(): string;

/**
 * @fileoverview Deduplication key helper.
 *
 * When a caller sets `JobOptions.uniqueFor` but does not provide an
 * explicit `uniqueId`, the dispatcher derives one from the job name and
 * a stable hash of the payload. Same payload in the uniqueness window →
 * same id → dedup.
 *
 * @module utils/compute-unique-id
 * @category Utils
 */
/**
 * Derive a stable deduplication id for a job name + payload pair.
 *
 * Uses FNV-1a 32-bit hashing — fast, zero-dependency, and sufficient for
 * the "is this the same dispatch" question a deduplication window asks.
 * Payloads are JSON-stringified with deterministic key ordering so that
 * property order does not affect the hash.
 *
 * @param name - The job name.
 * @param data - The payload. Must be JSON-serialisable.
 * @returns A stable string id like `"u_1a2b3c4d"`.
 */
declare function computeUniqueId(name: string, data: unknown): string;

/**
 * @fileoverview IQueuedJob factory.
 *
 * Every driver builds {@link IQueuedJob} instances the same way — consistent
 * defaults, timestamps, status, and id. Centralising the construction
 * here keeps drivers focused on storage and prevents subtle field drift
 * between implementations.
 *
 * @module utils/create-queued-job
 * @category Utils
 */

/**
 * Arguments for creating a queued job.
 */
interface CreateQueuedJobArgs<T> {
    /** Job name/type identifier. */
    name: string;
    /** Job payload data. */
    data: T;
    /** Connection name. */
    connection: string;
    /** Per-dispatch job options. */
    options?: IJobOptions;
    /** Worker-level defaults. */
    workerDefaults?: Partial<IWorkerOptions>;
}
/**
 * Build a fully-populated {@link IQueuedJob} from the dispatch arguments.
 *
 * Applies the precedence chain: per-dispatch `options` → `workerDefaults`
 * → hard-coded fallbacks. Fills in timestamps, a fresh id, the computed
 * `availableAt` for delayed jobs, and the deduplication id when
 * `uniqueFor` is set.
 *
 * @typeParam T - Payload type.
 * @param args - The inputs needed to build the job.
 * @returns A new {@link IQueuedJob}.
 */
declare function createQueuedJob<T>(args: CreateQueuedJobArgs<T>): IQueuedJob<T>;

/**
 * @fileoverview Exponential backoff helper.
 *
 * Centralises the retry delay formula so every driver and worker uses
 * the same policy. Mirrors Laravel's exponential backoff semantics.
 *
 * @module utils/compute-backoff
 * @category Utils
 */
/**
 * Compute the exponential backoff delay for a given attempt.
 *
 * Formula: `min(baseMs * 2^(attempt - 1), maxMs)`.
 *
 * @param attempt - 1-based attempt number (first retry is attempt = 2).
 * @param baseMs  - Base backoff in milliseconds.
 * @param maxMs   - Upper bound for the backoff in milliseconds.
 * @returns Backoff delay in milliseconds.
 *
 * @example
 * ```typescript
 * computeBackoff(1, 1000, 30_000); // 1000   (first retry)
 * computeBackoff(2, 1000, 30_000); // 2000
 * computeBackoff(5, 1000, 30_000); // 16_000
 * computeBackoff(6, 1000, 30_000); // 30_000 (clamped)
 * ```
 */
declare function computeBackoff(attempt: number, baseMs: number, maxMs: number): number;

/**
 * Queue connection token utility.
 *
 * Returns the DI injection token for a named queue connection (driver).
 * Used internally by `@InjectQueueConnection()` and by
 * `QueueModule.forRoot()` to register per-connection providers.
 *
 * @module @stackra/ts-queue/utils/get-queue-connection-token
 */
/**
 * Build the DI injection token for a named queue **connection** (driver).
 *
 * A connection is one row in `IQueueModuleOptions.connections` — it owns a
 * driver (memory, indexeddb, qstash, …) and its configuration. When a
 * consumer writes `@InjectQueueConnection('primary')`, the decorator
 * resolves the token returned here.
 *
 * @param name - The connection name from the module config.
 *   Defaults to `"default"` when omitted.
 * @returns A Symbol unique to the given connection name.
 *
 * @example
 * ```typescript
 * getQueueConnectionToken();            // Symbol.for('QUEUE_CONNECTION_default')
 * getQueueConnectionToken('indexeddb'); // Symbol.for('QUEUE_CONNECTION_indexeddb')
 * ```
 */
declare const getQueueConnectionToken: (name?: string) => symbol;

/**
 * Queue token utility.
 *
 * Returns the DI injection token for a named queue within a connection.
 * Used internally by `@InjectQueue()` and by `QueueModule.forRoot()` /
 * `forFeature()` to register per-queue handle providers.
 *
 * @module @stackra/ts-queue/utils/get-queue-token
 */
/**
 * Build the DI injection token for a named **queue** within a connection.
 *
 * A queue is a named tube of work on top of a connection. Laravel calls
 * this the `queue` argument to `push()`; BullMQ calls it a Queue name.
 * In our model, `@InjectQueue('scans')` returns a bound handle that
 * pushes/polls the `scans` queue on the configured default connection.
 *
 * The token name is the full `connection:queue` pair so the same queue
 * name can appear on different connections without collision.
 *
 * @param name       - The queue name (defaults to `"default"`).
 * @param connection - The connection name (defaults to `"default"`).
 * @returns A Symbol unique to the `connection:queue` pair.
 *
 * @example
 * ```typescript
 * getQueueToken('scans');              // Symbol.for('QUEUE_default:scans')
 * getQueueToken('scans', 'indexeddb'); // Symbol.for('QUEUE_indexeddb:scans')
 * ```
 */
declare const getQueueToken: (name?: string, connection?: string) => symbol;

export { BaseConnection, BroadcastChannelConnection, BroadcastChannelConnector, type IOnJobEventMetadata, type IWorkerConfig, IndexedDBConnection, IndexedDBConnector, InjectQueue, InjectQueueConnection, JOB_METADATA, Job, LocalStorageConnection, LocalStorageConnector, MaxAttemptsExceededError, MemoryConnection, MemoryConnector, NullConnection, NullConnector, ON_JOB_EVENT_METADATA, OnJobEvent, PROCESSOR_METADATA, Processor, ProcessorMetadataAccessor, ProcessorSubscribersLoader, QStashConnection, QStashConnector, QueueDriverError, QueueError, QueueEventBus, QueueHandle, QueueManager, QueueModule, QueueModuleOptionsError, SyncConnection, SyncConnector, type SyncJobHandler, TimeoutExceededError, Worker, WorkerHost, computeBackoff, computeUniqueId, createQueuedJob, generateJobId, getQueueConnectionToken, getQueueToken };
