'use strict';

require('reflect-metadata');
var tsContainer = require('@stackra/ts-container');
var tsLogger = require('@stackra/ts-logger');
var contracts = require('@stackra/contracts');
var tsSupport = require('@stackra/ts-support');
var metadata = require('@vivtel/metadata');

/**
 * @stackra/ts-queue v0.1.0
 * (c) 2026 [object Object]
 * @license MIT
 */
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});
var __decorateClass = (decorators, target, key, kind) => {
  var result = kind > 1 ? void 0 : kind ? __getOwnPropDesc(target, key) : target;
  for (var i = decorators.length - 1, decorator; i >= 0; i--)
    if (decorator = decorators[i])
      result = (decorator(result)) || result;
  return result;
};
var __decorateParam = (index, decorator) => (target, key) => decorator(target, key, index);

// src/errors/queue.error.ts
var QueueError = class extends Error {
  /**
   * Create a new QueueError.
   *
   * @param message - Human-readable error message.
   * @param cause   - Optional underlying error that caused this failure.
   */
  constructor(message, cause) {
    super(message);
    /** Error name for identification in logs and stack traces. */
    this.name = "QueueError";
    /** Machine-readable error code for programmatic handling. */
    this.code = "QUEUE_ERROR";
    this.cause = cause;
    if (typeof Error.captureStackTrace === "function") {
      Error.captureStackTrace(this, this.constructor);
    }
  }
};

// src/errors/queue-driver.error.ts
var QueueDriverError = class extends QueueError {
  constructor() {
    super(...arguments);
    this.name = "QueueDriverError";
    this.code = "QUEUE_DRIVER_ERROR";
  }
};

// src/services/queue-handle.service.ts
var QueueHandle = class {
  /**
   * @param connection - Underlying queue connection.
   * @param queue      - Queue tube name this handle is bound to.
   */
  constructor(connection, queue) {
    this.connection = connection;
    this.queue = queue;
  }
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
  async push(name, data, options) {
    return this.connection.push(name, data, { ...options, queue: this.queue });
  }
  /**
   * Dispatch a job with a delay.
   *
   * @param delayMs - Delay in milliseconds before the job becomes eligible.
   */
  async later(delayMs, name, data, options) {
    return this.connection.later(delayMs, name, data, { ...options, queue: this.queue });
  }
  /**
   * Bulk dispatch.
   *
   * @typeParam T - Payload type for every job.
   * @param jobs - Array of `{ name, data, options }` tuples.
   * @returns Array of dispatched job ids.
   */
  async bulk(jobs) {
    return this.connection.bulk(
      jobs.map((j) => ({
        ...j,
        options: { ...j.options, queue: this.queue }
      }))
    );
  }
  /**
   * Total in-flight job count for the bound queue.
   *
   * @returns The number of in-flight jobs.
   */
  size() {
    return this.connection.size(this.queue);
  }
  /**
   * Pending-only count.
   *
   * @returns The number of jobs in the Pending state.
   */
  pendingSize() {
    return this.connection.pendingSize(this.queue);
  }
  /**
   * Delayed-only count.
   *
   * @returns The number of jobs in the Delayed state.
   */
  delayedSize() {
    return this.connection.delayedSize(this.queue);
  }
  /**
   * Reserved-only count.
   *
   * @returns The number of jobs in the Reserved state.
   */
  reservedSize() {
    return this.connection.reservedSize(this.queue);
  }
  /**
   * Wipe every job on the bound queue tube.
   */
  clear() {
    return this.connection.clear(this.queue);
  }
  /**
   * Pause processing on the bound queue tube.
   */
  pause() {
    return this.connection.pause(this.queue);
  }
  /**
   * Resume processing on the bound queue tube.
   */
  resume() {
    return this.connection.resume(this.queue);
  }
  /**
   * Whether the bound queue tube is currently paused.
   *
   * @returns `true` when paused, `false` otherwise.
   */
  isPaused() {
    return this.connection.isPaused(this.queue);
  }
};

// src/services/queue-manager.service.ts
exports.QueueManager = class QueueManager extends tsSupport.MultipleInstanceManager {
  /**
   * @param config - Queue module configuration.
   */
  constructor(config) {
    super();
    this.config = config;
    /** Scoped logger. */
    this.logger = new tsLogger.Logger(exports.QueueManager.name);
    /**
     * Registered connectors keyed by driver name.
     *
     * Populated by `QueueModule.forRoot()` (built-ins) and
     * `QueueModule.forFeature()` (extensions).
     */
    this.connectors = /* @__PURE__ */ new Map();
    /**
     * Cached `(connection, queue)` handles. Handles are light, but
     * keeping the cache means the same instance is returned for the same
     * pair — which keeps tests stable when they patch handle methods.
     */
    this.handles = /* @__PURE__ */ new Map();
  }
  // ────────────────────────────────────────────────────────────────────
  // Lifecycle
  // ────────────────────────────────────────────────────────────────────
  /**
   * Validate the default connection name and warm it eagerly so config
   * errors surface at bootstrap rather than at the first dispatch.
   */
  async onModuleInit() {
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
    } catch (err) {
      this.logger.warn(
        `[QueueManager] Failed to warm default connection '${defaultName}': ${err.message}`
      );
    }
  }
  /**
   * Close every active connection on shutdown so the process can exit
   * cleanly. Errors per-connection are swallowed — partial cleanup
   * shouldn't block the rest.
   */
  async onModuleDestroy() {
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
  registerConnector(driver, connector) {
    this.connectors.set(driver, connector);
  }
  /**
   * List the driver names that have a registered connector.
   *
   * @returns Driver name array (e.g. `["memory", "indexeddb"]`).
   */
  getRegisteredDrivers() {
    return Array.from(this.connectors.keys());
  }
  // ────────────────────────────────────────────────────────────────────
  // MultipleInstanceManager contract
  // ────────────────────────────────────────────────────────────────────
  /** @inheritdoc */
  getDefaultInstance() {
    return this.config.default;
  }
  /** @inheritdoc */
  setDefaultInstance(name) {
    this.config.default = name;
  }
  /** @inheritdoc */
  getInstanceConfig(name) {
    const raw = this.config.connections[name];
    if (!raw) return void 0;
    return { ...raw, name };
  }
  /**
   * Sync driver creation is not supported — every queue driver requires
   * async setup (IndexedDB open, BroadcastChannel handshake, ...).
   *
   * @inheritdoc
   */
  createDriver(_driver, _config) {
    throw new QueueDriverError("QueueManager: connections are async; call connection() instead.");
  }
  /**
   * Resolve a connector by driver name and delegate to it.
   *
   * @inheritdoc
   */
  async createDriverAsync(driver, config) {
    const connector = this.connectors.get(driver);
    if (!connector) {
      throw new QueueDriverError(
        `Queue driver "${driver}" is not registered. Registered drivers: ${this.getRegisteredDrivers().join(", ") || "(none)"}.`
      );
    }
    return connector.connect(config);
  }
  // ────────────────────────────────────────────────────────────────────
  // Public API — connections & queues
  // ────────────────────────────────────────────────────────────────────
  /** @inheritdoc */
  async connection(name) {
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
  async queue(queue = "default", connection) {
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
  async disconnect(name) {
    const connectionName = name ?? this.config.default;
    if (!this.hasInstance(connectionName)) return;
    const conn = this.instance(connectionName);
    try {
      await conn.close();
    } catch (err) {
      this.logger.warn(
        `[QueueManager] Failed to close connection '${connectionName}': ${err.message}`
      );
    } finally {
      this.forgetInstance(connectionName);
      const prefix = `${connectionName}:`;
      for (const key of Array.from(this.handles.keys())) {
        if (key.startsWith(prefix)) this.handles.delete(key);
      }
    }
  }
  /** @inheritdoc */
  async disconnectAll() {
    const names = this.getResolvedInstances();
    await Promise.all(names.map((n) => this.disconnect(n)));
    this.purge();
  }
  // ────────────────────────────────────────────────────────────────────
  // Introspection
  // ────────────────────────────────────────────────────────────────────
  /** @inheritdoc */
  getConnectionNames() {
    return Object.keys(this.config.connections);
  }
  /** @inheritdoc */
  getDefaultConnectionName() {
    return this.config.default;
  }
  /** @inheritdoc */
  isConnectionActive(name) {
    return this.hasInstance(name ?? this.config.default);
  }
  /**
   * Whether the named connection is configured.
   *
   * @param name - Connection name.
   * @returns `true` when the name appears in module config.
   */
  hasConnection(name) {
    return name in this.config.connections;
  }
  /**
   * Currently active (resolved) connection names.
   *
   * @returns Array of connection names that have been resolved at
   *   least once and are currently cached.
   */
  getActiveConnectionNames() {
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
  getWorkerOptions() {
    const w = this.config.worker ?? {};
    return {
      tries: w.tries ?? 1,
      backoffMs: w.backoffMs ?? 1e3,
      maxBackoffMs: w.maxBackoffMs ?? 3e4,
      timeoutMs: w.timeoutMs ?? 3e4,
      pollIntervalMs: w.pollIntervalMs ?? 500,
      autoStart: w.autoStart ?? true,
      failOnTimeout: w.failOnTimeout ?? true
    };
  }
};
exports.QueueManager = __decorateClass([
  tsContainer.Injectable(),
  __decorateParam(0, tsContainer.Inject(contracts.QUEUE_CONFIG))
], exports.QueueManager);
exports.QueueEventBus = class QueueEventBus {
  constructor(events) {
    this.events = events;
  }
  /**
   * Emit a queue lifecycle event.
   *
   * @param event   - One of the {@link QUEUE_EVENTS} constants from
   *                  `@stackra/contracts`.
   * @param payload - Event payload (`{ job, error?, … }`).
   */
  emit(event, payload) {
    if (!this.events) return;
    try {
      this.events.emit(event, payload);
    } catch {
    }
  }
};
exports.QueueEventBus = __decorateClass([
  tsContainer.Injectable(),
  __decorateParam(0, tsContainer.Optional()),
  __decorateParam(0, tsContainer.Inject(contracts.EVENT_EMITTER))
], exports.QueueEventBus);

// src/constants/tokens.constant.ts
var PROCESSOR_METADATA = "QUEUE_PROCESSOR_METADATA";
var ON_JOB_EVENT_METADATA = "QUEUE_ON_JOB_EVENT_METADATA";
var JOB_METADATA = "QUEUE_JOB_METADATA";

// src/accessors/processor-metadata.accessor.ts
exports.ProcessorMetadataAccessor = class ProcessorMetadataAccessor {
  /**
   * Return the `@Processor` metadata for a class, or `undefined` if the
   * class isn't decorated.
   */
  getProcessorMetadata(target) {
    if (!target || typeof target !== "function") return void 0;
    return metadata.getMetadata(PROCESSOR_METADATA, target);
  }
  /**
   * Return the `@OnJobEvent` metadata array for a method, or `undefined`
   * if the method isn't decorated. Always returns an array — decorators
   * can stack, so a method may carry more than one listener entry.
   */
  getOnJobEventMetadata(target) {
    if (!target || typeof target !== "function" && typeof target !== "object") {
      return void 0;
    }
    const meta = metadata.getMetadata(
      ON_JOB_EVENT_METADATA,
      target
    );
    if (!meta) return void 0;
    return Array.isArray(meta) ? meta : [meta];
  }
};
exports.ProcessorMetadataAccessor = __decorateClass([
  tsContainer.Injectable()
], exports.ProcessorMetadataAccessor);

// src/hosts/worker-host.ts
var WorkerHost = class {
};

// src/utils/generate-job-id.util.ts
function generateJobId() {
  const g = globalThis;
  if (g.crypto?.randomUUID) {
    return g.crypto.randomUUID();
  }
  const ts = Date.now().toString(36);
  const rand = Math.random().toString(36).slice(2, 11);
  return `job_${ts}_${rand}`;
}

// src/utils/compute-unique-id.util.ts
function computeUniqueId(name, data) {
  const canonical = stableStringify(data);
  const h = fnv1a(`${name}:${canonical}`);
  return `u_${h.toString(16)}`;
}
function fnv1a(input) {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = hash * 16777619 >>> 0;
  }
  return hash;
}
function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  const obj = value;
  const keys = Object.keys(obj).sort();
  const pairs = keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`);
  return `{${pairs.join(",")}}`;
}

// src/utils/create-queued-job.util.ts
function createQueuedJob(args) {
  const { name, data, connection, options = {}, workerDefaults = {} } = args;
  const now = Date.now();
  const queue = options.queue ?? "default";
  const delayMs = options.delayMs ?? 0;
  const maxAttempts = options.tries ?? workerDefaults.tries ?? 1;
  const backoffMs = options.backoffMs ?? workerDefaults.backoffMs ?? 1e3;
  const timeoutMs = options.timeoutMs ?? workerDefaults.timeoutMs ?? 3e4;
  const uniqueId = options.uniqueFor !== void 0 ? options.uniqueId ?? computeUniqueId(name, data) : void 0;
  return {
    id: generateJobId(),
    name,
    data,
    queue,
    connection,
    status: delayMs > 0 ? contracts.JobStatus.Delayed : contracts.JobStatus.Pending,
    attempts: 0,
    maxAttempts,
    backoffMs,
    timeoutMs,
    availableAt: now + delayMs,
    createdAt: now,
    updatedAt: now,
    tags: options.tags ?? [],
    uniqueId,
    driverMeta: {}
  };
}

// src/connections/base.connection.ts
var BaseConnection = class {
  /**
   * @param name - Connection name from module config.
   */
  constructor(name) {
    this.name = name;
    /**
     * Set of paused queue names.
     *
     * Drivers that need cross-tab pause coordination override
     * {@link pause}/{@link resume}/{@link isPaused} and persist the
     * state themselves (see the broadcast-channel driver).
     */
    this.pausedQueues = /* @__PURE__ */ new Set();
  }
  // ────────────────────────────────────────────────────────────────────
  // Shared defaults
  // ────────────────────────────────────────────────────────────────────
  /**
   * Delayed dispatch — forwards to `push()` with an additional `delayMs`.
   *
   * Drivers that persist delayed jobs differently from immediate ones
   * override this method.
   */
  async later(delayMs, name, data, options) {
    return this.push(name, data, { ...options, delayMs });
  }
  /**
   * Bulk dispatch — loops over `push()`.
   *
   * Drivers with native batch support (IndexedDB transactions, QStash
   * batch publish) override this for efficiency.
   */
  async bulk(jobs) {
    const ids = [];
    for (const job of jobs) {
      ids.push(await this.push(job.name, job.data, job.options));
    }
    return ids;
  }
  /**
   * Pause a named queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  async pause(queue = "default") {
    this.pausedQueues.add(queue);
  }
  /**
   * Resume a previously paused queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  async resume(queue = "default") {
    this.pausedQueues.delete(queue);
  }
  /**
   * Whether the named queue is currently paused.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns `true` when paused, `false` otherwise.
   */
  async isPaused(queue = "default") {
    return this.pausedQueues.has(queue);
  }
};

// src/connections/sync.connection.ts
var SyncConnection = class extends BaseConnection {
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
  setHandler(handler) {
    this.handler = handler;
  }
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
  async push(name, data, options) {
    const job = createQueuedJob({
      name,
      data,
      connection: this.name,
      options
    });
    if (this.pausedQueues.has(job.queue)) return job.id;
    if (!this.handler) return job.id;
    job.status = contracts.JobStatus.Reserved;
    job.attempts = 1;
    await this.handler(job);
    return job.id;
  }
  /**
   * No-op pop.
   *
   * The sync driver runs jobs in `push()` — there is never anything to
   * pop. Always resolves to `null`.
   *
   * @returns Always `null`.
   */
  async pop() {
    return null;
  }
  /**
   * Always-zero job count.
   *
   * @returns `0`.
   */
  async size() {
    return 0;
  }
  /**
   * Always-zero pending count.
   *
   * @returns `0`.
   */
  async pendingSize() {
    return 0;
  }
  /**
   * Always-zero delayed count.
   *
   * @returns `0`.
   */
  async delayedSize() {
    return 0;
  }
  /**
   * Always-zero reserved count.
   *
   * @returns `0`.
   */
  async reservedSize() {
    return 0;
  }
  /**
   * No-op remove.
   *
   * Sync jobs never enter a queue, so removal is meaningless.
   */
  async remove() {
  }
  /**
   * No-op release.
   *
   * Sync jobs never enter a queue, so release is meaningless.
   */
  async release() {
  }
  /**
   * No-op fail.
   *
   * Failures propagate from the handler in `push()` — there is nothing
   * to record after the fact.
   */
  async fail() {
  }
  /**
   * No-op clear.
   *
   * Sync jobs never enter a queue, so clearing is meaningless.
   */
  async clear() {
  }
  /**
   * Tear down the driver.
   *
   * Drops the registered handler so the next `push()` degrades to the
   * null-driver behaviour. Used in tests to reset between cases.
   */
  async close() {
    this.handler = void 0;
  }
};

// src/errors/max-attempts-exceeded.error.ts
var MaxAttemptsExceededError = class extends QueueError {
  /**
   * Create a new MaxAttemptsExceededError.
   *
   * @param jobId    - The job identifier.
   * @param attempts - Total attempts that were made before giving up.
   * @param cause    - The underlying exception from the last attempt.
   */
  constructor(jobId, attempts, cause) {
    super(`Job [${jobId}] has been attempted too many times (${attempts} attempts).`, cause);
    this.jobId = jobId;
    this.attempts = attempts;
    this.name = "MaxAttemptsExceededError";
    this.code = "QUEUE_MAX_ATTEMPTS_EXCEEDED";
  }
};

// src/errors/timeout-exceeded.error.ts
var TimeoutExceededError = class extends QueueError {
  /**
   * Create a new TimeoutExceededError.
   *
   * @param jobId       - The job identifier.
   * @param elapsedMs   - How long the processor ran before being killed.
   * @param timeoutMs   - The configured timeout in milliseconds.
   */
  constructor(jobId, elapsedMs, timeoutMs) {
    super(`Job [${jobId}] exceeded its timeout after ${elapsedMs}ms (configured: ${timeoutMs}ms).`);
    this.jobId = jobId;
    this.elapsedMs = elapsedMs;
    this.timeoutMs = timeoutMs;
    this.name = "TimeoutExceededError";
    this.code = "QUEUE_TIMEOUT_EXCEEDED";
  }
};

// src/utils/compute-backoff.util.ts
function computeBackoff(attempt, baseMs, maxMs) {
  if (attempt <= 1) return baseMs;
  const exp = baseMs * Math.pow(2, attempt - 1);
  return Math.min(exp, maxMs);
}
var Worker = class _Worker {
  constructor(config) {
    this.config = config;
    /**
     * Logger instance scoped to the Worker context.
     */
    this.logger = new tsLogger.Logger(_Worker.name);
    /** Whether the worker has been started and is currently polling. */
    this.running = false;
    /** The scheduled next-tick handle, cleared on stop(). */
    this.pendingTick = null;
  }
  /**
   * Begin polling for jobs.
   *
   * Emits {@link QUEUE_EVENTS.WORKER_STARTING} once on first call.
   * Subsequent calls are no-ops — safe to call after reloads.
   */
  start() {
    if (this.running) return;
    this.running = true;
    this.emit(contracts.QUEUE_EVENTS.WORKER_STARTING, {
      connection: this.config.connection.name,
      queue: this.config.queue
    });
    this.scheduleNext(0);
  }
  /**
   * Stop polling.
   *
   * Clears the next-tick timer. In-flight processors are not
   * interrupted — they complete naturally and the worker is marked
   * stopped. Emits {@link QUEUE_EVENTS.WORKER_STOPPING}.
   */
  stop() {
    if (!this.running) return;
    this.running = false;
    if (this.pendingTick) {
      clearTimeout(this.pendingTick);
      this.pendingTick = null;
    }
    this.emit(contracts.QUEUE_EVENTS.WORKER_STOPPING, {
      connection: this.config.connection.name,
      queue: this.config.queue
    });
  }
  // ── Polling ──────────────────────────────────────────────────────────
  /**
   * Schedule the next polling tick.
   *
   * Wraps `setTimeout` so we can cancel the next tick during `stop()`.
   * Errors thrown by the tick handler are caught and logged so the
   * polling loop never crashes silently.
   *
   * @param delayMs - Delay in milliseconds before the next tick fires.
   */
  scheduleNext(delayMs) {
    if (!this.running) return;
    this.pendingTick = setTimeout(() => {
      this.tick().catch((err) => {
        this.logger.error(
          `[Worker:${this.config.connection.name}:${this.config.queue}] tick error: ${err.message}`
        );
        this.scheduleNext(this.config.options.pollIntervalMs);
      });
    }, delayMs);
  }
  /**
   * One polling iteration: pop a job, run it, schedule the next tick.
   *
   * Keeps the next-poll delay short when a job was processed (so the
   * worker drains the queue quickly) and longer when the queue was
   * empty.
   */
  async tick() {
    const job = await this.config.connection.pop(this.config.queue);
    if (!job) {
      this.scheduleNext(this.config.options.pollIntervalMs);
      return;
    }
    await this.handle(job);
    this.scheduleNext(0);
  }
  // ── Job handling ─────────────────────────────────────────────────────
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
  async handle(job) {
    this.emit(contracts.QUEUE_EVENTS.JOB_PROCESSING, { job });
    let error;
    let timedOut = false;
    try {
      await this.runWithTimeout(job);
      await this.config.connection.remove(job.id);
      this.emit(contracts.QUEUE_EVENTS.JOB_PROCESSED, { job });
    } catch (e) {
      error = e;
      timedOut = error instanceof TimeoutExceededError;
      if (timedOut) {
        this.emit(contracts.QUEUE_EVENTS.JOB_TIMED_OUT, { job, error });
      }
      await this.handleFailure(job, error, timedOut);
    } finally {
      this.emit(contracts.QUEUE_EVENTS.JOB_ATTEMPTED, {
        job,
        attempts: job.attempts,
        error
      });
    }
  }
  /**
   * Race the processor against a timeout timer.
   *
   * The timer rejects with a {@link TimeoutExceededError}, which the
   * caller differentiates from a regular throw for reporting purposes.
   */
  async runWithTimeout(job) {
    const timeoutMs = job.timeoutMs;
    const startedAt = Date.now();
    let timer = null;
    const timeoutPromise = new Promise((_, reject) => {
      timer = setTimeout(() => {
        reject(new TimeoutExceededError(job.id, Date.now() - startedAt, timeoutMs));
      }, timeoutMs);
    });
    try {
      await Promise.race([Promise.resolve(this.config.host.process(job)), timeoutPromise]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }
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
  async handleFailure(job, error, timedOut) {
    const policy = this.config.options;
    const failOnTimeout = policy.failOnTimeout;
    const attemptsLeft = job.maxAttempts - job.attempts;
    const shouldFailNow = timedOut && failOnTimeout || attemptsLeft <= 0;
    if (shouldFailNow) {
      const finalError = timedOut && failOnTimeout ? error : new MaxAttemptsExceededError(job.id, job.attempts, error);
      await this.config.connection.fail(job.id, finalError.message);
      this.emit(contracts.QUEUE_EVENTS.JOB_FAILED, {
        job: { ...job, status: contracts.JobStatus.Failed },
        error: finalError
      });
      return;
    }
    const delayMs = computeBackoff(job.attempts + 1, job.backoffMs, policy.maxBackoffMs);
    await this.config.connection.release(job.id, delayMs);
    this.emit(contracts.QUEUE_EVENTS.JOB_RELEASED, { job, delayMs, error });
  }
  /**
   * Safely emit a queue event.
   *
   * Wraps `eventBus.emit` so a misbehaving subscriber can't crash the
   * worker loop. Subscriber errors are logged at warning level.
   *
   * @param event   - Lifecycle event name.
   * @param payload - Event payload (shape depends on the event).
   */
  emit(event, payload) {
    try {
      this.config.eventBus.emit(event, payload);
    } catch (err) {
      this.logger.warn(`[Worker] Event bus emit failed for ${event}: ${err.message}`);
    }
  }
};
exports.ProcessorSubscribersLoader = class ProcessorSubscribersLoader {
  constructor(manager, metadataAccessor, eventBus, events) {
    this.manager = manager;
    this.metadataAccessor = metadataAccessor;
    this.eventBus = eventBus;
    this.events = events;
    /** Every worker started by the loader — stopped on shutdown. */
    this.workers = [];
    /**
     * Logger instance scoped to the ProcessorSubscribersLoader context.
     */
    this.logger = new tsLogger.Logger(exports.ProcessorSubscribersLoader.name);
    /**
     * Event manager listeners the loader registered — removed on shutdown.
     * Stored as `[event, listener]` tuples so we can call `.off(...)`
     * symmetrically with the `.on(...)` we did at bootstrap.
     */
    this.listeners = [];
  }
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
  async onApplicationBootstrap() {
    const providers = this.collectProviderInstances();
    if (!providers) return;
    for (const instance of providers) {
      const meta = this.metadataAccessor.getProcessorMetadata(
        instance.constructor
      );
      if (!meta) continue;
      await this.registerProcessor(instance, meta);
      this.registerEventListeners(instance);
    }
  }
  /**
   * Stop every worker and unsubscribe every event listener.
   *
   * Idempotent — safe to call multiple times during shutdown. Errors
   * inside individual stops are swallowed to ensure a partial failure
   * doesn't prevent the rest of the cleanup from running.
   */
  async onApplicationShutdown() {
    for (const worker of this.workers) {
      try {
        worker.stop();
      } catch {
      }
    }
    this.workers.length = 0;
    if (this.events) {
      for (const { event, listener } of this.listeners) {
        try {
          this.events.off(event, listener);
        } catch {
        }
      }
    }
    this.listeners.length = 0;
  }
  // ── Private helpers ──────────────────────────────────────────────────
  /**
   * Grab every provider instance from the running application.
   *
   * We don't want a hard dependency on the exact shape of the global app
   * — just the contract that it exposes `getContainer().getModules()`
   * returning a `Map<string, { providers: Map<string, { instance }> }>`.
   */
  collectProviderInstances() {
    try {
      const container = __require("@stackra/ts-container");
      const app = container.getGlobalApplication?.() ?? globalThis.__APP__;
      if (!app) return null;
      const out = [];
      const containerRef = app.getContainer();
      const modules = containerRef.getModules();
      for (const [, moduleRef] of modules) {
        for (const [, wrapper] of moduleRef.providers) {
          if (!wrapper.instance || wrapper.isAlias) continue;
          out.push(wrapper.instance);
        }
      }
      return out;
    } catch (err) {
      this.logger.warn(
        `[QueueLoader] Failed to access global application: ${err.message}`
      );
      return null;
    }
  }
  /**
   * Create worker instances for a processor and start them.
   *
   * Throws if the class doesn't extend {@link WorkerHost} — this catches
   * the common misuse of decorating an arbitrary class.
   */
  async registerProcessor(instance, meta) {
    if (!(instance instanceof WorkerHost)) {
      throw new QueueDriverError(
        `[QueueLoader] Processor class '${instance.constructor?.name}' must extend WorkerHost.`
      );
    }
    const connectionName = meta.connection ?? this.manager.getDefaultConnectionName();
    const connection = await this.manager.connection(connectionName);
    const options = this.manager.getWorkerOptions();
    if (connection instanceof SyncConnection) {
      connection.setHandler(async (job) => {
        await Promise.resolve(instance.process(job));
      });
      return;
    }
    const concurrency = Math.max(1, meta.concurrency ?? 1);
    for (let i = 0; i < concurrency; i++) {
      const worker = new Worker({
        connection,
        queue: meta.queue,
        host: instance,
        options,
        eventBus: this.eventBus
      });
      worker.start();
      this.workers.push(worker);
    }
  }
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
  registerEventListeners(instance) {
    if (!this.events) return;
    const proto = Object.getPrototypeOf(instance);
    if (!proto) return;
    for (const key of Object.getOwnPropertyNames(proto)) {
      if (key === "constructor") continue;
      const method = instance[key];
      if (typeof method !== "function") continue;
      const entries = this.metadataAccessor.getOnJobEventMetadata(method);
      if (!entries) continue;
      for (const entry of entries) {
        this.subscribe(instance, key, entry);
      }
    }
  }
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
  subscribe(instance, methodKey, meta) {
    if (!this.events) return;
    const listener = (...args) => {
      if (meta.connection) {
        const payload = args[0];
        if (payload?.job?.connection && payload.job.connection !== meta.connection) return;
      }
      try {
        const fn = instance[methodKey];
        const result = fn.call(instance, ...args);
        if (result && typeof result.then === "function") {
          result.catch(
            (err) => this.logger.error(`[QueueLoader] ${methodKey} handler threw: ${String(err)}`)
          );
        }
      } catch (err) {
        this.logger.error(`[QueueLoader] ${methodKey} handler threw: ${String(err)}`);
      }
    };
    this.events.on(meta.event, listener);
    this.listeners.push({ event: meta.event, listener });
  }
};
exports.ProcessorSubscribersLoader = __decorateClass([
  tsContainer.Injectable(),
  __decorateParam(3, tsContainer.Optional()),
  __decorateParam(3, tsContainer.Inject(contracts.EVENT_EMITTER))
], exports.ProcessorSubscribersLoader);
var MemoryConnection = class extends BaseConnection {
  constructor() {
    super(...arguments);
    /** Every job lives here — pending, reserved, delayed, or failed. */
    this.jobs = /* @__PURE__ */ new Map();
  }
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
  async push(name, data, options) {
    const job = createQueuedJob({ name, data, connection: this.name, options });
    if (job.uniqueId) {
      for (const existing of this.jobs.values()) {
        if (existing.uniqueId === job.uniqueId && existing.status !== contracts.JobStatus.Completed && existing.status !== contracts.JobStatus.Failed) {
          return existing.id;
        }
      }
    }
    this.jobs.set(job.id, job);
    return job.id;
  }
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
  async pop(queue = "default") {
    if (this.pausedQueues.has(queue)) return null;
    const now = Date.now();
    let next;
    for (const job of this.jobs.values()) {
      if (job.queue !== queue) continue;
      if (job.status !== contracts.JobStatus.Pending && job.status !== contracts.JobStatus.Delayed) continue;
      if (job.availableAt > now) continue;
      if (!next || job.createdAt < next.createdAt) {
        next = job;
      }
    }
    if (!next) return null;
    next.status = contracts.JobStatus.Reserved;
    next.attempts += 1;
    next.updatedAt = now;
    return next;
  }
  /**
   * Total in-flight job count for a queue.
   *
   * Counts every job whose status is not terminal (i.e. neither
   * Completed nor Failed). Useful for "is the queue drained?" checks.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The number of in-flight jobs.
   */
  async size(queue = "default") {
    let count = 0;
    for (const job of this.jobs.values()) {
      if (job.queue !== queue) continue;
      if (job.status === contracts.JobStatus.Completed || job.status === contracts.JobStatus.Failed) continue;
      count++;
    }
    return count;
  }
  /**
   * Number of jobs currently in the Pending state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The pending job count.
   */
  async pendingSize(queue = "default") {
    return this.countByStatus(queue, contracts.JobStatus.Pending);
  }
  /**
   * Number of jobs currently in the Delayed state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The delayed job count.
   */
  async delayedSize(queue = "default") {
    return this.countByStatus(queue, contracts.JobStatus.Delayed);
  }
  /**
   * Number of jobs currently in the Reserved state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The reserved job count.
   */
  async reservedSize(queue = "default") {
    return this.countByStatus(queue, contracts.JobStatus.Reserved);
  }
  /**
   * Remove a single job from the in-memory map.
   *
   * Called by the worker after a job is processed successfully. No-op
   * when the id is unknown — the worker may have already removed it.
   *
   * @param jobId - The id of the job to remove.
   */
  async remove(jobId) {
    this.jobs.delete(jobId);
  }
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
  async release(jobId, delayMs = 0) {
    const job = this.jobs.get(jobId);
    if (!job) return;
    job.status = delayMs > 0 ? contracts.JobStatus.Delayed : contracts.JobStatus.Pending;
    job.availableAt = Date.now() + delayMs;
    job.updatedAt = Date.now();
  }
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
  async fail(jobId, reason) {
    const job = this.jobs.get(jobId);
    if (!job) return;
    job.status = contracts.JobStatus.Failed;
    job.lastError = reason;
    job.updatedAt = Date.now();
  }
  /**
   * Wipe every job belonging to a queue tube.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  async clear(queue = "default") {
    for (const [id, job] of this.jobs) {
      if (job.queue === queue) this.jobs.delete(id);
    }
  }
  /**
   * Drop the entire job map and forget any paused queues.
   *
   * Used by tests and by `OnModuleDestroy` so the driver doesn't leak
   * state between runs or hot reloads.
   */
  async close() {
    this.jobs.clear();
    this.pausedQueues.clear();
  }
  // ── Internal helpers ───────────────────────────────────────────────────
  /**
   * Count jobs on a queue that match the given status.
   *
   * @param queue  - Queue tube name to filter by.
   * @param status - The status to count.
   * @returns The number of matching jobs.
   */
  countByStatus(queue, status) {
    let count = 0;
    for (const job of this.jobs.values()) {
      if (job.queue === queue && job.status === status) count++;
    }
    return count;
  }
};

// src/connectors/memory.connector.ts
exports.MemoryConnector = class MemoryConnector {
  /**
   * Build a `MemoryConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use memory connection.
   */
  async connect(config) {
    if (config.driver !== "memory") {
      throw new Error(`MemoryConnector received non-memory driver: ${config.driver}`);
    }
    const name = config.name ?? "memory";
    return new MemoryConnection(name);
  }
};
exports.MemoryConnector = __decorateClass([
  tsContainer.Injectable()
], exports.MemoryConnector);
exports.SyncConnector = class SyncConnector {
  /**
   * Build a `SyncConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use sync connection.
   */
  async connect(config) {
    if (config.driver !== "sync") {
      throw new Error(`SyncConnector received non-sync driver: ${config.driver}`);
    }
    const name = config.name ?? "sync";
    return new SyncConnection(name);
  }
};
exports.SyncConnector = __decorateClass([
  tsContainer.Injectable()
], exports.SyncConnector);

// src/connections/null.connection.ts
var NullConnection = class extends BaseConnection {
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
  async push(_name, _data, _options) {
    return generateJobId();
  }
  /**
   * Always-empty pop.
   *
   * The null driver never stores jobs, so there is never anything to
   * pop. Always resolves to `null`.
   *
   * @param _queue - Queue tube name (ignored).
   * @returns Always `null`.
   */
  async pop(_queue) {
    return null;
  }
  /**
   * Always-zero size.
   *
   * @returns `0`.
   */
  async size() {
    return 0;
  }
  /**
   * Always-zero pending count.
   *
   * @returns `0`.
   */
  async pendingSize() {
    return 0;
  }
  /**
   * Always-zero delayed count.
   *
   * @returns `0`.
   */
  async delayedSize() {
    return 0;
  }
  /**
   * Always-zero reserved count.
   *
   * @returns `0`.
   */
  async reservedSize() {
    return 0;
  }
  /**
   * No-op remove.
   *
   * Null jobs never exist in storage.
   */
  async remove() {
  }
  /**
   * No-op release.
   *
   * Null jobs never exist in storage.
   */
  async release() {
  }
  /**
   * No-op fail.
   *
   * Null jobs never exist in storage.
   */
  async fail() {
  }
  /**
   * No-op clear.
   *
   * There is no storage to clear.
   */
  async clear() {
  }
  /**
   * No-op close.
   *
   * The null driver holds no resources.
   */
  async close() {
  }
};

// src/connectors/null.connector.ts
exports.NullConnector = class NullConnector {
  /**
   * Build a `NullConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use null connection.
   */
  async connect(config) {
    if (config.driver !== "null") {
      throw new Error(`NullConnector received non-null driver: ${config.driver}`);
    }
    const name = config.name ?? "null";
    return new NullConnection(name);
  }
};
exports.NullConnector = __decorateClass([
  tsContainer.Injectable()
], exports.NullConnector);
var LocalStorageConnection = class extends BaseConnection {
  /**
   * @param name   - Connection name from module config.
   * @param prefix - Optional key prefix. Keys become
   *   `${prefix}queue:${name}:${queueName}`.
   */
  constructor(name, prefix = "") {
    super(name);
    this.prefix = prefix;
  }
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
  async push(jobName, data, options) {
    const job = createQueuedJob({ name: jobName, data, connection: this.name, options });
    const blob = this.read(job.queue);
    if (job.uniqueId) {
      const existing = blob.jobs.find(
        (j) => j.uniqueId === job.uniqueId && j.status !== contracts.JobStatus.Completed && j.status !== contracts.JobStatus.Failed
      );
      if (existing) return existing.id;
    }
    blob.jobs.push(job);
    this.write(job.queue, blob);
    return job.id;
  }
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
  async pop(queue = "default") {
    const blob = this.read(queue);
    if (blob.paused) return null;
    const now = Date.now();
    let chosen;
    for (const job of blob.jobs) {
      const isEligible = (job.status === contracts.JobStatus.Pending || job.status === contracts.JobStatus.Delayed) && job.availableAt <= now;
      if (!isEligible) continue;
      if (!chosen || job.createdAt < chosen.createdAt) chosen = job;
    }
    if (!chosen) return null;
    chosen.status = contracts.JobStatus.Reserved;
    chosen.attempts += 1;
    chosen.updatedAt = now;
    this.write(queue, blob);
    return chosen;
  }
  /**
   * Total in-flight job count for a queue.
   *
   * Counts jobs whose status is not terminal.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The number of in-flight jobs.
   */
  async size(queue = "default") {
    const blob = this.read(queue);
    return blob.jobs.filter(
      (j) => j.status !== contracts.JobStatus.Completed && j.status !== contracts.JobStatus.Failed
    ).length;
  }
  /**
   * Number of jobs currently in the Pending state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The pending job count.
   */
  async pendingSize(queue = "default") {
    return this.countByStatus(queue, contracts.JobStatus.Pending);
  }
  /**
   * Number of jobs currently in the Delayed state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The delayed job count.
   */
  async delayedSize(queue = "default") {
    return this.countByStatus(queue, contracts.JobStatus.Delayed);
  }
  /**
   * Number of jobs currently in the Reserved state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The reserved job count.
   */
  async reservedSize(queue = "default") {
    return this.countByStatus(queue, contracts.JobStatus.Reserved);
  }
  /**
   * Permanently remove a job from storage.
   *
   * Iterates every queue tube the prefix owns to find the job, since
   * the localStorage layout is per-queue rather than per-job.
   *
   * @param jobId - The id of the job to remove.
   */
  async remove(jobId) {
    for (const queue of this.listQueues()) {
      const blob = this.read(queue);
      const next = blob.jobs.filter((j) => j.id !== jobId);
      if (next.length !== blob.jobs.length) {
        blob.jobs = next;
        this.write(queue, blob);
        return;
      }
    }
  }
  /**
   * Release a reserved job back to the queue.
   *
   * @param jobId   - The id of the job to release.
   * @param delayMs - Delay in milliseconds before the job becomes
   *   eligible again. `0` releases it immediately.
   */
  async release(jobId, delayMs = 0) {
    for (const queue of this.listQueues()) {
      const blob = this.read(queue);
      const job = blob.jobs.find((j) => j.id === jobId);
      if (!job) continue;
      job.status = delayMs > 0 ? contracts.JobStatus.Delayed : contracts.JobStatus.Pending;
      job.availableAt = Date.now() + delayMs;
      job.updatedAt = Date.now();
      this.write(queue, blob);
      return;
    }
  }
  /**
   * Mark a job permanently failed.
   *
   * @param jobId  - The id of the job to fail.
   * @param reason - Human-readable failure message.
   */
  async fail(jobId, reason) {
    for (const queue of this.listQueues()) {
      const blob = this.read(queue);
      const job = blob.jobs.find((j) => j.id === jobId);
      if (!job) continue;
      job.status = contracts.JobStatus.Failed;
      job.lastError = reason;
      job.updatedAt = Date.now();
      this.write(queue, blob);
      return;
    }
  }
  /**
   * Wipe every job belonging to a queue tube.
   *
   * Implemented as a single `localStorage.removeItem` call since the
   * blob is per-queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  async clear(queue = "default") {
    if (typeof localStorage === "undefined") return;
    localStorage.removeItem(this.keyFor(queue));
  }
  /**
   * Persist the paused flag for a queue tube.
   *
   * Overrides the in-memory base implementation so the pause survives
   * reloads.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  async pause(queue = "default") {
    const blob = this.read(queue);
    blob.paused = true;
    this.write(queue, blob);
  }
  /**
   * Clear the paused flag for a queue tube.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  async resume(queue = "default") {
    const blob = this.read(queue);
    blob.paused = false;
    this.write(queue, blob);
  }
  /**
   * Whether the queue tube is currently paused.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns `true` when paused, `false` otherwise.
   */
  async isPaused(queue = "default") {
    return this.read(queue).paused;
  }
  /**
   * No-op close.
   *
   * localStorage doesn't expose an explicit handle to release.
   */
  async close() {
  }
  // ── Storage helpers ────────────────────────────────────────────────────
  /**
   * Build the storage key for a given queue tube.
   *
   * @param queue - Queue tube name.
   * @returns The fully prefixed localStorage key.
   */
  keyFor(queue) {
    return `${this.prefix}queue:${this.name}:${queue}`;
  }
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
  read(queue) {
    if (typeof localStorage === "undefined") return { jobs: [], paused: false };
    try {
      const raw = localStorage.getItem(this.keyFor(queue));
      if (!raw) return { jobs: [], paused: false };
      const parsed = JSON.parse(raw);
      return {
        jobs: Array.isArray(parsed.jobs) ? parsed.jobs : [],
        paused: Boolean(parsed.paused)
      };
    } catch {
      return { jobs: [], paused: false };
    }
  }
  /**
   * Persist a blob back to storage.
   *
   * Silently ignores quota errors. Callers can detect quota issues via
   * `size()` if they care to handle it; today none do.
   *
   * @param queue - Queue tube name to write.
   * @param blob  - Blob to persist.
   */
  write(queue, blob) {
    if (typeof localStorage === "undefined") return;
    try {
      localStorage.setItem(this.keyFor(queue), JSON.stringify(blob));
    } catch {
    }
  }
  /**
   * Discover every queue tube this connection owns by prefix scan.
   *
   * Used by `remove()`, `release()`, and `fail()` since the
   * localStorage layout is per-queue and we don't know which tube a
   * given job id lives on.
   *
   * @returns Array of queue tube names found in storage.
   */
  listQueues() {
    if (typeof localStorage === "undefined") return [];
    const out = [];
    const prefix = `${this.prefix}queue:${this.name}:`;
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && tsSupport.Str.startsWith(key, prefix)) {
        out.push(key.slice(prefix.length));
      }
    }
    return out;
  }
  /**
   * Count jobs on a queue tube that match the given status.
   *
   * @param queue  - Queue tube name to filter by.
   * @param status - The status to count.
   * @returns The number of matching jobs.
   */
  countByStatus(queue, status) {
    return this.read(queue).jobs.filter((j) => j.status === status).length;
  }
};

// src/connectors/local-storage.connector.ts
exports.LocalStorageConnector = class LocalStorageConnector {
  /**
   * Build a `LocalStorageConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use local-storage connection.
   */
  async connect(config) {
    if (config.driver !== "local-storage") {
      throw new Error(`LocalStorageConnector received non-local-storage driver: ${config.driver}`);
    }
    const name = config.name ?? "local-storage";
    return new LocalStorageConnection(name, config.prefix ?? "");
  }
};
exports.LocalStorageConnector = __decorateClass([
  tsContainer.Injectable()
], exports.LocalStorageConnector);
var IndexedDBConnection = class extends BaseConnection {
  /**
   * @param name    - Connection name from module config.
   * @param dbName  - IndexedDB database name.
   * @param version - Schema version (bumped when the driver changes schema).
   * @param prefix  - Object store name prefix.
   */
  constructor(name, dbName = "stackra-queue", version = 1, prefix = "") {
    super(name);
    this.dbName = dbName;
    this.version = version;
    this.prefix = prefix;
    /** Cached database handle, opened lazily on first operation. */
    this.dbPromise = null;
  }
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
  async push(jobName, data, options) {
    const job = createQueuedJob({ name: jobName, data, connection: this.name, options });
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
      const store = tx.objectStore(this.jobsStore());
      const finish = (id) => resolve(id);
      tx.onerror = () => reject(tx.error);
      if (!job.uniqueId) {
        store.put(job);
        tx.oncomplete = () => finish(job.id);
        return;
      }
      const idx = store.index("uniqueId");
      const req = idx.openCursor(IDBKeyRange.only(job.uniqueId));
      let existingId = null;
      req.onsuccess = () => {
        const cursor = req.result;
        if (cursor) {
          const existing = cursor.value;
          if (existing.status !== contracts.JobStatus.Completed && existing.status !== contracts.JobStatus.Failed) {
            existingId = existing.id;
          }
          cursor.continue();
        } else if (existingId) {
          finish(existingId);
        } else {
          store.put(job);
          tx.oncomplete = () => finish(job.id);
        }
      };
    });
  }
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
  async pop(queue = "default") {
    if (await this.isPaused(queue)) return null;
    const db = await this.openDb();
    const now = Date.now();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
      const store = tx.objectStore(this.jobsStore());
      const idx = store.index("queue");
      const req = idx.openCursor(IDBKeyRange.only(queue));
      let chosen = null;
      req.onsuccess = () => {
        const cursor = req.result;
        if (!cursor) return;
        const job = cursor.value;
        const eligible = (job.status === contracts.JobStatus.Pending || job.status === contracts.JobStatus.Delayed) && job.availableAt <= now;
        if (eligible && (!chosen || job.createdAt < chosen.createdAt)) {
          chosen = job;
        }
        cursor.continue();
      };
      tx.onerror = () => reject(tx.error);
      tx.oncomplete = () => {
        if (!chosen) return resolve(null);
        const tx2 = db.transaction([this.jobsStore()], "readwrite");
        const store2 = tx2.objectStore(this.jobsStore());
        const reserved = {
          ...chosen,
          status: contracts.JobStatus.Reserved,
          attempts: chosen.attempts + 1,
          updatedAt: Date.now()
        };
        store2.put(reserved);
        tx2.oncomplete = () => resolve(reserved);
        tx2.onerror = () => reject(tx2.error);
      };
    });
  }
  /**
   * Total in-flight job count for a queue.
   *
   * Counts jobs whose status is not terminal (i.e. neither Completed
   * nor Failed).
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The number of in-flight jobs.
   */
  async size(queue = "default") {
    return this.countMatching(
      queue,
      (j) => j.status !== contracts.JobStatus.Completed && j.status !== contracts.JobStatus.Failed
    );
  }
  /**
   * Number of jobs currently in the Pending state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The pending job count.
   */
  async pendingSize(queue = "default") {
    return this.countMatching(queue, (j) => j.status === contracts.JobStatus.Pending);
  }
  /**
   * Number of jobs currently in the Delayed state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The delayed job count.
   */
  async delayedSize(queue = "default") {
    return this.countMatching(queue, (j) => j.status === contracts.JobStatus.Delayed);
  }
  /**
   * Number of jobs currently in the Reserved state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The reserved job count.
   */
  async reservedSize(queue = "default") {
    return this.countMatching(queue, (j) => j.status === contracts.JobStatus.Reserved);
  }
  /**
   * Permanently remove a job from the object store.
   *
   * Called by the worker after successful processing. No-op when the
   * id is unknown.
   *
   * @param jobId - The id of the job to remove.
   */
  async remove(jobId) {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
      tx.objectStore(this.jobsStore()).delete(jobId);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }
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
  async release(jobId, delayMs = 0) {
    return this.mutate(jobId, (job) => {
      job.status = delayMs > 0 ? contracts.JobStatus.Delayed : contracts.JobStatus.Pending;
      job.availableAt = Date.now() + delayMs;
      job.updatedAt = Date.now();
    });
  }
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
  async fail(jobId, reason) {
    return this.mutate(jobId, (job) => {
      job.status = contracts.JobStatus.Failed;
      job.lastError = reason;
      job.updatedAt = Date.now();
    });
  }
  /**
   * Wipe every job belonging to a queue tube.
   *
   * Iterates the `queue` index with a key cursor and deletes each
   * matching record in one readwrite transaction.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  async clear(queue = "default") {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
      const store = tx.objectStore(this.jobsStore());
      const idx = store.index("queue");
      const req = idx.openKeyCursor(IDBKeyRange.only(queue));
      req.onsuccess = () => {
        const cursor = req.result;
        if (cursor) {
          store.delete(cursor.primaryKey);
          cursor.continue();
        }
      };
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }
  /**
   * Persist the paused flag for a queue tube.
   *
   * Overrides the in-memory base implementation so the pause survives
   * reloads and is visible to other tabs sharing this database.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  async pause(queue = "default") {
    await this.writeMeta({ queue, paused: true });
  }
  /**
   * Clear the paused flag for a queue tube.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  async resume(queue = "default") {
    await this.writeMeta({ queue, paused: false });
  }
  /**
   * Whether the queue tube is currently paused.
   *
   * Reads the persisted meta record. Defaults to `false` when no record
   * exists yet.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns `true` when paused, `false` otherwise.
   */
  async isPaused(queue = "default") {
    const record = await this.readMeta(queue);
    return record?.paused ?? false;
  }
  /**
   * Close the IndexedDB handle and forget the lazy promise.
   *
   * Required during teardown so tests don't leak open databases and
   * hot-reload doesn't accumulate handles.
   */
  async close() {
    if (!this.dbPromise) return;
    const db = await this.dbPromise;
    db.close();
    this.dbPromise = null;
  }
  // ── Private: store names & db lifecycle ───────────────────────────────
  /**
   * Build the object store name for job records.
   *
   * @returns The fully qualified store name.
   */
  jobsStore() {
    return `${this.prefix}jobs:${this.name}`;
  }
  /**
   * Build the object store name for per-queue metadata (pause flag).
   *
   * @returns The fully qualified store name.
   */
  metaStore() {
    return `${this.prefix}meta:${this.name}`;
  }
  /**
   * Open (or create) the IndexedDB database.
   *
   * Runs the schema migration in `onupgradeneeded` — which fires on a
   * fresh install or a version bump. We add the stores and indexes for
   * this connection if they don't already exist so multiple connections
   * can share the same database without stepping on each other.
   */
  openDb() {
    if (this.dbPromise) return this.dbPromise;
    this.dbPromise = new Promise((resolve, reject) => {
      if (typeof indexedDB === "undefined") {
        reject(new Error("IndexedDB is not available in this environment."));
        return;
      }
      const req = indexedDB.open(this.dbName, this.version);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(this.jobsStore())) {
          const store = db.createObjectStore(this.jobsStore(), { keyPath: "id" });
          store.createIndex("queue", "queue", { unique: false });
          store.createIndex("status", "status", { unique: false });
          store.createIndex("availableAt", "availableAt", { unique: false });
          store.createIndex("uniqueId", "uniqueId", { unique: false });
        }
        if (!db.objectStoreNames.contains(this.metaStore())) {
          db.createObjectStore(this.metaStore(), { keyPath: "queue" });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    return this.dbPromise;
  }
  // ── Private: record helpers ───────────────────────────────────────────
  /**
   * Iterate jobs on the given queue and count those matching a predicate.
   */
  async countMatching(queue, predicate) {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readonly");
      const idx = tx.objectStore(this.jobsStore()).index("queue");
      const req = idx.openCursor(IDBKeyRange.only(queue));
      let count = 0;
      req.onsuccess = () => {
        const cursor = req.result;
        if (!cursor) return;
        if (predicate(cursor.value)) count++;
        cursor.continue();
      };
      tx.oncomplete = () => resolve(count);
      tx.onerror = () => reject(tx.error);
    });
  }
  /**
   * Load a single job, apply the mutator, write it back. Runs in a single
   * readwrite transaction.
   */
  async mutate(jobId, mutator) {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
      const store = tx.objectStore(this.jobsStore());
      const getReq = store.get(jobId);
      getReq.onsuccess = () => {
        const job = getReq.result;
        if (!job) return;
        mutator(job);
        store.put(job);
      };
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }
  /**
   * Read the meta record for a queue tube (pause state).
   *
   * @param queue - Queue tube name to read metadata for.
   * @returns The stored meta record, or `null` when no record exists.
   */
  async readMeta(queue) {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([this.metaStore()], "readonly");
      const req = tx.objectStore(this.metaStore()).get(queue);
      req.onsuccess = () => resolve(req.result ?? null);
      req.onerror = () => reject(req.error);
    });
  }
  /**
   * Write the meta record for a queue tube.
   *
   * Used by `pause()`/`resume()` to persist the paused flag.
   *
   * @param record - The meta record to persist.
   */
  async writeMeta(record) {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([this.metaStore()], "readwrite");
      tx.objectStore(this.metaStore()).put(record);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }
};

// src/connectors/indexeddb.connector.ts
exports.IndexedDBConnector = class IndexedDBConnector {
  /**
   * Build an `IndexedDBConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use IndexedDB connection.
   */
  async connect(config) {
    if (config.driver !== "indexeddb") {
      throw new Error(`IndexedDBConnector received non-indexeddb driver: ${config.driver}`);
    }
    const name = config.name ?? "indexeddb";
    return new IndexedDBConnection(
      name,
      config.dbName ?? "stackra-queue",
      config.dbVersion ?? 1,
      config.prefix ?? ""
    );
  }
};
exports.IndexedDBConnector = __decorateClass([
  tsContainer.Injectable()
], exports.IndexedDBConnector);

// src/connections/broadcast-channel.connection.ts
var BroadcastChannelConnection = class extends IndexedDBConnection {
  /**
   * @param name        - Connection name.
   * @param coordinator - The shared TabCoordinator instance (null for SSR).
   * @param dbName      - IndexedDB database name.
   * @param dbVersion   - IndexedDB schema version.
   * @param prefix      - Object store prefix.
   */
  constructor(name, coordinator, dbName = "stackra-queue", dbVersion = 1, prefix = "") {
    super(name, dbName, dbVersion, prefix);
    this.coordinator = coordinator;
  }
  /**
   * Push is leader-agnostic — any tab may enqueue. Storage is shared so
   * the leader sees it on its next `pop()`.
   */
  async push(name, data, options) {
    return super.push(name, data, options);
  }
  /**
   * Only the leader returns jobs from `pop()` — followers always see an
   * empty queue. This keeps work serialised without requiring locks on
   * the underlying IndexedDB transactions.
   *
   * Leadership is determined by the shared `TabCoordinator` from
   * `@stackra/ts-coordinator`.
   */
  async pop(queue = "default") {
    if (this.coordinator && !this.coordinator.isLeader()) return null;
    return super.pop(queue);
  }
  /**
   * Teardown — release the IndexedDB handle.
   *
   * Leadership resignation is handled by the TabCoordinator's own
   * lifecycle — no need to manage it here.
   */
  async close() {
    await super.close();
  }
};

// src/connectors/broadcast-channel.connector.ts
exports.BroadcastChannelConnector = class BroadcastChannelConnector {
  /**
   * @param coordinator - Optional shared `TabCoordinator` instance.
   *   When unavailable, the resulting connection acts as always-leader.
   */
  constructor(coordinator) {
    this.coordinator = coordinator;
  }
  /**
   * Build a `BroadcastChannelConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use broadcast-channel connection.
   */
  async connect(config) {
    if (config.driver !== "broadcast-channel") {
      throw new Error(
        `BroadcastChannelConnector received non-broadcast-channel driver: ${config.driver}`
      );
    }
    const name = config.name ?? "broadcast-channel";
    return new BroadcastChannelConnection(
      name,
      this.coordinator ?? null,
      config.dbName ?? "stackra-queue",
      1,
      config.prefix ?? ""
    );
  }
};
exports.BroadcastChannelConnector = __decorateClass([
  tsContainer.Injectable(),
  __decorateParam(0, tsContainer.Optional()),
  __decorateParam(0, tsContainer.Inject(contracts.TAB_COORDINATOR))
], exports.BroadcastChannelConnector);

// src/connections/qstash.connection.ts
var QStashConnection = class extends BaseConnection {
  constructor(name, config) {
    super(name);
    this.config = config;
    /**
     * Lazy-loaded QStash client for `mode: 'direct'`.
     *
     * `null` until the first `pushDirect()` call. The dynamic import of
     * `@upstash/qstash` is deferred so apps using proxy mode never ship
     * the QStash SDK.
     */
    this.directClient = null;
  }
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
  async push(jobName, data, options) {
    const mode = this.config.mode ?? "proxy";
    const destination = options?.driverOptions?.destination ?? this.config.defaultDestination;
    if (!destination) {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] push() requires a destination. Pass 'defaultDestination' in config or 'driverOptions.destination' at push time.`
      );
    }
    return mode === "direct" ? this.pushDirect(jobName, data, destination, options) : this.pushProxy(jobName, data, destination, options);
  }
  /**
   * Delayed dispatch — forwards to push() with the delay option set.
   *
   * QStash natively supports delayed delivery via the `Upstash-Delay`
   * header (proxy mode) or the `delay` option (direct mode); the driver
   * translates `delayMs` into the correct unit for each path.
   */
  async later(delayMs, jobName, data, options) {
    return this.push(jobName, data, { ...options, delayMs });
  }
  // ── Producer-only contract stubs ──────────────────────────────────────
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
  async pop(_queue) {
    return null;
  }
  /**
   * Always-zero size.
   *
   * Producer-only driver — the browser does not track in-flight jobs.
   *
   * @returns `0`.
   */
  async size() {
    return 0;
  }
  /**
   * Always-zero pending count.
   *
   * @returns `0`.
   */
  async pendingSize() {
    return 0;
  }
  /**
   * Always-zero delayed count.
   *
   * @returns `0`.
   */
  async delayedSize() {
    return 0;
  }
  /**
   * Always-zero reserved count.
   *
   * @returns `0`.
   */
  async reservedSize() {
    return 0;
  }
  /**
   * No-op remove.
   *
   * Lifecycle is owned by the server-side consumer that receives the
   * QStash callback.
   *
   * @param _jobId - Job id (ignored).
   */
  async remove(_jobId) {
  }
  /**
   * No-op release.
   *
   * Lifecycle is owned by the server-side consumer.
   *
   * @param _jobId - Job id (ignored).
   */
  async release(_jobId) {
  }
  /**
   * No-op fail.
   *
   * Lifecycle is owned by the server-side consumer.
   *
   * @param _jobId - Job id (ignored).
   */
  async fail(_jobId) {
  }
  /**
   * Unsupported clear.
   *
   * QStash doesn't expose a "clear queue" primitive over its HTTP API
   * for browsers — use the QStash console or the API server-side.
   *
   * @throws {QueueDriverError} Always.
   */
  async clear() {
    throw new QueueDriverError(
      `[QStashConnection:${this.name}] clear() is not supported. Use the QStash console or API from your backend to clear messages.`
    );
  }
  /**
   * Drop the lazy QStash client reference.
   *
   * Called on `OnModuleDestroy` so subsequent calls re-import the SDK
   * cleanly during hot reloads.
   */
  async close() {
    this.directClient = null;
  }
  // ── Private mode implementations ──────────────────────────────────────
  /**
   * Publish via the server proxy endpoint.
   *
   * The request body is intentionally declarative — the backend stays
   * in control of which QStash options are honoured and which are
   * ignored, so a compromised browser cannot request, say, unbounded
   * retry counts.
   */
  async pushProxy(name, data, destination, options) {
    if (!this.config.proxyUrl) {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] mode: 'proxy' requires 'proxyUrl' in config.`
      );
    }
    const body = {
      name,
      data,
      destination,
      delaySec: options?.delayMs !== void 0 ? Math.floor(options.delayMs / 1e3) : void 0,
      retries: options?.tries,
      deduplicationId: options?.uniqueId,
      tags: options?.tags
    };
    const response = await fetch(this.config.proxyUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body)
    });
    if (!response.ok) {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] Proxy returned HTTP ${response.status}: ${await response.text()}`
      );
    }
    const result = await response.json();
    return result.messageId ?? result.scheduleId ?? "";
  }
  /**
   * Publish directly from the browser using @upstash/qstash.
   *
   * Only safe in trusted contexts. The client is lazy-loaded so apps
   * using proxy mode don't ship the QStash SDK at all.
   */
  async pushDirect(name, data, destination, options) {
    if (!this.config.token) {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] mode: 'direct' requires 'token' in config. Do NOT ship this token in a public client \u2014 prefer 'mode: proxy' instead.`
      );
    }
    const client = await this.getDirectClient();
    const delaySec = options?.delayMs !== void 0 ? Math.floor(options.delayMs / 1e3) : void 0;
    const res = await client.publishJSON({
      url: destination,
      body: { name, data },
      retries: options?.tries,
      delay: delaySec,
      deduplicationId: options?.uniqueId
    });
    return res.messageId;
  }
  /**
   * Lazy-load the QStash client. Throws a clear error if the peer dep
   * isn't installed so the failure mode is obvious.
   */
  async getDirectClient() {
    if (this.directClient) return this.directClient;
    try {
      const mod = await import(
        /* @vite-ignore */
        '@upstash/qstash'
      );
      const Client = mod.Client;
      this.directClient = new Client({
        token: this.config.token,
        baseUrl: this.config.baseUrl
      });
      return this.directClient;
    } catch {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] mode: 'direct' requires '@upstash/qstash' to be installed. Run 'pnpm add @upstash/qstash' or switch to 'mode: proxy'.`
      );
    }
  }
};

// src/connectors/qstash.connector.ts
exports.QStashConnector = class QStashConnector {
  /**
   * Build a `QStashConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use QStash connection.
   */
  async connect(config) {
    if (config.driver !== "qstash") {
      throw new Error(`QStashConnector received non-qstash driver: ${config.driver}`);
    }
    const name = config.name ?? "qstash";
    return new QStashConnection(name, {
      driver: "qstash",
      mode: config.mode ?? "proxy",
      ...config.token !== void 0 ? { token: config.token } : {},
      ...config.proxyUrl !== void 0 ? { proxyUrl: config.proxyUrl } : {},
      ...config.defaultDestination !== void 0 ? { defaultDestination: config.defaultDestination } : {},
      ...config.baseUrl !== void 0 ? { baseUrl: config.baseUrl } : {}
    });
  }
};
exports.QStashConnector = __decorateClass([
  tsContainer.Injectable()
], exports.QStashConnector);

// src/utils/get-queue-connection-token.util.ts
var getQueueConnectionToken = (name = "default") => /* @__PURE__ */ Symbol.for(`QUEUE_CONNECTION_${name}`);

// src/utils/get-queue-token.util.ts
var getQueueToken = (name = "default", connection = "default") => /* @__PURE__ */ Symbol.for(`QUEUE_${connection}:${name}`);

// src/errors/queue-module-options.error.ts
var QueueModuleOptionsError = class extends QueueError {
  constructor() {
    super(...arguments);
    this.name = "QueueModuleOptionsError";
    this.code = "QUEUE_MODULE_OPTIONS_ERROR";
  }
};

// src/queue.module.ts
var BUILT_IN_CONNECTORS = Object.freeze([
  { driver: "memory", type: exports.MemoryConnector },
  { driver: "sync", type: exports.SyncConnector },
  { driver: "null", type: exports.NullConnector },
  { driver: "local-storage", type: exports.LocalStorageConnector },
  { driver: "indexeddb", type: exports.IndexedDBConnector },
  { driver: "broadcast-channel", type: exports.BroadcastChannelConnector },
  { driver: "qstash", type: exports.QStashConnector }
]);
exports.QueueModule = class QueueModule {
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
  static forRoot(config) {
    exports.QueueModule.validate(config);
    const connectorRegistrations = exports.QueueModule.buildConnectorRegistrations(BUILT_IN_CONNECTORS);
    const connectionProviders = Object.keys(config.connections).map((connectionName) => ({
      provide: getQueueConnectionToken(connectionName),
      useFactory: async (manager) => manager.connection(connectionName),
      inject: [exports.QueueManager]
    }));
    const defaultConnectionProvider = {
      provide: contracts.DEFAULT_QUEUE_CONNECTION_TOKEN,
      useFactory: async (manager) => manager.connection(config.default),
      inject: [exports.QueueManager]
    };
    const defaultHandleProviders = Object.keys(config.connections).map((connectionName) => ({
      provide: getQueueToken("default", connectionName),
      useFactory: async (manager) => manager.queue("default", connectionName),
      inject: [exports.QueueManager]
    }));
    const defaultQueueHandleProvider = {
      provide: getQueueToken(),
      useFactory: async (manager) => manager.queue(),
      inject: [exports.QueueManager]
    };
    return {
      module: exports.QueueModule,
      global: true,
      providers: [
        // Config
        { provide: contracts.QUEUE_CONFIG, useValue: config },
        // Manager
        exports.QueueManager,
        { provide: contracts.QUEUE_MANAGER, useExisting: exports.QueueManager },
        // Built-in connectors and their auto-registration side-effects.
        ...connectorRegistrations.providers,
        // Connection-level providers
        defaultConnectionProvider,
        ...connectionProviders,
        // Default queue handle providers
        defaultQueueHandleProvider,
        ...defaultHandleProviders,
        // Bootstrap infrastructure
        exports.QueueEventBus,
        exports.ProcessorMetadataAccessor,
        exports.ProcessorSubscribersLoader
      ],
      exports: [
        contracts.QUEUE_CONFIG,
        exports.QueueManager,
        contracts.QUEUE_MANAGER,
        contracts.DEFAULT_QUEUE_CONNECTION_TOKEN,
        ...connectionProviders.map((p) => p.provide),
        ...defaultHandleProviders.map((p) => p.provide),
        getQueueToken(),
        exports.QueueEventBus
      ]
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
  static forRootAsync(options) {
    if (!options.useFactory) {
      exports.QueueModule.logger.warn("[QueueModule] forRootAsync requires useFactory.");
      return { module: exports.QueueModule, providers: [], exports: [] };
    }
    const connectorRegistrations = exports.QueueModule.buildConnectorRegistrations(BUILT_IN_CONNECTORS);
    return {
      module: exports.QueueModule,
      global: true,
      imports: options.imports ?? [],
      providers: [
        {
          provide: contracts.QUEUE_CONFIG,
          useFactory: options.useFactory,
          inject: options.inject ?? []
        },
        exports.QueueManager,
        { provide: contracts.QUEUE_MANAGER, useExisting: exports.QueueManager },
        ...connectorRegistrations.providers,
        exports.QueueEventBus,
        exports.ProcessorMetadataAccessor,
        exports.ProcessorSubscribersLoader
      ],
      exports: [contracts.QUEUE_CONFIG, exports.QueueManager, contracts.QUEUE_MANAGER, exports.QueueEventBus]
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
  static forFeature(driver, connectorType) {
    const registrationToken = /* @__PURE__ */ Symbol.for(`QUEUE_CONNECTOR_REGISTRATION:${driver}`);
    return {
      module: exports.QueueModule,
      providers: [
        connectorType,
        {
          provide: registrationToken,
          useFactory: (manager, connector) => {
            manager.registerConnector(driver, connector);
            return null;
          },
          inject: [exports.QueueManager, connectorType]
        }
      ],
      exports: [connectorType]
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
  static forFeatureQueues(queues) {
    const providers = queues.map(({ queue, connection }) => ({
      provide: getQueueToken(queue, connection ?? "default"),
      useFactory: async (manager) => manager.queue(queue, connection),
      inject: [exports.QueueManager]
    }));
    return {
      module: exports.QueueModule,
      providers,
      exports: providers.map((p) => p.provide)
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
  static buildConnectorRegistrations(connectors) {
    const providers = [];
    for (const { driver, type } of connectors) {
      const registrationToken = /* @__PURE__ */ Symbol.for(`QUEUE_CONNECTOR_REGISTRATION:${driver}`);
      providers.push(type);
      providers.push({
        provide: registrationToken,
        useFactory: (manager, connector) => {
          manager.registerConnector(driver, connector);
          return null;
        },
        inject: [exports.QueueManager, type]
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
  static validate(config) {
    if (!config) {
      throw new QueueModuleOptionsError("[QueueModule] forRoot() requires a configuration object.");
    }
    if (!config.default) {
      throw new QueueModuleOptionsError("[QueueModule] config.default is required.");
    }
    if (!config.connections || Object.keys(config.connections).length === 0) {
      throw new QueueModuleOptionsError(
        "[QueueModule] config.connections must define at least one entry."
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
};
/** Scoped logger for module-level diagnostics. */
exports.QueueModule.logger = new tsLogger.Logger(exports.QueueModule.name);
exports.QueueModule = __decorateClass([
  tsContainer.Global(),
  tsContainer.Module({})
], exports.QueueModule);
function Processor(arg) {
  const options = typeof arg === "string" ? { queue: arg } : arg;
  return (target) => {
    tsContainer.Injectable()(target);
    metadata.defineMetadata(PROCESSOR_METADATA, options, target);
  };
}
function OnJobEvent(event, connection) {
  return (_target, _key, descriptor) => {
    metadata.updateMetadata(
      ON_JOB_EVENT_METADATA,
      [],
      (items) => [...items, { event, connection }],
      descriptor.value
    );
    return descriptor;
  };
}
function Job(options) {
  const injectableDecorator = tsContainer.Injectable();
  return (target) => {
    injectableDecorator(target);
    metadata.defineMetadata(JOB_METADATA, options, target);
  };
}
function InjectQueue(queue, connection) {
  return tsContainer.Inject(getQueueToken(queue, connection));
}
function InjectQueueConnection(name) {
  const token = name ? getQueueConnectionToken(name) : contracts.DEFAULT_QUEUE_CONNECTION_TOKEN;
  return tsContainer.Inject(token);
}

exports.BaseConnection = BaseConnection;
exports.BroadcastChannelConnection = BroadcastChannelConnection;
exports.IndexedDBConnection = IndexedDBConnection;
exports.InjectQueue = InjectQueue;
exports.InjectQueueConnection = InjectQueueConnection;
exports.JOB_METADATA = JOB_METADATA;
exports.Job = Job;
exports.LocalStorageConnection = LocalStorageConnection;
exports.MaxAttemptsExceededError = MaxAttemptsExceededError;
exports.MemoryConnection = MemoryConnection;
exports.NullConnection = NullConnection;
exports.ON_JOB_EVENT_METADATA = ON_JOB_EVENT_METADATA;
exports.OnJobEvent = OnJobEvent;
exports.PROCESSOR_METADATA = PROCESSOR_METADATA;
exports.Processor = Processor;
exports.QStashConnection = QStashConnection;
exports.QueueDriverError = QueueDriverError;
exports.QueueError = QueueError;
exports.QueueHandle = QueueHandle;
exports.QueueModuleOptionsError = QueueModuleOptionsError;
exports.SyncConnection = SyncConnection;
exports.TimeoutExceededError = TimeoutExceededError;
exports.Worker = Worker;
exports.WorkerHost = WorkerHost;
exports.computeBackoff = computeBackoff;
exports.computeUniqueId = computeUniqueId;
exports.createQueuedJob = createQueuedJob;
exports.generateJobId = generateJobId;
exports.getQueueConnectionToken = getQueueConnectionToken;
exports.getQueueToken = getQueueToken;
