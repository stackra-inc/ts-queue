'use strict';

require('reflect-metadata');
var tsContainer = require('@stackra/ts-container');
var contracts = require('@stackra/contracts');
var tsSupport = require('@stackra/ts-support');
var tsLogger = require('@stackra/ts-logger');
var metadata = require('@vivtel/metadata');
var react = require('@stackra/ts-container/react');

/**
 * @stackra/ts-queue v0.1.0
 * (c) 2026 [object Object]
 * @license MIT
 */
var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });
var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});

// src/constants/tokens.constant.ts
var getQueueConnectionToken = /* @__PURE__ */ __name((name = "default") => /* @__PURE__ */ Symbol.for(`QUEUE_CONNECTION_${name}`), "getQueueConnectionToken");
var getQueueToken = /* @__PURE__ */ __name((name = "default", connection = "default") => /* @__PURE__ */ Symbol.for(`QUEUE_${connection}:${name}`), "getQueueToken");
var PROCESSOR_METADATA = "QUEUE_PROCESSOR_METADATA";
var ON_JOB_EVENT_METADATA = "QUEUE_ON_JOB_EVENT_METADATA";
var JOB_METADATA = "QUEUE_JOB_METADATA";

// src/errors/queue.error.ts
var QueueError = class extends Error {
  static {
    __name(this, "QueueError");
  }
  /** Error name for identification in logs and stack traces. */
  name = "QueueError";
  /** Machine-readable error code for programmatic handling. */
  code = "QUEUE_ERROR";
  /** Optional underlying cause that triggered this error. */
  cause;
  /**
  * Create a new QueueError.
  *
  * @param message - Human-readable error message.
  * @param cause   - Optional underlying error that caused this failure.
  */
  constructor(message, cause) {
    super(message);
    this.cause = cause;
    if (typeof Error.captureStackTrace === "function") {
      Error.captureStackTrace(this, this.constructor);
    }
  }
};

// src/errors/queue-driver.error.ts
var QueueDriverError = class extends QueueError {
  static {
    __name(this, "QueueDriverError");
  }
  name = "QueueDriverError";
  code = "QUEUE_DRIVER_ERROR";
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
__name(generateJobId, "generateJobId");

// src/utils/compute-unique-id.util.ts
function computeUniqueId(name, data) {
  const canonical = stableStringify(data);
  const h = fnv1a(`${name}:${canonical}`);
  return `u_${h.toString(16)}`;
}
__name(computeUniqueId, "computeUniqueId");
function fnv1a(input) {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = hash * 16777619 >>> 0;
  }
  return hash;
}
__name(fnv1a, "fnv1a");
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
__name(stableStringify, "stableStringify");

// src/utils/create-queued-job.util.ts
function createQueuedJob(args) {
  const { name, data, connection, options = {}, workerDefaults = {} } = args;
  const now = Date.now();
  const queue2 = options.queue ?? "default";
  const delayMs = options.delayMs ?? 0;
  const maxAttempts = options.tries ?? workerDefaults.tries ?? 1;
  const backoffMs = options.backoffMs ?? workerDefaults.backoffMs ?? 1e3;
  const timeoutMs = options.timeoutMs ?? workerDefaults.timeoutMs ?? 3e4;
  const uniqueId = options.uniqueFor !== void 0 ? options.uniqueId ?? computeUniqueId(name, data) : void 0;
  return {
    id: generateJobId(),
    name,
    data,
    queue: queue2,
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
__name(createQueuedJob, "createQueuedJob");

// src/connections/base.connection.ts
var BaseConnection = class {
  static {
    __name(this, "BaseConnection");
  }
  name;
  /**
  * Set of paused queue names.
  *
  * Using an in-memory `Set` for pause state is intentionally simple —
  * drivers that need cross-tab pause coordination override
  * {@link pause}/{@link resume}/{@link isPaused} and persist the state
  * themselves (see the broadcast-channel driver).
  */
  pausedQueues = /* @__PURE__ */ new Set();
  /**
  * @param name - The human-friendly connection name from module config.
  */
  constructor(name) {
    this.name = name;
  }
  // ── Shared defaults ────────────────────────────────────────────────────
  /**
  * Delayed dispatch — forwards to `push()` with an additional `delayMs`.
  *
  * Drivers that persist delayed jobs differently from immediate ones
  * override this method.
  */
  async later(delayMs, name, data, options) {
    return this.push(name, data, {
      ...options,
      delayMs
    });
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
  * The default implementation stores the name in an in-memory set;
  * persistent drivers (IndexedDB, BroadcastChannel) override this to
  * persist the pause flag so it survives reloads and is visible to
  * other tabs.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  */
  async pause(queue2 = "default") {
    this.pausedQueues.add(queue2);
  }
  /**
  * Resume a previously paused queue.
  *
  * Removes the entry from the in-memory paused set. Persistent drivers
  * override to clear the flag in their storage layer.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  */
  async resume(queue2 = "default") {
    this.pausedQueues.delete(queue2);
  }
  /**
  * Whether the named queue is currently paused.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns `true` when the queue is paused, `false` otherwise.
  */
  async isPaused(queue2 = "default") {
    return this.pausedQueues.has(queue2);
  }
};

// src/connections/memory.connection.ts
var MemoryConnection = class extends BaseConnection {
  static {
    __name(this, "MemoryConnection");
  }
  /** Every job lives here — pending, reserved, delayed, or failed. */
  jobs = /* @__PURE__ */ new Map();
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
    const job = createQueuedJob({
      name,
      data,
      connection: this.name,
      options
    });
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
  async pop(queue2 = "default") {
    if (this.pausedQueues.has(queue2)) return null;
    const now = Date.now();
    let next;
    for (const job of this.jobs.values()) {
      if (job.queue !== queue2) continue;
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
  async size(queue2 = "default") {
    let count = 0;
    for (const job of this.jobs.values()) {
      if (job.queue !== queue2) continue;
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
  async pendingSize(queue2 = "default") {
    return this.countByStatus(queue2, contracts.JobStatus.Pending);
  }
  /**
  * Number of jobs currently in the Delayed state on a queue.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns The delayed job count.
  */
  async delayedSize(queue2 = "default") {
    return this.countByStatus(queue2, contracts.JobStatus.Delayed);
  }
  /**
  * Number of jobs currently in the Reserved state on a queue.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns The reserved job count.
  */
  async reservedSize(queue2 = "default") {
    return this.countByStatus(queue2, contracts.JobStatus.Reserved);
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
  async clear(queue2 = "default") {
    for (const [id, job] of this.jobs) {
      if (job.queue === queue2) this.jobs.delete(id);
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
  countByStatus(queue2, status) {
    let count = 0;
    for (const job of this.jobs.values()) {
      if (job.queue === queue2 && job.status === status) count++;
    }
    return count;
  }
};
var SyncConnection = class extends BaseConnection {
  static {
    __name(this, "SyncConnection");
  }
  /** The synchronous processor resolver, if one has been registered. */
  handler;
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
  * The sync driver bypasses any queueing — it constructs a {@link QueuedJob}
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

// src/connections/null.connection.ts
var NullConnection = class extends BaseConnection {
  static {
    __name(this, "NullConnection");
  }
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
var LocalStorageConnection = class extends BaseConnection {
  static {
    __name(this, "LocalStorageConnection");
  }
  prefix;
  /**
  * @param name   - Connection name from module config.
  * @param prefix - Optional key prefix. Keys become
  *   `${prefix}queue:${name}:${queueName}`.
  */
  constructor(name, prefix = "") {
    super(name), this.prefix = prefix;
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
    const job = createQueuedJob({
      name: jobName,
      data,
      connection: this.name,
      options
    });
    const blob = this.read(job.queue);
    if (job.uniqueId) {
      const existing = blob.jobs.find((j) => j.uniqueId === job.uniqueId && j.status !== contracts.JobStatus.Completed && j.status !== contracts.JobStatus.Failed);
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
  async pop(queue2 = "default") {
    const blob = this.read(queue2);
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
    this.write(queue2, blob);
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
  async size(queue2 = "default") {
    const blob = this.read(queue2);
    return blob.jobs.filter((j) => j.status !== contracts.JobStatus.Completed && j.status !== contracts.JobStatus.Failed).length;
  }
  /**
  * Number of jobs currently in the Pending state on a queue.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns The pending job count.
  */
  async pendingSize(queue2 = "default") {
    return this.countByStatus(queue2, contracts.JobStatus.Pending);
  }
  /**
  * Number of jobs currently in the Delayed state on a queue.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns The delayed job count.
  */
  async delayedSize(queue2 = "default") {
    return this.countByStatus(queue2, contracts.JobStatus.Delayed);
  }
  /**
  * Number of jobs currently in the Reserved state on a queue.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns The reserved job count.
  */
  async reservedSize(queue2 = "default") {
    return this.countByStatus(queue2, contracts.JobStatus.Reserved);
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
    for (const queue2 of this.listQueues()) {
      const blob = this.read(queue2);
      const next = blob.jobs.filter((j) => j.id !== jobId);
      if (next.length !== blob.jobs.length) {
        blob.jobs = next;
        this.write(queue2, blob);
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
    for (const queue2 of this.listQueues()) {
      const blob = this.read(queue2);
      const job = blob.jobs.find((j) => j.id === jobId);
      if (!job) continue;
      job.status = delayMs > 0 ? contracts.JobStatus.Delayed : contracts.JobStatus.Pending;
      job.availableAt = Date.now() + delayMs;
      job.updatedAt = Date.now();
      this.write(queue2, blob);
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
    for (const queue2 of this.listQueues()) {
      const blob = this.read(queue2);
      const job = blob.jobs.find((j) => j.id === jobId);
      if (!job) continue;
      job.status = contracts.JobStatus.Failed;
      job.lastError = reason;
      job.updatedAt = Date.now();
      this.write(queue2, blob);
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
  async clear(queue2 = "default") {
    if (typeof localStorage === "undefined") return;
    localStorage.removeItem(this.keyFor(queue2));
  }
  /**
  * Persist the paused flag for a queue tube.
  *
  * Overrides the in-memory base implementation so the pause survives
  * reloads.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  */
  async pause(queue2 = "default") {
    const blob = this.read(queue2);
    blob.paused = true;
    this.write(queue2, blob);
  }
  /**
  * Clear the paused flag for a queue tube.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  */
  async resume(queue2 = "default") {
    const blob = this.read(queue2);
    blob.paused = false;
    this.write(queue2, blob);
  }
  /**
  * Whether the queue tube is currently paused.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns `true` when paused, `false` otherwise.
  */
  async isPaused(queue2 = "default") {
    return this.read(queue2).paused;
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
  keyFor(queue2) {
    return `${this.prefix}queue:${this.name}:${queue2}`;
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
  read(queue2) {
    if (typeof localStorage === "undefined") return {
      jobs: [],
      paused: false
    };
    try {
      const raw = localStorage.getItem(this.keyFor(queue2));
      if (!raw) return {
        jobs: [],
        paused: false
      };
      const parsed = JSON.parse(raw);
      return {
        jobs: Array.isArray(parsed.jobs) ? parsed.jobs : [],
        paused: Boolean(parsed.paused)
      };
    } catch {
      return {
        jobs: [],
        paused: false
      };
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
  write(queue2, blob) {
    if (typeof localStorage === "undefined") return;
    try {
      localStorage.setItem(this.keyFor(queue2), JSON.stringify(blob));
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
  countByStatus(queue2, status) {
    return this.read(queue2).jobs.filter((j) => j.status === status).length;
  }
};
var IndexedDBConnection = class extends BaseConnection {
  static {
    __name(this, "IndexedDBConnection");
  }
  dbName;
  version;
  prefix;
  /** Cached database handle, opened lazily on first operation. */
  dbPromise = null;
  /**
  * @param name    - Connection name from module config.
  * @param dbName  - IndexedDB database name.
  * @param version - Schema version (bumped when the driver changes schema).
  * @param prefix  - Object store name prefix.
  */
  constructor(name, dbName = "stackra-queue", version = 1, prefix = "") {
    super(name), this.dbName = dbName, this.version = version, this.prefix = prefix;
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
    const job = createQueuedJob({
      name: jobName,
      data,
      connection: this.name,
      options
    });
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([
        this.jobsStore()
      ], "readwrite");
      const store = tx.objectStore(this.jobsStore());
      const finish = /* @__PURE__ */ __name((id) => resolve(id), "finish");
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
  async pop(queue2 = "default") {
    if (await this.isPaused(queue2)) return null;
    const db = await this.openDb();
    const now = Date.now();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([
        this.jobsStore()
      ], "readwrite");
      const store = tx.objectStore(this.jobsStore());
      const idx = store.index("queue");
      const req = idx.openCursor(IDBKeyRange.only(queue2));
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
        const tx2 = db.transaction([
          this.jobsStore()
        ], "readwrite");
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
  async size(queue2 = "default") {
    return this.countMatching(queue2, (j) => j.status !== contracts.JobStatus.Completed && j.status !== contracts.JobStatus.Failed);
  }
  /**
  * Number of jobs currently in the Pending state on a queue.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns The pending job count.
  */
  async pendingSize(queue2 = "default") {
    return this.countMatching(queue2, (j) => j.status === contracts.JobStatus.Pending);
  }
  /**
  * Number of jobs currently in the Delayed state on a queue.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns The delayed job count.
  */
  async delayedSize(queue2 = "default") {
    return this.countMatching(queue2, (j) => j.status === contracts.JobStatus.Delayed);
  }
  /**
  * Number of jobs currently in the Reserved state on a queue.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  * @returns The reserved job count.
  */
  async reservedSize(queue2 = "default") {
    return this.countMatching(queue2, (j) => j.status === contracts.JobStatus.Reserved);
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
      const tx = db.transaction([
        this.jobsStore()
      ], "readwrite");
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
  async clear(queue2 = "default") {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([
        this.jobsStore()
      ], "readwrite");
      const store = tx.objectStore(this.jobsStore());
      const idx = store.index("queue");
      const req = idx.openKeyCursor(IDBKeyRange.only(queue2));
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
  async pause(queue2 = "default") {
    await this.writeMeta({
      queue: queue2,
      paused: true
    });
  }
  /**
  * Clear the paused flag for a queue tube.
  *
  * @param queue - Queue tube name (defaults to `"default"`).
  */
  async resume(queue2 = "default") {
    await this.writeMeta({
      queue: queue2,
      paused: false
    });
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
  async isPaused(queue2 = "default") {
    const record = await this.readMeta(queue2);
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
          const store = db.createObjectStore(this.jobsStore(), {
            keyPath: "id"
          });
          store.createIndex("queue", "queue", {
            unique: false
          });
          store.createIndex("status", "status", {
            unique: false
          });
          store.createIndex("availableAt", "availableAt", {
            unique: false
          });
          store.createIndex("uniqueId", "uniqueId", {
            unique: false
          });
        }
        if (!db.objectStoreNames.contains(this.metaStore())) {
          db.createObjectStore(this.metaStore(), {
            keyPath: "queue"
          });
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
  async countMatching(queue2, predicate) {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([
        this.jobsStore()
      ], "readonly");
      const idx = tx.objectStore(this.jobsStore()).index("queue");
      const req = idx.openCursor(IDBKeyRange.only(queue2));
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
      const tx = db.transaction([
        this.jobsStore()
      ], "readwrite");
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
  async readMeta(queue2) {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([
        this.metaStore()
      ], "readonly");
      const req = tx.objectStore(this.metaStore()).get(queue2);
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
      const tx = db.transaction([
        this.metaStore()
      ], "readwrite");
      tx.objectStore(this.metaStore()).put(record);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }
};

// src/connections/broadcast-channel.connection.ts
var BroadcastChannelConnection = class extends IndexedDBConnection {
  static {
    __name(this, "BroadcastChannelConnection");
  }
  /**
  * Reference to the shared TabCoordinator from `@stackra/ts-coordinator`.
  *
  * Used to determine if this tab is the leader and should drain the queue.
  * When `null` (SSR/non-browser), the connection behaves as always-leader.
  */
  coordinator;
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
  async pop(queue2 = "default") {
    if (this.coordinator && !this.coordinator.isLeader()) return null;
    return super.pop(queue2);
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

// src/connections/qstash.connection.ts
var QStashConnection = class extends BaseConnection {
  static {
    __name(this, "QStashConnection");
  }
  config;
  /**
  * Lazy-loaded QStash client for `mode: 'direct'`.
  *
  * `null` until the first `pushDirect()` call. The dynamic import of
  * `@upstash/qstash` is deferred so apps using proxy mode never ship
  * the QStash SDK.
  */
  directClient = null;
  constructor(name, config) {
    super(name), this.config = config;
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
      throw new QueueDriverError(`[QStashConnection:${this.name}] push() requires a destination. Pass 'defaultDestination' in config or 'driverOptions.destination' at push time.`);
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
    return this.push(jobName, data, {
      ...options,
      delayMs
    });
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
    throw new QueueDriverError(`[QStashConnection:${this.name}] clear() is not supported. Use the QStash console or API from your backend to clear messages.`);
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
      throw new QueueDriverError(`[QStashConnection:${this.name}] mode: 'proxy' requires 'proxyUrl' in config.`);
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
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify(body)
    });
    if (!response.ok) {
      throw new QueueDriverError(`[QStashConnection:${this.name}] Proxy returned HTTP ${response.status}: ${await response.text()}`);
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
      throw new QueueDriverError(`[QStashConnection:${this.name}] mode: 'direct' requires 'token' in config. Do NOT ship this token in a public client \u2014 prefer 'mode: proxy' instead.`);
    }
    const client = await this.getDirectClient();
    const delaySec = options?.delayMs !== void 0 ? Math.floor(options.delayMs / 1e3) : void 0;
    const res = await client.publishJSON({
      url: destination,
      body: {
        name,
        data
      },
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
      throw new QueueDriverError(`[QStashConnection:${this.name}] mode: 'direct' requires '@upstash/qstash' to be installed. Run 'pnpm add @upstash/qstash' or switch to 'mode: proxy'.`);
    }
  }
};

// src/services/queue-handle.service.ts
var QueueHandle = class {
  static {
    __name(this, "QueueHandle");
  }
  connection;
  queue;
  /**
  * @param connection - The underlying driver.
  * @param queue      - Queue tube name this handle is bound to.
  */
  constructor(connection, queue2) {
    this.connection = connection;
    this.queue = queue2;
  }
  /**
  * Dispatch a job onto the bound queue.
  *
  * Forwards to the underlying connection with this handle's `queue`
  * pre-applied so callers don't repeat it at every dispatch site.
  *
  * @typeParam T - Type of the job payload.
  * @param name    - Application-level job name.
  * @param data    - The job payload.
  * @param options - Optional dispatch options (delay, retries, …).
  *   `queue` is overridden by the handle.
  * @returns The dispatched job's id.
  */
  async push(name, data, options) {
    return this.connection.push(name, data, {
      ...options,
      queue: this.queue
    });
  }
  /**
  * Dispatch a job with a delay.
  *
  * @typeParam T - Type of the job payload.
  * @param delayMs - Delay in milliseconds before the job becomes
  *   eligible for processing.
  * @param name    - Application-level job name.
  * @param data    - The job payload.
  * @param options - Optional dispatch options. `queue` is overridden
  *   by the handle.
  * @returns The dispatched job's id.
  */
  async later(delayMs, name, data, options) {
    return this.connection.later(delayMs, name, data, {
      ...options,
      queue: this.queue
    });
  }
  /**
  * Bulk dispatch.
  *
  * Forwards each job's options through `bulk()` with the handle's
  * `queue` pre-applied. Drivers with native batch support (IndexedDB
  * transactions, QStash) handle this efficiently; others fall back to
  * a sequential loop.
  *
  * @typeParam T - Type of every job's payload.
  * @param jobs - Array of `{ name, data, options }` tuples.
  * @returns Array of dispatched job ids in the same order as `jobs`.
  */
  async bulk(jobs) {
    return this.connection.bulk(jobs.map((j) => ({
      ...j,
      options: {
        ...j.options,
        queue: this.queue
      }
    })));
  }
  /**
  * Total in-flight job count for the bound queue.
  *
  * Includes pending, delayed, and reserved jobs — terminal states
  * (Completed/Failed) are excluded.
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
  *
  * Useful in tests — call this in `beforeEach`/`afterEach` to keep
  * runs isolated.
  */
  clear() {
    return this.connection.clear(this.queue);
  }
  /**
  * Pause processing on the bound queue tube.
  *
  * Workers polling this tube will see `pop()` return `null` until
  * resumed.
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
function _ts_decorate(decorators, target, key, desc) {
  var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
  if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
  else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
  return c > 3 && r && Object.defineProperty(target, key, r), r;
}
__name(_ts_decorate, "_ts_decorate");
function _ts_metadata(k, v) {
  if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
}
__name(_ts_metadata, "_ts_metadata");
function _ts_param(paramIndex, decorator) {
  return function(target, key) {
    decorator(target, key, paramIndex);
  };
}
__name(_ts_param, "_ts_param");
exports.QueueManager = class _QueueManager extends tsSupport.MultipleInstanceManager {
  static {
    __name(this, "QueueManager");
  }
  config;
  coordinator;
  /**
  * Cached {@link QueueHandle} wrappers, keyed by `"${connection}:${queue}"`.
  *
  * Handles are light, but caching them lets consumers get the same
  * instance for the same pair — which is useful for tests that
  * patch methods on the handle.
  */
  handles = /* @__PURE__ */ new Map();
  constructor(config, coordinator) {
    super(), this.config = config, this.coordinator = coordinator;
  }
  /**
  * Logger instance scoped to the QueueManager context.
  */
  logger = new tsLogger.Logger(_QueueManager.name);
  // ── Lifecycle hooks ────────────────────────────────────────────────────
  /**
  * Validate the default connection exists and eagerly warm it so
  * config errors surface at bootstrap rather than at the first dispatch.
  */
  onModuleInit() {
    if (!this.config.connections[this.config.default]) {
      throw new QueueDriverError(`[QueueManager] Default connection '${this.config.default}' is not defined. Available connections: ${Object.keys(this.config.connections).join(", ")}`);
    }
    try {
      this.connection();
    } catch (err) {
      this.logger.warn(`[QueueManager] Failed to warm default connection '${this.config.default}': ${err.message}`);
    }
  }
  /**
  * Close every resolved connection on app shutdown.
  *
  * Drivers release IndexedDB handles, stop heartbeats, and clear
  * timers — important to do explicitly so tests don't leave handles
  * open and hot-reload doesn't leak resources.
  */
  async onModuleDestroy() {
    const names = this.getResolvedInstances();
    for (const name of names) {
      try {
        const conn = this.instance(name);
        await conn.close();
      } catch {
      }
    }
    this.handles.clear();
    this.purge();
  }
  // ── MultipleInstanceManager contract ──────────────────────────────────
  getDefaultInstance() {
    return this.config.default;
  }
  setDefaultInstance(name) {
    this.config.default = name;
  }
  getInstanceConfig(name) {
    const raw = this.config.connections[name];
    return raw;
  }
  /**
  * Build a driver instance for the given name + config pair.
  *
  * Dispatches on `config.driver`. Consumers can add custom drivers via
  * `manager.extend('my-driver', factory)` inherited from the base class.
  */
  createDriver(driver, config) {
    const cfg = config;
    const prefix = (this.config.prefix ?? "") + (cfg.prefix ?? "");
    const connName = cfg.__name ?? driver;
    switch (driver) {
      case contracts.QueueDriverName.Memory:
      case "memory": {
        return new MemoryConnection(connName);
      }
      case contracts.QueueDriverName.Sync:
      case "sync":
        return new SyncConnection(connName);
      case contracts.QueueDriverName.Null:
      case "null":
        return new NullConnection(connName);
      case contracts.QueueDriverName.LocalStorage:
      case "local-storage": {
        return new LocalStorageConnection(connName, prefix);
      }
      case contracts.QueueDriverName.IndexedDB:
      case "indexeddb": {
        const c = cfg;
        return new IndexedDBConnection(connName, c.dbName, c.dbVersion, prefix);
      }
      case contracts.QueueDriverName.BroadcastChannel:
      case "broadcast-channel": {
        const c = cfg;
        return new BroadcastChannelConnection(connName, this.coordinator ?? null, c.dbName, 1, prefix);
      }
      case contracts.QueueDriverName.QStash:
      case "qstash":
        return new QStashConnection(connName, cfg);
      default:
        throw new QueueDriverError(`[QueueManager] Queue driver '${driver}' is not supported. Use extend() to register a custom driver.`);
    }
  }
  // ── Public API ────────────────────────────────────────────────────────
  /**
  * Get a {@link QueueConnection} for the named driver.
  *
  * Lazily creates the connection on first access. Pass no argument to
  * use the configured default.
  */
  connection(name) {
    return this.instance(name);
  }
  /**
  * Get a {@link QueueHandle} bound to a specific `(connection, queue)`.
  *
  * @param queue      - Queue tube name (defaults to `"default"`).
  * @param connection - Connection name (defaults to the module default).
  */
  queue(queue2 = "default", connection) {
    const connName = connection ?? this.config.default;
    const key = `${connName}:${queue2}`;
    const cached = this.handles.get(key);
    if (cached) return cached;
    const handle = new QueueHandle(this.connection(connName), queue2);
    this.handles.set(key, handle);
    return handle;
  }
  /**
  * All configured connection names.
  *
  * @returns Array of connection names declared in
  *   {@link QueueModuleOptions.connections}.
  */
  getConnectionNames() {
    return Object.keys(this.config.connections);
  }
  /**
  * The default connection name.
  *
  * Used by `connection()` and `queue()` when no name is supplied.
  *
  * @returns The default connection's name.
  */
  getDefaultConnectionName() {
    return this.config.default;
  }
  /**
  * Whether a connection name is configured.
  *
  * @param name - Connection name to check.
  * @returns `true` when the name appears in module config.
  */
  hasConnection(name) {
    return name in this.config.connections;
  }
  /**
  * Shared worker options resolved with sensible defaults.
  *
  * Workers consult this for poll interval, retry policy, timeouts,
  * etc. The defaults here mirror Laravel's worker defaults so apps
  * familiar with the Laravel queue feel at home.
  *
  * @returns The fully resolved worker options.
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
exports.QueueManager = _ts_decorate([
  tsContainer.Injectable(),
  _ts_param(0, tsContainer.Inject(contracts.QUEUE_CONFIG)),
  _ts_param(1, tsContainer.Optional()),
  _ts_param(1, tsContainer.Inject(contracts.TAB_COORDINATOR)),
  _ts_metadata("design:type", Function),
  _ts_metadata("design:paramtypes", [
    typeof QueueModuleOptions === "undefined" ? Object : QueueModuleOptions,
    typeof TabCoordinator === "undefined" ? Object : TabCoordinator
  ])
], exports.QueueManager);
function _ts_decorate2(decorators, target, key, desc) {
  var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
  if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
  else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
  return c > 3 && r && Object.defineProperty(target, key, r), r;
}
__name(_ts_decorate2, "_ts_decorate");
function _ts_metadata2(k, v) {
  if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
}
__name(_ts_metadata2, "_ts_metadata");
function _ts_param2(paramIndex, decorator) {
  return function(target, key) {
    decorator(target, key, paramIndex);
  };
}
__name(_ts_param2, "_ts_param");
exports.QueueEventBus = class QueueEventBus {
  static {
    __name(this, "QueueEventBus");
  }
  events;
  constructor(events) {
    this.events = events;
  }
  /**
  * Emit a queue lifecycle event.
  *
  * @param event   - One of {@link QueueEvent} constants.
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
exports.QueueEventBus = _ts_decorate2([
  tsContainer.Injectable(),
  _ts_param2(0, tsContainer.Optional()),
  _ts_param2(0, tsContainer.Inject(contracts.EVENT_EMITTER)),
  _ts_metadata2("design:type", Function),
  _ts_metadata2("design:paramtypes", [
    typeof IEventEmitter === "undefined" ? Object : IEventEmitter
  ])
], exports.QueueEventBus);
function _ts_decorate3(decorators, target, key, desc) {
  var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
  if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
  else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
  return c > 3 && r && Object.defineProperty(target, key, r), r;
}
__name(_ts_decorate3, "_ts_decorate");
exports.ProcessorMetadataAccessor = class ProcessorMetadataAccessor {
  static {
    __name(this, "ProcessorMetadataAccessor");
  }
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
    const meta = metadata.getMetadata(ON_JOB_EVENT_METADATA, target);
    if (!meta) return void 0;
    return Array.isArray(meta) ? meta : [
      meta
    ];
  }
};
exports.ProcessorMetadataAccessor = _ts_decorate3([
  tsContainer.Injectable()
], exports.ProcessorMetadataAccessor);

// src/hosts/worker-host.ts
var WorkerHost = class {
  static {
    __name(this, "WorkerHost");
  }
};

// src/errors/max-attempts-exceeded.error.ts
var MaxAttemptsExceededError = class extends QueueError {
  static {
    __name(this, "MaxAttemptsExceededError");
  }
  jobId;
  attempts;
  name = "MaxAttemptsExceededError";
  code = "QUEUE_MAX_ATTEMPTS_EXCEEDED";
  /**
  * Create a new MaxAttemptsExceededError.
  *
  * @param jobId    - The job identifier.
  * @param attempts - Total attempts that were made before giving up.
  * @param cause    - The underlying exception from the last attempt.
  */
  constructor(jobId, attempts, cause) {
    super(`Job [${jobId}] has been attempted too many times (${attempts} attempts).`, cause), this.jobId = jobId, this.attempts = attempts;
  }
};

// src/errors/timeout-exceeded.error.ts
var TimeoutExceededError = class extends QueueError {
  static {
    __name(this, "TimeoutExceededError");
  }
  jobId;
  elapsedMs;
  timeoutMs;
  name = "TimeoutExceededError";
  code = "QUEUE_TIMEOUT_EXCEEDED";
  /**
  * Create a new TimeoutExceededError.
  *
  * @param jobId       - The job identifier.
  * @param elapsedMs   - How long the processor ran before being killed.
  * @param timeoutMs   - The configured timeout in milliseconds.
  */
  constructor(jobId, elapsedMs, timeoutMs) {
    super(`Job [${jobId}] exceeded its timeout after ${elapsedMs}ms (configured: ${timeoutMs}ms).`), this.jobId = jobId, this.elapsedMs = elapsedMs, this.timeoutMs = timeoutMs;
  }
};

// src/utils/compute-backoff.util.ts
function computeBackoff(attempt, baseMs, maxMs) {
  if (attempt <= 1) return baseMs;
  const exp = baseMs * Math.pow(2, attempt - 1);
  return Math.min(exp, maxMs);
}
__name(computeBackoff, "computeBackoff");
var Worker = class _Worker {
  static {
    __name(this, "Worker");
  }
  config;
  /**
  * Logger instance scoped to the Worker context.
  */
  logger = new tsLogger.Logger(_Worker.name);
  /** Whether the worker has been started and is currently polling. */
  running = false;
  /** The scheduled next-tick handle, cleared on stop(). */
  pendingTick = null;
  constructor(config) {
    this.config = config;
  }
  /**
  * Begin polling for jobs.
  *
  * Emits {@link QueueEvent.WorkerStarting} once on first call.
  * Subsequent calls are no-ops — safe to call after reloads.
  */
  start() {
    if (this.running) return;
    this.running = true;
    this.emit(contracts.QueueEvent.WorkerStarting, {
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
  * stopped. Emits {@link QueueEvent.WorkerStopping}.
  */
  stop() {
    if (!this.running) return;
    this.running = false;
    if (this.pendingTick) {
      clearTimeout(this.pendingTick);
      this.pendingTick = null;
    }
    this.emit(contracts.QueueEvent.WorkerStopping, {
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
        this.logger.error(`[Worker:${this.config.connection.name}:${this.config.queue}] tick error: ${err.message}`);
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
  * 1. Emit `JobProcessing`.
  * 2. Race `host.process(job)` vs. the timeout timer.
  * 3. On success: mark complete, emit `JobProcessed`.
  * 4. On timeout/failure: apply the retry policy, release or fail,
  *    emit `JobFailed` + `JobReleased` accordingly.
  * 5. Always emit `JobAttempted` for instrumentation.
  */
  async handle(job) {
    this.emit(contracts.QueueEvent.JobProcessing, {
      job
    });
    let error;
    let timedOut = false;
    try {
      await this.runWithTimeout(job);
      await this.config.connection.remove(job.id);
      this.emit(contracts.QueueEvent.JobProcessed, {
        job
      });
    } catch (e) {
      error = e;
      timedOut = error instanceof TimeoutExceededError;
      if (timedOut) {
        this.emit(contracts.QueueEvent.JobTimedOut, {
          job,
          error
        });
      }
      await this.handleFailure(job, error, timedOut);
    } finally {
      this.emit(contracts.QueueEvent.JobAttempted, {
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
      await Promise.race([
        Promise.resolve(this.config.host.process(job)),
        timeoutPromise
      ]);
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
  * - Otherwise, mark it permanently failed and emit `JobFailed` with a
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
      this.emit(contracts.QueueEvent.JobFailed, {
        job: {
          ...job,
          status: contracts.JobStatus.Failed
        },
        error: finalError
      });
      return;
    }
    const delayMs = computeBackoff(job.attempts + 1, job.backoffMs, policy.maxBackoffMs);
    await this.config.connection.release(job.id, delayMs);
    this.emit(contracts.QueueEvent.JobReleased, {
      job,
      delayMs,
      error
    });
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
function _ts_decorate4(decorators, target, key, desc) {
  var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
  if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
  else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
  return c > 3 && r && Object.defineProperty(target, key, r), r;
}
__name(_ts_decorate4, "_ts_decorate");
function _ts_metadata3(k, v) {
  if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
}
__name(_ts_metadata3, "_ts_metadata");
function _ts_param3(paramIndex, decorator) {
  return function(target, key) {
    decorator(target, key, paramIndex);
  };
}
__name(_ts_param3, "_ts_param");
exports.ProcessorSubscribersLoader = class _ProcessorSubscribersLoader {
  static {
    __name(this, "ProcessorSubscribersLoader");
  }
  manager;
  metadataAccessor;
  eventBus;
  events;
  /** Every worker started by the loader — stopped on shutdown. */
  workers = [];
  /**
  * Logger instance scoped to the ProcessorSubscribersLoader context.
  */
  logger = new tsLogger.Logger(_ProcessorSubscribersLoader.name);
  /**
  * Event manager listeners the loader registered — removed on shutdown.
  * Stored as `[event, listener]` tuples so we can call `.off(...)`
  * symmetrically with the `.on(...)` we did at bootstrap.
  */
  listeners = [];
  constructor(manager, metadataAccessor, eventBus, events) {
    this.manager = manager;
    this.metadataAccessor = metadataAccessor;
    this.eventBus = eventBus;
    this.events = events;
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
  onApplicationBootstrap() {
    const providers = this.collectProviderInstances();
    if (!providers) return;
    for (const instance of providers) {
      const meta = this.metadataAccessor.getProcessorMetadata(instance.constructor);
      if (!meta) continue;
      this.registerProcessor(instance, meta);
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
      this.logger.warn(`[QueueLoader] Failed to access global application: ${err.message}`);
      return null;
    }
  }
  /**
  * Create worker instances for a processor and start them.
  *
  * Throws if the class doesn't extend {@link WorkerHost} — this catches
  * the common misuse of decorating an arbitrary class.
  */
  registerProcessor(instance, meta) {
    if (!(instance instanceof WorkerHost)) {
      throw new QueueDriverError(`[QueueLoader] Processor class '${instance.constructor?.name}' must extend WorkerHost.`);
    }
    const connectionName = meta.connection ?? this.manager.getDefaultConnectionName();
    const connection = this.manager.connection(connectionName);
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
    const listener = /* @__PURE__ */ __name((...args) => {
      if (meta.connection) {
        const payload = args[0];
        if (payload?.job?.connection && payload.job.connection !== meta.connection) return;
      }
      try {
        const fn = instance[methodKey];
        const result = fn.call(instance, ...args);
        if (result && typeof result.then === "function") {
          result.catch((err) => this.logger.error(`[QueueLoader] ${methodKey} handler threw: ${String(err)}`));
        }
      } catch (err) {
        this.logger.error(`[QueueLoader] ${methodKey} handler threw: ${String(err)}`);
      }
    }, "listener");
    this.events.on(meta.event, listener);
    this.listeners.push({
      event: meta.event,
      listener
    });
  }
};
exports.ProcessorSubscribersLoader = _ts_decorate4([
  tsContainer.Injectable(),
  _ts_param3(3, tsContainer.Optional()),
  _ts_param3(3, tsContainer.Inject(contracts.EVENT_EMITTER)),
  _ts_metadata3("design:type", Function),
  _ts_metadata3("design:paramtypes", [
    typeof exports.QueueManager === "undefined" ? Object : exports.QueueManager,
    typeof exports.ProcessorMetadataAccessor === "undefined" ? Object : exports.ProcessorMetadataAccessor,
    typeof exports.QueueEventBus === "undefined" ? Object : exports.QueueEventBus,
    typeof EventManagerLike === "undefined" ? Object : EventManagerLike
  ])
], exports.ProcessorSubscribersLoader);

// src/queue.module.ts
function _ts_decorate5(decorators, target, key, desc) {
  var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
  if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
  else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
  return c > 3 && r && Object.defineProperty(target, key, r), r;
}
__name(_ts_decorate5, "_ts_decorate");
exports.QueueModule = class _QueueModule {
  static {
    __name(this, "QueueModule");
  }
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
  static forRoot(config) {
    const connectionProviders = Object.keys(config.connections).map((name) => ({
      provide: getQueueConnectionToken(name),
      useFactory: /* @__PURE__ */ __name((manager) => manager.connection(name), "useFactory"),
      inject: [
        exports.QueueManager
      ]
    }));
    const defaultConnectionProvider = {
      provide: contracts.DEFAULT_QUEUE_CONNECTION_TOKEN,
      useFactory: /* @__PURE__ */ __name((manager) => manager.connection(config.default), "useFactory"),
      inject: [
        exports.QueueManager
      ]
    };
    const defaultHandleProviders = Object.keys(config.connections).map((name) => ({
      provide: getQueueToken("default", name),
      useFactory: /* @__PURE__ */ __name((manager) => manager.queue("default", name), "useFactory"),
      inject: [
        exports.QueueManager
      ]
    }));
    const defaultQueueHandleProvider = {
      provide: getQueueToken(),
      useFactory: /* @__PURE__ */ __name((manager) => manager.queue(), "useFactory"),
      inject: [
        exports.QueueManager
      ]
    };
    return {
      module: _QueueModule,
      global: true,
      providers: [
        // Config
        {
          provide: contracts.QUEUE_CONFIG,
          useValue: config
        },
        // Manager
        {
          provide: exports.QueueManager,
          useClass: exports.QueueManager
        },
        {
          provide: contracts.QUEUE_MANAGER,
          useExisting: exports.QueueManager
        },
        // Connection-level providers
        defaultConnectionProvider,
        ...connectionProviders,
        // Queue handle providers
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
  static forFeature(queues) {
    const providers = queues.map(({ queue: queue2, connection }) => ({
      provide: getQueueToken(queue2, connection ?? "default"),
      useFactory: /* @__PURE__ */ __name((manager) => manager.queue(queue2, connection), "useFactory"),
      inject: [
        exports.QueueManager
      ]
    }));
    return {
      module: _QueueModule,
      providers,
      exports: providers.map((p) => p.provide)
    };
  }
};
exports.QueueModule = _ts_decorate5([
  tsContainer.Module({})
], exports.QueueModule);
function Processor(arg) {
  const options = typeof arg === "string" ? {
    queue: arg
  } : arg;
  return (target) => {
    tsContainer.Injectable()(target);
    metadata.defineMetadata(PROCESSOR_METADATA, options, target);
  };
}
__name(Processor, "Processor");
function OnJobEvent(event, connection) {
  return (_target, _key, descriptor) => {
    metadata.updateMetadata(ON_JOB_EVENT_METADATA, [], (items) => [
      ...items,
      {
        event,
        connection
      }
    ], descriptor.value);
    return descriptor;
  };
}
__name(OnJobEvent, "OnJobEvent");
function Job(options) {
  const injectableDecorator = tsContainer.Injectable();
  return (target) => {
    injectableDecorator(target);
    metadata.defineMetadata(JOB_METADATA, options, target);
  };
}
__name(Job, "Job");
function InjectQueue(queue2, connection) {
  return tsContainer.Inject(getQueueToken(queue2, connection));
}
__name(InjectQueue, "InjectQueue");
function InjectQueueConnection(name) {
  const token = name ? getQueueConnectionToken(name) : contracts.DEFAULT_QUEUE_CONNECTION_TOKEN;
  return tsContainer.Inject(token);
}
__name(InjectQueueConnection, "InjectQueueConnection");
function useQueue(queue2, connection) {
  const manager = react.useInject(exports.QueueManager);
  return manager.queue(queue2, connection);
}
__name(useQueue, "useQueue");
function useQueueConnection(name) {
  const manager = react.useInject(exports.QueueManager);
  return manager.connection(name);
}
__name(useQueueConnection, "useQueueConnection");
function useQueueManager() {
  return react.useInject(exports.QueueManager);
}
__name(useQueueManager, "useQueueManager");
var queue = tsContainer.inject(contracts.QUEUE_MANAGER);

// src/index.ts
try {
  const vite = __require("@stackra/vite-config");
  vite.DecoratorDiscoveryModule?.forFeature([
    {
      name: "Processor",
      virtualModule: "virtual:decorator-registry/queue-processors",
      output: "metadata",
      exportName: "QUEUE_PROCESSOR_CLASSES",
      priority: 58
    }
  ]);
} catch {
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
exports.SyncConnection = SyncConnection;
exports.TimeoutExceededError = TimeoutExceededError;
exports.Worker = Worker;
exports.WorkerHost = WorkerHost;
exports.getQueueConnectionToken = getQueueConnectionToken;
exports.getQueueToken = getQueueToken;
exports.queue = queue;
exports.useQueue = useQueue;
exports.useQueueConnection = useQueueConnection;
exports.useQueueManager = useQueueManager;
