/**
 * @fileoverview The driver contract.
 *
 * Every driver (memory, indexeddb, qstash, …) implements
 * {@link QueueConnection}. The {@link QueueManager} holds a map of these
 * keyed by connection name and hands them out to consumers.
 *
 * The interface is intentionally narrow — only the operations that are
 * meaningful for both browser-local queues and external brokers. Drivers
 * that don't support a method (e.g. QStash has no `pop`) throw
 * `QueueDriverError` with a clear message.
 *
 * @module interfaces/queue-connection
 * @category Interfaces
 */

import type { JobOptions } from "./job-options.interface";
import type { QueuedJob } from "./queued-job.interface";

/**
 * Driver contract — one implementation per storage backend.
 *
 * @example
 * ```typescript
 * class MyDriver implements QueueConnection {
 *   async push(name, data, options) { ... }
 *   async pop() { ... }
 *   ...
 * }
 * ```
 */
export interface QueueConnection {
  /**
   * The human-friendly connection name (from module config).
   *
   * Drivers persist this on every job so popped jobs know where they
   * came from — helpful for mixed-driver worker pools.
   */
  readonly name: string;

  /**
   * Enqueue a job for immediate processing.
   *
   * @typeParam T - Payload type.
   * @param name    - Application-level job name.
   * @param data    - Arbitrary JSON-serialisable payload.
   * @param options - Optional per-job overrides.
   * @returns The driver-assigned job id.
   */
  push<T = unknown>(name: string, data: T, options?: JobOptions): Promise<string>;

  /**
   * Enqueue a job for processing after a delay.
   *
   * Equivalent to `push()` with `options.delayMs = delayMs`. Exposed as a
   * separate method because many drivers have distinct storage paths for
   * delayed vs immediate jobs.
   *
   * @param delayMs - Delay in milliseconds before the job is eligible to run.
   */
  later<T = unknown>(delayMs: number, name: string, data: T, options?: JobOptions): Promise<string>;

  /**
   * Enqueue many jobs in a single call.
   *
   * Drivers that support batching (IndexedDB transactions, QStash batch
   * publish) implement this directly; others fall back to a loop.
   */
  bulk<T = unknown>(
    jobs: Array<{ name: string; data: T; options?: JobOptions }>,
  ): Promise<string[]>;

  /**
   * Pop the next available job from the given queue (FIFO).
   *
   * Returns `null` when the queue is empty or paused. Implementations
   * that represent producer-only drivers (e.g. QStash) always return
   * `null` because the broker itself drives delivery.
   *
   * @param queue - Queue tube name. Defaults to `"default"`.
   */
  pop(queue?: string): Promise<QueuedJob | null>;

  /** Count of pending + delayed + reserved jobs in the named queue. */
  size(queue?: string): Promise<number>;

  /** Count of pending jobs only (no delayed, no reserved). */
  pendingSize(queue?: string): Promise<number>;

  /** Count of delayed jobs with `availableAt > now`. */
  delayedSize(queue?: string): Promise<number>;

  /** Count of reserved (in-flight) jobs. */
  reservedSize(queue?: string): Promise<number>;

  /** Mark a job as completed and remove it from the queue. */
  remove(jobId: string): Promise<void>;

  /**
   * Release a reserved job back to the queue for retry.
   *
   * @param jobId  - The job id to release.
   * @param delayMs - Optional additional delay before the job becomes
   *   available again (applied on top of the backoff policy).
   */
  release(jobId: string, delayMs?: number): Promise<void>;

  /** Mark a job permanently failed. */
  fail(jobId: string, reason: string): Promise<void>;

  /** Remove every job (pending, delayed, reserved) from the queue. */
  clear(queue?: string): Promise<void>;

  /** Pause processing for the named queue. */
  pause(queue?: string): Promise<void>;

  /** Resume processing for the named queue. */
  resume(queue?: string): Promise<void>;

  /** Whether the named queue is currently paused. */
  isPaused(queue?: string): Promise<boolean>;

  /**
   * Release all driver resources — close connections, clear timers, etc.
   *
   * Called by {@link QueueManager} during `OnModuleDestroy`.
   */
  close(): Promise<void>;
}
