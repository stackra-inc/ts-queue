/**
 * @fileoverview Per-job dispatch options.
 *
 * These options travel with a single `push()` / `later()` call and override
 * any defaults set on the job class via `@Job({...})`. They are the
 * client-side equivalent of Laravel's per-dispatch overrides
 * (`->onQueue()`, `->delay()`, `->tries()`, etc.).
 *
 * @module interfaces/job-options
 * @category Interfaces
 */

/**
 * Options that can be supplied at the point of dispatching a job.
 *
 * All fields are optional — sensible defaults come from the worker
 * configuration ({@link QueueModuleOptions.worker}) or from the
 * `@Job({...})` decorator on the processor class.
 *
 * @example
 * ```typescript
 * await queue.push('pixel.fireEvent', payload, {
 *   tries: 3,
 *   backoff: 1000,
 *   uniqueFor: 60_000,
 *   queue: 'tracking',
 * });
 * ```
 */
export interface JobOptions {
  /**
   * Target queue name within the connection.
   *
   * Lets a single connection carry multiple named tubes — `'default'`,
   * `'high'`, `'scans'`, etc. Defaults to `"default"`.
   *
   * @default "default"
   */
  queue?: string;

  /**
   * Delay before the job becomes available for processing, in milliseconds.
   *
   * Equivalent to Laravel's `later($delay, ...)`. Ignored by drivers that
   * don't support delayed dispatch (the job runs immediately instead).
   *
   * @default 0
   */
  delayMs?: number;

  /**
   * Maximum number of processing attempts before the job is marked failed.
   *
   * Includes the first attempt, so `tries: 3` means "try three times
   * total." When omitted, falls back to {@link WorkerOptions.tries}.
   *
   * @default 1
   */
  tries?: number;

  /**
   * Initial backoff between retries, in milliseconds.
   *
   * The worker multiplies this by `2^(attempts - 1)` for exponential
   * backoff, capped at {@link WorkerOptions.maxBackoffMs}.
   *
   * @default 1000
   */
  backoffMs?: number;

  /**
   * Maximum processor runtime for a single attempt, in milliseconds.
   *
   * When exceeded, the worker raises a {@link TimeoutExceededError}. The
   * {@link JobOptions.failOnTimeout} flag controls whether that error
   * retries or permanently fails.
   *
   * @default 30_000
   */
  timeoutMs?: number;

  /**
   * Whether a timeout should permanently fail the job (`true`) or release
   * it for another attempt (`false`).
   *
   * @default true
   */
  failOnTimeout?: boolean;

  /**
   * Deduplication window for this job, in milliseconds.
   *
   * When set, enqueueing a job with the same `uniqueId` (or computed
   * uniqueness key from the job name + payload hash) within the window
   * is a no-op. Mirrors Laravel's `ShouldBeUnique` behaviour.
   *
   * @default undefined
   */
  uniqueFor?: number;

  /**
   * Explicit deduplication key for {@link JobOptions.uniqueFor}.
   *
   * When omitted, the dispatcher derives one from the job name and a
   * stable hash of the payload.
   *
   * @default undefined
   */
  uniqueId?: string;

  /**
   * Maximum number of exceptions a job may throw before being failed
   * (independent of `tries`). Useful when some attempts complete without
   * throwing — e.g., empty-result fast paths.
   *
   * @default undefined (no separate limit)
   */
  maxExceptions?: number;

  /**
   * Arbitrary tag array for observability and log correlation.
   *
   * Drivers persist these alongside the job payload. Tags are not used
   * for routing — they're purely descriptive.
   *
   * @default []
   */
  tags?: string[];

  /**
   * Driver-specific extension bag.
   *
   * Drivers that expose extra knobs (QStash URL Groups, IndexedDB store
   * names, …) read from this object to avoid crowding the shared
   * interface.
   */
  driverOptions?: Record<string, unknown>;
}
