/**
 * @fileoverview Global worker policy options.
 *
 * Configures how workers poll, retry, and fail jobs. Per-job options on
 * {@link JobOptions} override these on a case-by-case basis.
 *
 * @module interfaces/worker-options
 * @category Interfaces
 */

/**
 * Worker behaviour options — shared defaults across all processors.
 *
 * @example
 * ```typescript
 * QueueModule.forRoot({
 *   default: 'indexeddb',
 *   connections: { ... },
 *   worker: {
 *     tries: 3,
 *     backoffMs: 1000,
 *     maxBackoffMs: 30_000,
 *     timeoutMs: 30_000,
 *     pollIntervalMs: 500,
 *   },
 * });
 * ```
 */
export interface WorkerOptions {
  /**
   * Maximum attempts before a job is permanently failed.
   *
   * @default 1
   */
  tries?: number;

  /**
   * Initial backoff between retries, in milliseconds.
   *
   * Grows exponentially (`backoffMs * 2^(attempts - 1)`) up to
   * {@link WorkerOptions.maxBackoffMs}.
   *
   * @default 1000
   */
  backoffMs?: number;

  /**
   * Upper bound on exponential backoff, in milliseconds.
   *
   * @default 30_000
   */
  maxBackoffMs?: number;

  /**
   * Processor timeout in milliseconds.
   *
   * @default 30_000
   */
  timeoutMs?: number;

  /**
   * Polling interval for drivers that do not push work to workers
   * (memory, indexeddb, local-storage).
   *
   * @default 500
   */
  pollIntervalMs?: number;

  /**
   * Whether to start worker loops automatically during `OnModuleInit`.
   *
   * Set `false` in tests or in apps that want to defer processing until
   * explicit setup (e.g., wait for auth before pulling tracking jobs).
   *
   * @default true
   */
  autoStart?: boolean;

  /**
   * Whether a timeout results in permanent failure instead of a retry.
   *
   * @default true
   */
  failOnTimeout?: boolean;
}
