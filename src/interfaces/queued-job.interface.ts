/**
 * @fileoverview Shape of a job as stored by a driver.
 *
 * {@link QueuedJob} is the wire format shared across every driver. When a
 * driver persists a job, it serialises this shape (usually as JSON). When
 * a worker pops a job, it rehydrates this shape and hands it to the
 * processor.
 *
 * The generic parameter `T` is the payload type — the caller's data.
 * Everything else is queue-system metadata.
 *
 * @module interfaces/queued-job
 * @category Interfaces
 */

import { JobStatus } from "@stackra/contracts";

/**
 * A single job as held by a queue driver.
 *
 * @typeParam T - Shape of the application payload carried by this job.
 *
 * @example
 * ```typescript
 * interface SalePayload { saleId: string; amount: number; }
 *
 * async process(job: QueuedJob<SalePayload>) {
 *   const { saleId, amount } = job.data;
 *   // ...
 * }
 * ```
 */
export interface QueuedJob<T = unknown> {
  /** Driver-assigned unique identifier (UUID or driver-native id). */
  id: string;

  /**
   * Application-level job name — used by the processor registry to route
   * a job to its handler. Matches the string passed to `queue.push(name, …)`.
   */
  name: string;

  /** Application payload — whatever the caller handed to `push(name, data)`. */
  data: T;

  /** Queue tube this job belongs to within the connection. */
  queue: string;

  /** Name of the driver connection that owns this job. */
  connection: string;

  /** Current lifecycle state — see {@link JobStatus}. */
  status: JobStatus;

  /** Number of attempts made so far (including the current one). */
  attempts: number;

  /** Maximum attempts allowed before permanent failure. */
  maxAttempts: number;

  /** Initial backoff between retries in milliseconds. */
  backoffMs: number;

  /** Processor timeout in milliseconds. */
  timeoutMs: number;

  /**
   * Epoch milliseconds the job becomes available for processing.
   *
   * Drivers that support delayed dispatch compare this to `Date.now()`
   * during `pop()` — jobs with `availableAt > now` are skipped.
   */
  availableAt: number;

  /** Epoch milliseconds when the job was first enqueued. */
  createdAt: number;

  /** Epoch milliseconds when the job was last updated (attempts, status). */
  updatedAt: number;

  /**
   * Error message from the last failing attempt, if any.
   *
   * Kept as a plain string so JSON serialisation is stable across
   * drivers.
   */
  lastError?: string;

  /**
   * Optional deduplication key. See {@link JobOptions.uniqueId}.
   */
  uniqueId?: string;

  /** Descriptive tags for observability — not used for routing. */
  tags: string[];

  /**
   * Driver-specific opaque metadata.
   *
   * Drivers may stash connection-specific bookkeeping here (e.g. QStash
   * message id, IndexedDB cursor position).
   */
  driverMeta?: Record<string, unknown>;
}
