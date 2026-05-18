/**
 * @fileoverview A typed handle bound to one `(connection, queue)` pair.
 *
 * `@InjectQueue('scans')` resolves to one of these. The handle forwards
 * `push/later/size/clear` to the underlying {@link QueueConnection} with
 * the `queue` option pre-applied, so callers don't have to repeat the
 * queue name at every dispatch site.
 *
 * @module services/queue-handle
 * @category Services
 */

import type { JobOptions } from "@/interfaces/job-options.interface";
import type { QueueConnection } from "@/interfaces/queue-connection.interface";

/**
 * Thin wrapper around a {@link QueueConnection} scoped to a queue tube.
 *
 * Keeping this as a class (not a proxy object) gives `instanceof`
 * semantics and makes the API surface visible in IDE autocomplete.
 */
export class QueueHandle {
  /**
   * @param connection - The underlying driver.
   * @param queue      - Queue tube name this handle is bound to.
   */
  constructor(
    private readonly connection: QueueConnection,
    public readonly queue: string,
  ) {}

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
  public async push<T = unknown>(name: string, data: T, options?: JobOptions): Promise<string> {
    return this.connection.push(name, data, { ...options, queue: this.queue });
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
  public async later<T = unknown>(
    delayMs: number,
    name: string,
    data: T,
    options?: JobOptions,
  ): Promise<string> {
    return this.connection.later(delayMs, name, data, { ...options, queue: this.queue });
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
  public async bulk<T = unknown>(
    jobs: Array<{ name: string; data: T; options?: JobOptions }>,
  ): Promise<string[]> {
    return this.connection.bulk(
      jobs.map((j) => ({
        ...j,
        options: { ...j.options, queue: this.queue },
      })),
    );
  }

  /**
   * Total in-flight job count for the bound queue.
   *
   * Includes pending, delayed, and reserved jobs — terminal states
   * (Completed/Failed) are excluded.
   *
   * @returns The number of in-flight jobs.
   */
  public size(): Promise<number> {
    return this.connection.size(this.queue);
  }

  /**
   * Pending-only count.
   *
   * @returns The number of jobs in the Pending state.
   */
  public pendingSize(): Promise<number> {
    return this.connection.pendingSize(this.queue);
  }

  /**
   * Delayed-only count.
   *
   * @returns The number of jobs in the Delayed state.
   */
  public delayedSize(): Promise<number> {
    return this.connection.delayedSize(this.queue);
  }

  /**
   * Reserved-only count.
   *
   * @returns The number of jobs in the Reserved state.
   */
  public reservedSize(): Promise<number> {
    return this.connection.reservedSize(this.queue);
  }

  /**
   * Wipe every job on the bound queue tube.
   *
   * Useful in tests — call this in `beforeEach`/`afterEach` to keep
   * runs isolated.
   */
  public clear(): Promise<void> {
    return this.connection.clear(this.queue);
  }

  /**
   * Pause processing on the bound queue tube.
   *
   * Workers polling this tube will see `pop()` return `null` until
   * resumed.
   */
  public pause(): Promise<void> {
    return this.connection.pause(this.queue);
  }

  /**
   * Resume processing on the bound queue tube.
   */
  public resume(): Promise<void> {
    return this.connection.resume(this.queue);
  }

  /**
   * Whether the bound queue tube is currently paused.
   *
   * @returns `true` when paused, `false` otherwise.
   */
  public isPaused(): Promise<boolean> {
    return this.connection.isPaused(this.queue);
  }
}
