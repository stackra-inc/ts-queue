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

import type { IJobOptions, IQueueConnection } from '@stackra/contracts';

/**
 * Bound handle scoping a queue tube on a single connection.
 */
export class QueueHandle {
  /**
   * @param connection - Underlying queue connection.
   * @param queue      - Queue tube name this handle is bound to.
   */
  public constructor(
    private readonly connection: IQueueConnection,
    public readonly queue: string
  ) {}

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
  public async push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string> {
    return this.connection.push(name, data, { ...options, queue: this.queue });
  }

  /**
   * Dispatch a job with a delay.
   *
   * @param delayMs - Delay in milliseconds before the job becomes eligible.
   */
  public async later<T = unknown>(
    delayMs: number,
    name: string,
    data: T,
    options?: IJobOptions
  ): Promise<string> {
    return this.connection.later(delayMs, name, data, { ...options, queue: this.queue });
  }

  /**
   * Bulk dispatch.
   *
   * @typeParam T - Payload type for every job.
   * @param jobs - Array of `{ name, data, options }` tuples.
   * @returns Array of dispatched job ids.
   */
  public async bulk<T = unknown>(
    jobs: Array<{ name: string; data: T; options?: IJobOptions }>
  ): Promise<string[]> {
    return this.connection.bulk(
      jobs.map((j) => ({
        ...j,
        options: { ...j.options, queue: this.queue },
      }))
    );
  }

  /**
   * Total in-flight job count for the bound queue.
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
   */
  public clear(): Promise<void> {
    return this.connection.clear(this.queue);
  }

  /**
   * Pause processing on the bound queue tube.
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
