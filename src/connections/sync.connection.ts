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

import { JobStatus } from "@stackra/contracts";
import { generateJobId } from "@/utils/generate-job-id.util";
import { createQueuedJob } from "@/utils/create-queued-job.util";
import type { IJobOptions } from "@stackra/contracts";
import type { SyncJobHandler } from "@/types/sync-job-handler.type";
import type { IQueuedJob } from "@stackra/contracts";
import { BaseConnection } from "./base.connection";

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
export class SyncConnection extends BaseConnection {
  /** The synchronous processor resolver, if one has been registered. */
  private handler?: SyncJobHandler;

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
  public setHandler(handler: SyncJobHandler): void {
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
  public async push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string> {
    const job = createQueuedJob({
      name,
      data,
      connection: this.name,
      options,
    });

    // Skip processing when paused — return the id as if the job were
    // queued so callers don't see surprising behaviour.
    if (this.pausedQueues.has(job.queue)) return job.id;

    // When no handler is registered, the sync driver degrades to the null
    // driver. This is useful for tests that don't care about processor
    // side-effects.
    if (!this.handler) return job.id;

    job.status = JobStatus.Reserved;
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
  public async pop(): Promise<IQueuedJob | null> {
    return null;
  }

  /**
   * Always-zero job count.
   *
   * @returns `0`.
   */
  public async size(): Promise<number> {
    return 0;
  }

  /**
   * Always-zero pending count.
   *
   * @returns `0`.
   */
  public async pendingSize(): Promise<number> {
    return 0;
  }

  /**
   * Always-zero delayed count.
   *
   * @returns `0`.
   */
  public async delayedSize(): Promise<number> {
    return 0;
  }

  /**
   * Always-zero reserved count.
   *
   * @returns `0`.
   */
  public async reservedSize(): Promise<number> {
    return 0;
  }

  /**
   * No-op remove.
   *
   * Sync jobs never enter a queue, so removal is meaningless.
   */
  public async remove(): Promise<void> {
    /* noop — sync jobs never leave the driver. */
  }

  /**
   * No-op release.
   *
   * Sync jobs never enter a queue, so release is meaningless.
   */
  public async release(): Promise<void> {
    /* noop */
  }

  /**
   * No-op fail.
   *
   * Failures propagate from the handler in `push()` — there is nothing
   * to record after the fact.
   */
  public async fail(): Promise<void> {
    /* noop */
  }

  /**
   * No-op clear.
   *
   * Sync jobs never enter a queue, so clearing is meaningless.
   */
  public async clear(): Promise<void> {
    /* noop */
  }

  /**
   * Tear down the driver.
   *
   * Drops the registered handler so the next `push()` degrades to the
   * null-driver behaviour. Used in tests to reset between cases.
   */
  public async close(): Promise<void> {
    this.handler = undefined;
    void generateJobId; // keep import used — the helper is needed in tests
  }
}
