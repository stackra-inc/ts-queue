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

import { JobStatus } from '@stackra/contracts';
import { createQueuedJob } from '@/utils/create-queued-job.util';
import type { IJobOptions, IQueuedJob } from '@stackra/contracts';
import { BaseConnection } from './base.connection';

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
export class MemoryConnection extends BaseConnection {
  /** Every job lives here — pending, reserved, delayed, or failed. */
  private readonly jobs: Map<string, IQueuedJob> = new Map();

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
  public async push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string> {
    const job = createQueuedJob({ name, data, connection: this.name, options });

    // Enforce `uniqueFor` by inspecting in-flight jobs with the same
    // `uniqueId`. A job is "in-flight" if it's still pending, reserved,
    // processing, or delayed — terminal states don't block new dispatches.
    if (job.uniqueId) {
      for (const existing of this.jobs.values()) {
        if (
          existing.uniqueId === job.uniqueId &&
          existing.status !== JobStatus.Completed &&
          existing.status !== JobStatus.Failed
        ) {
          // Return the existing id so callers can treat dedup as "already
          // queued" rather than as a hard error.
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
  public async pop(queue: string = 'default'): Promise<IQueuedJob | null> {
    if (this.pausedQueues.has(queue)) return null;

    const now = Date.now();
    let next: IQueuedJob | undefined;

    // Linear scan — fine for the memory driver since it's intended for
    // short-lived queues. Drivers that might hold thousands of jobs use
    // indexed storage instead.
    for (const job of this.jobs.values()) {
      if (job.queue !== queue) continue;
      if (job.status !== JobStatus.Pending && job.status !== JobStatus.Delayed) continue;
      if (job.availableAt > now) continue;

      // Keep the oldest eligible job.
      if (!next || job.createdAt < next.createdAt) {
        next = job;
      }
    }

    if (!next) return null;

    // Mark the job reserved so concurrent `pop()` calls don't double-dispatch.
    next.status = JobStatus.Reserved;
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
  public async size(queue: string = 'default'): Promise<number> {
    let count = 0;
    for (const job of this.jobs.values()) {
      if (job.queue !== queue) continue;
      if (job.status === JobStatus.Completed || job.status === JobStatus.Failed) continue;
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
  public async pendingSize(queue: string = 'default'): Promise<number> {
    return this.countByStatus(queue, JobStatus.Pending);
  }

  /**
   * Number of jobs currently in the Delayed state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The delayed job count.
   */
  public async delayedSize(queue: string = 'default'): Promise<number> {
    return this.countByStatus(queue, JobStatus.Delayed);
  }

  /**
   * Number of jobs currently in the Reserved state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The reserved job count.
   */
  public async reservedSize(queue: string = 'default'): Promise<number> {
    return this.countByStatus(queue, JobStatus.Reserved);
  }

  /**
   * Remove a single job from the in-memory map.
   *
   * Called by the worker after a job is processed successfully. No-op
   * when the id is unknown — the worker may have already removed it.
   *
   * @param jobId - The id of the job to remove.
   */
  public async remove(jobId: string): Promise<void> {
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
  public async release(jobId: string, delayMs: number = 0): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;

    job.status = delayMs > 0 ? JobStatus.Delayed : JobStatus.Pending;
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
  public async fail(jobId: string, reason: string): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;

    job.status = JobStatus.Failed;
    job.lastError = reason;
    job.updatedAt = Date.now();
  }

  /**
   * Wipe every job belonging to a queue tube.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  public async clear(queue: string = 'default'): Promise<void> {
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
  public async close(): Promise<void> {
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
  private countByStatus(queue: string, status: JobStatus): number {
    let count = 0;
    for (const job of this.jobs.values()) {
      if (job.queue === queue && job.status === status) count++;
    }
    return count;
  }
}
