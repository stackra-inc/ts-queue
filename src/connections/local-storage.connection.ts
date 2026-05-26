/**
 * @fileoverview LocalStorage driver.
 *
 * Persists the job map for each queue tube in a single JSON blob under
 * `${prefix}queue:${connection}:${queue}`. Synchronous storage is fine
 * for small payloads — typically tracking pixels, autosave debouncers,
 * UI preferences.
 *
 * Per-queue isolation keeps the serialisation cost linear in the queue
 * being read/written, not the entire app's queue traffic.
 *
 * @module connections/local-storage
 * @category Connections
 */

import { JobStatus } from "@stackra/contracts";
import { createQueuedJob } from "@/utils/create-queued-job.util";
import type { IJobOptions } from "@stackra/contracts";
import type { IQueuedJob } from "@stackra/contracts";
import { BaseConnection } from "./base.connection";

import { Str } from "@stackra/ts-support";
/** Local shape for per-queue storage blobs. */
interface StorageBlob {
  /** All jobs persisted under this queue tube. */
  jobs: IQueuedJob[];
  /** Whether processing on this queue tube is currently paused. */
  paused: boolean;
}

/**
 * localStorage-backed queue driver.
 *
 * @example
 * ```typescript
 * const conn = new LocalStorageConnection('ls', { prefix: 'app:' });
 * await conn.push('track', { event: 'click' });
 * ```
 */
export class LocalStorageConnection extends BaseConnection {
  /**
   * @param name   - Connection name from module config.
   * @param prefix - Optional key prefix. Keys become
   *   `${prefix}queue:${name}:${queueName}`.
   */
  constructor(
    name: string,
    private readonly prefix: string = "",
  ) {
    super(name);
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
  public async push<T = unknown>(jobName: string, data: T, options?: IJobOptions): Promise<string> {
    const job = createQueuedJob({ name: jobName, data, connection: this.name, options });
    const blob = this.read(job.queue);

    // Short-circuit on unique-id collisions — same as the memory driver.
    if (job.uniqueId) {
      const existing = blob.jobs.find(
        (j) =>
          j.uniqueId === job.uniqueId &&
          j.status !== JobStatus.Completed &&
          j.status !== JobStatus.Failed,
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
  public async pop(queue: string = "default"): Promise<IQueuedJob | null> {
    const blob = this.read(queue);
    if (blob.paused) return null;

    const now = Date.now();

    // Find the oldest eligible job (Pending or Delayed with availableAt in the past).
    let chosen: IQueuedJob | undefined;
    for (const job of blob.jobs) {
      const isEligible =
        (job.status === JobStatus.Pending || job.status === JobStatus.Delayed) &&
        job.availableAt <= now;
      if (!isEligible) continue;
      if (!chosen || job.createdAt < chosen.createdAt) chosen = job;
    }

    if (!chosen) return null;

    chosen.status = JobStatus.Reserved;
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
  public async size(queue: string = "default"): Promise<number> {
    const blob = this.read(queue);
    return blob.jobs.filter(
      (j) => j.status !== JobStatus.Completed && j.status !== JobStatus.Failed,
    ).length;
  }

  /**
   * Number of jobs currently in the Pending state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The pending job count.
   */
  public async pendingSize(queue: string = "default"): Promise<number> {
    return this.countByStatus(queue, JobStatus.Pending);
  }

  /**
   * Number of jobs currently in the Delayed state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The delayed job count.
   */
  public async delayedSize(queue: string = "default"): Promise<number> {
    return this.countByStatus(queue, JobStatus.Delayed);
  }

  /**
   * Number of jobs currently in the Reserved state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The reserved job count.
   */
  public async reservedSize(queue: string = "default"): Promise<number> {
    return this.countByStatus(queue, JobStatus.Reserved);
  }

  /**
   * Permanently remove a job from storage.
   *
   * Iterates every queue tube the prefix owns to find the job, since
   * the localStorage layout is per-queue rather than per-job.
   *
   * @param jobId - The id of the job to remove.
   */
  public async remove(jobId: string): Promise<void> {
    // We don't know which queue the job belongs to without scanning. Loop
    // over every known queue tube the prefix holds.
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
  public async release(jobId: string, delayMs: number = 0): Promise<void> {
    for (const queue of this.listQueues()) {
      const blob = this.read(queue);
      const job = blob.jobs.find((j) => j.id === jobId);
      if (!job) continue;

      job.status = delayMs > 0 ? JobStatus.Delayed : JobStatus.Pending;
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
  public async fail(jobId: string, reason: string): Promise<void> {
    for (const queue of this.listQueues()) {
      const blob = this.read(queue);
      const job = blob.jobs.find((j) => j.id === jobId);
      if (!job) continue;

      job.status = JobStatus.Failed;
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
  public async clear(queue: string = "default"): Promise<void> {
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
  public override async pause(queue: string = "default"): Promise<void> {
    const blob = this.read(queue);
    blob.paused = true;
    this.write(queue, blob);
  }

  /**
   * Clear the paused flag for a queue tube.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  public override async resume(queue: string = "default"): Promise<void> {
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
  public override async isPaused(queue: string = "default"): Promise<boolean> {
    return this.read(queue).paused;
  }

  /**
   * No-op close.
   *
   * localStorage doesn't expose an explicit handle to release.
   */
  public async close(): Promise<void> {
    /* localStorage doesn't need explicit closing */
  }

  // ── Storage helpers ────────────────────────────────────────────────────

  /**
   * Build the storage key for a given queue tube.
   *
   * @param queue - Queue tube name.
   * @returns The fully prefixed localStorage key.
   */
  private keyFor(queue: string): string {
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
  private read(queue: string): StorageBlob {
    if (typeof localStorage === "undefined") return { jobs: [], paused: false };

    try {
      const raw = localStorage.getItem(this.keyFor(queue));
      if (!raw) return { jobs: [], paused: false };
      const parsed = JSON.parse(raw) as StorageBlob;
      return {
        jobs: Array.isArray(parsed.jobs) ? parsed.jobs : [],
        paused: Boolean(parsed.paused),
      };
    } catch {
      // Corrupted entry — start fresh instead of throwing.
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
  private write(queue: string, blob: StorageBlob): void {
    if (typeof localStorage === "undefined") return;
    try {
      localStorage.setItem(this.keyFor(queue), JSON.stringify(blob));
    } catch {
      // Quota exceeded — discard. Callers can detect this via size() if
      // they care to handle it (none do today).
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
  private listQueues(): string[] {
    if (typeof localStorage === "undefined") return [];
    const out: string[] = [];
    const prefix = `${this.prefix}queue:${this.name}:`;
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && Str.startsWith(key, prefix)) {
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
  private countByStatus(queue: string, status: JobStatus): number {
    return this.read(queue).jobs.filter((j) => j.status === status).length;
  }
}
