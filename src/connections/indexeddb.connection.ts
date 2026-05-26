/**
 * @fileoverview IndexedDB driver.
 *
 * The browser default for offline-first queues. Uses one object store per
 * connection (`queue:${connection}`) with indexes on `queue`,
 * `availableAt`, and `status` so `pop()` and delayed-job scans stay fast
 * at tens of thousands of jobs.
 *
 * Pause state is stored in a second object store (`meta:${connection}`).
 * All operations go through transactions to keep the on-disk state
 * consistent even when multiple tabs touch the same database (combined
 * with the broadcast-channel driver for leader election).
 *
 * @module connections/indexeddb
 * @category Connections
 */

import { JobStatus } from "@stackra/contracts";
import { createQueuedJob } from "@/utils/create-queued-job.util";
import type { IJobOptions } from "@stackra/contracts";
import type { IQueuedJob } from "@stackra/contracts";
import { BaseConnection } from "./base.connection";

/** Metadata record shape stored in the `meta` object store. */
interface QueueMetaRecord {
  /**
   * The queue tube name.
   *
   * Acts as the object store keyPath, so the record is keyed by tube.
   */
  queue: string;
  /**
   * Whether the tube is currently paused.
   *
   * Workers consult this flag before each `pop()` to skip processing
   * when the tube is paused.
   */
  paused: boolean;
}

/**
 * IndexedDB-backed queue driver.
 *
 * @example
 * ```typescript
 * const conn = new IndexedDBConnection('idx', {
 *   dbName: 'app-queue',
 *   dbVersion: 1,
 *   prefix: 'app:',
 * });
 * await conn.push('sale.sync', saleData, { tries: 5 });
 * ```
 */
export class IndexedDBConnection extends BaseConnection {
  /** Cached database handle, opened lazily on first operation. */
  private dbPromise: Promise<IDBDatabase> | null = null;

  /**
   * @param name    - Connection name from module config.
   * @param dbName  - IndexedDB database name.
   * @param version - Schema version (bumped when the driver changes schema).
   * @param prefix  - Object store name prefix.
   */
  constructor(
    name: string,
    private readonly dbName: string = "stackra-queue",
    private readonly version: number = 1,
    private readonly prefix: string = "",
  ) {
    super(name);
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
  public async push<T = unknown>(jobName: string, data: T, options?: IJobOptions): Promise<string> {
    const job = createQueuedJob({ name: jobName, data, connection: this.name, options });
    const db = await this.openDb();

    // Uniqueness check + insert in one transaction so concurrent pushes
    // never both succeed when they shouldn't.
    return new Promise<string>((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
      const store = tx.objectStore(this.jobsStore());

      const finish = (id: string) => resolve(id);
      tx.onerror = () => reject(tx.error);

      if (!job.uniqueId) {
        store.put(job);
        tx.oncomplete = () => finish(job.id);
        return;
      }

      // Walk jobs that share the uniqueId. The `uniqueId` index is defined
      // in `upgradeNeeded` below.
      const idx = store.index("uniqueId");
      const req = idx.openCursor(IDBKeyRange.only(job.uniqueId));
      let existingId: string | null = null;

      req.onsuccess = () => {
        const cursor = req.result;
        if (cursor) {
          const existing = cursor.value as IQueuedJob;
          if (existing.status !== JobStatus.Completed && existing.status !== JobStatus.Failed) {
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
  public async pop(queue: string = "default"): Promise<IQueuedJob | null> {
    if (await this.isPaused(queue)) return null;

    const db = await this.openDb();
    const now = Date.now();

    return new Promise<IQueuedJob | null>((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
      const store = tx.objectStore(this.jobsStore());

      // Use the queue index to iterate only jobs belonging to this tube,
      // oldest first (insertion-ordered since `id` is unique and we sort
      // by `createdAt` afterwards).
      const idx = store.index("queue");
      const req = idx.openCursor(IDBKeyRange.only(queue));

      let chosen: IQueuedJob | null = null;

      req.onsuccess = () => {
        const cursor = req.result;
        if (!cursor) return;

        const job = cursor.value as IQueuedJob;
        const eligible =
          (job.status === JobStatus.Pending || job.status === JobStatus.Delayed) &&
          job.availableAt <= now;

        if (eligible && (!chosen || job.createdAt < chosen.createdAt)) {
          chosen = job;
        }
        cursor.continue();
      };

      tx.onerror = () => reject(tx.error);
      tx.oncomplete = () => {
        if (!chosen) return resolve(null);

        // Mark the chosen job Reserved in a follow-up transaction.
        const tx2 = db.transaction([this.jobsStore()], "readwrite");
        const store2 = tx2.objectStore(this.jobsStore());
        const reserved: IQueuedJob = {
          ...(chosen as IQueuedJob),
          status: JobStatus.Reserved,
          attempts: (chosen as IQueuedJob).attempts + 1,
          updatedAt: Date.now(),
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
  public async size(queue: string = "default"): Promise<number> {
    return this.countMatching(
      queue,
      (j) => j.status !== JobStatus.Completed && j.status !== JobStatus.Failed,
    );
  }

  /**
   * Number of jobs currently in the Pending state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The pending job count.
   */
  public async pendingSize(queue: string = "default"): Promise<number> {
    return this.countMatching(queue, (j) => j.status === JobStatus.Pending);
  }

  /**
   * Number of jobs currently in the Delayed state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The delayed job count.
   */
  public async delayedSize(queue: string = "default"): Promise<number> {
    return this.countMatching(queue, (j) => j.status === JobStatus.Delayed);
  }

  /**
   * Number of jobs currently in the Reserved state on a queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns The reserved job count.
   */
  public async reservedSize(queue: string = "default"): Promise<number> {
    return this.countMatching(queue, (j) => j.status === JobStatus.Reserved);
  }

  /**
   * Permanently remove a job from the object store.
   *
   * Called by the worker after successful processing. No-op when the
   * id is unknown.
   *
   * @param jobId - The id of the job to remove.
   */
  public async remove(jobId: string): Promise<void> {
    const db = await this.openDb();
    return new Promise<void>((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
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
  public async release(jobId: string, delayMs: number = 0): Promise<void> {
    return this.mutate(jobId, (job) => {
      job.status = delayMs > 0 ? JobStatus.Delayed : JobStatus.Pending;
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
  public async fail(jobId: string, reason: string): Promise<void> {
    return this.mutate(jobId, (job) => {
      job.status = JobStatus.Failed;
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
  public async clear(queue: string = "default"): Promise<void> {
    const db = await this.openDb();
    return new Promise<void>((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
      const store = tx.objectStore(this.jobsStore());
      const idx = store.index("queue");
      const req = idx.openKeyCursor(IDBKeyRange.only(queue));

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
  public override async pause(queue: string = "default"): Promise<void> {
    await this.writeMeta({ queue, paused: true });
  }

  /**
   * Clear the paused flag for a queue tube.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  public override async resume(queue: string = "default"): Promise<void> {
    await this.writeMeta({ queue, paused: false });
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
  public override async isPaused(queue: string = "default"): Promise<boolean> {
    const record = await this.readMeta(queue);
    return record?.paused ?? false;
  }

  /**
   * Close the IndexedDB handle and forget the lazy promise.
   *
   * Required during teardown so tests don't leak open databases and
   * hot-reload doesn't accumulate handles.
   */
  public async close(): Promise<void> {
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
  private jobsStore(): string {
    return `${this.prefix}jobs:${this.name}`;
  }

  /**
   * Build the object store name for per-queue metadata (pause flag).
   *
   * @returns The fully qualified store name.
   */
  private metaStore(): string {
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
  private openDb(): Promise<IDBDatabase> {
    if (this.dbPromise) return this.dbPromise;

    this.dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
      if (typeof indexedDB === "undefined") {
        reject(new Error("IndexedDB is not available in this environment."));
        return;
      }

      const req = indexedDB.open(this.dbName, this.version);

      req.onupgradeneeded = () => {
        const db = req.result;

        if (!db.objectStoreNames.contains(this.jobsStore())) {
          const store = db.createObjectStore(this.jobsStore(), { keyPath: "id" });
          // Indexes used by pop/clear/uniqueness checks. Names chosen to
          // be obvious in devtools when debugging storage state.
          store.createIndex("queue", "queue", { unique: false });
          store.createIndex("status", "status", { unique: false });
          store.createIndex("availableAt", "availableAt", { unique: false });
          store.createIndex("uniqueId", "uniqueId", { unique: false });
        }

        if (!db.objectStoreNames.contains(this.metaStore())) {
          db.createObjectStore(this.metaStore(), { keyPath: "queue" });
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
  private async countMatching(
    queue: string,
    predicate: (job: IQueuedJob) => boolean,
  ): Promise<number> {
    const db = await this.openDb();
    return new Promise<number>((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readonly");
      const idx = tx.objectStore(this.jobsStore()).index("queue");
      const req = idx.openCursor(IDBKeyRange.only(queue));

      let count = 0;
      req.onsuccess = () => {
        const cursor = req.result;
        if (!cursor) return;
        if (predicate(cursor.value as IQueuedJob)) count++;
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
  private async mutate(jobId: string, mutator: (job: IQueuedJob) => void): Promise<void> {
    const db = await this.openDb();
    return new Promise<void>((resolve, reject) => {
      const tx = db.transaction([this.jobsStore()], "readwrite");
      const store = tx.objectStore(this.jobsStore());
      const getReq = store.get(jobId);

      getReq.onsuccess = () => {
        const job = getReq.result as IQueuedJob | undefined;
        if (!job) return; // nothing to mutate
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
  private async readMeta(queue: string): Promise<QueueMetaRecord | null> {
    const db = await this.openDb();
    return new Promise<QueueMetaRecord | null>((resolve, reject) => {
      const tx = db.transaction([this.metaStore()], "readonly");
      const req = tx.objectStore(this.metaStore()).get(queue);
      req.onsuccess = () => resolve((req.result as QueueMetaRecord | undefined) ?? null);
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
  private async writeMeta(record: QueueMetaRecord): Promise<void> {
    const db = await this.openDb();
    return new Promise<void>((resolve, reject) => {
      const tx = db.transaction([this.metaStore()], "readwrite");
      tx.objectStore(this.metaStore()).put(record);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }
}
