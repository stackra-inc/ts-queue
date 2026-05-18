/**
 * @fileoverview IndexedDB driver configuration.
 *
 * @module interfaces/indexeddb-connection-config
 * @category Interfaces
 */

import type { QueueDriverName } from "@stackra/contracts";

/**
 * Configuration for the IndexedDB driver.
 *
 * IndexedDB is the browser default for offline-first queues: it survives
 * reloads, handles large payloads, supports efficient range queries for
 * delayed job scheduling, and is transactional per-operation.
 *
 * The driver uses a single database with one object store per queue name.
 * Indexes on `availableAt` and `status` keep `pop()` and `delayedSize()`
 * fast even at tens of thousands of jobs.
 */
export interface IndexedDBConnectionConfig {
  /** Driver discriminator. */
  driver: QueueDriverName.IndexedDB | "indexeddb";

  /**
   * IndexedDB database name.
   *
   * Pick something unique per application so you don't collide with
   * other libraries that also use IndexedDB.
   *
   * @default "stackra-queue"
   */
  dbName?: string;

  /**
   * Database version. Bump this when the driver schema changes so the
   * migration path runs. Consumers should not need to change this — it
   * is maintained by the driver implementation.
   *
   * @default 1
   */
  dbVersion?: number;

  /**
   * Object store prefix — useful when sharing a database with other
   * consumers who also create stores.
   *
   * @default ""
   */
  prefix?: string;
}
