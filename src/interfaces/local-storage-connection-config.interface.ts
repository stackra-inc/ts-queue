/**
 * @fileoverview LocalStorage driver configuration.
 *
 * @module interfaces/local-storage-connection-config
 * @category Interfaces
 */

import type { QueueDriverName } from "@stackra/contracts";

/**
 * Configuration for the `localStorage` driver.
 *
 * Persists jobs as a single JSON blob under one storage key per queue.
 * Best for small payloads and low-volume queues — `localStorage` is
 * synchronous and capped at ~5 MB across all keys for the origin.
 *
 * For larger payloads or higher throughput, use the `indexeddb` driver.
 */
export interface LocalStorageConnectionConfig {
  /** Driver discriminator. */
  driver: QueueDriverName.LocalStorage | "local-storage";

  /**
   * Key prefix for the storage entries.
   *
   * Full key format: `${prefix}queue:${connectionName}:${queueName}`.
   *
   * @default ""
   */
  prefix?: string;
}
