/**
 * @fileoverview Memory driver configuration.
 *
 * @module interfaces/memory-connection-config
 * @category Interfaces
 */

import type { QueueDriverName } from "@stackra/contracts";

/**
 * Configuration for the memory driver.
 *
 * Jobs live in a `Map` keyed by job id. Nothing is persisted — restarting
 * the tab wipes every job. Ideal for tests and transient work where
 * durability is not required.
 */
export interface MemoryConnectionConfig {
  /** Driver discriminator. */
  driver: QueueDriverName.Memory | "memory";

  /**
   * Optional per-driver key prefix.
   *
   * Rarely needed for memory queues, but supported for parity with
   * persistent drivers so configs can be swapped without losing the
   * prefix.
   *
   * @default ""
   */
  prefix?: string;
}
