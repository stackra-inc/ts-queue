/**
 * @fileoverview Null driver configuration.
 *
 * @module interfaces/null-connection-config
 * @category Interfaces
 */

import type { QueueDriverName } from "@stackra/contracts";

/**
 * Configuration for the null driver.
 *
 * The null driver silently discards every job. Use it in SSR
 * environments, in storybook, or as a safe fallback when the intended
 * driver is unavailable.
 */
export interface NullConnectionConfig {
  /** Driver discriminator. */
  driver: QueueDriverName.Null | "null";
}
