/**
 * @fileoverview Sync driver configuration.
 *
 * @module interfaces/sync-connection-config
 * @category Interfaces
 */

import type { QueueDriverName } from "@stackra/contracts";

/**
 * Configuration for the sync driver.
 *
 * The sync driver executes each job's processor inline on `push()` — no
 * queue, no worker, no delay, no retry. Useful for deterministic test
 * scenarios and for scripts where async behaviour would mask bugs.
 *
 * Because processing happens inline, the event lifecycle still fires
 * (`JobProcessing`, `JobProcessed`/`JobFailed`) so downstream observers
 * behave identically to durable drivers.
 */
export interface SyncConnectionConfig {
  /** Driver discriminator. */
  driver: QueueDriverName.Sync | "sync";
}
