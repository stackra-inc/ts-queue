/**
 * @fileoverview Queue driver error.
 *
 * Thrown whenever {@link QueueManager} cannot instantiate or resolve a
 * driver — unknown `driver` name, missing peer dependency, or invalid
 * per-connection configuration.
 *
 * @module errors/queue-driver
 * @category Errors
 */

import { QueueError } from "./queue.error";

/**
 * Error thrown when a queue driver cannot be resolved.
 *
 * Typical causes:
 * - The `driver` string in `QueueModuleOptions.connections` is not
 *   registered (typo, or `manager.extend()` was not called).
 * - A driver peer dependency is missing — for example, the `qstash`
 *   driver requires `@upstash/qstash` to be installed.
 * - Invalid driver-specific configuration — missing token, unreachable
 *   URL, unsupported option combination.
 *
 * @example
 * ```typescript
 * try {
 *   manager.connection('mysterious');
 * } catch (error: Error | any) {
 *   if (error instanceof QueueDriverError) {
 *     // Handle driver resolution failure
 *   }
 * }
 * ```
 */
export class QueueDriverError extends QueueError {
  public override readonly name: string = "QueueDriverError";
  public override readonly code: string = "QUEUE_DRIVER_ERROR";
}
