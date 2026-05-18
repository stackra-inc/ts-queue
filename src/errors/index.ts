/**
 * @fileoverview Errors barrel export.
 *
 * Exported error classes:
 * - {@link QueueError} — shared base for every queue error.
 * - {@link QueueDriverError} — driver cannot be resolved or instantiated.
 * - {@link MaxAttemptsExceededError} — job exhausted its retry budget.
 * - {@link TimeoutExceededError} — processor ran longer than allowed.
 *
 * @module errors
 * @category Errors
 */

export { QueueError } from "./queue.error";
export { QueueDriverError } from "./queue-driver.error";
export { MaxAttemptsExceededError } from "./max-attempts-exceeded.error";
export { TimeoutExceededError } from "./timeout-exceeded.error";
