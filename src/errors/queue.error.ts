/**
 * @fileoverview Base error class for the queue package.
 *
 * Every error thrown by `@stackra/ts-queue` extends {@link QueueError}.
 * Having one shared base makes `instanceof` checks easy for consumers:
 * `err instanceof QueueError` will be `true` for any error raised inside
 * the package regardless of the specific subclass.
 *
 * @module errors/queue
 * @category Errors
 */

/**
 * Base error class for all errors thrown by the queue package.
 *
 * Provides a typed `code` property for programmatic handling and captures
 * the stack trace at construction time so the thrower frame is visible in
 * logs instead of the base-class frame.
 *
 * @example
 * ```typescript
 * try {
 *   await queue.push('sale.sync', payload);
 * } catch (error: Error | any) {
 *   if (error instanceof QueueError) {
 *     logger.error('Queue error:', error.code, error.message);
 *   }
 * }
 * ```
 */
export class QueueError extends Error {
  /** Error name for identification in logs and stack traces. */
  public readonly name: string = "QueueError";

  /** Machine-readable error code for programmatic handling. */
  public readonly code: string = "QUEUE_ERROR";

  /** Optional underlying cause that triggered this error. */
  public readonly cause?: Error;

  /**
   * Create a new QueueError.
   *
   * @param message - Human-readable error message.
   * @param cause   - Optional underlying error that caused this failure.
   */
  constructor(message: string, cause?: Error) {
    super(message);
    this.cause = cause;

    if (typeof (Error as any).captureStackTrace === "function") {
      (Error as any).captureStackTrace(this, this.constructor);
    }
  }
}
