/**
 * @fileoverview Timeout exceeded error.
 *
 * Raised by the worker when a processor runs longer than the job's
 * `timeout` setting. What happens next depends on `failOnTimeout`: if
 * true, the job is marked {@link JobStatus.Failed}; otherwise it is
 * released back to the queue for retry.
 *
 * @module errors/timeout-exceeded
 * @category Errors
 */

import { QueueError } from "./queue.error";

/**
 * Error thrown when a job processor exceeds its configured timeout.
 *
 * Mirrors Laravel's `TimeoutExceededException`. Includes the job id and
 * elapsed milliseconds so instrumentation can track slow processors.
 *
 * @example
 * ```typescript
 * @OnEvent(QueueEvent.JobTimedOut)
 * onTimedOut(payload: { job: QueuedJob; error: TimeoutExceededError }) {
 *   metrics.increment('queue.timeouts', { name: payload.job.name });
 * }
 * ```
 */
export class TimeoutExceededError extends QueueError {
  public override readonly name: string = "TimeoutExceededError";
  public override readonly code: string = "QUEUE_TIMEOUT_EXCEEDED";

  /**
   * Create a new TimeoutExceededError.
   *
   * @param jobId       - The job identifier.
   * @param elapsedMs   - How long the processor ran before being killed.
   * @param timeoutMs   - The configured timeout in milliseconds.
   */
  constructor(
    public readonly jobId: string,
    public readonly elapsedMs: number,
    public readonly timeoutMs: number,
  ) {
    super(`Job [${jobId}] exceeded its timeout after ${elapsedMs}ms (configured: ${timeoutMs}ms).`);
  }
}
