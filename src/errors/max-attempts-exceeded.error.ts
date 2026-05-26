/**
 * @fileoverview Max attempts exceeded error.
 *
 * Raised by the worker when a job has failed more times than its
 * configured `tries` budget allows. The worker marks the job as
 * {@link JobStatus.Failed} and emits {@link QUEUE_EVENTS.JOB_FAILED};
 * this error is the cause attached to that event.
 *
 * @module errors/max-attempts-exceeded
 * @category Errors
 */

import { QueueError } from './queue.error';

/**
 * Error thrown when a job exceeds its retry budget.
 *
 * Mirrors Laravel's `MaxAttemptsExceededException`. Carries the job id
 * and attempt count so telemetry can differentiate retry-exhaustion from
 * first-attempt failures.
 *
 * @example
 * ```typescript
 * import { QUEUE_EVENTS } from '@stackra/contracts';
 *
 * @OnEvent(QUEUE_EVENTS.JOB_FAILED)
 * onFailed(payload: { job: QueuedJob; error: Error }): void {
 *   if (payload.error instanceof MaxAttemptsExceededError) {
 *     alerting.notifyOncall(payload.job.id);
 *   }
 * }
 * ```
 */
export class MaxAttemptsExceededError extends QueueError {
  public override readonly name: string = 'MaxAttemptsExceededError';
  public override readonly code: string = 'QUEUE_MAX_ATTEMPTS_EXCEEDED';

  /**
   * Create a new MaxAttemptsExceededError.
   *
   * @param jobId    - The job identifier.
   * @param attempts - Total attempts that were made before giving up.
   * @param cause    - The underlying exception from the last attempt.
   */
  constructor(
    public readonly jobId: string,
    public readonly attempts: number,
    cause?: Error
  ) {
    super(`Job [${jobId}] has been attempted too many times (${attempts} attempts).`, cause);
  }
}
