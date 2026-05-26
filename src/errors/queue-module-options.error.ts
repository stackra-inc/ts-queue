/**
 * Module-options validation error.
 *
 * Thrown by `QueueModule.forRoot()` when the supplied configuration is
 * malformed (missing `default`, empty `connections`, mismatched
 * default key, ...). Surfaces immediately at bootstrap so the
 * developer sees the problem rather than a confusing runtime failure
 * on first dispatch.
 *
 * @module @stackra/ts-queue/errors/queue-module-options
 */

import { QueueError } from './queue.error';

/**
 * Configuration validation error for the queue module.
 */
export class QueueModuleOptionsError extends QueueError {
  public override readonly name: string = 'QueueModuleOptionsError';
  public override readonly code: string = 'QUEUE_MODULE_OPTIONS_ERROR';
}
