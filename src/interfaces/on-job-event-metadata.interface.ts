/**
 * `@OnJobEvent` decorator metadata shape.
 *
 * Internal — used only by the `@OnJobEvent` decorator and the
 * processor-subscribers loader. Not promoted to contracts because
 * no other package needs to type-check against it.
 *
 * @module @stackra/ts-queue/interfaces/on-job-event-metadata
 */

import type { QueueEventName } from '@stackra/contracts';

/**
 * Metadata stored on processor methods by the `@OnJobEvent` decorator.
 *
 * Multiple decorators on the same method accumulate into an array
 * under the metadata key, so the loader inspects the array shape and
 * subscribes each entry independently.
 */
export interface IOnJobEventMetadata {
  /** Lifecycle event name. */
  event: QueueEventName;

  /**
   * Restrict the listener to events for a specific connection. When
   * omitted, the handler fires for jobs on any connection.
   */
  connection?: string;
}
