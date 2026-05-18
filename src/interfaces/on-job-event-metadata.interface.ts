/**
 * @fileoverview `@OnJobEvent` decorator metadata shape.
 *
 * @module interfaces/on-job-event-metadata
 * @category Interfaces
 */

import type { QueueEventDetailName as QueueEventName } from "@stackra/contracts";

/**
 * Metadata stored on processor methods by the `@OnJobEvent` decorator.
 *
 * Consumed by the bootstrap loader which wires each method to the
 * matching event on the `EventManager`. Multiple method decorators
 * accumulate into an array under the same metadata key.
 */
export interface OnJobEventMetadata {
  /** The lifecycle event name — see {@link QueueEvent}. */
  event: QueueEventName;

  /**
   * Restrict to events for a specific connection. When omitted, the
   * handler fires for jobs on any connection.
   */
  connection?: string;
}
