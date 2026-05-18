/**
 * @fileoverview `@OnJobEvent(event, connection?)` method decorator.
 *
 * Marks a method as a lifecycle event listener on a processor class.
 * Accumulates metadata using `updateMetadata` so a single method can
 * carry multiple `@OnJobEvent` decorators (rare, but supported — e.g.
 * one method handling both `processed` and `failed`).
 *
 * @module decorators/on-job-event
 * @category Decorators
 */

import { updateMetadata } from "@vivtel/metadata";

import { ON_JOB_EVENT_METADATA } from "@/constants/tokens.constant";
import type { QueueEventDetailName as QueueEventName } from "@stackra/contracts";
import type { OnJobEventMetadata } from "@/interfaces/on-job-event-metadata.interface";

/**
 * Mark a method as a queue lifecycle event listener.
 *
 * @param event      - Queue event name from {@link QueueEvent}.
 * @param connection - Optional connection name to scope the listener to.
 *
 * @example
 * ```typescript
 * @Processor('tracking')
 * class PixelProcessor extends WorkerHost {
 *   async process(job: QueuedJob) { ... }
 *
 *   @OnJobEvent(QueueEvent.JobFailed)
 *   onFailed(payload: { job: QueuedJob; error: Error }) {
 *     logger.warn(`Pixel job ${payload.job.id} failed`, payload.error);
 *   }
 * }
 * ```
 */
export function OnJobEvent(event: QueueEventName, connection?: string): MethodDecorator {
  return (_target, _key, descriptor) => {
    updateMetadata<OnJobEventMetadata[]>(
      ON_JOB_EVENT_METADATA,
      [],
      (items) => [...items, { event, connection }],
      descriptor.value as object,
    );
    return descriptor;
  };
}
