/**
 * @fileoverview Decorators barrel export.
 *
 * - {@link Processor} — marks a class as a queue processor.
 * - {@link OnJobEvent} — method listener for queue lifecycle events.
 * - {@link Job} — class-level dispatch option defaults.
 * - {@link InjectQueue} — inject a queue handle scoped to `(conn, queue)`.
 * - {@link InjectQueueConnection} — inject a driver by connection name.
 *
 * @module decorators
 * @category Decorators
 */

export { Processor } from "./processor.decorator";
export { OnJobEvent } from "./on-job-event.decorator";
export { Job } from "./job.decorator";
export { InjectQueue } from "./inject-queue.decorator";
export { InjectQueueConnection } from "./inject-queue-connection.decorator";
