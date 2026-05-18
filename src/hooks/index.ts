/**
 * @fileoverview Hooks barrel export.
 *
 * - {@link useQueue}            — get a {@link QueueHandle} bound to `(conn, queue)`.
 * - {@link useQueueConnection}  — get a raw {@link QueueConnection}.
 * - {@link useQueueManager}     — get the {@link QueueManager} directly.
 *
 * @module hooks
 * @category Hooks
 */

export { useQueue } from "./use-queue/use-queue.hook";
export { useQueueConnection } from "./use-queue-connection/use-queue-connection.hook";
export { useQueueManager } from "./use-queue-manager/use-queue-manager.hook";
