/**
 * @fileoverview `useQueue` React hook.
 *
 * Returns a {@link QueueHandle} for the given `(connection, queue)` pair.
 * Thin wrapper around `useInject(QueueManager)` that keeps React call
 * sites concise.
 *
 * @module hooks/use-queue
 * @category Hooks
 */

import { useInject } from "@stackra/ts-container/react";

import { QueueManager } from "@/services/queue-manager.service";
import type { QueueHandle } from "@/services/queue-handle.service";

/**
 * Access a queue handle from a React component.
 *
 * @param queue      - Queue tube name (defaults to `"default"`).
 * @param connection - Optional connection name (defaults to configured default).
 * @returns A {@link QueueHandle}.
 *
 * @example
 * ```tsx
 * function EnqueueButton() {
 *   const queue = useQueue('tracking');
 *   return <button onClick={() => queue.push('click', {})}>Track click</button>;
 * }
 * ```
 */
export function useQueue(queue?: string, connection?: string): QueueHandle {
  const manager = useInject(QueueManager);
  return manager.queue(queue, connection);
}
