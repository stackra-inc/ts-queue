/**
 * @fileoverview `useQueueManager` React hook.
 *
 * Returns the {@link QueueManager} — for components that need access to
 * multiple connections or to the list of configured connections.
 *
 * @module hooks/use-queue-manager
 * @category Hooks
 */

import { useInject } from "@stackra/ts-container/react";

import { QueueManager } from "@/services/queue-manager.service";

/**
 * Access the {@link QueueManager} directly from a React component.
 *
 * @example
 * ```tsx
 * function AdminPanel() {
 *   const queues = useQueueManager();
 *   const connections = queues.getConnectionNames();
 *   return <ul>{connections.map((c) => <li key={c}>{c}</li>)}</ul>;
 * }
 * ```
 */
export function useQueueManager(): QueueManager {
  return useInject(QueueManager);
}
