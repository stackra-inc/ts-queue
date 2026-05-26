/**
 * `useQueueManager` React hook.
 *
 * Resolves the `QueueManager` for components that need access to
 * multiple connections or to module introspection.
 *
 * @module @stackra/ts-queue/react/hooks/use-queue-manager
 */

import { useInject } from '@stackra/ts-container/react';

import { QueueManager } from '@/services/queue-manager.service';

/**
 * Access the `QueueManager` directly from a React component.
 *
 * @returns The `QueueManager` instance from DI.
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
