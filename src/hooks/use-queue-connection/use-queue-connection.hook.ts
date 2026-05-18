/**
 * @fileoverview `useQueueConnection` React hook.
 *
 * Returns a raw {@link QueueConnection} — useful for advanced operations
 * like pausing/resuming a whole connection or pushing to a non-default
 * queue tube without going through a {@link QueueHandle}.
 *
 * @module hooks/use-queue-connection
 * @category Hooks
 */

import { useInject } from "@stackra/ts-container/react";

import { QueueManager } from "@/services/queue-manager.service";
import type { QueueConnection } from "@/interfaces/queue-connection.interface";

/**
 * Access a {@link QueueConnection} from a React component.
 *
 * @param name - Connection name. Defaults to the module default.
 *
 * @example
 * ```tsx
 * function PauseButton() {
 *   const conn = useQueueConnection('indexeddb');
 *   return <button onClick={() => conn.pause('scans')}>Pause scans</button>;
 * }
 * ```
 */
export function useQueueConnection(name?: string): QueueConnection {
  const manager = useInject(QueueManager);
  return manager.connection(name);
}
