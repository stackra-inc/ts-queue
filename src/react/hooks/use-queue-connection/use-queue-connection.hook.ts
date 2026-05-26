/**
 * `useQueueConnection` React hook.
 *
 * Resolves a raw `IQueueConnection` for advanced operations like
 * pausing a whole connection or pushing to a non-default queue tube
 * without going through a `QueueHandle`.
 *
 * @module @stackra/ts-queue/react/hooks/use-queue-connection
 */

import { useEffect, useState } from 'react';
import { useInject } from '@stackra/ts-container/react';
import type { IQueueConnection } from '@stackra/contracts';

import { QueueManager } from '@/services/queue-manager.service';

/**
 * Result of {@link useQueueConnection}.
 */
export interface IUseQueueConnectionResult {
  /** Resolved connection (`null` until ready). */
  connection: IQueueConnection | null;
  /** Whether the connection has been resolved. */
  ready: boolean;
  /** Setup error, if any. */
  error: Error | null;
}

/**
 * Access an `IQueueConnection` from a React component.
 *
 * @param name - Connection name. Defaults to module default.
 * @returns Result object with `connection`, `ready`, `error`.
 *
 * @example
 * ```tsx
 * function PauseButton() {
 *   const { connection, ready } = useQueueConnection('indexeddb');
 *   const onClick = () => connection?.pause('scans');
 *   return <button disabled={!ready} onClick={onClick}>Pause</button>;
 * }
 * ```
 */
export function useQueueConnection(name?: string): IUseQueueConnectionResult {
  const manager = useInject(QueueManager);
  const [state, setState] = useState<IUseQueueConnectionResult>({
    connection: null,
    ready: false,
    error: null,
  });

  useEffect(() => {
    let cancelled = false;
    manager
      .connection(name)
      .then((connection) => {
        if (cancelled) return;
        setState({ connection, ready: true, error: null });
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setState({ connection: null, ready: false, error: err as Error });
      });
    return () => {
      cancelled = true;
    };
  }, [manager, name]);

  return state;
}
