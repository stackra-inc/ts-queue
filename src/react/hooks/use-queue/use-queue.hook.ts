/**
 * `useQueue` React hook.
 *
 * Resolves a `QueueHandle` for the given `(connection, queue)` pair.
 * The handle is fetched asynchronously (the underlying connection
 * needs async setup), so the hook returns `{ handle, ready, error }`.
 *
 * @module @stackra/ts-queue/react/hooks/use-queue
 */

import { useEffect, useState } from 'react';
import { useInject } from '@stackra/ts-container/react';

import { QueueManager } from '@/services/queue-manager.service';
import type { QueueHandle } from '@/services/queue-handle.service';

/**
 * Result of {@link useQueue}.
 */
export interface IUseQueueResult {
  /** Resolved queue handle (`null` until ready). */
  handle: QueueHandle | null;
  /** Whether the handle has been resolved. */
  ready: boolean;
  /** Setup error, if any. */
  error: Error | null;
}

/**
 * Access a queue handle from a React component.
 *
 * @param queue      - Queue tube name (defaults to `"default"`).
 * @param connection - Connection name (defaults to module default).
 * @returns Result object with `handle`, `ready`, `error`.
 *
 * @example
 * ```tsx
 * function EnqueueButton() {
 *   const { handle, ready } = useQueue('tracking');
 *   const onClick = () => handle?.push('click', {});
 *   return <button disabled={!ready} onClick={onClick}>Track</button>;
 * }
 * ```
 */
export function useQueue(queue?: string, connection?: string): IUseQueueResult {
  const manager = useInject(QueueManager);
  const [state, setState] = useState<IUseQueueResult>({
    handle: null,
    ready: false,
    error: null,
  });

  useEffect(() => {
    let cancelled = false;
    manager
      .queue(queue, connection)
      .then((handle) => {
        if (cancelled) return;
        setState({ handle, ready: true, error: null });
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setState({ handle: null, ready: false, error: err as Error });
      });
    return () => {
      cancelled = true;
    };
  }, [manager, queue, connection]);

  return state;
}
