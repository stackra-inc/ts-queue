/**
 * Queue token utility.
 *
 * Returns the DI injection token for a named queue within a connection.
 * Used internally by `@InjectQueue()` and by `QueueModule.forRoot()` /
 * `forFeature()` to register per-queue handle providers.
 *
 * @module @stackra/ts-queue/utils/get-queue-token
 */

/**
 * Build the DI injection token for a named **queue** within a connection.
 *
 * A queue is a named tube of work on top of a connection. Laravel calls
 * this the `queue` argument to `push()`; BullMQ calls it a Queue name.
 * In our model, `@InjectQueue('scans')` returns a bound handle that
 * pushes/polls the `scans` queue on the configured default connection.
 *
 * The token name is the full `connection:queue` pair so the same queue
 * name can appear on different connections without collision.
 *
 * @param name       - The queue name (defaults to `"default"`).
 * @param connection - The connection name (defaults to `"default"`).
 * @returns A Symbol unique to the `connection:queue` pair.
 *
 * @example
 * ```typescript
 * getQueueToken('scans');              // Symbol.for('QUEUE_default:scans')
 * getQueueToken('scans', 'indexeddb'); // Symbol.for('QUEUE_indexeddb:scans')
 * ```
 */
export const getQueueToken = (name: string = 'default', connection: string = 'default'): symbol =>
  Symbol.for(`QUEUE_${connection}:${name}`);
