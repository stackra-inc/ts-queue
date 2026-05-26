/**
 * Queue connection token utility.
 *
 * Returns the DI injection token for a named queue connection (driver).
 * Used internally by `@InjectQueueConnection()` and by
 * `QueueModule.forRoot()` to register per-connection providers.
 *
 * @module @stackra/ts-queue/utils/get-queue-connection-token
 */

/**
 * Build the DI injection token for a named queue **connection** (driver).
 *
 * A connection is one row in `IQueueModuleOptions.connections` — it owns a
 * driver (memory, indexeddb, qstash, …) and its configuration. When a
 * consumer writes `@InjectQueueConnection('primary')`, the decorator
 * resolves the token returned here.
 *
 * @param name - The connection name from the module config.
 *   Defaults to `"default"` when omitted.
 * @returns A Symbol unique to the given connection name.
 *
 * @example
 * ```typescript
 * getQueueConnectionToken();            // Symbol.for('QUEUE_CONNECTION_default')
 * getQueueConnectionToken('indexeddb'); // Symbol.for('QUEUE_CONNECTION_indexeddb')
 * ```
 */
export const getQueueConnectionToken = (name: string = 'default'): symbol =>
  Symbol.for(`QUEUE_CONNECTION_${name}`);
