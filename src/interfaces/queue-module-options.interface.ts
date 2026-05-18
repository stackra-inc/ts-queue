/**
 * @fileoverview Top-level module configuration.
 *
 * The object passed to `QueueModule.forRoot({...})` defining every driver
 * connection, the default connection name, and shared worker policy.
 * Analogous to Laravel's `config/queue.php`.
 *
 * @module interfaces/queue-module-options
 * @category Interfaces
 */

import type { WorkerOptions } from "./worker-options.interface";
import type { QueueConnectionConfig } from "@/types/queue-connection-config.type";

/**
 * Top-level configuration for the queue module.
 *
 * @example
 * ```typescript
 * QueueModule.forRoot({
 *   default: 'indexeddb',
 *   connections: {
 *     memory: { driver: 'memory' },
 *     indexeddb: { driver: 'indexeddb', dbName: 'app-queue', prefix: 'app:' },
 *     qstash: {
 *       driver: 'qstash',
 *       token: import.meta.env.VITE_QSTASH_TOKEN,
 *       defaultDestination: 'https://api.example.com/webhooks/queue',
 *     },
 *   },
 *   worker: {
 *     tries: 3,
 *     backoffMs: 1000,
 *     timeoutMs: 30_000,
 *   },
 * });
 * ```
 */
export interface QueueModuleOptions {
  /**
   * Name of the connection used when callers don't specify one.
   *
   * Must match a key in {@link QueueModuleOptions.connections}.
   */
  default: string;

  /**
   * Map of connection name → driver configuration.
   *
   * Each entry becomes a lazy {@link QueueConnection} that can be
   * addressed by `@InjectQueueConnection('name')` or
   * `queue.connection('name')`.
   */
  connections: Record<string, QueueConnectionConfig>;

  /**
   * Shared worker policy. Overridden per-job by {@link JobOptions}.
   */
  worker?: WorkerOptions;

  /**
   * Global prefix for every driver-persisted key.
   *
   * Lets multiple apps share a browser storage origin without collisions.
   *
   * @default ""
   */
  prefix?: string;
}
