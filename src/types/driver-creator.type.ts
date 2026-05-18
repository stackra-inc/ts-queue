/**
 * @fileoverview Custom driver factory signature.
 *
 * @module types/driver-creator
 * @category Types
 */

import type { QueueConnection } from "@/interfaces/queue-connection.interface";

/**
 * Factory signature for custom drivers registered with
 * `QueueManager.extend(driverName, creator)`.
 *
 * The factory receives the connection name and the raw config object (the
 * entry from `QueueModuleOptions.connections[name]`) and must return a
 * fully-initialised {@link QueueConnection}.
 *
 * @example
 * ```typescript
 * manager.extend('sqs', (name, config) => new SqsConnection(name, config));
 * ```
 */
export type DriverCreator = (name: string, config: Record<string, unknown>) => QueueConnection;
