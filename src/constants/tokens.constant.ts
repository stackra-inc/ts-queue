/**
 * @fileoverview Dependency Injection tokens and metadata keys.
 *
 * Centralised registry of every `Symbol.for()` token and metadata key used
 * across `@stackra/ts-queue`. Follows the code standards rule that all DI
 * tokens must live in a single file per package (see
 * `.kiro/steering/code-standards.md §8`).
 *
 * @module constants/tokens
 * @category Constants
 */

// DI tokens from `@stackra/contracts` should be imported directly:
// `import { QUEUE_CONFIG, QUEUE_MANAGER, DEFAULT_QUEUE_CONNECTION_TOKEN, EVENT_EMITTER_MANAGER } from "@stackra/contracts";`

// ============================================================================
// Per-connection and per-queue tokens
// ============================================================================

/**
 * Build the DI injection token for a named queue **connection** (driver).
 *
 * A connection is one row in `QueueModuleOptions.connections` — it owns a
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
 * getQueueConnectionToken();          // Symbol.for('QUEUE_CONNECTION_default')
 * getQueueConnectionToken('indexeddb'); // Symbol.for('QUEUE_CONNECTION_indexeddb')
 * ```
 */
export const getQueueConnectionToken = (name: string = "default"): symbol =>
  Symbol.for(`QUEUE_CONNECTION_${name}`);

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
export const getQueueToken = (name: string = "default", connection: string = "default"): symbol =>
  Symbol.for(`QUEUE_${connection}:${name}`);

// ============================================================================
// Decorator metadata keys
// ============================================================================

/**
 * Metadata key for `@Processor(queueName, options?)` class decorators.
 *
 * Stored on the processor class via `@stackra/ts-metadata`. The
 * {@link ProcessorSubscribersLoader} reads this at bootstrap to bind the
 * class instance's `process()` method as a consumer of the named queue.
 */
export const PROCESSOR_METADATA = "QUEUE_PROCESSOR_METADATA";

/**
 * Metadata key for `@OnJobEvent(eventName)` method decorators.
 *
 * Stored on the method function via `@stackra/ts-metadata`. The
 * {@link ProcessorSubscribersLoader} reads this at bootstrap and wires the
 * method to the appropriate lifecycle event on the `EventManager`.
 */
export const ON_JOB_EVENT_METADATA = "QUEUE_ON_JOB_EVENT_METADATA";

/**
 * Metadata key for `@Job({ tries, backoff, timeout, ... })` class decorators.
 *
 * Stored on the job class. {@link JobDispatcher} merges this with the
 * per-push options when enqueueing so every dispatch inherits the class
 * defaults without callers repeating them.
 */
export const JOB_METADATA = "QUEUE_JOB_METADATA";
