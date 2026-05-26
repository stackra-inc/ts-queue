/**
 * @fileoverview Decorator metadata keys.
 *
 * Centralised registry of every metadata key used by `@stackra/ts-queue`
 * decorators. DI tokens (`Symbol.for(...)`) live in `@stackra/contracts`,
 * and the per-connection / per-queue token factories live in
 * `@/utils/get-queue-connection-token.util` and
 * `@/utils/get-queue-token.util` — runtime helpers belong in `utils/`,
 * not `constants/`.
 *
 * @module constants/tokens
 * @category Constants
 */

// DI tokens from `@stackra/contracts` should be imported directly:
// `import { QUEUE_CONFIG, QUEUE_MANAGER, DEFAULT_QUEUE_CONNECTION_TOKEN, EVENT_EMITTER_MANAGER } from "@stackra/contracts";`

/**
 * Metadata key for `@Processor(queueName, options?)` class decorators.
 *
 * Stored on the processor class via `@vivtel/metadata`. The
 * {@link ProcessorSubscribersLoader} reads this at bootstrap to bind the
 * class instance's `process()` method as a consumer of the named queue.
 */
export const PROCESSOR_METADATA = 'QUEUE_PROCESSOR_METADATA';

/**
 * Metadata key for `@OnJobEvent(eventName)` method decorators.
 *
 * Stored on the method function via `@vivtel/metadata`. The
 * {@link ProcessorSubscribersLoader} reads this at bootstrap and wires the
 * method to the appropriate lifecycle event on the `EventManager`.
 */
export const ON_JOB_EVENT_METADATA = 'QUEUE_ON_JOB_EVENT_METADATA';

/**
 * Metadata key for `@Job({ tries, backoff, timeout, ... })` class decorators.
 *
 * Stored on the job class. {@link JobDispatcher} merges this with the
 * per-push options when enqueueing so every dispatch inherits the class
 * defaults without callers repeating them.
 */
export const JOB_METADATA = 'QUEUE_JOB_METADATA';
