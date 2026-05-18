/**
 * @fileoverview `@Processor` decorator metadata shape.
 *
 * @module interfaces/processor-metadata
 * @category Interfaces
 */

/**
 * Metadata stored on a processor class by the `@Processor` decorator.
 *
 * Read at bootstrap by {@link ProcessorSubscribersLoader} to bind the
 * processor's `process()` method to the appropriate queue connection.
 */
export interface ProcessorMetadata {
  /** Queue tube name this processor consumes from. */
  queue: string;

  /**
   * Connection name. Defaults to the module's configured default when
   * omitted, matching the behaviour of `@InjectQueueConnection()`.
   */
  connection?: string;

  /** Concurrency — how many jobs this processor may handle in parallel. */
  concurrency?: number;
}
