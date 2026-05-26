/**
 * Internal worker configuration.
 *
 * Carries everything a `Worker` instance needs to drive a single
 * `(connection, queue)` polling loop. Lives in this package because
 * no other package needs to construct a `Worker` directly.
 *
 * @module @stackra/ts-queue/interfaces/worker-config
 */

import type { IQueueConnection, IWorkerOptions } from '@stackra/contracts';

import type { WorkerHost } from '@/hosts/worker-host';
import type { QueueEventBus } from '@/services/event-bus.service';

/**
 * Per-worker configuration assembled by the loader.
 */
export interface IWorkerConfig {
  /** Driver this worker pulls from. */
  connection: IQueueConnection;
  /** Queue tube name within the connection. */
  queue: string;
  /** Processor host providing `process(job)`. */
  host: WorkerHost;
  /** Shared worker policy resolved with defaults. */
  options: Required<IWorkerOptions>;
  /** Event bus for lifecycle notifications. */
  eventBus: QueueEventBus;
}
