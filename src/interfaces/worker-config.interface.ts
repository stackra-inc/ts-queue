/**
 * WorkerConfig — Interface.
 *
 * @module @stackra/queue/interfaces
 */

import type { QueueConnection } from "./queue-connection.interface";
import type { WorkerOptions } from "./worker-options.interface";
import type { WorkerHost } from "@/hosts/worker-host";
import type { QueueEventBus } from "@/services/event-bus.service";

/**
 * Options passed to a {@link Worker} instance.
 */
export interface WorkerConfig {
  /** The driver this worker pulls from. */
  connection: QueueConnection;
  /** Queue tube name within the connection. */
  queue: string;
  /** The processor host providing `process(job)`. */
  host: WorkerHost;
  /** Shared worker policy (poll interval, defaults). */
  options: Required<WorkerOptions>;
  /** Event bus for lifecycle notifications. */
  eventBus: QueueEventBus;
}
