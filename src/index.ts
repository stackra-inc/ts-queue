/**
 * `@stackra/ts-queue` — multi-driver client-side job queue.
 *
 * Drivers: `memory`, `sync`, `null`, `local-storage`, `indexeddb`,
 * `broadcast-channel`, `qstash`. The package ships every built-in
 * connector pre-registered via `QueueModule.forRoot()`. Custom drivers
 * (SQS, Kafka, internal HTTP brokers) plug in through
 * `QueueModule.forFeature(driver, ConnectorClass)`.
 *
 * NestJS-style `@Processor` / `@OnJobEvent` auto-discovery via the
 * `ProcessorSubscribersLoader`. Optional integration with
 * `@stackra/ts-events` for lifecycle observability and with
 * `@stackra/ts-logger` for structured diagnostics.
 *
 * Cross-package contracts (`IJobOptions`, `IQueuedJob`,
 * `IQueueConnection`, `IQueueModuleOptions`, `IWorkerOptions`,
 * `IProcessorMetadata`, `QueueConnectionConfig`, `JobStatus`,
 * `QUEUE_EVENTS`, ...) live in `@stackra/contracts`.
 *
 * @example
 * ```typescript
 * import { Module } from "@stackra/ts-container";
 * import { QueueModule } from "@stackra/ts-queue";
 *
 * @Module({
 *   imports: [
 *     QueueModule.forRoot({
 *       default: "indexeddb",
 *       connections: {
 *         indexeddb: { driver: "indexeddb", dbName: "pos-queue" },
 *         qstash:    { driver: "qstash", mode: "proxy", proxyUrl: "/api/q" },
 *       },
 *       worker: { tries: 3, backoffMs: 1000, timeoutMs: 30_000 },
 *     }),
 *   ],
 * })
 * export class AppModule {}
 * ```
 *
 * @module @stackra/ts-queue
 */

import 'reflect-metadata';

// ============================================================================
// Module
// ============================================================================
export { QueueModule } from './queue.module';

// ============================================================================
// Services
// ============================================================================
export {
  QueueManager,
  QueueHandle,
  QueueEventBus,
  Worker,
  ProcessorMetadataAccessor,
  ProcessorSubscribersLoader,
} from './services';

// ============================================================================
// Connectors
// ============================================================================
export {
  MemoryConnector,
  SyncConnector,
  NullConnector,
  LocalStorageConnector,
  IndexedDBConnector,
  BroadcastChannelConnector,
  QStashConnector,
} from './connectors';

// ============================================================================
// Connections (concrete driver implementations)
// ============================================================================
export {
  BaseConnection,
  MemoryConnection,
  SyncConnection,
  NullConnection,
  LocalStorageConnection,
  IndexedDBConnection,
  BroadcastChannelConnection,
  QStashConnection,
} from './connections';

// ============================================================================
// Hosts
// ============================================================================
export { WorkerHost } from './hosts';

// ============================================================================
// Decorators
// ============================================================================
export { Processor, OnJobEvent, Job, InjectQueue, InjectQueueConnection } from './decorators';

// ============================================================================
// Internal Interfaces (decorator metadata, worker config)
// ============================================================================
export type { IOnJobEventMetadata, IWorkerConfig } from './interfaces';

// ============================================================================
// Types
// ============================================================================
export type { SyncJobHandler } from './types';

// ============================================================================
// Constants (decorator metadata keys)
// ============================================================================
export { PROCESSOR_METADATA, ON_JOB_EVENT_METADATA, JOB_METADATA } from './constants';

// ============================================================================
// Errors
// ============================================================================
export {
  QueueError,
  QueueDriverError,
  QueueModuleOptionsError,
  MaxAttemptsExceededError,
  TimeoutExceededError,
} from './errors';

// ============================================================================
// Utilities
// ============================================================================
export {
  generateJobId,
  computeUniqueId,
  createQueuedJob,
  computeBackoff,
  getQueueConnectionToken,
  getQueueToken,
} from './utils';
