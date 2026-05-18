/**
 * @fileoverview `@stackra/ts-queue` public API.
 *
 * Laravel-inspired client-side job queue. Multiple drivers (memory,
 * indexeddb, local-storage, broadcast-channel, qstash, sync, null).
 * NestJS-style `@Processor` / `@OnJobEvent` auto-discovery. Integrates
 * with `@stackra/ts-events` for lifecycle observability and with
 * `@stackra/ts-logger` for structured diagnostics.
 *
 * ## Quick start
 *
 * ```typescript
 * // 1. Register the module.
 * QueueModule.forRoot({
 *   default: 'indexeddb',
 *   connections: {
 *     indexeddb: { driver: 'indexeddb', dbName: 'pos-queue' },
 *     qstash:    { driver: 'qstash', mode: 'proxy', proxyUrl: '/api/q' },
 *   },
 *   worker: { tries: 3, backoffMs: 1000, timeoutMs: 30_000 },
 * });
 *
 * // 2. Define a processor.
 * @Processor('sales')
 * export class SaleProcessor extends WorkerHost<SalePayload> {
 *   constructor(@InjectHttp() private readonly http: HttpClient) { super(); }
 *
 *   async process(job: QueuedJob<SalePayload>): Promise<void> {
 *     await this.http.post('/sales', job.data);
 *   }
 *
 *   @OnJobEvent(QueueEvent.JobFailed)
 *   onFailed({ job, error }: { job: QueuedJob; error: Error }): void {
 *     alerting.capture(job.id, error);
 *   }
 * }
 *
 * // 3. Dispatch from anywhere.
 * queue.queue('sales').push('sale.sync', saleData, { tries: 5 });
 * ```
 *
 * @module @stackra/ts-queue
 */

import "reflect-metadata";

// ============================================================================
// Module (DI configuration)
// ============================================================================
export { QueueModule } from "./queue.module";

// ============================================================================
// Core Services
// ============================================================================
export {
  QueueManager,
  QueueHandle,
  QueueEventBus,
  Worker,
  ProcessorMetadataAccessor,
  ProcessorSubscribersLoader,
} from "./services";

// ============================================================================
// Connections (driver implementations)
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
} from "./connections";
export type { SyncJobHandler } from "./connections";

// ============================================================================
// Hosts
// ============================================================================
export { WorkerHost } from "./hosts";

// ============================================================================
// Decorators
// ============================================================================
export { Processor, OnJobEvent, Job, InjectQueue, InjectQueueConnection } from "./decorators";

// ============================================================================
// React Hooks
// ============================================================================
export { useQueue, useQueueConnection, useQueueManager } from "./hooks";

// ============================================================================
// Facades
// ============================================================================
export { queue } from "./facades";

// ============================================================================
// Interfaces
// ============================================================================
export type {
  QueueModuleOptions,
  WorkerOptions,
  QueueConnection,
  QueuedJob,
  JobOptions,
  JobMetadata,
  ProcessorMetadata,
  OnJobEventMetadata,
  MemoryConnectionConfig,
  SyncConnectionConfig,
  NullConnectionConfig,
  LocalStorageConnectionConfig,
  IndexedDBConnectionConfig,
  BroadcastChannelConnectionConfig,
  QStashConnectionConfig,
} from "./interfaces";

// ============================================================================
// Types
// ============================================================================
export type { QueueConnectionConfig, DriverCreator } from "./types";

// ============================================================================
// Enums
// ============================================================================

// ============================================================================
// Constants
// ============================================================================
export {
  PROCESSOR_METADATA,
  ON_JOB_EVENT_METADATA,
  JOB_METADATA,
  getQueueConnectionToken,
  getQueueToken,
} from "./constants";

// ============================================================================
// Errors
// ============================================================================
export {
  QueueError,
  QueueDriverError,
  MaxAttemptsExceededError,
  TimeoutExceededError,
} from "./errors";

// ============================================================================
// Decorator Discovery Registration
// ============================================================================
//
// `@Processor` is a class decorator — so the discovery plugin needs to
// include its files in the scan path. This side-effect registration
// ensures the build-time scanner picks up every @Processor class and
// emits a virtual module the bootstrap loader can read.
//
// The loader itself also walks the DI container at runtime, so this
// registration is an optimisation (and enables HMR) rather than a hard
// requirement. When `@stackra/vite-config` is absent, the package still
// works through runtime discovery.
// ----------------------------------------------------------------------------

// `require` is present in CJS output and in Node; declare for TS.
declare const require: (id: string) => unknown;

try {
  // Intentionally dynamic require so the vite-config dep stays optional.
  const vite = require("@stackra/vite-config") as {
    DecoratorDiscoveryModule?: { forFeature: (specs: unknown[]) => void };
  };
  vite.DecoratorDiscoveryModule?.forFeature([
    {
      name: "Processor",
      virtualModule: "virtual:decorator-registry/queue-processors",
      output: "metadata",
      exportName: "QUEUE_PROCESSOR_CLASSES",
      priority: 58,
    },
  ]);
} catch {
  // `@stackra/vite-config` is optional — runtime discovery still works.
}
