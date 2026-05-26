/**
 * Internal interfaces barrel.
 *
 * Cross-package interfaces (`IJobOptions`, `IQueuedJob`,
 * `IQueueConnection`, `IQueueModuleOptions`, `IWorkerOptions`,
 * `IProcessorMetadata`, …) live in `@stackra/contracts`. Only
 * decorator-metadata shapes that no other package consumes stay
 * here.
 *
 * @module @stackra/ts-queue/interfaces
 */

export type { IOnJobEventMetadata } from './on-job-event-metadata.interface';
export type { IWorkerConfig } from './worker-config.interface';
