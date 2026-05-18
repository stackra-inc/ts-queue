/**
 * @fileoverview Abstract base class for `@Processor`-decorated classes.
 *
 * Mirrors NestJS Bull's `WorkerHost`. Processor classes extend it and
 * implement `process(job)` — the bootstrap loader uses an `instanceof`
 * check to catch misuse at runtime.
 *
 * @module hosts/worker-host
 * @category Hosts
 */

import type { QueuedJob } from "@/interfaces/queued-job.interface";

/**
 * Base class every `@Processor`-decorated class must extend.
 *
 * Subclasses implement `process()` — called once per popped job. The
 * method may be sync or async; async implementations allow awaiting I/O
 * (API calls, database writes) naturally.
 *
 * @typeParam T - Payload type for the queue this processor consumes.
 *
 * @example
 * ```typescript
 * @Processor('tracking')
 * export class PixelProcessor extends WorkerHost<PixelPayload> {
 *   async process(job: QueuedJob<PixelPayload>): Promise<void> {
 *     await this.pixels.fireEvent(job.data.eventName, job.data.params);
 *   }
 * }
 * ```
 */
export abstract class WorkerHost<T = unknown> {
  /**
   * Handle a single queued job.
   *
   * Implementations should throw on failure — the worker catches the
   * throw and decides whether to retry (attempts remaining) or mark the
   * job permanently failed. Return normally to indicate success.
   *
   * @param job - The job as rehydrated from the driver.
   */
  public abstract process(job: QueuedJob<T>): Promise<void> | void;
}
