/**
 * @fileoverview Null driver.
 *
 * Silently discards every dispatched job. Useful as an SSR-safe fallback
 * or as an off-switch that keeps call sites working unchanged.
 *
 * @module connections/null
 * @category Connections
 */

import { generateJobId } from "@/utils/generate-job-id.util";
import type { JobOptions } from "@/interfaces/job-options.interface";
import type { QueuedJob } from "@/interfaces/queued-job.interface";
import { BaseConnection } from "./base.connection";

/**
 * No-op driver. Every method is a cheap success.
 *
 * @example
 * ```typescript
 * QueueModule.forRoot({
 *   default: 'silent',
 *   connections: { silent: { driver: 'null' } },
 * });
 * ```
 */
export class NullConnection extends BaseConnection {
  /**
   * Discard the job and return a freshly generated id.
   *
   * The null driver intentionally drops every payload — useful as an
   * SSR-safe fallback or as a "queue disabled" toggle that keeps call
   * sites working unchanged.
   *
   * @typeParam T - Type of the job payload (ignored).
   * @param _name    - Application-level job name (ignored).
   * @param _data    - The job payload (ignored).
   * @param _options - Optional dispatch options (ignored).
   * @returns A freshly generated job id.
   */
  public async push<T = unknown>(_name: string, _data: T, _options?: JobOptions): Promise<string> {
    return generateJobId();
  }

  /**
   * Always-empty pop.
   *
   * The null driver never stores jobs, so there is never anything to
   * pop. Always resolves to `null`.
   *
   * @param _queue - Queue tube name (ignored).
   * @returns Always `null`.
   */
  public async pop(_queue?: string): Promise<QueuedJob | null> {
    return null;
  }

  /**
   * Always-zero size.
   *
   * @returns `0`.
   */
  public async size(): Promise<number> {
    return 0;
  }

  /**
   * Always-zero pending count.
   *
   * @returns `0`.
   */
  public async pendingSize(): Promise<number> {
    return 0;
  }

  /**
   * Always-zero delayed count.
   *
   * @returns `0`.
   */
  public async delayedSize(): Promise<number> {
    return 0;
  }

  /**
   * Always-zero reserved count.
   *
   * @returns `0`.
   */
  public async reservedSize(): Promise<number> {
    return 0;
  }

  /**
   * No-op remove.
   *
   * Null jobs never exist in storage.
   */
  public async remove(): Promise<void> {
    /* noop */
  }

  /**
   * No-op release.
   *
   * Null jobs never exist in storage.
   */
  public async release(): Promise<void> {
    /* noop */
  }

  /**
   * No-op fail.
   *
   * Null jobs never exist in storage.
   */
  public async fail(): Promise<void> {
    /* noop */
  }

  /**
   * No-op clear.
   *
   * There is no storage to clear.
   */
  public async clear(): Promise<void> {
    /* noop */
  }

  /**
   * No-op close.
   *
   * The null driver holds no resources.
   */
  public async close(): Promise<void> {
    /* noop */
  }
}
