/**
 * @fileoverview `@Job` decorator metadata shape.
 *
 * @module interfaces/job-metadata
 * @category Interfaces
 */

import type { JobOptions } from "./job-options.interface";

/**
 * Metadata stored on job classes via the `@Job({...})` decorator.
 *
 * Merged by {@link JobDispatcher} with per-push options so class-level
 * defaults (`tries`, `backoffMs`, `queue`, …) flow automatically to every
 * dispatch site without the caller repeating them.
 */
export interface JobMetadata extends JobOptions {
  /**
   * Optional canonical name override.
   *
   * When the job class is dispatched via `dispatcher.dispatch(JobClass, data)`,
   * the dispatcher uses this name on the wire instead of the class name.
   */
  name?: string;
}
