/**
 * @fileoverview IQueuedJob factory.
 *
 * Every driver builds {@link IQueuedJob} instances the same way — consistent
 * defaults, timestamps, status, and id. Centralising the construction
 * here keeps drivers focused on storage and prevents subtle field drift
 * between implementations.
 *
 * @module utils/create-queued-job
 * @category Utils
 */

import { JobStatus } from '@stackra/contracts';
import type { IJobOptions, IQueuedJob, IWorkerOptions } from '@stackra/contracts';
import { generateJobId } from './generate-job-id.util';
import { computeUniqueId } from './compute-unique-id.util';

/**
 * Arguments for creating a queued job.
 */
interface CreateQueuedJobArgs<T> {
  /** Job name/type identifier. */
  name: string;
  /** Job payload data. */
  data: T;
  /** Connection name. */
  connection: string;
  /** Per-dispatch job options. */
  options?: IJobOptions;
  /** Worker-level defaults. */
  workerDefaults?: Partial<IWorkerOptions>;
}

/**
 * Build a fully-populated {@link IQueuedJob} from the dispatch arguments.
 *
 * Applies the precedence chain: per-dispatch `options` → `workerDefaults`
 * → hard-coded fallbacks. Fills in timestamps, a fresh id, the computed
 * `availableAt` for delayed jobs, and the deduplication id when
 * `uniqueFor` is set.
 *
 * @typeParam T - Payload type.
 * @param args - The inputs needed to build the job.
 * @returns A new {@link IQueuedJob}.
 */
export function createQueuedJob<T>(args: CreateQueuedJobArgs<T>): IQueuedJob<T> {
  const { name, data, connection, options = {}, workerDefaults = {} } = args;
  const now = Date.now();

  // Resolve each tunable using the precedence chain.
  const queue = options.queue ?? 'default';
  const delayMs = options.delayMs ?? 0;
  const maxAttempts = options.tries ?? workerDefaults.tries ?? 1;
  const backoffMs = options.backoffMs ?? workerDefaults.backoffMs ?? 1000;
  const timeoutMs = options.timeoutMs ?? workerDefaults.timeoutMs ?? 30_000;

  // Derive the deduplication id if the caller opted into uniqueness.
  const uniqueId =
    options.uniqueFor !== undefined ? (options.uniqueId ?? computeUniqueId(name, data)) : undefined;

  return {
    id: generateJobId(),
    name,
    data,
    queue,
    connection,
    status: delayMs > 0 ? JobStatus.Delayed : JobStatus.Pending,
    attempts: 0,
    maxAttempts,
    backoffMs,
    timeoutMs,
    availableAt: now + delayMs,
    createdAt: now,
    updatedAt: now,
    tags: options.tags ?? [],
    uniqueId,
    driverMeta: {},
  };
}
