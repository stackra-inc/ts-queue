import type { QueuedJob } from "@/interfaces/queued-job.interface";
/**
 * SyncJobHandler — Type.
 *
 * @module @stackra/queue/types
 */

/**
 * Function that handles a single popped job.
 *
 * The sync driver calls this right after building the {@link QueuedJob}.
 * When no resolver is registered, the driver discards the job and returns
 * — identical to the null driver.
 */
export type SyncJobHandler = (job: QueuedJob) => void | Promise<void>;
