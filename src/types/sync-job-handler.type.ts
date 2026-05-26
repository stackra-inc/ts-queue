import type { IQueuedJob } from "@stackra/contracts";
/**
 * SyncJobHandler — Type.
 *
 * @module @stackra/queue/types
 */

/**
 * Function that handles a single popped job.
 *
 * The sync driver calls this right after building the {@link IQueuedJob}.
 * When no resolver is registered, the driver discards the job and returns
 * — identical to the null driver.
 */
export type SyncJobHandler = (job: IQueuedJob) => void | Promise<void>;
