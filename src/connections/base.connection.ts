/**
 * @fileoverview Shared abstract base class for every driver.
 *
 * Reduces boilerplate in the individual drivers by implementing the
 * methods that always look the same: `later()` forwards to `push()` with
 * a delay, `bulk()` falls back to a loop, pause state lives in a `Set`.
 *
 * Drivers still own the real work — persistence, pop semantics, and
 * anything storage-specific — but they no longer re-implement the
 * cosmetic wrappers.
 *
 * @module connections/base
 * @category Connections
 */

import type { JobOptions } from "@/interfaces/job-options.interface";
import type { QueueConnection } from "@/interfaces/queue-connection.interface";
import type { QueuedJob } from "@/interfaces/queued-job.interface";

/**
 * Abstract base that every built-in driver extends.
 *
 * Subclasses must implement: {@link push}, {@link pop}, {@link size},
 * {@link pendingSize}, {@link delayedSize}, {@link reservedSize},
 * {@link remove}, {@link release}, {@link fail}, {@link clear}, and
 * {@link close}.
 */
export abstract class BaseConnection implements QueueConnection {
  /**
   * Set of paused queue names.
   *
   * Using an in-memory `Set` for pause state is intentionally simple —
   * drivers that need cross-tab pause coordination override
   * {@link pause}/{@link resume}/{@link isPaused} and persist the state
   * themselves (see the broadcast-channel driver).
   */
  protected readonly pausedQueues: Set<string> = new Set();

  /**
   * @param name - The human-friendly connection name from module config.
   */
  constructor(public readonly name: string) {}

  // ── Abstract — drivers must implement ──────────────────────────────────

  abstract push<T = unknown>(name: string, data: T, options?: JobOptions): Promise<string>;
  abstract pop(queue?: string): Promise<QueuedJob | null>;
  abstract size(queue?: string): Promise<number>;
  abstract pendingSize(queue?: string): Promise<number>;
  abstract delayedSize(queue?: string): Promise<number>;
  abstract reservedSize(queue?: string): Promise<number>;
  abstract remove(jobId: string): Promise<void>;
  abstract release(jobId: string, delayMs?: number): Promise<void>;
  abstract fail(jobId: string, reason: string): Promise<void>;
  abstract clear(queue?: string): Promise<void>;
  abstract close(): Promise<void>;

  // ── Shared defaults ────────────────────────────────────────────────────

  /**
   * Delayed dispatch — forwards to `push()` with an additional `delayMs`.
   *
   * Drivers that persist delayed jobs differently from immediate ones
   * override this method.
   */
  public async later<T = unknown>(
    delayMs: number,
    name: string,
    data: T,
    options?: JobOptions,
  ): Promise<string> {
    return this.push(name, data, { ...options, delayMs });
  }

  /**
   * Bulk dispatch — loops over `push()`.
   *
   * Drivers with native batch support (IndexedDB transactions, QStash
   * batch publish) override this for efficiency.
   */
  public async bulk<T = unknown>(
    jobs: Array<{ name: string; data: T; options?: JobOptions }>,
  ): Promise<string[]> {
    const ids: string[] = [];
    for (const job of jobs) {
      ids.push(await this.push(job.name, job.data, job.options));
    }
    return ids;
  }

  /**
   * Pause a named queue.
   *
   * The default implementation stores the name in an in-memory set;
   * persistent drivers (IndexedDB, BroadcastChannel) override this to
   * persist the pause flag so it survives reloads and is visible to
   * other tabs.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  public async pause(queue: string = "default"): Promise<void> {
    this.pausedQueues.add(queue);
  }

  /**
   * Resume a previously paused queue.
   *
   * Removes the entry from the in-memory paused set. Persistent drivers
   * override to clear the flag in their storage layer.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  public async resume(queue: string = "default"): Promise<void> {
    this.pausedQueues.delete(queue);
  }

  /**
   * Whether the named queue is currently paused.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns `true` when the queue is paused, `false` otherwise.
   */
  public async isPaused(queue: string = "default"): Promise<boolean> {
    return this.pausedQueues.has(queue);
  }
}
