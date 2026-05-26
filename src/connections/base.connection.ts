/**
 * Shared abstract base class for every driver.
 *
 * Reduces boilerplate in the individual drivers by implementing the
 * methods that always look the same: `later()` forwards to `push()`
 * with a delay, `bulk()` falls back to a loop, pause state lives in a
 * `Set`. Drivers still own the real work — persistence, pop semantics,
 * and anything storage-specific — but they no longer re-implement the
 * cosmetic wrappers.
 *
 * @module @stackra/ts-queue/connections/base
 */

import type { IJobOptions, IQueueConnection, IQueuedJob } from '@stackra/contracts';

/**
 * Abstract base every built-in driver extends.
 *
 * Subclasses must implement {@link push}, {@link pop}, {@link size},
 * {@link pendingSize}, {@link delayedSize}, {@link reservedSize},
 * {@link remove}, {@link release}, {@link fail}, {@link clear} and
 * {@link close}.
 */
export abstract class BaseConnection implements IQueueConnection {
  /**
   * Set of paused queue names.
   *
   * Drivers that need cross-tab pause coordination override
   * {@link pause}/{@link resume}/{@link isPaused} and persist the
   * state themselves (see the broadcast-channel driver).
   */
  protected readonly pausedQueues: Set<string> = new Set();

  /**
   * @param name - Connection name from module config.
   */
  public constructor(public readonly name: string) {}

  // ────────────────────────────────────────────────────────────────────
  // Abstract — drivers must implement
  // ────────────────────────────────────────────────────────────────────

  public abstract push<T = unknown>(name: string, data: T, options?: IJobOptions): Promise<string>;
  public abstract pop(queue?: string): Promise<IQueuedJob | null>;
  public abstract size(queue?: string): Promise<number>;
  public abstract pendingSize(queue?: string): Promise<number>;
  public abstract delayedSize(queue?: string): Promise<number>;
  public abstract reservedSize(queue?: string): Promise<number>;
  public abstract remove(jobId: string): Promise<void>;
  public abstract release(jobId: string, delayMs?: number): Promise<void>;
  public abstract fail(jobId: string, reason: string): Promise<void>;
  public abstract clear(queue?: string): Promise<void>;
  public abstract close(): Promise<void>;

  // ────────────────────────────────────────────────────────────────────
  // Shared defaults
  // ────────────────────────────────────────────────────────────────────

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
    options?: IJobOptions
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
    jobs: Array<{ name: string; data: T; options?: IJobOptions }>
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
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  public async pause(queue: string = 'default'): Promise<void> {
    this.pausedQueues.add(queue);
  }

  /**
   * Resume a previously paused queue.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   */
  public async resume(queue: string = 'default'): Promise<void> {
    this.pausedQueues.delete(queue);
  }

  /**
   * Whether the named queue is currently paused.
   *
   * @param queue - Queue tube name (defaults to `"default"`).
   * @returns `true` when paused, `false` otherwise.
   */
  public async isPaused(queue: string = 'default'): Promise<boolean> {
    return this.pausedQueues.has(queue);
  }
}
