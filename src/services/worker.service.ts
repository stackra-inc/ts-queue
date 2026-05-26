/**
 * @fileoverview Per-processor worker loop.
 *
 * Each `@Processor` class gets its own {@link Worker} instance at
 * bootstrap. The worker polls the connection for jobs, enforces
 * timeouts, applies the retry/backoff policy, and emits lifecycle
 * events via {@link QueueEventBus}.
 *
 * Workers run on a simple `setTimeout` loop instead of `setInterval` so
 * stopping the worker is deterministic — we clear the pending timeout
 * and no further invocations happen.
 *
 * @module services/worker
 * @category Services
 */

import { JobStatus, QUEUE_EVENTS } from '@stackra/contracts';
import type { IQueuedJob, QueueEventName } from '@stackra/contracts';

import { MaxAttemptsExceededError } from '@/errors/max-attempts-exceeded.error';
import { TimeoutExceededError } from '@/errors/timeout-exceeded.error';
import { computeBackoff } from '@/utils/compute-backoff.util';
import type { IWorkerConfig } from '@/interfaces/worker-config.interface';
import type { QueueEventBus } from './event-bus.service';
import { Logger } from '@stackra/ts-logger';

/**
 * One polling loop per (connection, queue, host).
 *
 * The design is intentionally "one worker = one loop." Concurrency is
 * achieved by instantiating multiple workers for the same queue — the
 * bootstrap loader reads `ProcessorMetadata.concurrency` and creates N
 * workers to fan out processing.
 */
export class Worker {
  /**
   * Logger instance scoped to the Worker context.
   */
  private readonly logger = new Logger(Worker.name);

  /** Whether the worker has been started and is currently polling. */
  private running: boolean = false;

  /** The scheduled next-tick handle, cleared on stop(). */
  private pendingTick: ReturnType<typeof setTimeout> | null = null;

  constructor(private readonly config: IWorkerConfig) {}

  /**
   * Begin polling for jobs.
   *
   * Emits {@link QUEUE_EVENTS.WORKER_STARTING} once on first call.
   * Subsequent calls are no-ops — safe to call after reloads.
   */
  public start(): void {
    if (this.running) return;
    this.running = true;
    this.emit(QUEUE_EVENTS.WORKER_STARTING, {
      connection: this.config.connection.name,
      queue: this.config.queue,
    });
    this.scheduleNext(0);
  }

  /**
   * Stop polling.
   *
   * Clears the next-tick timer. In-flight processors are not
   * interrupted — they complete naturally and the worker is marked
   * stopped. Emits {@link QUEUE_EVENTS.WORKER_STOPPING}.
   */
  public stop(): void {
    if (!this.running) return;
    this.running = false;
    if (this.pendingTick) {
      clearTimeout(this.pendingTick);
      this.pendingTick = null;
    }
    this.emit(QUEUE_EVENTS.WORKER_STOPPING, {
      connection: this.config.connection.name,
      queue: this.config.queue,
    });
  }

  // ── Polling ──────────────────────────────────────────────────────────

  /**
   * Schedule the next polling tick.
   *
   * Wraps `setTimeout` so we can cancel the next tick during `stop()`.
   * Errors thrown by the tick handler are caught and logged so the
   * polling loop never crashes silently.
   *
   * @param delayMs - Delay in milliseconds before the next tick fires.
   */
  private scheduleNext(delayMs: number): void {
    if (!this.running) return;
    this.pendingTick = setTimeout(() => {
      this.tick().catch((err: Error | any) => {
        // Tick errors should never kill the loop — log and move on.
        this.logger.error(
          `[Worker:${this.config.connection.name}:${this.config.queue}] tick error: ${(err as Error).message}`
        );
        this.scheduleNext(this.config.options.pollIntervalMs);
      });
    }, delayMs);
  }

  /**
   * One polling iteration: pop a job, run it, schedule the next tick.
   *
   * Keeps the next-poll delay short when a job was processed (so the
   * worker drains the queue quickly) and longer when the queue was
   * empty.
   */
  private async tick(): Promise<void> {
    const job = await this.config.connection.pop(this.config.queue);

    if (!job) {
      this.scheduleNext(this.config.options.pollIntervalMs);
      return;
    }

    await this.handle(job);
    // Immediately try another tick — drains bursts without waiting.
    this.scheduleNext(0);
  }

  // ── Job handling ─────────────────────────────────────────────────────

  /**
   * Execute a single job under timeout and retry policy.
   *
   * The try/catch/finally choreography mirrors Laravel's Worker:
   *
   * 1. Emit `JOB_PROCESSING`.
   * 2. Race `host.process(job)` vs. the timeout timer.
   * 3. On success: mark complete, emit `JOB_PROCESSED`.
   * 4. On timeout/failure: apply the retry policy, release or fail,
   *    emit `JOB_FAILED` + `JOB_RELEASED` accordingly.
   * 5. Always emit `JOB_ATTEMPTED` for instrumentation.
   */
  private async handle(job: IQueuedJob): Promise<void> {
    this.emit(QUEUE_EVENTS.JOB_PROCESSING, { job });

    let error: Error | undefined;
    let timedOut = false;

    try {
      await this.runWithTimeout(job);
      // Success path — take the job off the queue entirely.
      await this.config.connection.remove(job.id);
      this.emit(QUEUE_EVENTS.JOB_PROCESSED, { job });
    } catch (e: Error | any) {
      error = e as Error;
      timedOut = error instanceof TimeoutExceededError;

      if (timedOut) {
        this.emit(QUEUE_EVENTS.JOB_TIMED_OUT, { job, error });
      }

      await this.handleFailure(job, error, timedOut);
    } finally {
      this.emit(QUEUE_EVENTS.JOB_ATTEMPTED, {
        job,
        attempts: job.attempts,
        error,
      });
    }
  }

  /**
   * Race the processor against a timeout timer.
   *
   * The timer rejects with a {@link TimeoutExceededError}, which the
   * caller differentiates from a regular throw for reporting purposes.
   */
  private async runWithTimeout(job: IQueuedJob): Promise<void> {
    const timeoutMs = job.timeoutMs;
    const startedAt = Date.now();

    let timer: ReturnType<typeof setTimeout> | null = null;
    const timeoutPromise = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        reject(new TimeoutExceededError(job.id, Date.now() - startedAt, timeoutMs));
      }, timeoutMs);
    });

    try {
      await Promise.race([Promise.resolve(this.config.host.process(job)), timeoutPromise]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  /**
   * Apply the retry/backoff/fail policy after a processor throws.
   *
   * Flow:
   * - If the job has retries left, release it for another attempt with
   *   exponential backoff.
   * - Otherwise, mark it permanently failed and emit `JOB_FAILED` with a
   *   {@link MaxAttemptsExceededError}.
   * - If `failOnTimeout` is true, a timeout short-circuits to permanent
   *   failure even when retries remain.
   */
  private async handleFailure(job: IQueuedJob, error: Error, timedOut: boolean): Promise<void> {
    const policy = this.config.options;
    const failOnTimeout = policy.failOnTimeout;

    const attemptsLeft = job.maxAttempts - job.attempts;
    const shouldFailNow = (timedOut && failOnTimeout) || attemptsLeft <= 0;

    if (shouldFailNow) {
      const finalError =
        timedOut && failOnTimeout
          ? error
          : new MaxAttemptsExceededError(job.id, job.attempts, error);
      await this.config.connection.fail(job.id, finalError.message);
      this.emit(QUEUE_EVENTS.JOB_FAILED, {
        job: { ...job, status: JobStatus.Failed },
        error: finalError,
      });
      return;
    }

    const delayMs = computeBackoff(job.attempts + 1, job.backoffMs, policy.maxBackoffMs);
    await this.config.connection.release(job.id, delayMs);
    this.emit(QUEUE_EVENTS.JOB_RELEASED, { job, delayMs, error });
  }

  /**
   * Safely emit a queue event.
   *
   * Wraps `eventBus.emit` so a misbehaving subscriber can't crash the
   * worker loop. Subscriber errors are logged at warning level.
   *
   * @param event   - Lifecycle event name.
   * @param payload - Event payload (shape depends on the event).
   */
  private emit(event: QueueEventName, payload: unknown): void {
    try {
      this.config.eventBus.emit(event, payload);
    } catch (err: Error | any) {
      this.logger.warn(`[Worker] Event bus emit failed for ${event}: ${(err as Error).message}`);
    }
  }
}
