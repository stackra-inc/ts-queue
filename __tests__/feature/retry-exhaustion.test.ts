import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { QueueEvent, JobStatus } from "@stackra/contracts";
import { MemoryConnection } from "@/connections/memory.connection";
import { Worker } from "@/services/worker.service";
import { WorkerHost } from "@/hosts/worker-host";
import type { QueuedJob } from "@/interfaces/queued-job.interface";
import type { WorkerConfig } from "@/interfaces/worker-config.interface";

class FailingProcessor extends WorkerHost {
  public callCount = 0;

  public async process(job: QueuedJob): Promise<void> {
    this.callCount++;
    throw new Error(`Attempt ${this.callCount} failed`);
  }
}

describe("Retry Exhaustion", () => {
  let connection: MemoryConnection;
  let processor: FailingProcessor;
  let worker: Worker;
  let emittedEvents: Array<{ event: string; payload: any }>;

  beforeEach(() => {
    vi.useFakeTimers();

    connection = new (MemoryConnection as any)("memory");
    processor = new FailingProcessor();
    emittedEvents = [];

    const eventBus = {
      emit: vi.fn((event: string, payload: any) => {
        emittedEvents.push({ event, payload });
      }),
    } as any;

    const config: WorkerConfig = {
      connection,
      queue: "default",
      host: processor,
      options: {
        tries: 3,
        backoffMs: 100,
        maxBackoffMs: 5000,
        timeoutMs: 30_000,
        pollIntervalMs: 50,
        autoStart: true,
        failOnTimeout: true,
      },
      eventBus,
    };

    worker = new (Worker as any)(config);
  });

  afterEach(() => {
    worker.stop();
    vi.useRealTimers();
  });

  /**
   * Helper to advance timers enough for the full async chain to complete.
   */
  async function drainWorkerCycles(totalMs: number): Promise<void> {
    const step = 10;
    for (let elapsed = 0; elapsed < totalMs; elapsed += step) {
      await vi.advanceTimersByTimeAsync(step);
    }
  }

  it("should retry a job 3 times then mark it as Failed", async () => {
    // Push a job with tries: 3 and explicit backoffMs: 100
    // This means job.backoffMs = 100
    // Backoff for attempt 2: computeBackoff(2, 100, 5000) = min(100*2, 5000) = 200ms
    // Backoff for attempt 3: computeBackoff(3, 100, 5000) = min(100*4, 5000) = 400ms
    // Total time needed: ~200 + 50 (poll) + 400 + 50 (poll) + overhead ≈ 1000ms
    await connection.push("flaky-job", { x: 1 }, { tries: 3, backoffMs: 100 });

    worker.start();
    await drainWorkerCycles(2000);

    expect(processor.callCount).toBe(3);

    // Verify JobFailed was emitted
    const failedEvents = emittedEvents.filter((e) => e.event === QueueEvent.JobFailed);
    expect(failedEvents).toHaveLength(1);
    expect(failedEvents[0].payload.job.status).toBe(JobStatus.Failed);
    expect(failedEvents[0].payload.error).toBeDefined();
    expect(failedEvents[0].payload.error.name).toBe("MaxAttemptsExceededError");

    // Verify the job is no longer in-flight
    expect(await connection.size()).toBe(0);
  });

  it("should emit JobAttempted for each attempt", async () => {
    await connection.push("flaky-job", { x: 1 }, { tries: 3, backoffMs: 100 });

    worker.start();
    await drainWorkerCycles(2000);

    const attemptedEvents = emittedEvents.filter((e) => e.event === QueueEvent.JobAttempted);
    expect(attemptedEvents).toHaveLength(3);
  });

  it("should emit JobProcessing for each attempt", async () => {
    await connection.push("flaky-job", { x: 1 }, { tries: 3, backoffMs: 100 });

    worker.start();
    await drainWorkerCycles(2000);

    const processingEvents = emittedEvents.filter((e) => e.event === QueueEvent.JobProcessing);
    expect(processingEvents).toHaveLength(3);
  });

  it("should not emit JobProcessed when all attempts fail", async () => {
    await connection.push("flaky-job", { x: 1 }, { tries: 3, backoffMs: 100 });

    worker.start();
    await drainWorkerCycles(2000);

    const processedEvents = emittedEvents.filter((e) => e.event === QueueEvent.JobProcessed);
    expect(processedEvents).toHaveLength(0);
  });

  it("should apply exponential backoff between retries", async () => {
    await connection.push("flaky-job", { x: 1 }, { tries: 3, backoffMs: 100 });

    worker.start();
    await drainWorkerCycles(2000);

    const releases = emittedEvents.filter((e) => e.event === QueueEvent.JobReleased);
    expect(releases).toHaveLength(2); // 2 releases before final failure

    // First release: computeBackoff(attempt+1=2, 100, 5000) = 100 * 2^1 = 200ms
    expect(releases[0].payload.delayMs).toBe(200);

    // Second release: computeBackoff(attempt+1=3, 100, 5000) = 100 * 2^2 = 400ms
    expect(releases[1].payload.delayMs).toBe(400);
  });

  it("should succeed if a retry succeeds before exhaustion", async () => {
    let callCount = 0;
    const sometimesFailingProcessor = {
      process: vi.fn(async () => {
        callCount++;
        if (callCount < 3) throw new Error(`Attempt ${callCount} failed`);
        // Third attempt succeeds
      }),
    } as any;

    const localEvents: Array<{ event: string; payload: any }> = [];
    const eventBus = {
      emit: vi.fn((event: string, payload: any) => {
        localEvents.push({ event, payload });
      }),
    } as any;

    const config: WorkerConfig = {
      connection,
      queue: "default",
      host: sometimesFailingProcessor,
      options: {
        tries: 3,
        backoffMs: 100,
        maxBackoffMs: 5000,
        timeoutMs: 30_000,
        pollIntervalMs: 50,
        autoStart: true,
        failOnTimeout: true,
      },
      eventBus,
    };

    const retryWorker = new (Worker as any)(config);

    await connection.push("eventually-ok", { x: 1 }, { tries: 3, backoffMs: 100 });

    retryWorker.start();
    await drainWorkerCycles(2000);

    const processedEvents = localEvents.filter((e) => e.event === QueueEvent.JobProcessed);
    const failedEvents = localEvents.filter((e) => e.event === QueueEvent.JobFailed);

    expect(processedEvents).toHaveLength(1);
    expect(failedEvents).toHaveLength(0);
    expect(callCount).toBe(3);
    expect(await connection.size()).toBe(0);

    retryWorker.stop();
  });
});
