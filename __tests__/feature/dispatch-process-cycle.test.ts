import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { QueueEvent, JobStatus } from "@stackra/contracts";
import { MemoryConnection } from "@/connections/memory.connection";
import { Worker } from "@/services/worker.service";
import { QueueEventBus } from "@/services/event-bus.service";
import { WorkerHost } from "@/hosts/worker-host";
import type { QueuedJob } from "@/interfaces/queued-job.interface";
import type { WorkerConfig } from "@/interfaces/worker-config.interface";

class TestProcessor extends WorkerHost {
  public processed: QueuedJob[] = [];

  public async process(job: QueuedJob): Promise<void> {
    this.processed.push(job);
  }
}

describe("Dispatch-Process Cycle", () => {
  let connection: MemoryConnection;
  let processor: TestProcessor;
  let eventBus: QueueEventBus;
  let worker: Worker;
  let emittedEvents: Array<{ event: string; payload: any }>;

  beforeEach(() => {
    vi.useFakeTimers();

    connection = new (MemoryConnection as any)("memory");
    processor = new TestProcessor();
    emittedEvents = [];

    eventBus = {
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
        backoffMs: 1000,
        maxBackoffMs: 30_000,
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

  it("should complete the full push → pop → process → remove cycle", async () => {
    // 1. Push a job
    const jobId = await connection.push("send-email", { to: "user@example.com" });
    expect(await connection.size()).toBe(1);

    // 2. Start the worker
    worker.start();

    // 3. Let the first tick run
    await vi.advanceTimersByTimeAsync(0);

    // 4. Verify the job was processed
    expect(processor.processed).toHaveLength(1);
    expect(processor.processed[0].name).toBe("send-email");
    expect(processor.processed[0].data).toEqual({ to: "user@example.com" });

    // 5. Verify the job was removed from the queue
    expect(await connection.size()).toBe(0);
  });

  it("should emit JobProcessing → JobProcessed → JobAttempted on success", async () => {
    await connection.push("job", { x: 1 });

    worker.start();
    await vi.advanceTimersByTimeAsync(0);

    const eventNames = emittedEvents.map((e) => e.event);
    expect(eventNames).toContain(QueueEvent.WorkerStarting);
    expect(eventNames).toContain(QueueEvent.JobProcessing);
    expect(eventNames).toContain(QueueEvent.JobProcessed);
    expect(eventNames).toContain(QueueEvent.JobAttempted);

    // Verify order: Processing before Processed
    const processingIdx = eventNames.indexOf(QueueEvent.JobProcessing);
    const processedIdx = eventNames.indexOf(QueueEvent.JobProcessed);
    expect(processingIdx).toBeLessThan(processedIdx);
  });

  it("should process multiple jobs in sequence", async () => {
    await connection.push("job-1", { order: 1 });
    await connection.push("job-2", { order: 2 });
    await connection.push("job-3", { order: 3 });

    worker.start();

    // Each job needs multiple timer advances to complete the async chain:
    // setTimeout fires → pop resolves → process resolves → scheduleNext(0)
    for (let i = 0; i < 10; i++) {
      await vi.advanceTimersByTimeAsync(1);
    }

    expect(processor.processed).toHaveLength(3);
    expect(processor.processed[0].data).toEqual({ order: 1 });
    expect(processor.processed[1].data).toEqual({ order: 2 });
    expect(processor.processed[2].data).toEqual({ order: 3 });
    expect(await connection.size()).toBe(0);
  });

  it("should respect queue isolation — only process jobs from the configured queue", async () => {
    await connection.push("job-default", { q: "default" });
    await connection.push("job-high", { q: "high" }, { queue: "high" });

    worker.start();
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(50); // wait for next poll

    // Only the default queue job should be processed
    expect(processor.processed).toHaveLength(1);
    expect(processor.processed[0].name).toBe("job-default");
    expect(await connection.size("high")).toBe(1);
  });

  it("should handle the full lifecycle with delayed jobs", async () => {
    // Push a delayed job
    await connection.push("delayed-job", { x: 1 }, { delayMs: 200 });
    expect(await connection.delayedSize()).toBe(1);

    worker.start();

    // First poll — job not available yet
    await vi.advanceTimersByTimeAsync(0);
    expect(processor.processed).toHaveLength(0);

    // Advance past the delay
    await vi.advanceTimersByTimeAsync(250);

    // Now the job should be processed
    expect(processor.processed).toHaveLength(1);
    expect(processor.processed[0].name).toBe("delayed-job");
  });

  it("should emit events with correct job payload", async () => {
    await connection.push("tracked-job", { userId: "u123" }, { tags: ["billing"] });

    worker.start();
    await vi.advanceTimersByTimeAsync(0);

    const processingEvent = emittedEvents.find((e) => e.event === QueueEvent.JobProcessing);
    expect(processingEvent).toBeDefined();
    expect(processingEvent!.payload.job.name).toBe("tracked-job");
    expect(processingEvent!.payload.job.data).toEqual({ userId: "u123" });
    expect(processingEvent!.payload.job.tags).toEqual(["billing"]);
  });
});
