import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { QueueEvent, JobStatus } from "@stackra/contracts";
import { Worker } from "@/services/worker.service";
import type { WorkerConfig } from "@/interfaces/worker-config.interface";
import type { QueuedJob } from "@/interfaces/queued-job.interface";

function createMockJob(overrides: Partial<QueuedJob> = {}): QueuedJob {
  return {
    id: "job-1",
    name: "test-job",
    data: { x: 1 },
    queue: "default",
    connection: "memory",
    status: JobStatus.Reserved,
    attempts: 1,
    maxAttempts: 3,
    backoffMs: 1000,
    timeoutMs: 30_000,
    availableAt: Date.now(),
    createdAt: Date.now(),
    updatedAt: Date.now(),
    tags: [],
    driverMeta: {},
    ...overrides,
  };
}

function createMockConfig(overrides: Partial<WorkerConfig> = {}): WorkerConfig {
  return {
    connection: {
      name: "memory",
      pop: vi.fn().mockResolvedValue(null),
      push: vi.fn(),
      remove: vi.fn().mockResolvedValue(undefined),
      release: vi.fn().mockResolvedValue(undefined),
      fail: vi.fn().mockResolvedValue(undefined),
      size: vi.fn(),
      pendingSize: vi.fn(),
      delayedSize: vi.fn(),
      reservedSize: vi.fn(),
      clear: vi.fn(),
      close: vi.fn(),
      pause: vi.fn(),
      resume: vi.fn(),
      isPaused: vi.fn(),
      later: vi.fn(),
      bulk: vi.fn(),
    } as any,
    queue: "default",
    host: {
      process: vi.fn().mockResolvedValue(undefined),
    } as any,
    options: {
      tries: 3,
      backoffMs: 1000,
      maxBackoffMs: 30_000,
      timeoutMs: 30_000,
      pollIntervalMs: 100,
      autoStart: true,
      failOnTimeout: true,
    },
    eventBus: {
      emit: vi.fn(),
    } as any,
    ...overrides,
  };
}

describe("Worker", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("start/stop", () => {
    it("should emit WorkerStarting on start", () => {
      const config = createMockConfig();
      const worker = new (Worker as any)(config);

      worker.start();
      expect(config.eventBus.emit).toHaveBeenCalledWith(QueueEvent.WorkerStarting, {
        connection: "memory",
        queue: "default",
      });
      worker.stop();
    });

    it("should emit WorkerStopping on stop", () => {
      const config = createMockConfig();
      const worker = new (Worker as any)(config);

      worker.start();
      worker.stop();
      expect(config.eventBus.emit).toHaveBeenCalledWith(QueueEvent.WorkerStopping, {
        connection: "memory",
        queue: "default",
      });
    });

    it("should not emit WorkerStarting twice on double start", () => {
      const config = createMockConfig();
      const worker = new (Worker as any)(config);

      worker.start();
      worker.start();
      const startCalls = (config.eventBus.emit as any).mock.calls.filter(
        (c: any[]) => c[0] === QueueEvent.WorkerStarting,
      );
      expect(startCalls).toHaveLength(1);
      worker.stop();
    });

    it("should not emit WorkerStopping if not running", () => {
      const config = createMockConfig();
      const worker = new (Worker as any)(config);

      worker.stop();
      expect(config.eventBus.emit).not.toHaveBeenCalled();
    });
  });

  describe("polling", () => {
    it("should call pop on the connection after start", async () => {
      const config = createMockConfig();
      const worker = new (Worker as any)(config);

      worker.start();
      await vi.advanceTimersByTimeAsync(0);

      expect(config.connection.pop).toHaveBeenCalledWith("default");
      worker.stop();
    });

    it("should poll again after pollIntervalMs when queue is empty", async () => {
      const config = createMockConfig();
      const worker = new (Worker as any)(config);

      worker.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(config.connection.pop).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(100);
      expect(config.connection.pop).toHaveBeenCalledTimes(2);
      worker.stop();
    });

    it("should poll immediately after processing a job", async () => {
      const job = createMockJob();
      const config = createMockConfig();
      (config.connection.pop as any).mockResolvedValueOnce(job).mockResolvedValue(null);

      const worker = new (Worker as any)(config);
      worker.start();

      // First tick fires, processes the job, then schedules next with delay 0
      // Multiple advances needed to let microtask queue flush between ticks
      await vi.advanceTimersByTimeAsync(1);
      await vi.advanceTimersByTimeAsync(1);
      await vi.advanceTimersByTimeAsync(1);

      expect(config.connection.pop).toHaveBeenCalledTimes(2);
      worker.stop();
    });
  });

  describe("job processing", () => {
    it("should emit JobProcessing and JobProcessed on success", async () => {
      const job = createMockJob();
      const config = createMockConfig();
      (config.connection.pop as any).mockResolvedValueOnce(job).mockResolvedValue(null);

      const worker = new (Worker as any)(config);
      worker.start();
      await vi.advanceTimersByTimeAsync(0);

      expect(config.eventBus.emit).toHaveBeenCalledWith(QueueEvent.JobProcessing, { job });
      expect(config.eventBus.emit).toHaveBeenCalledWith(QueueEvent.JobProcessed, { job });
      expect(config.connection.remove).toHaveBeenCalledWith(job.id);
      worker.stop();
    });

    it("should emit JobAttempted after processing", async () => {
      const job = createMockJob();
      const config = createMockConfig();
      (config.connection.pop as any).mockResolvedValueOnce(job).mockResolvedValue(null);

      const worker = new (Worker as any)(config);
      worker.start();
      await vi.advanceTimersByTimeAsync(0);

      expect(config.eventBus.emit).toHaveBeenCalledWith(
        QueueEvent.JobAttempted,
        expect.objectContaining({ job, attempts: 1 }),
      );
      worker.stop();
    });
  });

  describe("retry/backoff", () => {
    it("should release a job with backoff when retries remain", async () => {
      const job = createMockJob({ attempts: 1, maxAttempts: 3 });
      const config = createMockConfig();
      (config.connection.pop as any).mockResolvedValueOnce(job).mockResolvedValue(null);
      (config.host.process as any).mockRejectedValueOnce(new Error("fail"));

      const worker = new (Worker as any)(config);
      worker.start();
      await vi.advanceTimersByTimeAsync(0);

      expect(config.connection.release).toHaveBeenCalledWith(job.id, expect.any(Number));
      expect(config.eventBus.emit).toHaveBeenCalledWith(
        QueueEvent.JobReleased,
        expect.objectContaining({ job, delayMs: expect.any(Number) }),
      );
      worker.stop();
    });

    it("should fail a job when max attempts exhausted", async () => {
      const job = createMockJob({ attempts: 3, maxAttempts: 3 });
      const config = createMockConfig();
      (config.connection.pop as any).mockResolvedValueOnce(job).mockResolvedValue(null);
      (config.host.process as any).mockRejectedValueOnce(new Error("fail"));

      const worker = new (Worker as any)(config);
      worker.start();
      await vi.advanceTimersByTimeAsync(0);

      expect(config.connection.fail).toHaveBeenCalledWith(job.id, expect.any(String));
      expect(config.eventBus.emit).toHaveBeenCalledWith(
        QueueEvent.JobFailed,
        expect.objectContaining({
          job: expect.objectContaining({ status: JobStatus.Failed }),
        }),
      );
      worker.stop();
    });
  });

  describe("timeout", () => {
    it("should emit JobTimedOut when processor exceeds timeout", async () => {
      const job = createMockJob({ timeoutMs: 50 });
      const config = createMockConfig();
      (config.connection.pop as any).mockResolvedValueOnce(job).mockResolvedValue(null);
      (config.host.process as any).mockImplementation(
        () => new Promise((resolve) => setTimeout(resolve, 200)),
      );

      const worker = new (Worker as any)(config);
      worker.start();
      await vi.advanceTimersByTimeAsync(50);
      // Allow the promise race to settle
      await vi.advanceTimersByTimeAsync(0);

      expect(config.eventBus.emit).toHaveBeenCalledWith(
        QueueEvent.JobTimedOut,
        expect.objectContaining({ job }),
      );
      worker.stop();
    });

    it("should fail immediately on timeout when failOnTimeout is true", async () => {
      const job = createMockJob({ timeoutMs: 50, attempts: 1, maxAttempts: 3 });
      const config = createMockConfig();
      config.options.failOnTimeout = true;
      (config.connection.pop as any).mockResolvedValueOnce(job).mockResolvedValue(null);
      (config.host.process as any).mockImplementation(
        () => new Promise((resolve) => setTimeout(resolve, 200)),
      );

      const worker = new (Worker as any)(config);
      worker.start();
      await vi.advanceTimersByTimeAsync(50);
      await vi.advanceTimersByTimeAsync(0);

      expect(config.connection.fail).toHaveBeenCalledWith(job.id, expect.any(String));
      worker.stop();
    });
  });

  describe("event emission error isolation", () => {
    it("should not crash the worker when eventBus.emit throws", async () => {
      const job = createMockJob();
      const config = createMockConfig();
      (config.connection.pop as any).mockResolvedValueOnce(job).mockResolvedValue(null);
      (config.eventBus.emit as any).mockImplementation(() => {
        throw new Error("subscriber error");
      });

      const worker = new (Worker as any)(config);
      worker.start();
      // Should not throw
      await vi.advanceTimersByTimeAsync(0);
      worker.stop();
    });
  });
});
