import { describe, it, expect } from "vitest";
import { JobStatus } from "@stackra/contracts";
import { createQueuedJob } from "@/utils/create-queued-job.util";

describe("createQueuedJob", () => {
  describe("defaults", () => {
    it("should create a job with default values", () => {
      const job = createQueuedJob({
        name: "send-email",
        data: { to: "a@b.com" },
        connection: "memory",
      });

      expect(job.name).toBe("send-email");
      expect(job.data).toEqual({ to: "a@b.com" });
      expect(job.connection).toBe("memory");
      expect(job.queue).toBe("default");
      expect(job.status).toBe(JobStatus.Pending);
      expect(job.attempts).toBe(0);
      expect(job.maxAttempts).toBe(1);
      expect(job.backoffMs).toBe(1000);
      expect(job.timeoutMs).toBe(30_000);
      expect(job.tags).toEqual([]);
      expect(job.uniqueId).toBeUndefined();
      expect(job.driverMeta).toEqual({});
    });

    it("should generate a unique id for the job", () => {
      const job1 = createQueuedJob({ name: "a", data: {}, connection: "mem" });
      const job2 = createQueuedJob({ name: "b", data: {}, connection: "mem" });
      expect(job1.id).toBeDefined();
      expect(job2.id).toBeDefined();
      expect(job1.id).not.toBe(job2.id);
    });

    it("should set timestamps to approximately now", () => {
      const before = Date.now();
      const job = createQueuedJob({ name: "a", data: {}, connection: "mem" });
      const after = Date.now();

      expect(job.createdAt).toBeGreaterThanOrEqual(before);
      expect(job.createdAt).toBeLessThanOrEqual(after);
      expect(job.updatedAt).toBe(job.createdAt);
      expect(job.availableAt).toBe(job.createdAt);
    });
  });

  describe("option merging", () => {
    it("should apply queue from options", () => {
      const job = createQueuedJob({
        name: "a",
        data: {},
        connection: "mem",
        options: { queue: "high" },
      });
      expect(job.queue).toBe("high");
    });

    it("should apply tries from options", () => {
      const job = createQueuedJob({
        name: "a",
        data: {},
        connection: "mem",
        options: { tries: 5 },
      });
      expect(job.maxAttempts).toBe(5);
    });

    it("should apply backoffMs from options", () => {
      const job = createQueuedJob({
        name: "a",
        data: {},
        connection: "mem",
        options: { backoffMs: 2000 },
      });
      expect(job.backoffMs).toBe(2000);
    });

    it("should apply timeoutMs from options", () => {
      const job = createQueuedJob({
        name: "a",
        data: {},
        connection: "mem",
        options: { timeoutMs: 10_000 },
      });
      expect(job.timeoutMs).toBe(10_000);
    });

    it("should apply tags from options", () => {
      const job = createQueuedJob({
        name: "a",
        data: {},
        connection: "mem",
        options: { tags: ["urgent", "billing"] },
      });
      expect(job.tags).toEqual(["urgent", "billing"]);
    });

    it("should set status to Delayed when delayMs > 0", () => {
      const job = createQueuedJob({
        name: "a",
        data: {},
        connection: "mem",
        options: { delayMs: 5000 },
      });
      expect(job.status).toBe(JobStatus.Delayed);
      expect(job.availableAt).toBeGreaterThan(job.createdAt);
    });

    it("should set availableAt = now + delayMs", () => {
      const before = Date.now();
      const job = createQueuedJob({
        name: "a",
        data: {},
        connection: "mem",
        options: { delayMs: 3000 },
      });
      expect(job.availableAt).toBeGreaterThanOrEqual(before + 3000);
    });
  });

  describe("workerDefaults precedence", () => {
    it("should use workerDefaults when options are not provided", () => {
      const job = createQueuedJob({
        name: "a",
        data: {},
        connection: "mem",
        workerDefaults: { tries: 5, backoffMs: 2000, timeoutMs: 60_000 },
      });
      expect(job.maxAttempts).toBe(5);
      expect(job.backoffMs).toBe(2000);
      expect(job.timeoutMs).toBe(60_000);
    });

    it("should prefer per-dispatch options over workerDefaults", () => {
      const job = createQueuedJob({
        name: "a",
        data: {},
        connection: "mem",
        options: { tries: 2, backoffMs: 500 },
        workerDefaults: { tries: 5, backoffMs: 2000 },
      });
      expect(job.maxAttempts).toBe(2);
      expect(job.backoffMs).toBe(500);
    });
  });

  describe("uniqueId", () => {
    it("should compute uniqueId when uniqueFor is set", () => {
      const job = createQueuedJob({
        name: "a",
        data: { x: 1 },
        connection: "mem",
        options: { uniqueFor: 60_000 },
      });
      expect(job.uniqueId).toBeDefined();
      expect(job.uniqueId).toMatch(/^u_/);
    });

    it("should use explicit uniqueId when provided", () => {
      const job = createQueuedJob({
        name: "a",
        data: { x: 1 },
        connection: "mem",
        options: { uniqueFor: 60_000, uniqueId: "my-custom-key" },
      });
      expect(job.uniqueId).toBe("my-custom-key");
    });

    it("should not set uniqueId when uniqueFor is not set", () => {
      const job = createQueuedJob({
        name: "a",
        data: { x: 1 },
        connection: "mem",
      });
      expect(job.uniqueId).toBeUndefined();
    });

    it("should produce the same uniqueId for the same name+data", () => {
      const job1 = createQueuedJob({
        name: "a",
        data: { x: 1, y: 2 },
        connection: "mem",
        options: { uniqueFor: 60_000 },
      });
      const job2 = createQueuedJob({
        name: "a",
        data: { x: 1, y: 2 },
        connection: "mem",
        options: { uniqueFor: 60_000 },
      });
      expect(job1.uniqueId).toBe(job2.uniqueId);
    });

    it("should produce different uniqueId for different payloads", () => {
      const job1 = createQueuedJob({
        name: "a",
        data: { x: 1 },
        connection: "mem",
        options: { uniqueFor: 60_000 },
      });
      const job2 = createQueuedJob({
        name: "a",
        data: { x: 2 },
        connection: "mem",
        options: { uniqueFor: 60_000 },
      });
      expect(job1.uniqueId).not.toBe(job2.uniqueId);
    });
  });
});
