import { describe, it, expect, beforeEach } from "vitest";
import { JobStatus } from "@stackra/contracts";
import { MemoryConnection } from "@/connections/memory.connection";

describe("MemoryConnection", () => {
  let conn: MemoryConnection;

  beforeEach(async () => {
    conn = new (MemoryConnection as any)("memory");
  });

  describe("push", () => {
    it("should enqueue a job and return its id", async () => {
      const id = await conn.push("send-email", { to: "a@b.com" });
      expect(id).toBeDefined();
      expect(typeof id).toBe("string");
    });

    it("should increment size after push", async () => {
      await conn.push("job-a", { x: 1 });
      await conn.push("job-b", { x: 2 });
      expect(await conn.size()).toBe(2);
    });

    it("should respect queue option", async () => {
      await conn.push("job-a", {}, { queue: "high" });
      expect(await conn.size("high")).toBe(1);
      expect(await conn.size("default")).toBe(0);
    });
  });

  describe("pop", () => {
    it("should return null when queue is empty", async () => {
      const job = await conn.pop();
      expect(job).toBeNull();
    });

    it("should return the oldest pending job", async () => {
      await conn.push("first", { order: 1 });
      await conn.push("second", { order: 2 });

      const job = await conn.pop();
      expect(job).not.toBeNull();
      expect(job!.name).toBe("first");
      expect(job!.status).toBe(JobStatus.Reserved);
      expect(job!.attempts).toBe(1);
    });

    it("should not pop delayed jobs whose availableAt is in the future", async () => {
      await conn.push("delayed", { x: 1 }, { delayMs: 60_000 });
      const job = await conn.pop();
      expect(job).toBeNull();
    });

    it("should pop from the specified queue only", async () => {
      await conn.push("job-a", {}, { queue: "high" });
      await conn.push("job-b", {}, { queue: "default" });

      const job = await conn.pop("high");
      expect(job!.name).toBe("job-a");
    });
  });

  describe("release", () => {
    it("should release a reserved job back to pending", async () => {
      await conn.push("job", { x: 1 });
      const job = await conn.pop();
      expect(job!.status).toBe(JobStatus.Reserved);

      await conn.release(job!.id, 0);
      const next = await conn.pop();
      expect(next).not.toBeNull();
      expect(next!.id).toBe(job!.id);
    });

    it("should release with delay setting status to Delayed", async () => {
      await conn.push("job", { x: 1 });
      const job = await conn.pop();

      await conn.release(job!.id, 60_000);
      // Job is delayed so pop should return null
      const next = await conn.pop();
      expect(next).toBeNull();
    });

    it("should no-op for unknown job id", async () => {
      await expect(conn.release("unknown-id", 0)).resolves.toBeUndefined();
    });
  });

  describe("fail", () => {
    it("should mark a job as failed with a reason", async () => {
      await conn.push("job", { x: 1 });
      const job = await conn.pop();

      await conn.fail(job!.id, "Something went wrong");
      // Failed jobs are not popped
      const next = await conn.pop();
      expect(next).toBeNull();
      // Size should be 0 since failed jobs are terminal
      expect(await conn.size()).toBe(0);
    });

    it("should no-op for unknown job id", async () => {
      await expect(conn.fail("unknown-id", "reason")).resolves.toBeUndefined();
    });
  });

  describe("remove", () => {
    it("should remove a job from the map", async () => {
      const id = await conn.push("job", { x: 1 });
      expect(await conn.size()).toBe(1);

      await conn.remove(id);
      expect(await conn.size()).toBe(0);
    });

    it("should no-op for unknown job id", async () => {
      await expect(conn.remove("unknown-id")).resolves.toBeUndefined();
    });
  });

  describe("clear", () => {
    it("should remove all jobs from the specified queue", async () => {
      await conn.push("a", {});
      await conn.push("b", {});
      await conn.push("c", {}, { queue: "high" });

      await conn.clear("default");
      expect(await conn.size("default")).toBe(0);
      expect(await conn.size("high")).toBe(1);
    });
  });

  describe("pause and resume", () => {
    it("should return null from pop when paused", async () => {
      await conn.push("job", { x: 1 });
      await conn.pause("default");

      const job = await conn.pop("default");
      expect(job).toBeNull();
    });

    it("should resume popping after resume", async () => {
      await conn.push("job", { x: 1 });
      await conn.pause("default");
      await conn.resume("default");

      const job = await conn.pop("default");
      expect(job).not.toBeNull();
    });

    it("should report isPaused correctly", async () => {
      expect(await conn.isPaused("default")).toBe(false);
      await conn.pause("default");
      expect(await conn.isPaused("default")).toBe(true);
      await conn.resume("default");
      expect(await conn.isPaused("default")).toBe(false);
    });
  });

  describe("size methods", () => {
    it("should count pending jobs", async () => {
      await conn.push("a", {});
      await conn.push("b", {});
      expect(await conn.pendingSize()).toBe(2);
    });

    it("should count delayed jobs", async () => {
      await conn.push("a", {}, { delayMs: 60_000 });
      expect(await conn.delayedSize()).toBe(1);
      expect(await conn.pendingSize()).toBe(0);
    });

    it("should count reserved jobs", async () => {
      await conn.push("a", {});
      await conn.pop();
      expect(await conn.reservedSize()).toBe(1);
    });

    it("should exclude terminal states from size()", async () => {
      await conn.push("a", {});
      const job = await conn.pop();
      await conn.fail(job!.id, "err");
      expect(await conn.size()).toBe(0);
    });
  });

  describe("uniqueness", () => {
    it("should deduplicate jobs with the same uniqueId when in-flight", async () => {
      const id1 = await conn.push("job", { x: 1 }, { uniqueFor: 60_000, uniqueId: "dedup-key" });
      const id2 = await conn.push("job", { x: 1 }, { uniqueFor: 60_000, uniqueId: "dedup-key" });

      expect(id1).toBe(id2);
      expect(await conn.size()).toBe(1);
    });

    it("should allow duplicate after the first job is removed", async () => {
      const id1 = await conn.push("job", { x: 1 }, { uniqueFor: 60_000, uniqueId: "dedup-key" });
      await conn.remove(id1);

      const id2 = await conn.push("job", { x: 1 }, { uniqueFor: 60_000, uniqueId: "dedup-key" });
      expect(id2).not.toBe(id1);
    });

    it("should allow duplicate after the first job is failed", async () => {
      await conn.push("job", { x: 1 }, { uniqueFor: 60_000, uniqueId: "dedup-key" });
      const job = await conn.pop();
      await conn.fail(job!.id, "err");

      const id2 = await conn.push("job", { x: 1 }, { uniqueFor: 60_000, uniqueId: "dedup-key" });
      expect(id2).not.toBe(job!.id);
    });
  });

  describe("close", () => {
    it("should clear all jobs and paused queues", async () => {
      await conn.push("a", {});
      await conn.pause("default");
      await conn.close();

      expect(await conn.size()).toBe(0);
      expect(await conn.isPaused("default")).toBe(false);
    });
  });

  describe("later", () => {
    it("should push a delayed job", async () => {
      const id = await conn.later(5000, "delayed-job", { x: 1 });
      expect(id).toBeDefined();
      expect(await conn.delayedSize()).toBe(1);
    });
  });

  describe("bulk", () => {
    it("should push multiple jobs and return all ids", async () => {
      const ids = await conn.bulk([
        { name: "a", data: { x: 1 } },
        { name: "b", data: { x: 2 } },
        { name: "c", data: { x: 3 } },
      ]);
      expect(ids).toHaveLength(3);
      expect(await conn.size()).toBe(3);
    });
  });
});
