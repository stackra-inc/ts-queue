import { describe, it, expect, vi, beforeEach } from "vitest";
import { QueueHandle } from "@/services/queue-handle.service";
import type { QueueConnection } from "@/interfaces/queue-connection.interface";

function createMockConnection(): QueueConnection {
  return {
    name: "memory",
    push: vi.fn().mockResolvedValue("job-1"),
    pop: vi.fn().mockResolvedValue(null),
    later: vi.fn().mockResolvedValue("job-2"),
    bulk: vi.fn().mockResolvedValue(["job-3", "job-4"]),
    size: vi.fn().mockResolvedValue(5),
    pendingSize: vi.fn().mockResolvedValue(3),
    delayedSize: vi.fn().mockResolvedValue(1),
    reservedSize: vi.fn().mockResolvedValue(1),
    remove: vi.fn().mockResolvedValue(undefined),
    release: vi.fn().mockResolvedValue(undefined),
    fail: vi.fn().mockResolvedValue(undefined),
    clear: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined),
    pause: vi.fn().mockResolvedValue(undefined),
    resume: vi.fn().mockResolvedValue(undefined),
    isPaused: vi.fn().mockResolvedValue(false),
  } as any;
}

describe("QueueHandle", () => {
  let conn: QueueConnection;
  let handle: QueueHandle;

  beforeEach(() => {
    conn = createMockConnection();
    handle = new (QueueHandle as any)(conn, "emails");
  });

  it("should expose the queue name", () => {
    expect(handle.queue).toBe("emails");
  });

  describe("push", () => {
    it("should delegate to connection.push with queue pre-applied", async () => {
      const id = await handle.push("send-email", { to: "a@b.com" }, { tries: 3 });
      expect(id).toBe("job-1");
      expect(conn.push).toHaveBeenCalledWith(
        "send-email",
        { to: "a@b.com" },
        {
          tries: 3,
          queue: "emails",
        },
      );
    });

    it("should override caller-supplied queue with the handle queue", async () => {
      await handle.push("job", {}, { queue: "other" });
      expect(conn.push).toHaveBeenCalledWith(
        "job",
        {},
        {
          queue: "emails",
        },
      );
    });
  });

  describe("later", () => {
    it("should delegate to connection.later with queue pre-applied", async () => {
      const id = await handle.later(5000, "delayed-job", { x: 1 });
      expect(id).toBe("job-2");
      expect(conn.later).toHaveBeenCalledWith(
        5000,
        "delayed-job",
        { x: 1 },
        {
          queue: "emails",
        },
      );
    });
  });

  describe("bulk", () => {
    it("should delegate to connection.bulk with queue pre-applied on each job", async () => {
      const jobs = [
        { name: "a", data: { x: 1 } },
        { name: "b", data: { x: 2 }, options: { tries: 2 } },
      ];
      const ids = await handle.bulk(jobs);
      expect(ids).toEqual(["job-3", "job-4"]);
      expect(conn.bulk).toHaveBeenCalledWith([
        { name: "a", data: { x: 1 }, options: { queue: "emails" } },
        { name: "b", data: { x: 2 }, options: { tries: 2, queue: "emails" } },
      ]);
    });
  });

  describe("size methods", () => {
    it("should delegate size to connection with queue name", async () => {
      const s = await handle.size();
      expect(s).toBe(5);
      expect(conn.size).toHaveBeenCalledWith("emails");
    });

    it("should delegate pendingSize", async () => {
      await handle.pendingSize();
      expect(conn.pendingSize).toHaveBeenCalledWith("emails");
    });

    it("should delegate delayedSize", async () => {
      await handle.delayedSize();
      expect(conn.delayedSize).toHaveBeenCalledWith("emails");
    });

    it("should delegate reservedSize", async () => {
      await handle.reservedSize();
      expect(conn.reservedSize).toHaveBeenCalledWith("emails");
    });
  });

  describe("clear", () => {
    it("should delegate clear with queue name", async () => {
      await handle.clear();
      expect(conn.clear).toHaveBeenCalledWith("emails");
    });
  });

  describe("pause/resume", () => {
    it("should delegate pause with queue name", async () => {
      await handle.pause();
      expect(conn.pause).toHaveBeenCalledWith("emails");
    });

    it("should delegate resume with queue name", async () => {
      await handle.resume();
      expect(conn.resume).toHaveBeenCalledWith("emails");
    });

    it("should delegate isPaused with queue name", async () => {
      await handle.isPaused();
      expect(conn.isPaused).toHaveBeenCalledWith("emails");
    });
  });
});
