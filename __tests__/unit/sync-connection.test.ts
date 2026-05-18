import { describe, it, expect, vi, beforeEach } from "vitest";
import { JobStatus } from "@stackra/contracts";
import { SyncConnection } from "@/connections/sync.connection";

describe("SyncConnection", () => {
  let conn: SyncConnection;

  beforeEach(() => {
    conn = new (SyncConnection as any)("sync");
  });

  describe("inline execution", () => {
    it("should invoke the handler immediately on push", async () => {
      const handler = vi.fn();
      conn.setHandler(handler);

      await conn.push("test-job", { x: 1 });
      expect(handler).toHaveBeenCalledTimes(1);
      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({
          name: "test-job",
          data: { x: 1 },
          status: JobStatus.Reserved,
          attempts: 1,
        }),
      );
    });

    it("should return a job id on push", async () => {
      conn.setHandler(vi.fn());
      const id = await conn.push("job", {});
      expect(id).toBeDefined();
      expect(typeof id).toBe("string");
    });

    it("should propagate handler errors to the caller", async () => {
      conn.setHandler(async () => {
        throw new Error("handler failed");
      });

      await expect(conn.push("job", {})).rejects.toThrow("handler failed");
    });
  });

  describe("handler registration", () => {
    it("should silently discard jobs when no handler is registered", async () => {
      const id = await conn.push("job", {});
      expect(id).toBeDefined();
    });

    it("should use the latest registered handler", async () => {
      const first = vi.fn();
      const second = vi.fn();

      conn.setHandler(first);
      conn.setHandler(second);

      await conn.push("job", {});
      expect(first).not.toHaveBeenCalled();
      expect(second).toHaveBeenCalledTimes(1);
    });
  });

  describe("pause bypass", () => {
    it("should not invoke handler when queue is paused", async () => {
      const handler = vi.fn();
      conn.setHandler(handler);

      await conn.pause("default");
      const id = await conn.push("job", {});

      expect(handler).not.toHaveBeenCalled();
      expect(id).toBeDefined();
    });

    it("should invoke handler after resume", async () => {
      const handler = vi.fn();
      conn.setHandler(handler);

      await conn.pause("default");
      await conn.resume("default");
      await conn.push("job", {});

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it("should only pause the specified queue", async () => {
      const handler = vi.fn();
      conn.setHandler(handler);

      await conn.pause("high");
      await conn.push("job", {}, { queue: "default" });

      expect(handler).toHaveBeenCalledTimes(1);
    });
  });

  describe("no-op methods", () => {
    it("pop should always return null", async () => {
      conn.setHandler(vi.fn());
      expect(await conn.pop()).toBeNull();
    });

    it("size should always return 0", async () => {
      expect(await conn.size()).toBe(0);
    });

    it("pendingSize should always return 0", async () => {
      expect(await conn.pendingSize()).toBe(0);
    });

    it("delayedSize should always return 0", async () => {
      expect(await conn.delayedSize()).toBe(0);
    });

    it("reservedSize should always return 0", async () => {
      expect(await conn.reservedSize()).toBe(0);
    });

    it("remove should be a no-op", async () => {
      await expect(conn.remove("any-id" as any)).resolves.toBeUndefined();
    });

    it("release should be a no-op", async () => {
      await expect(conn.release("any-id" as any)).resolves.toBeUndefined();
    });

    it("fail should be a no-op", async () => {
      await expect(conn.fail("any-id" as any, "reason" as any)).resolves.toBeUndefined();
    });

    it("clear should be a no-op", async () => {
      await expect(conn.clear()).resolves.toBeUndefined();
    });
  });

  describe("close", () => {
    it("should drop the handler so subsequent pushes are no-ops", async () => {
      const handler = vi.fn();
      conn.setHandler(handler);
      await conn.close();

      await conn.push("job", {});
      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe("later and bulk (inherited)", () => {
    it("later should invoke handler with delay option", async () => {
      const handler = vi.fn();
      conn.setHandler(handler);

      await conn.later(5000, "delayed", { x: 1 });
      expect(handler).toHaveBeenCalledTimes(1);
    });

    it("bulk should invoke handler for each job", async () => {
      const handler = vi.fn();
      conn.setHandler(handler);

      await conn.bulk([
        { name: "a", data: { x: 1 } },
        { name: "b", data: { x: 2 } },
      ]);
      expect(handler).toHaveBeenCalledTimes(2);
    });
  });
});
