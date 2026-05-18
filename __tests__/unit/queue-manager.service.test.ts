import { describe, it, expect, vi, beforeEach } from "vitest";
import { QUEUE_CONFIG } from "@stackra/contracts";
import { QueueManager } from "@/services/queue-manager.service";
import { QueueHandle } from "@/services/queue-handle.service";
import { QueueDriverError } from "@/errors/queue-driver.error";
import type { QueueModuleOptions } from "@/interfaces/queue-module-options.interface";

function createConfig(overrides: Partial<QueueModuleOptions> = {}): QueueModuleOptions {
  return {
    default: "memory",
    connections: {
      memory: { driver: "memory" } as any,
    },
    worker: {
      tries: 3,
      backoffMs: 1000,
      maxBackoffMs: 30_000,
      timeoutMs: 30_000,
      pollIntervalMs: 500,
    },
    ...overrides,
  };
}

describe("QueueManager", () => {
  let manager: QueueManager;
  let config: QueueModuleOptions;

  beforeEach(() => {
    config = createConfig();
    manager = new (QueueManager as any)(config);
  });

  describe("connection creation", () => {
    it("should create a MemoryConnection for the memory driver", () => {
      manager.onModuleInit();
      const conn = manager.connection("memory");
      expect(conn).toBeDefined();
      expect(conn.name).toBe("memory");
    });

    it("should create a SyncConnection for the sync driver", () => {
      config.connections.sync = { driver: "sync" } as any;
      manager = new (QueueManager as any)(config);
      manager.onModuleInit();

      const conn = manager.connection("sync");
      expect(conn).toBeDefined();
      expect(conn.name).toBe("sync");
    });

    it("should throw for unsupported driver", () => {
      config.connections.bad = { driver: "unsupported" } as any;
      manager = new (QueueManager as any)(config);
      manager.onModuleInit();

      expect(() => manager.connection("bad")).toThrow(QueueDriverError);
    });
  });

  describe("caching", () => {
    it("should return the same connection instance on repeated calls", () => {
      manager.onModuleInit();
      const conn1 = manager.connection("memory");
      const conn2 = manager.connection("memory");
      expect(conn1).toBe(conn2);
    });
  });

  describe("default resolution", () => {
    it("should resolve the default connection when no name is given", () => {
      manager.onModuleInit();
      const conn = manager.connection();
      expect(conn.name).toBe("memory");
    });

    it("should throw on init if default connection is not defined", () => {
      const badConfig = createConfig({ default: "nonexistent" });
      const badManager = new (QueueManager as any)(badConfig);
      expect(() => badManager.onModuleInit()).toThrow(QueueDriverError);
    });
  });

  describe("queue handle", () => {
    it("should return a QueueHandle bound to the specified queue", () => {
      manager.onModuleInit();
      const handle = manager.queue("high", "memory");
      expect(handle).toBeInstanceOf(QueueHandle);
      expect(handle.queue).toBe("high");
    });

    it("should cache handles for the same connection:queue pair", () => {
      manager.onModuleInit();
      const h1 = manager.queue("default", "memory");
      const h2 = manager.queue("default", "memory");
      expect(h1).toBe(h2);
    });

    it("should use default connection when none specified", () => {
      manager.onModuleInit();
      const handle = manager.queue("default");
      expect(handle).toBeInstanceOf(QueueHandle);
    });
  });

  describe("lifecycle hooks", () => {
    it("should close all resolved connections on destroy", async () => {
      manager.onModuleInit();
      const conn = manager.connection("memory");
      const closeSpy = vi.spyOn(conn, "close");

      await manager.onModuleDestroy();
      expect(closeSpy).toHaveBeenCalled();
    });
  });

  describe("getWorkerOptions", () => {
    it("should return fully resolved worker options with defaults", () => {
      const opts = manager.getWorkerOptions();
      expect(opts.tries).toBe(3);
      expect(opts.backoffMs).toBe(1000);
      expect(opts.maxBackoffMs).toBe(30_000);
      expect(opts.timeoutMs).toBe(30_000);
      expect(opts.pollIntervalMs).toBe(500);
      expect(opts.autoStart).toBe(true);
      expect(opts.failOnTimeout).toBe(true);
    });

    it("should apply defaults when worker config is empty", () => {
      const emptyConfig = createConfig({ worker: undefined });
      const m = new (QueueManager as any)(emptyConfig);
      const opts = m.getWorkerOptions();
      expect(opts.tries).toBe(1);
      expect(opts.pollIntervalMs).toBe(500);
    });
  });

  describe("utility methods", () => {
    it("should list connection names", () => {
      expect(manager.getConnectionNames()).toEqual(["memory"]);
    });

    it("should return the default connection name", () => {
      expect(manager.getDefaultConnectionName()).toBe("memory");
    });

    it("should check if a connection exists", () => {
      expect(manager.hasConnection("memory")).toBe(true);
      expect(manager.hasConnection("nonexistent")).toBe(false);
    });
  });
});
