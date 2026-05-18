import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { QueueEvent } from "@stackra/contracts";
import { ProcessorSubscribersLoader } from "@/services/processor-subscribers.loader";
import { ProcessorMetadataAccessor } from "@/services/processor-metadata.accessor";
import { QueueManager } from "@/services/queue-manager.service";
import { QueueEventBus } from "@/services/event-bus.service";
import { WorkerHost } from "@/hosts/worker-host";
import { SyncConnection } from "@/connections/sync.connection";
import { MemoryConnection } from "@/connections/memory.connection";
import { QueueDriverError } from "@/errors/queue-driver.error";
import { Worker } from "@/services/worker.service";
import type { QueuedJob } from "@/interfaces/queued-job.interface";

class TestProcessor extends WorkerHost {
  public async process(job: QueuedJob): Promise<void> {
    // no-op
  }
}

class NonWorkerHostClass {
  public async process(job: QueuedJob): Promise<void> {
    // no-op
  }
}

/**
 * The ProcessorSubscribersLoader uses `require("@stackra/ts-container").getGlobalApplication()`
 * internally to discover providers. Since this is hard to mock in ESM test environments,
 * we test the loader's internal methods directly by accessing them through the instance.
 */
describe("ProcessorSubscribersLoader", () => {
  let loader: ProcessorSubscribersLoader;
  let manager: QueueManager;
  let accessor: ProcessorMetadataAccessor;
  let eventBus: QueueEventBus;
  let mockEvents: { on: any; off: any; emit: any };

  beforeEach(() => {
    vi.useFakeTimers();

    manager = new (QueueManager as any)({
      default: "memory",
      connections: { memory: { driver: "memory" } },
      worker: {
        tries: 1,
        backoffMs: 1000,
        maxBackoffMs: 30_000,
        timeoutMs: 30_000,
        pollIntervalMs: 100,
        autoStart: true,
        failOnTimeout: true,
      },
    });
    manager.onModuleInit();

    accessor = new (ProcessorMetadataAccessor as any)();
    eventBus = new (QueueEventBus as any)(undefined);
    mockEvents = { on: vi.fn(), off: vi.fn(), emit: vi.fn() };

    loader = new (ProcessorSubscribersLoader as any)(manager, accessor, eventBus, mockEvents);
  });

  afterEach(async () => {
    await loader.onApplicationShutdown();
    vi.useRealTimers();
  });

  describe("auto-discovery (via collectProviderInstances)", () => {
    it("should gracefully handle when global application is not available", () => {
      // The loader's onApplicationBootstrap calls collectProviderInstances internally.
      // When the global app isn't available, it returns null and does nothing.
      // This should not throw.
      loader.onApplicationBootstrap();
      expect((loader as any).workers).toHaveLength(0);
    });
  });

  describe("registerProcessor (internal method)", () => {
    it("should create workers for a WorkerHost processor on memory connection", () => {
      const processor = new TestProcessor();
      const meta = { queue: "default", concurrency: 2 };

      // Call the private method directly
      (loader as any).registerProcessor(processor, meta);

      expect((loader as any).workers).toHaveLength(2);
    });

    it("should throw if processor does not extend WorkerHost", () => {
      const badProcessor = new NonWorkerHostClass();
      const meta = { queue: "default" };

      expect(() => (loader as any).registerProcessor(badProcessor, meta)).toThrow(QueueDriverError);
    });

    it("should create workers with concurrency of 1 by default", () => {
      const processor = new TestProcessor();
      const meta = { queue: "default" };

      (loader as any).registerProcessor(processor, meta);

      expect((loader as any).workers).toHaveLength(1);
    });

    it("should use the specified connection from metadata", () => {
      const processor = new TestProcessor();
      const meta = { queue: "default", connection: "memory" };

      (loader as any).registerProcessor(processor, meta);

      expect((loader as any).workers).toHaveLength(1);
    });

    it("should register handler on SyncConnection instead of creating workers", () => {
      // Set up a sync connection manager
      const syncManager = new (QueueManager as any)({
        default: "sync",
        connections: { sync: { driver: "sync" } },
        worker: {
          tries: 1,
          backoffMs: 1000,
          maxBackoffMs: 30_000,
          timeoutMs: 30_000,
          pollIntervalMs: 100,
          autoStart: true,
          failOnTimeout: true,
        },
      });
      syncManager.onModuleInit();

      const syncLoader = new (ProcessorSubscribersLoader as any)(
        syncManager,
        accessor,
        eventBus,
        mockEvents,
      );

      const processor = new TestProcessor();
      const meta = { queue: "default" };

      (syncLoader as any).registerProcessor(processor, meta);

      // No workers should be created for sync connections
      expect((syncLoader as any).workers).toHaveLength(0);

      // The sync connection should have a handler set
      const conn = syncManager.connection("sync") as SyncConnection;
      expect((conn as any).handler).toBeDefined();
    });
  });

  describe("event wiring (registerEventListeners)", () => {
    it("should subscribe @OnJobEvent methods to the event manager", () => {
      const processor = new TestProcessor();

      // Add a method to the prototype that has event metadata
      Object.defineProperty(Object.getPrototypeOf(processor), "onProcessed", {
        value: function () {},
        writable: true,
        configurable: true,
        enumerable: false,
      });

      vi.spyOn(accessor, "getOnJobEventMetadata").mockImplementation((target) => {
        if (typeof target === "function" && target === (processor as any).onProcessed) {
          return [{ event: QueueEvent.JobProcessed }];
        }
        return undefined;
      });

      (loader as any).registerEventListeners(processor);

      expect(mockEvents.on).toHaveBeenCalledWith(QueueEvent.JobProcessed, expect.any(Function));
      expect((loader as any).listeners).toHaveLength(1);
    });

    it("should not subscribe when no event manager is available", () => {
      const loaderNoEvents = new (ProcessorSubscribersLoader as any)(
        manager,
        accessor,
        eventBus,
        undefined,
      );

      const processor = new TestProcessor();
      (loaderNoEvents as any).registerEventListeners(processor);

      // No listeners should be registered
      expect((loaderNoEvents as any).listeners).toHaveLength(0);
    });
  });

  describe("shutdown", () => {
    it("should stop all workers on shutdown", async () => {
      const processor = new TestProcessor();
      (loader as any).registerProcessor(processor, { queue: "default", concurrency: 3 });

      expect((loader as any).workers).toHaveLength(3);

      await loader.onApplicationShutdown();
      expect((loader as any).workers).toHaveLength(0);
    });

    it("should unsubscribe event listeners on shutdown", async () => {
      // Manually add a listener entry
      const listener = vi.fn();
      (loader as any).listeners.push({ event: QueueEvent.JobProcessed, listener });

      await loader.onApplicationShutdown();

      expect(mockEvents.off).toHaveBeenCalledWith(QueueEvent.JobProcessed, listener);
      expect((loader as any).listeners).toHaveLength(0);
    });

    it("should be idempotent — safe to call multiple times", async () => {
      const processor = new TestProcessor();
      (loader as any).registerProcessor(processor, { queue: "default" });

      await loader.onApplicationShutdown();
      await loader.onApplicationShutdown();
      // Should not throw
      expect((loader as any).workers).toHaveLength(0);
    });
  });
});
