import { describe, it, expect, vi, beforeEach } from "vitest";
import { QueueEvent } from "@stackra/contracts";
import { QueueEventBus } from "@/services/event-bus.service";

describe("QueueEventBus", () => {
  describe("with EventManager", () => {
    it("should emit events through the event manager", () => {
      const events = { emit: vi.fn() };
      const bus = new (QueueEventBus as any)(events);

      const payload = { job: { id: "1" } };
      bus.emit(QueueEvent.JobProcessing, payload);

      expect(events.emit).toHaveBeenCalledWith(QueueEvent.JobProcessing, payload);
    });

    it("should emit different event types", () => {
      const events = { emit: vi.fn() };
      const bus = new (QueueEventBus as any)(events);

      bus.emit(QueueEvent.JobProcessed, { job: { id: "1" } });
      bus.emit(QueueEvent.JobFailed, { job: { id: "2" }, error: new Error("x") });

      expect(events.emit).toHaveBeenCalledTimes(2);
      expect(events.emit).toHaveBeenCalledWith(QueueEvent.JobProcessed, expect.any(Object));
      expect(events.emit).toHaveBeenCalledWith(QueueEvent.JobFailed, expect.any(Object));
    });
  });

  describe("without EventManager", () => {
    it("should no-op when no event manager is provided", () => {
      const bus = new (QueueEventBus as any)(undefined);

      // Should not throw
      expect(() => bus.emit(QueueEvent.JobProcessing, { job: { id: "1" } })).not.toThrow();
    });
  });

  describe("error isolation", () => {
    it("should swallow errors thrown by the event manager", () => {
      const events = {
        emit: vi.fn().mockImplementation(() => {
          throw new Error("subscriber exploded");
        }),
      };
      const bus = new (QueueEventBus as any)(events);

      // Should not throw
      expect(() => bus.emit(QueueEvent.JobProcessing, { job: { id: "1" } })).not.toThrow();
    });

    it("should still call emit even if previous calls threw", () => {
      const events = { emit: vi.fn() };
      const bus = new (QueueEventBus as any)(events);

      // First call throws
      events.emit.mockImplementationOnce(() => {
        throw new Error("first error");
      });
      bus.emit(QueueEvent.JobProcessing, {});

      // Second call should still work
      events.emit.mockImplementation(() => {});
      bus.emit(QueueEvent.JobProcessed, {});

      expect(events.emit).toHaveBeenCalledTimes(2);
    });
  });
});
