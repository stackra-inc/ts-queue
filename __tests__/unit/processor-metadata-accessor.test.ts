import { describe, it, expect, vi, beforeEach } from "vitest";
import { ProcessorMetadataAccessor } from "@/services/processor-metadata.accessor";
import { PROCESSOR_METADATA, ON_JOB_EVENT_METADATA } from "@/constants/tokens.constant";

// Mock @stackra/ts-metadata
vi.mock("@stackra/ts-metadata", () => ({
  getMetadata: vi.fn(),
}));

import { getMetadata } from "@stackra/ts-metadata";

describe("ProcessorMetadataAccessor", () => {
  let accessor: ProcessorMetadataAccessor;

  beforeEach(() => {
    accessor = new (ProcessorMetadataAccessor as any)();
    vi.clearAllMocks();
  });

  describe("getProcessorMetadata", () => {
    it("should return metadata for a decorated class", () => {
      const meta = { queue: "emails", connection: "memory", concurrency: 2 };
      (getMetadata as any).mockReturnValue(meta);

      class MyProcessor {}
      const result = accessor.getProcessorMetadata(MyProcessor);

      expect(getMetadata).toHaveBeenCalledWith(PROCESSOR_METADATA, MyProcessor);
      expect(result).toEqual(meta);
    });

    it("should return undefined for an undecorated class", () => {
      (getMetadata as any).mockReturnValue(undefined);

      class PlainClass {}
      const result = accessor.getProcessorMetadata(PlainClass);

      expect(result).toBeUndefined();
    });

    it("should return undefined for null target", () => {
      const result = accessor.getProcessorMetadata(null);
      expect(result).toBeUndefined();
    });

    it("should return undefined for non-function target", () => {
      const result = accessor.getProcessorMetadata("not-a-class");
      expect(result).toBeUndefined();
    });

    it("should return undefined for undefined target", () => {
      const result = accessor.getProcessorMetadata(undefined);
      expect(result).toBeUndefined();
    });
  });

  describe("getOnJobEventMetadata", () => {
    it("should return an array for a single metadata entry", () => {
      const meta = { event: "queue.job.processed", connection: "memory" };
      (getMetadata as any).mockReturnValue(meta);

      const method = () => {};
      const result = accessor.getOnJobEventMetadata(method);

      expect(getMetadata).toHaveBeenCalledWith(ON_JOB_EVENT_METADATA, method);
      expect(result).toEqual([meta]);
    });

    it("should return the array as-is when metadata is already an array", () => {
      const meta = [{ event: "queue.job.processed" }, { event: "queue.job.failed" }];
      (getMetadata as any).mockReturnValue(meta);

      const method = () => {};
      const result = accessor.getOnJobEventMetadata(method);

      expect(result).toEqual(meta);
    });

    it("should return undefined for an undecorated method", () => {
      (getMetadata as any).mockReturnValue(undefined);

      const method = () => {};
      const result = accessor.getOnJobEventMetadata(method);

      expect(result).toBeUndefined();
    });

    it("should return undefined for null target", () => {
      const result = accessor.getOnJobEventMetadata(null);
      expect(result).toBeUndefined();
    });

    it("should return undefined for non-function/non-object target", () => {
      const result = accessor.getOnJobEventMetadata(42);
      expect(result).toBeUndefined();
    });

    it("should accept object targets (prototype methods)", () => {
      const meta = { event: "queue.job.processed" };
      (getMetadata as any).mockReturnValue(meta);

      const target = {};
      const result = accessor.getOnJobEventMetadata(target);

      expect(result).toEqual([meta]);
    });
  });
});
