import { describe, it, expect } from "vitest";
import { computeBackoff } from "@/utils/compute-backoff.util";

describe("computeBackoff", () => {
  describe("basic formula", () => {
    it("should return baseMs for attempt 1", () => {
      expect(computeBackoff(1, 1000, 30_000)).toBe(1000);
    });

    it("should double for attempt 2", () => {
      expect(computeBackoff(2, 1000, 30_000)).toBe(2000);
    });

    it("should quadruple for attempt 3", () => {
      expect(computeBackoff(3, 1000, 30_000)).toBe(4000);
    });

    it("should compute 2^(attempt-1) * baseMs", () => {
      expect(computeBackoff(4, 1000, 30_000)).toBe(8000);
      expect(computeBackoff(5, 1000, 30_000)).toBe(16_000);
    });
  });

  describe("monotonicity", () => {
    it("should produce non-decreasing values for increasing attempts", () => {
      const values = Array.from({ length: 10 }, (_, i) => computeBackoff(i + 1, 1000, 60_000));
      for (let i = 1; i < values.length; i++) {
        expect(values[i]).toBeGreaterThanOrEqual(values[i - 1]);
      }
    });
  });

  describe("clamping", () => {
    it("should clamp at maxMs", () => {
      expect(computeBackoff(6, 1000, 30_000)).toBe(30_000);
      expect(computeBackoff(10, 1000, 30_000)).toBe(30_000);
      expect(computeBackoff(100, 1000, 30_000)).toBe(30_000);
    });

    it("should clamp when baseMs * 2^(attempt-1) exceeds maxMs", () => {
      // 500 * 2^6 = 32000, clamped to 10000
      expect(computeBackoff(7, 500, 10_000)).toBe(10_000);
    });
  });

  describe("edge cases", () => {
    it("should return baseMs for attempt 0 (treated as <= 1)", () => {
      expect(computeBackoff(0, 1000, 30_000)).toBe(1000);
    });

    it("should return baseMs for negative attempt", () => {
      expect(computeBackoff(-1, 1000, 30_000)).toBe(1000);
    });

    it("should handle baseMs of 0", () => {
      expect(computeBackoff(1, 0, 30_000)).toBe(0);
      expect(computeBackoff(5, 0, 30_000)).toBe(0);
    });

    it("should handle maxMs of 0 with attempt > 1", () => {
      // For attempt <= 1, baseMs is returned directly (no clamping applied)
      // For attempt > 1, Math.min(exp, 0) = 0
      expect(computeBackoff(2, 1000, 0)).toBe(0);
      expect(computeBackoff(5, 1000, 0)).toBe(0);
    });

    it("should handle very large attempt numbers without overflow issues", () => {
      const result = computeBackoff(50, 1000, 60_000);
      expect(result).toBe(60_000);
    });
  });

  describe("different base values", () => {
    it("should work with baseMs of 500", () => {
      expect(computeBackoff(1, 500, 30_000)).toBe(500);
      expect(computeBackoff(2, 500, 30_000)).toBe(1000);
      expect(computeBackoff(3, 500, 30_000)).toBe(2000);
    });

    it("should work with baseMs of 2000", () => {
      expect(computeBackoff(1, 2000, 30_000)).toBe(2000);
      expect(computeBackoff(2, 2000, 30_000)).toBe(4000);
    });
  });
});
