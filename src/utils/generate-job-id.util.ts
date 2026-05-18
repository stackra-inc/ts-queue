/**
 * @fileoverview Unique job id generator.
 *
 * Uses `crypto.randomUUID()` when available (modern browsers, Node 19+).
 * Falls back to a timestamp + random suffix for environments where the
 * Web Crypto API is not yet exposed. Both paths are sufficiently unique
 * for queue ids — collisions are vanishingly unlikely.
 *
 * @module utils/generate-job-id
 * @category Utils
 */

/**
 * Generate a unique identifier suitable for a {@link QueuedJob.id}.
 *
 * @returns A globally unique string (UUID v4 when available).
 *
 * @example
 * ```typescript
 * const id = generateJobId();
 * // "8f3c5e62-7f4a-4b09-9e1c-24eeab5f3c72"
 * ```
 */
export function generateJobId(): string {
  // Web Crypto API — browsers + modern Node. Preferred path.
  const g = globalThis as { crypto?: { randomUUID?: () => string } };
  if (g.crypto?.randomUUID) {
    return g.crypto.randomUUID();
  }

  // Fallback for environments without randomUUID. 36^9 ≈ 101 trillion, more
  // than enough uniqueness when combined with the millisecond timestamp.
  const ts = Date.now().toString(36);
  const rand = Math.random().toString(36).slice(2, 11);
  return `job_${ts}_${rand}`;
}
