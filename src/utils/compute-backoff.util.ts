/**
 * @fileoverview Exponential backoff helper.
 *
 * Centralises the retry delay formula so every driver and worker uses
 * the same policy. Mirrors Laravel's exponential backoff semantics.
 *
 * @module utils/compute-backoff
 * @category Utils
 */

/**
 * Compute the exponential backoff delay for a given attempt.
 *
 * Formula: `min(baseMs * 2^(attempt - 1), maxMs)`.
 *
 * @param attempt - 1-based attempt number (first retry is attempt = 2).
 * @param baseMs  - Base backoff in milliseconds.
 * @param maxMs   - Upper bound for the backoff in milliseconds.
 * @returns Backoff delay in milliseconds.
 *
 * @example
 * ```typescript
 * computeBackoff(1, 1000, 30_000); // 1000   (first retry)
 * computeBackoff(2, 1000, 30_000); // 2000
 * computeBackoff(5, 1000, 30_000); // 16_000
 * computeBackoff(6, 1000, 30_000); // 30_000 (clamped)
 * ```
 */
export function computeBackoff(attempt: number, baseMs: number, maxMs: number): number {
  if (attempt <= 1) return baseMs;
  const exp = baseMs * Math.pow(2, attempt - 1);
  return Math.min(exp, maxMs);
}
