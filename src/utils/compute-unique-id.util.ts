/**
 * @fileoverview Deduplication key helper.
 *
 * When a caller sets `JobOptions.uniqueFor` but does not provide an
 * explicit `uniqueId`, the dispatcher derives one from the job name and
 * a stable hash of the payload. Same payload in the uniqueness window →
 * same id → dedup.
 *
 * @module utils/compute-unique-id
 * @category Utils
 */

/**
 * Derive a stable deduplication id for a job name + payload pair.
 *
 * Uses FNV-1a 32-bit hashing — fast, zero-dependency, and sufficient for
 * the "is this the same dispatch" question a deduplication window asks.
 * Payloads are JSON-stringified with deterministic key ordering so that
 * property order does not affect the hash.
 *
 * @param name - The job name.
 * @param data - The payload. Must be JSON-serialisable.
 * @returns A stable string id like `"u_1a2b3c4d"`.
 */
export function computeUniqueId(name: string, data: unknown): string {
  const canonical = stableStringify(data);
  const h = fnv1a(`${name}:${canonical}`);
  return `u_${h.toString(16)}`;
}

/**
 * FNV-1a 32-bit hash — non-cryptographic, fast, deterministic.
 *
 * @internal
 */
function fnv1a(input: string): number {
  // FNV-1a 32-bit constants.
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    // Avoid Math.imul for older runtimes — multiply + cast.
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash;
}

/**
 * Deterministic JSON.stringify — sorts object keys so property order
 * doesn't affect the hash.
 *
 * @internal
 */
function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  const pairs = keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`);
  return `{${pairs.join(",")}}`;
}
