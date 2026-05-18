/**
 * @fileoverview Interfaces barrel export.
 *
 * All shape definitions for the queue package. These are intentionally
 * separated from the types folder which holds type aliases and unions —
 * following the code standards rule "one export per file" (see
 * `.kiro/steering/code-standards.md §3`).
 *
 * @module interfaces
 * @category Interfaces
 */

// ── Module & configuration ───────────────────────────────────────────────
export type { QueueModuleOptions } from "./queue-module-options.interface";
export type { WorkerOptions } from "./worker-options.interface";

// ── Driver contract ──────────────────────────────────────────────────────
export type { QueueConnection } from "./queue-connection.interface";

// ── Job shapes ───────────────────────────────────────────────────────────
export type { QueuedJob } from "./queued-job.interface";
export type { JobOptions } from "./job-options.interface";

// ── Driver configurations ────────────────────────────────────────────────
export type { MemoryConnectionConfig } from "./memory-connection-config.interface";
export type { SyncConnectionConfig } from "./sync-connection-config.interface";
export type { NullConnectionConfig } from "./null-connection-config.interface";
export type { LocalStorageConnectionConfig } from "./local-storage-connection-config.interface";
export type { IndexedDBConnectionConfig } from "./indexeddb-connection-config.interface";
export type { BroadcastChannelConnectionConfig } from "./broadcast-channel-connection-config.interface";
export type { QStashConnectionConfig } from "./qstash-connection-config.interface";

// ── Decorator metadata shapes ────────────────────────────────────────────
export type { ProcessorMetadata } from "./processor-metadata.interface";
export type { OnJobEventMetadata } from "./on-job-event-metadata.interface";
export type { JobMetadata } from "./job-metadata.interface";
