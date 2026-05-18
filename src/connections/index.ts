/**
 * @fileoverview Connections barrel export.
 *
 * Re-exports every built-in driver. Consumers rarely import these
 * directly — the {@link QueueManager} instantiates them based on
 * `QueueModuleOptions.connections`. The exports exist so apps that need
 * a driver outside the DI container (e.g., a one-off script) can build
 * one manually.
 *
 * @module connections
 * @category Connections
 */

export { BaseConnection } from "./base.connection";
export { MemoryConnection } from "./memory.connection";
export { SyncConnection } from "./sync.connection";
export type { SyncJobHandler } from "@/types/sync-job-handler.type";
export { NullConnection } from "./null.connection";
export { LocalStorageConnection } from "./local-storage.connection";
export { IndexedDBConnection } from "./indexeddb.connection";
export { BroadcastChannelConnection } from "./broadcast-channel.connection";
export { QStashConnection } from "./qstash.connection";
