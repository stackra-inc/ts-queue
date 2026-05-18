/**
 * @fileoverview Discriminated union of every built-in driver config.
 *
 * @module types/queue-connection-config
 * @category Types
 */

import type { MemoryConnectionConfig } from "@/interfaces/memory-connection-config.interface";
import type { SyncConnectionConfig } from "@/interfaces/sync-connection-config.interface";
import type { NullConnectionConfig } from "@/interfaces/null-connection-config.interface";
import type { LocalStorageConnectionConfig } from "@/interfaces/local-storage-connection-config.interface";
import type { IndexedDBConnectionConfig } from "@/interfaces/indexeddb-connection-config.interface";
import type { BroadcastChannelConnectionConfig } from "@/interfaces/broadcast-channel-connection-config.interface";
import type { QStashConnectionConfig } from "@/interfaces/qstash-connection-config.interface";

/**
 * Discriminated union of every built-in connection config.
 *
 * The `driver` field is the discriminant — TypeScript narrows to the
 * correct sub-interface based on its value.
 *
 * Custom drivers registered via `manager.extend()` extend this union
 * through declaration merging — consumers add their own shapes to keep
 * type inference working without modifying this package.
 */
export type QueueConnectionConfig =
  | MemoryConnectionConfig
  | SyncConnectionConfig
  | NullConnectionConfig
  | LocalStorageConnectionConfig
  | IndexedDBConnectionConfig
  | BroadcastChannelConnectionConfig
  | QStashConnectionConfig;
