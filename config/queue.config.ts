/**
 * Queue Configuration Preset
 *
 * Default queue configuration that applications can import and customize.
 * Provides sensible defaults for development with IndexedDB as the
 * primary persistent driver.
 *
 * ## Usage
 *
 * ```typescript
 * import queueConfig from "@stackra/ts-queue/config/queue.config";
 *
 * // Use as-is
 * QueueModule.forRoot(queueConfig);
 *
 * // Or spread and override
 * QueueModule.forRoot({
 *   ...queueConfig,
 *   default: "memory",
 * });
 * ```
 *
 * @module @stackra/ts-queue/config
 */

import type { QueueModuleOptions } from "../src/interfaces/queue-module-options.interface";

/**
 * Default queue configuration preset.
 *
 * Connections:
 * - `memory` — volatile, fast (dev/testing)
 * - `indexeddb` — persistent, offline-first (default)
 * - `sync` — immediate execution, no queuing
 * - `null` — no-op (testing)
 */
const queueConfig: QueueModuleOptions = {
  /*
  |--------------------------------------------------------------------------
  | Default Queue Connection
  |--------------------------------------------------------------------------
  |
  | This option defines the default queue connection that will be used
  | to dispatch jobs. The name must match one of the connections defined
  | in the "connections" configuration below.
  |
  */
  default: "indexeddb",

  /*
  |--------------------------------------------------------------------------
  | Global Prefix
  |--------------------------------------------------------------------------
  |
  | Applied to every driver-persisted key to prevent collisions when
  | multiple apps share the same browser storage origin.
  |
  */
  prefix: "stackra:",

  /*
  |--------------------------------------------------------------------------
  | Queue Connections
  |--------------------------------------------------------------------------
  |
  | Here you may configure the queue connections for your application.
  | Each connection uses a specific driver for job storage and processing.
  |
  */
  connections: {
    /**
     * Memory — in-memory queue, fast, lost on page refresh.
     */
    memory: {
      driver: "memory",
    },

    /**
     * IndexedDB — persistent queue, survives refresh, large payloads.
     */
    indexeddb: {
      driver: "indexeddb",
      dbName: "stackra-queue",
      prefix: "q:",
    },

    /**
     * Sync — immediate inline execution (no actual queuing).
     */
    sync: {
      driver: "sync",
    },

    /**
     * Null — no-op driver for testing.
     */
    null: {
      driver: "null",
    },
  },

  /*
  |--------------------------------------------------------------------------
  | Worker Configuration
  |--------------------------------------------------------------------------
  |
  | Shared worker policy. Individual jobs can override via @Job() decorator
  | or dispatch options.
  |
  */
  worker: {
    tries: 3,
    backoffMs: 1000,
    timeoutMs: 30_000,
    concurrency: 3,
  },
};

export default queueConfig;
