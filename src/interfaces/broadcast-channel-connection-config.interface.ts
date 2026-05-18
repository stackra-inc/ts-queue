/**
 * @fileoverview BroadcastChannel driver configuration.
 *
 * @module interfaces/broadcast-channel-connection-config
 * @category Interfaces
 */

import type { QueueDriverName } from "@stackra/contracts";

/**
 * Configuration for the BroadcastChannel driver.
 *
 * Wraps the IndexedDB driver and adds cross-tab coordination via the
 * browser's native `BroadcastChannel` API. Every tab with the app open
 * shares the same persistent queue; a lightweight leader election
 * ensures only one tab's worker loop drains the queue at a time, while
 * the others hold warm standby state.
 *
 * This is the right default for POS terminals and dashboards where
 * multiple tabs running against the same origin must cooperate.
 */
export interface BroadcastChannelConnectionConfig {
  /** Driver discriminator. */
  driver: QueueDriverName.BroadcastChannel | "broadcast-channel";

  /**
   * BroadcastChannel name used for leader election and status fanout.
   *
   * Must match across all tabs that should share the queue.
   *
   * @default "stackra-queue"
   */
  channelName?: string;

  /**
   * Name of the underlying IndexedDB database.
   *
   * @default "stackra-queue"
   */
  dbName?: string;

  /**
   * Interval between leader heartbeat pings, in milliseconds.
   *
   * Shorter intervals mean faster failover when the leader tab closes,
   * at the cost of slightly more cross-tab chatter.
   *
   * @default 1000
   */
  heartbeatIntervalMs?: number;

  /**
   * Storage key prefix for the underlying IndexedDB stores.
   *
   * @default ""
   */
  prefix?: string;
}
