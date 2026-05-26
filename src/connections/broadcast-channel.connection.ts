/**
 * @fileoverview BroadcastChannel driver.
 *
 * Wraps the IndexedDB driver with cross-tab coordination via
 * `@stackra/ts-coordinator`. Only the leader tab (as determined by
 * the shared `TabCoordinator`) drains the queue — followers always
 * see an empty queue from `pop()`.
 *
 * This is the right default when multiple tabs may be open against the
 * same origin (POS multi-terminal, admin dashboards) and you want a
 * single source of truth for processing order.
 *
 * @module connections/broadcast-channel
 * @category Connections
 */

import type { IJobOptions } from "@stackra/contracts";
import type { IQueuedJob } from "@stackra/contracts";
import { IndexedDBConnection } from "./indexeddb.connection";
import type { TabCoordinator } from "@stackra/ts-coordinator";

/**
 * Cross-tab queue driver. Inherits storage semantics from
 * {@link IndexedDBConnection} and delegates leader election to
 * `@stackra/ts-coordinator`'s {@link TabCoordinator}.
 *
 * Only the leader tab calls `pop()` into a real result — followers
 * always resolve `null` so their worker loops sit idle.
 *
 * @example
 * ```typescript
 * const conn = new BroadcastChannelConnection('bc', coordinator, {
 *   dbName: 'stackra-queue',
 * });
 * const job = await conn.pop(); // only returns a job when this tab is leader
 * ```
 */
export class BroadcastChannelConnection extends IndexedDBConnection {
  /**
   * Reference to the shared TabCoordinator from `@stackra/ts-coordinator`.
   *
   * Used to determine if this tab is the leader and should drain the queue.
   * When `null` (SSR/non-browser), the connection behaves as always-leader.
   */
  private readonly coordinator: TabCoordinator | null;

  /**
   * @param name        - Connection name.
   * @param coordinator - The shared TabCoordinator instance (null for SSR).
   * @param dbName      - IndexedDB database name.
   * @param dbVersion   - IndexedDB schema version.
   * @param prefix      - Object store prefix.
   */
  constructor(
    name: string,
    coordinator: TabCoordinator | null,
    dbName: string = "stackra-queue",
    dbVersion: number = 1,
    prefix: string = "",
  ) {
    super(name, dbName, dbVersion, prefix);
    this.coordinator = coordinator;
  }

  /**
   * Push is leader-agnostic — any tab may enqueue. Storage is shared so
   * the leader sees it on its next `pop()`.
   */
  public override async push<T = unknown>(
    name: string,
    data: T,
    options?: IJobOptions,
  ): Promise<string> {
    return super.push(name, data, options);
  }

  /**
   * Only the leader returns jobs from `pop()` — followers always see an
   * empty queue. This keeps work serialised without requiring locks on
   * the underlying IndexedDB transactions.
   *
   * Leadership is determined by the shared `TabCoordinator` from
   * `@stackra/ts-coordinator`.
   */
  public override async pop(queue: string = "default"): Promise<IQueuedJob | null> {
    if (this.coordinator && !this.coordinator.isLeader()) return null;
    return super.pop(queue);
  }

  /**
   * Teardown — release the IndexedDB handle.
   *
   * Leadership resignation is handled by the TabCoordinator's own
   * lifecycle — no need to manage it here.
   */
  public override async close(): Promise<void> {
    await super.close();
  }
}
