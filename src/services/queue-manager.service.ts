/**
 * @fileoverview QueueManager — central orchestrator for the queue system.
 *
 * Extends `MultipleInstanceManager<QueueConnection>` so it gets lazy
 * resolution, cache, `extend()`, and `forget()` for free (same pattern
 * as `CacheManager`, `HttpManager`, `EventEmitterManager`, …).
 *
 * Responsibilities:
 * - Build {@link QueueConnection} instances from
 *   {@link QueueModuleOptions.connections}.
 * - Expose `connection(name?)` for driver-level access.
 * - Expose `queue(name?, connection?)` for handle-level access.
 * - Own the shared worker policy ({@link WorkerOptions}).
 * - Close every connection on `OnModuleDestroy`.
 *
 * Lifecycle event publishing and worker loops live in {@link Worker} and
 * {@link ProcessorSubscribersLoader} so this class stays focused on
 * creation and lookup.
 *
 * @module services/queue-manager
 * @category Services
 */

import {
  Injectable,
  Inject,
  Optional,
  type OnModuleInit,
  type OnModuleDestroy,
} from "@stackra/ts-container";
import { MultipleInstanceManager } from "@stackra/ts-support";

import { QueueDriverError } from "@/errors/queue-driver.error";
import { QUEUE_CONFIG, QueueDriverName, TAB_COORDINATOR } from "@stackra/contracts";
import type { QueueConnection } from "@/interfaces/queue-connection.interface";
import type { QueueModuleOptions } from "@/interfaces/queue-module-options.interface";
import type { WorkerOptions } from "@/interfaces/worker-options.interface";
import type { BroadcastChannelConnectionConfig } from "@/interfaces/broadcast-channel-connection-config.interface";
import type { IndexedDBConnectionConfig } from "@/interfaces/indexeddb-connection-config.interface";
import type { LocalStorageConnectionConfig } from "@/interfaces/local-storage-connection-config.interface";
import type { QStashConnectionConfig } from "@/interfaces/qstash-connection-config.interface";
import type { MemoryConnectionConfig } from "@/interfaces/memory-connection-config.interface";
import type { QueueConnectionConfig } from "@/types/queue-connection-config.type";
import type { TabCoordinator } from "@stackra/ts-coordinator";

import { MemoryConnection } from "@/connections/memory.connection";
import { SyncConnection } from "@/connections/sync.connection";
import { NullConnection } from "@/connections/null.connection";
import { LocalStorageConnection } from "@/connections/local-storage.connection";
import { IndexedDBConnection } from "@/connections/indexeddb.connection";
import { BroadcastChannelConnection } from "@/connections/broadcast-channel.connection";
import { QStashConnection } from "@/connections/qstash.connection";
import { QueueHandle } from "./queue-handle.service";
import { Logger } from "@stackra/ts-logger";

/**
 * The central queue orchestrator.
 *
 * @example
 * ```typescript
 * @Injectable()
 * class AdminService {
 *   constructor(private readonly queues: QueueManager) {}
 *
 *   async clearAll(): Promise<void> {
 *     for (const name of this.queues.getConnectionNames()) {
 *       await this.queues.connection(name).clear();
 *     }
 *   }
 * }
 * ```
 */
@Injectable()
export class QueueManager
  extends MultipleInstanceManager<QueueConnection>
  implements OnModuleInit, OnModuleDestroy
{
  /**
   * Cached {@link QueueHandle} wrappers, keyed by `"${connection}:${queue}"`.
   *
   * Handles are light, but caching them lets consumers get the same
   * instance for the same pair — which is useful for tests that
   * patch methods on the handle.
   */
  private readonly handles: Map<string, QueueHandle> = new Map();

  constructor(
    @Inject(QUEUE_CONFIG) private readonly config: QueueModuleOptions,
    @Optional() @Inject(TAB_COORDINATOR) private readonly coordinator?: TabCoordinator,
  ) {
    super();
  }

  /**
   * Logger instance scoped to the QueueManager context.
   */
  private readonly logger = new Logger(QueueManager.name);

  // ── Lifecycle hooks ────────────────────────────────────────────────────

  /**
   * Validate the default connection exists and eagerly warm it so
   * config errors surface at bootstrap rather than at the first dispatch.
   */
  public onModuleInit(): void {
    if (!this.config.connections[this.config.default]) {
      throw new QueueDriverError(
        `[QueueManager] Default connection '${this.config.default}' is not defined. ` +
          `Available connections: ${Object.keys(this.config.connections).join(", ")}`,
      );
    }

    try {
      this.connection();
    } catch (err: Error | any) {
      this.logger.warn(
        `[QueueManager] Failed to warm default connection '${this.config.default}': ${(err as Error).message}`,
      );
    }
  }

  /**
   * Close every resolved connection on app shutdown.
   *
   * Drivers release IndexedDB handles, stop heartbeats, and clear
   * timers — important to do explicitly so tests don't leave handles
   * open and hot-reload doesn't leak resources.
   */
  public async onModuleDestroy(): Promise<void> {
    const names = this.getResolvedInstances();
    for (const name of names) {
      try {
        const conn = this.instance(name);
        await conn.close();
      } catch {
        /* ignore */
      }
    }
    this.handles.clear();
    this.purge();
  }

  // ── MultipleInstanceManager contract ──────────────────────────────────

  public getDefaultInstance(): string {
    return this.config.default;
  }

  public setDefaultInstance(name: string): void {
    (this.config as { default: string }).default = name;
  }

  public getInstanceConfig(name: string): Record<string, unknown> | undefined {
    const raw = this.config.connections[name];
    return raw as unknown as Record<string, unknown> | undefined;
  }

  /**
   * Build a driver instance for the given name + config pair.
   *
   * Dispatches on `config.driver`. Consumers can add custom drivers via
   * `manager.extend('my-driver', factory)` inherited from the base class.
   */
  protected createDriver(driver: string, config: Record<string, unknown>): QueueConnection {
    // Cast through `unknown` so TS lets us narrow to each driver's config.
    // Using `QueueConnectionConfig` as the intermediate type keeps the
    // driver-specific casts below readable.
    const cfg = config as unknown as QueueConnectionConfig & { __name?: string; prefix?: string };

    const prefix = (this.config.prefix ?? "") + (cfg.prefix ?? "");
    const connName = cfg.__name ?? driver;

    switch (driver) {
      case QueueDriverName.Memory:
      case "memory": {
        // MemoryConnection currently needs only the connection name.
        void (cfg as MemoryConnectionConfig);
        return new MemoryConnection(connName);
      }
      case QueueDriverName.Sync:
      case "sync":
        return new SyncConnection(connName);
      case QueueDriverName.Null:
      case "null":
        return new NullConnection(connName);
      case QueueDriverName.LocalStorage:
      case "local-storage": {
        void (cfg as LocalStorageConnectionConfig);
        return new LocalStorageConnection(connName, prefix);
      }
      case QueueDriverName.IndexedDB:
      case "indexeddb": {
        const c = cfg as IndexedDBConnectionConfig;
        return new IndexedDBConnection(connName, c.dbName, c.dbVersion, prefix);
      }
      case QueueDriverName.BroadcastChannel:
      case "broadcast-channel": {
        const c = cfg as BroadcastChannelConnectionConfig;
        return new BroadcastChannelConnection(
          connName,
          this.coordinator ?? null,
          c.dbName,
          1,
          prefix,
        );
      }
      case QueueDriverName.QStash:
      case "qstash":
        return new QStashConnection(connName, cfg as QStashConnectionConfig);
      default:
        throw new QueueDriverError(
          `[QueueManager] Queue driver '${driver}' is not supported. Use extend() to register a custom driver.`,
        );
    }
  }

  // ── Public API ────────────────────────────────────────────────────────

  /**
   * Get a {@link QueueConnection} for the named driver.
   *
   * Lazily creates the connection on first access. Pass no argument to
   * use the configured default.
   */
  public connection(name?: string): QueueConnection {
    return this.instance(name);
  }

  /**
   * Get a {@link QueueHandle} bound to a specific `(connection, queue)`.
   *
   * @param queue      - Queue tube name (defaults to `"default"`).
   * @param connection - Connection name (defaults to the module default).
   */
  public queue(queue: string = "default", connection?: string): QueueHandle {
    const connName = connection ?? this.config.default;
    const key = `${connName}:${queue}`;

    const cached = this.handles.get(key);
    if (cached) return cached;

    const handle = new QueueHandle(this.connection(connName), queue);
    this.handles.set(key, handle);
    return handle;
  }

  /**
   * All configured connection names.
   *
   * @returns Array of connection names declared in
   *   {@link QueueModuleOptions.connections}.
   */
  public getConnectionNames(): string[] {
    return Object.keys(this.config.connections);
  }

  /**
   * The default connection name.
   *
   * Used by `connection()` and `queue()` when no name is supplied.
   *
   * @returns The default connection's name.
   */
  public getDefaultConnectionName(): string {
    return this.config.default;
  }

  /**
   * Whether a connection name is configured.
   *
   * @param name - Connection name to check.
   * @returns `true` when the name appears in module config.
   */
  public hasConnection(name: string): boolean {
    return name in this.config.connections;
  }

  /**
   * Shared worker options resolved with sensible defaults.
   *
   * Workers consult this for poll interval, retry policy, timeouts,
   * etc. The defaults here mirror Laravel's worker defaults so apps
   * familiar with the Laravel queue feel at home.
   *
   * @returns The fully resolved worker options.
   */
  public getWorkerOptions(): Required<WorkerOptions> {
    const w = this.config.worker ?? {};
    return {
      tries: w.tries ?? 1,
      backoffMs: w.backoffMs ?? 1000,
      maxBackoffMs: w.maxBackoffMs ?? 30_000,
      timeoutMs: w.timeoutMs ?? 30_000,
      pollIntervalMs: w.pollIntervalMs ?? 500,
      autoStart: w.autoStart ?? true,
      failOnTimeout: w.failOnTimeout ?? true,
    };
  }
}
