/**
 * @fileoverview Bootstrap loader.
 *
 * Mirrors the NestJS Bull `BullExplorer` + `BullRegistrar` pattern:
 *
 * 1. On `onApplicationBootstrap`, walk every DI provider.
 * 2. For each `@Processor`-decorated class, create one or more
 *    {@link Worker} instances (according to `concurrency`) and start them.
 * 3. For each `@OnJobEvent`-decorated method, subscribe it to the
 *    matching lifecycle event on the `EventManager` (via the event bus
 *    indirection so `@stackra/ts-events` stays optional).
 * 4. On `onApplicationShutdown`, stop every worker and remove every
 *    event listener.
 *
 * The loader uses the same "walk the container modules" trick
 * `EventSubscribersLoader` uses — it accesses the global application
 * exposed by `@stackra/ts-container` and iterates provider instances.
 *
 * @module services/processor-subscribers-loader
 * @category Services
 */

import {
  Injectable,
  Inject,
  Optional,
  type IOnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@stackra/ts-container';

import { EVENT_EMITTER } from '@stackra/contracts';
import { QueueDriverError } from '@/errors/queue-driver.error';
import { WorkerHost } from '@/hosts/worker-host';
import type { IOnJobEventMetadata } from '@/interfaces/on-job-event-metadata.interface';
import type { IProcessorMetadata } from '@stackra/contracts';
import type { IQueuedJob } from '@stackra/contracts';
import { ProcessorMetadataAccessor } from './../accessors/processor-metadata.accessor';
import { QueueManager } from './../services/queue-manager.service';
import { SyncConnection } from '@/connections/sync.connection';
import { QueueEventBus } from './../services/event-bus.service';
import { Worker } from './../services/worker.service';
import { Logger } from '@stackra/ts-logger';

// `require` is injected by tsup for CJS interop; declare it so TS is happy.
declare const require: (id: string) => { getGlobalApplication?: () => unknown };

/** Minimal event manager contract — see `event-bus.service.ts`. */
interface EventManagerLike {
  on(event: string, listener: (...args: unknown[]) => void): unknown;
  off(event: string, listener: (...args: unknown[]) => void): unknown;
}

/**
 * Discovers `@Processor` classes + `@OnJobEvent` methods and wires them
 * to the running {@link QueueManager}.
 */
@Injectable()
export class ProcessorSubscribersLoader implements IOnApplicationBootstrap, OnApplicationShutdown {
  /** Every worker started by the loader — stopped on shutdown. */
  private readonly workers: Worker[] = [];

  /**
   * Logger instance scoped to the ProcessorSubscribersLoader context.
   */
  private readonly logger = new Logger(ProcessorSubscribersLoader.name);

  /**
   * Event manager listeners the loader registered — removed on shutdown.
   * Stored as `[event, listener]` tuples so we can call `.off(...)`
   * symmetrically with the `.on(...)` we did at bootstrap.
   */
  private readonly listeners: Array<{
    event: string;
    listener: (...args: unknown[]) => void;
  }> = [];

  constructor(
    private readonly manager: QueueManager,
    private readonly metadataAccessor: ProcessorMetadataAccessor,
    private readonly eventBus: QueueEventBus,
    @Optional()
    @Inject(EVENT_EMITTER)
    private readonly events?: EventManagerLike
  ) {}

  /**
   * Walk every DI provider, wire workers and event listeners.
   *
   * Called once at application bootstrap. The implementation:
   * 1. Resolves the global application via `getGlobalApplication()`.
   * 2. Iterates every container module's providers.
   * 3. For each `@Processor`-decorated instance, spawns workers and
   *    subscribes its `@OnJobEvent` methods to the lifecycle bus.
   *
   * This mirrors NestJS Bull's `BullExplorer` and stays opt-in — apps
   * that don't decorate any classes simply do nothing here.
   */
  public async onApplicationBootstrap(): Promise<void> {
    const providers = this.collectProviderInstances();
    if (!providers) return;

    for (const instance of providers) {
      const meta = this.metadataAccessor.getProcessorMetadata(
        (instance as { constructor?: unknown }).constructor
      );
      if (!meta) continue;
      await this.registerProcessor(instance, meta);
      this.registerEventListeners(instance);
    }
  }

  /**
   * Stop every worker and unsubscribe every event listener.
   *
   * Idempotent — safe to call multiple times during shutdown. Errors
   * inside individual stops are swallowed to ensure a partial failure
   * doesn't prevent the rest of the cleanup from running.
   */
  public async onApplicationShutdown(): Promise<void> {
    for (const worker of this.workers) {
      try {
        worker.stop();
      } catch {
        /* ignore */
      }
    }
    this.workers.length = 0;

    if (this.events) {
      for (const { event, listener } of this.listeners) {
        try {
          this.events.off(event, listener);
        } catch {
          /* ignore */
        }
      }
    }
    this.listeners.length = 0;
  }

  // ── Private helpers ──────────────────────────────────────────────────

  /**
   * Grab every provider instance from the running application.
   *
   * We don't want a hard dependency on the exact shape of the global app
   * — just the contract that it exposes `getContainer().getModules()`
   * returning a `Map<string, { providers: Map<string, { instance }> }>`.
   */
  private collectProviderInstances(): object[] | null {
    try {
      const container = require('@stackra/ts-container');
      const app =
        container.getGlobalApplication?.() ?? (globalThis as { __APP__?: unknown }).__APP__;
      if (!app) return null;

      const out: object[] = [];
      const containerRef = (
        app as { getContainer: () => { getModules: () => Map<unknown, unknown> } }
      ).getContainer();
      const modules = containerRef.getModules();

      for (const [, moduleRef] of modules as Map<
        unknown,
        { providers: Map<unknown, { instance?: object; isAlias?: boolean }> }
      >) {
        for (const [, wrapper] of moduleRef.providers) {
          if (!wrapper.instance || wrapper.isAlias) continue;
          out.push(wrapper.instance);
        }
      }
      return out;
    } catch (err: Error | any) {
      this.logger.warn(
        `[QueueLoader] Failed to access global application: ${(err as Error).message}`
      );
      return null;
    }
  }

  /**
   * Create worker instances for a processor and start them.
   *
   * Throws if the class doesn't extend {@link WorkerHost} — this catches
   * the common misuse of decorating an arbitrary class.
   */
  private async registerProcessor(instance: object, meta: IProcessorMetadata): Promise<void> {
    if (!(instance instanceof WorkerHost)) {
      throw new QueueDriverError(
        `[QueueLoader] Processor class '${(instance as { constructor?: { name?: string } }).constructor?.name}' must extend WorkerHost.`
      );
    }

    const connectionName = meta.connection ?? this.manager.getDefaultConnectionName();
    const connection = await this.manager.connection(connectionName);
    const options = this.manager.getWorkerOptions();

    // Sync driver handles jobs inline on push — no worker loop needed.
    // Register the processor as the handler so push() routes to it.
    if (connection instanceof SyncConnection) {
      connection.setHandler(async (job: IQueuedJob) => {
        await Promise.resolve(instance.process(job));
      });
      return;
    }

    const concurrency = Math.max(1, meta.concurrency ?? 1);
    for (let i = 0; i < concurrency; i++) {
      const worker = new Worker({
        connection,
        queue: meta.queue,
        host: instance,
        options,
        eventBus: this.eventBus,
      });
      worker.start();
      this.workers.push(worker);
    }
  }

  /**
   * Subscribe every `@OnJobEvent` method on a processor to the bus.
   *
   * Walks the prototype's own property names looking for methods
   * annotated with `@OnJobEvent`, then binds each to the matching
   * lifecycle event name on the {@link EventManagerLike}.
   *
   * No-op when no `EventManager` is wired (the dependency is optional
   * so apps without `@stackra/ts-events` still bootstrap).
   *
   * @param instance - The processor instance to inspect.
   */
  private registerEventListeners(instance: object): void {
    if (!this.events) return;

    const proto = Object.getPrototypeOf(instance) as Record<string, unknown> | null;
    if (!proto) return;

    for (const key of Object.getOwnPropertyNames(proto)) {
      if (key === 'constructor') continue;
      const method = (instance as Record<string, unknown>)[key];
      if (typeof method !== 'function') continue;

      const entries = this.metadataAccessor.getOnJobEventMetadata(method);
      if (!entries) continue;

      for (const entry of entries) {
        this.subscribe(instance, key, entry);
      }
    }
  }

  /**
   * Bind one method to one lifecycle event.
   *
   * Wraps the method invocation in a closure that:
   * - Filters payloads by `meta.connection` when set.
   * - Logs but does not rethrow exceptions thrown by the handler.
   * - Catches Promise rejections from async handlers.
   *
   * @param instance  - The processor instance owning the method.
   * @param methodKey - Property name of the method on the prototype.
   * @param meta      - Decorator metadata describing the event binding.
   */
  private subscribe(instance: object, methodKey: string, meta: IOnJobEventMetadata): void {
    if (!this.events) return;

    const listener = (...args: unknown[]) => {
      // Connection filtering — skip payloads from other connections.
      if (meta.connection) {
        const payload = args[0] as { job?: { connection?: string } } | undefined;
        if (payload?.job?.connection && payload.job.connection !== meta.connection) return;
      }

      try {
        const fn = (instance as Record<string, unknown>)[methodKey] as (...a: unknown[]) => unknown;
        const result = fn.call(instance, ...args);
        if (result && typeof (result as Promise<unknown>).then === 'function') {
          (result as Promise<unknown>).catch((err: Error | any) =>
            this.logger.error(`[QueueLoader] ${methodKey} handler threw: ${String(err)}`)
          );
        }
      } catch (err: Error | any) {
        this.logger.error(`[QueueLoader] ${methodKey} handler threw: ${String(err)}`);
      }
    };

    this.events.on(meta.event, listener);
    this.listeners.push({ event: meta.event, listener });
  }
}
