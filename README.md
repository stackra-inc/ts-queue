# @stackra/ts-queue

Laravel-inspired, browser-first job queue with NestJS-style decorator discovery.
Multiple named connections, pluggable drivers, lifecycle events, retry with
exponential backoff, timeouts, unique-per-window dispatch, cross-tab
coordination, and optional Upstash QStash integration.

## Install

```bash
pnpm add @stackra/ts-queue
```

Peer dependencies (all workspace-linked in the monorepo):

- `@stackra/ts-container` — DI module, `@Module`, `@Injectable`, `@Inject`
- `@stackra/ts-support` — `MultipleInstanceManager`, `Facade`, `GlobalRegistry`
- `@stackra/ts-metadata` — `defineMetadata`, `getMetadata`, `updateMetadata`
- `reflect-metadata` — polyfill loaded once at the entry point

Optional:

- `@stackra/ts-events` — lifecycle event bus. Without it, the queue still works;
  only observability events are skipped.
- `@stackra/ts-logger` — structured logging. Falls back to silent on miss.
- `@stackra/vite-config` — build-time `@Processor` scan + HMR.
- `@upstash/qstash` — only required for the `qstash` driver in `mode: 'direct'`.
  `mode: 'proxy'` works without it.

## Quick start

```typescript
import { Module } from "@stackra/ts-container";
import { QueueModule } from "@stackra/ts-queue";

@Module({
  imports: [
    QueueModule.forRoot({
      default: "indexeddb",
      connections: {
        memory: { driver: "memory" },
        indexeddb: { driver: "indexeddb", dbName: "pos-queue" },
        qstash: {
          driver: "qstash",
          mode: "proxy",
          proxyUrl: "/api/queue/publish",
          defaultDestination: "https://api.example.com/webhooks/queue",
        },
      },
      worker: { tries: 3, backoffMs: 1000, timeoutMs: 30_000 },
    }),
  ],
})
export class AppModule {}
```

### Define a processor

```typescript
import {
  Processor,
  WorkerHost,
  OnJobEvent,
  QueueEvent,
  type QueuedJob,
} from "@stackra/ts-queue";

interface SalePayload {
  saleId: string;
  amount: number;
}

@Processor("sales")
export class SaleProcessor extends WorkerHost<SalePayload> {
  async process(job: QueuedJob<SalePayload>): Promise<void> {
    await api.syncSale(job.data);
  }

  @OnJobEvent(QueueEvent.JobFailed)
  onFailed({ job, error }: { job: QueuedJob; error: Error }): void {
    alerting.capture(job.id, error);
  }
}
```

### Dispatch from anywhere

```typescript
import { QueueFacade } from "@stackra/ts-queue";

QueueFacade.queue("sales").push("sale.sync", saleData, {
  tries: 5,
  backoffMs: 2000,
  uniqueFor: 60_000,
});
```

Or inject:

```typescript
import { Injectable, Inject } from "@stackra/ts-container";
import { InjectQueue, type QueueHandle } from "@stackra/ts-queue";

@Injectable()
export class CheckoutService {
  constructor(@InjectQueue("sales") private readonly queue: QueueHandle) {}

  finalize(sale: SalePayload) {
    return this.queue.push("sale.sync", sale, { tries: 5 });
  }
}
```

### React hook

```tsx
import { useQueue } from "@stackra/ts-queue";

function ScanButton({ code }: { code: string }) {
  const queue = useQueue("scans");
  return (
    <button onClick={() => queue.push("scan.submit", { code })}>Submit</button>
  );
}
```

## Drivers

| Driver              | Browser | Offline | Cross-tab | Delayed | Schedules | Notes                                     |
| ------------------- | :-----: | :-----: | :-------: | :-----: | :-------: | ----------------------------------------- |
| `memory`            |    ✓    |    —    |     —     |    ✓    |     —     | Tests and transient in-process work.      |
| `sync`              |    ✓    |    —    |     —     |    —    |     —     | Executes processors inline on push.       |
| `null`              |    ✓    |    —    |     —     |    —    |     —     | Discards everything. Safe SSR fallback.   |
| `local-storage`     |    ✓    |    ✓    |     —     |    ✓    |     —     | Small payloads, synchronous persistence.  |
| `indexeddb`         |    ✓    |    ✓    |     —     |    ✓    |     —     | **Browser default** for offline-first.    |
| `broadcast-channel` |    ✓    |    ✓    |     ✓     |    ✓    |     —     | IndexedDB + leader election across tabs.  |
| `qstash`            |    ✓    |    —    |     —     |    ✓    |     ✓     | Producer-only; backend consumes webhooks. |

## Concepts

- **Connection** — one configured driver. Addressable as
  `queue.connection('name')`.
- **Queue** — a named tube on top of a connection. Addressable as
  `queue.queue('name', 'connection')` or `@InjectQueue('name', 'connection')`.
- **Job** — a payload + metadata blob. Constructed by `createQueuedJob()`.
- **Processor** — the `@Processor('queue')`-decorated class whose `process(job)`
  method handles one job at a time. Extends `WorkerHost`.
- **Worker** — the poll loop tied to one processor × queue. Started at bootstrap
  by `ProcessorSubscribersLoader`, stopped on shutdown.
- **Lifecycle events** — emitted on the `@stackra/ts-events` default connection.
  Listen via `@OnJobEvent(QueueEvent.JobFailed)` on a method in your processor,
  or via `@OnEvent('queue.job.failed')` elsewhere.

## Laravel / NestJS mapping

| Concept             | Laravel `QueueManager`      | NestJS `BullModule`              | `@stackra/ts-queue`                 |
| ------------------- | --------------------------- | -------------------------------- | ----------------------------------- |
| Factory             | `QueueManager`              | `BullModule.registerQueue`       | `QueueManager` (multi-driver)       |
| Driver              | `SyncQueue`, `RedisQueue` … | BullMQ `Queue`                   | `QueueConnection` implementations   |
| Producer decorator  | trait-based                 | `@InjectQueue(name)`             | `@InjectQueue(name, connection?)`   |
| Consumer decorator  | `handle()` on `ShouldQueue` | `@Processor(name)`               | `@Processor(queue, options?)`       |
| Lifecycle decorator | events on `Queue::*`        | `@OnWorkerEvent('failed')`       | `@OnJobEvent(QueueEvent.JobFailed)` |
| Job metadata        | PHP 8 attributes            | options on `@Processor`          | `@Job({ tries, queue, uniqueFor })` |
| Auto discovery      | manual class names          | `BullExplorer` + DiscoveryModule | `DecoratorDiscoveryModule` + loader |

## License

MIT
