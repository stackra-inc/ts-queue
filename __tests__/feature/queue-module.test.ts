import { describe, it, expect } from "vitest";
import { QUEUE_CONFIG, QUEUE_MANAGER, DEFAULT_QUEUE_CONNECTION_TOKEN } from "@stackra/contracts";
import { QueueModule } from "@/queue.module";
import { QueueManager } from "@/services/queue-manager.service";
import { QueueEventBus } from "@/services/event-bus.service";
import { ProcessorMetadataAccessor } from "@/services/processor-metadata.accessor";
import { ProcessorSubscribersLoader } from "@/services/processor-subscribers.loader";
import { getQueueConnectionToken, getQueueToken } from "@/constants/tokens.constant";
import type { QueueModuleOptions } from "@/interfaces/queue-module-options.interface";

describe("QueueModule", () => {
  const config: QueueModuleOptions = {
    default: "memory",
    connections: {
      memory: { driver: "memory" } as any,
      sync: { driver: "sync" } as any,
    },
    worker: { tries: 3, backoffMs: 1000 },
  };

  describe("forRoot", () => {
    it("should return a dynamic module with global: true", () => {
      const result = QueueModule.forRoot(config);
      expect(result.global).toBe(true);
      expect(result.module).toBe(QueueModule);
    });

    it("should register QUEUE_CONFIG provider", () => {
      const result = QueueModule.forRoot(config);
      const configProvider = result.providers!.find((p: any) => p.provide === QUEUE_CONFIG) as any;
      expect(configProvider).toBeDefined();
      expect(configProvider.useValue).toBe(config);
    });

    it("should register QueueManager provider", () => {
      const result = QueueModule.forRoot(config);
      const managerProvider = result.providers!.find((p: any) => p.provide === QueueManager) as any;
      expect(managerProvider).toBeDefined();
      expect(managerProvider.useClass).toBe(QueueManager);
    });

    it("should register QUEUE_MANAGER alias token", () => {
      const result = QueueModule.forRoot(config);
      const aliasProvider = result.providers!.find((p: any) => p.provide === QUEUE_MANAGER) as any;
      expect(aliasProvider).toBeDefined();
      expect(aliasProvider.useExisting).toBe(QueueManager);
    });

    it("should register DEFAULT_QUEUE_CONNECTION_TOKEN provider", () => {
      const result = QueueModule.forRoot(config);
      const defaultConnProvider = result.providers!.find(
        (p: any) => p.provide === DEFAULT_QUEUE_CONNECTION_TOKEN,
      ) as any;
      expect(defaultConnProvider).toBeDefined();
      expect(defaultConnProvider.useFactory).toBeInstanceOf(Function);
    });

    it("should register per-connection providers", () => {
      const result = QueueModule.forRoot(config);
      const memoryToken = getQueueConnectionToken("memory");
      const syncToken = getQueueConnectionToken("sync");

      const memoryProvider = result.providers!.find((p: any) => p.provide === memoryToken);
      const syncProvider = result.providers!.find((p: any) => p.provide === syncToken);

      expect(memoryProvider).toBeDefined();
      expect(syncProvider).toBeDefined();
    });

    it("should register default queue handle provider", () => {
      const result = QueueModule.forRoot(config);
      const defaultQueueToken = getQueueToken();
      const provider = result.providers!.find((p: any) => p.provide === defaultQueueToken);
      expect(provider).toBeDefined();
    });

    it("should register per-connection default handle providers", () => {
      const result = QueueModule.forRoot(config);
      const memoryHandleToken = getQueueToken("default", "memory");
      const syncHandleToken = getQueueToken("default", "sync");

      expect(result.providers!.find((p: any) => p.provide === memoryHandleToken)).toBeDefined();
      expect(result.providers!.find((p: any) => p.provide === syncHandleToken)).toBeDefined();
    });

    it("should register bootstrap infrastructure services", () => {
      const result = QueueModule.forRoot(config);
      expect(result.providers!).toContain(QueueEventBus);
      expect(result.providers!).toContain(ProcessorMetadataAccessor);
      expect(result.providers!).toContain(ProcessorSubscribersLoader);
    });

    it("should export key tokens", () => {
      const result = QueueModule.forRoot(config);
      expect(result.exports).toContain(QUEUE_CONFIG);
      expect(result.exports).toContain(QueueManager);
      expect(result.exports).toContain(QUEUE_MANAGER);
      expect(result.exports).toContain(DEFAULT_QUEUE_CONNECTION_TOKEN);
      expect(result.exports).toContain(QueueEventBus);
    });
  });

  describe("forFeature", () => {
    it("should return a dynamic module with queue handle providers", () => {
      const result = QueueModule.forFeature([
        { queue: "scans", connection: "memory" },
        { queue: "receipts", connection: "sync" },
      ]);

      expect(result.module).toBe(QueueModule);
      expect(result.providers).toHaveLength(2);
    });

    it("should register providers with correct tokens", () => {
      const result = QueueModule.forFeature([{ queue: "scans", connection: "memory" }]);

      const token = getQueueToken("scans", "memory");
      const provider = result.providers!.find((p: any) => p.provide === token) as any;
      expect(provider).toBeDefined();
      expect(provider.useFactory).toBeInstanceOf(Function);
      expect(provider.inject).toContain(QueueManager);
    });

    it("should use default connection when not specified", () => {
      const result = QueueModule.forFeature([{ queue: "emails" }]);

      const token = getQueueToken("emails", "default");
      const provider = result.providers!.find((p: any) => p.provide === token);
      expect(provider).toBeDefined();
    });

    it("should export all registered tokens", () => {
      const result = QueueModule.forFeature([
        { queue: "scans", connection: "memory" },
        { queue: "receipts" },
      ]);

      expect(result.exports).toHaveLength(2);
      expect(result.exports).toContain(getQueueToken("scans", "memory"));
      expect(result.exports).toContain(getQueueToken("receipts", "default"));
    });
  });
});
