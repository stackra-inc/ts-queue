/**
 * @fileoverview Queueinject proxy — typed proxy for {@link QueueManager}.
 *
 * Resolves the `QueueManager` from the DI container lazily on first
 * property access. Safe to import at module scope before the app has
 * been bootstrapped.
 *
 * See `.kiro/steering/facade-pattern.md` for the shared facade pattern
 * used across the monorepo.
 *
 * @module facades/queue
 * @category Injectables
 */

import { inject } from "@stackra/ts-container";
import { QUEUE_MANAGER } from "@stackra/contracts";
import { QueueManager } from "@/services/queue-manager.service";

/**
 * Typed lazy proxy for {@link QueueManager}.
 *
 * Requires `Application.create(AppModule)` to be called once after
 * `Application.create(AppModule)` during bootstrap.
 *
 * @example
 * ```typescript
 * import { Queueinject proxy } from '@stackra/ts-queue';
 *
 * Queueinject proxy.queue('tracking').push('pixel.fireEvent', payload);
 * Queueinject proxy.connection('indexeddb').pause();
 * ```
 */
export const queue: QueueManager = inject<QueueManager>(QUEUE_MANAGER);
