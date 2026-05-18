/**
 * @fileoverview `@InjectQueue` parameter decorator.
 *
 * Resolves a queue **handle** bound to a specific `(connection, queue)`
 * pair. Mirrors NestJS Bull's `@InjectQueue(name)` — the caller gets a
 * thin typed wrapper around the underlying connection, pre-scoped to a
 * single queue tube.
 *
 * @module decorators/inject-queue
 * @category Decorators
 */

import { Inject } from "@stackra/ts-container";
import { getQueueToken } from "@/constants/tokens.constant";

/**
 * Inject a queue handle scoped to `(connection, queue)`.
 *
 * @param queue      - Queue tube name (defaults to `"default"`).
 * @param connection - Connection name (defaults to `"default"`).
 *
 * @example
 * ```typescript
 * @Injectable()
 * class TrackingService {
 *   constructor(@InjectQueue('tracking') private readonly queue: QueueHandle) {}
 *
 *   fireEvent(data: unknown) {
 *     return this.queue.push('pixel.fireEvent', data, { uniqueFor: 60_000 });
 *   }
 * }
 * ```
 */
export function InjectQueue(
  queue?: string,
  connection?: string,
): ParameterDecorator & PropertyDecorator {
  return Inject(getQueueToken(queue, connection));
}
