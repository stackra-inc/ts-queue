/**
 * @fileoverview `@InjectQueueConnection` parameter decorator.
 *
 * Resolves a {@link QueueConnection} for a named connection from the DI
 * container. When called without arguments, resolves the default
 * connection via {@link DEFAULT_QUEUE_CONNECTION_TOKEN}.
 *
 * @module decorators/inject-queue-connection
 * @category Decorators
 */

import { Inject } from "@stackra/ts-container";
import { DEFAULT_QUEUE_CONNECTION_TOKEN } from "@stackra/contracts";
import { getQueueConnectionToken } from "@/constants";

/**
 * Inject the {@link QueueConnection} for the named driver.
 *
 * @param name - Connection name from module config. Defaults to the
 *   configured default when omitted.
 *
 * @example
 * ```typescript
 * @Injectable()
 * class OrderService {
 *   constructor(
 *     @InjectQueueConnection('indexeddb') private readonly queue: QueueConnection,
 *   ) {}
 * }
 * ```
 */
export function InjectQueueConnection(name?: string): ParameterDecorator & PropertyDecorator {
  const token = name ? getQueueConnectionToken(name) : DEFAULT_QUEUE_CONNECTION_TOKEN;
  return Inject(token);
}
