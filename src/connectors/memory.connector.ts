/**
 * Memory connector.
 *
 * Resolves a `IMemoryQueueConnectionConfig` into a live
 * `MemoryConnection`. Used by `QueueModule.forRoot()` as one of the
 * default built-in connectors.
 *
 * @module @stackra/ts-queue/connectors/memory
 */

import { Injectable } from '@stackra/ts-container';
import type { IQueueConnection, IQueueConnector, QueueConnectionConfig } from '@stackra/contracts';

import { MemoryConnection } from '@/connections/memory.connection';

/**
 * Memory connector — wraps `MemoryConnection`.
 */
@Injectable()
export class MemoryConnector implements IQueueConnector {
  /**
   * Build a `MemoryConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use memory connection.
   */
  public async connect(config: QueueConnectionConfig): Promise<IQueueConnection> {
    if (config.driver !== 'memory') {
      throw new Error(`MemoryConnector received non-memory driver: ${config.driver}`);
    }

    const name = (config as { name?: string }).name ?? 'memory';
    return new MemoryConnection(name);
  }
}
