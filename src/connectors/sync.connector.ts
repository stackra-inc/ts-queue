/**
 * Sync connector.
 *
 * Resolves a `ISyncQueueConnectionConfig` into a live `SyncConnection`.
 *
 * @module @stackra/ts-queue/connectors/sync
 */

import { Injectable } from '@stackra/ts-container';
import type { IQueueConnection, IQueueConnector, QueueConnectionConfig } from '@stackra/contracts';

import { SyncConnection } from '@/connections/sync.connection';

/**
 * Sync connector — wraps `SyncConnection`.
 */
@Injectable()
export class SyncConnector implements IQueueConnector {
  /**
   * Build a `SyncConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use sync connection.
   */
  public async connect(config: QueueConnectionConfig): Promise<IQueueConnection> {
    if (config.driver !== 'sync') {
      throw new Error(`SyncConnector received non-sync driver: ${config.driver}`);
    }

    const name = (config as { name?: string }).name ?? 'sync';
    return new SyncConnection(name);
  }
}
