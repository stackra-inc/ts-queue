/**
 * Local-storage connector.
 *
 * Resolves a `ILocalStorageQueueConnectionConfig` into a live
 * `LocalStorageConnection`.
 *
 * @module @stackra/ts-queue/connectors/local-storage
 */

import { Injectable } from '@stackra/ts-container';
import type { IQueueConnection, IQueueConnector, QueueConnectionConfig } from '@stackra/contracts';

import { LocalStorageConnection } from '@/connections/local-storage.connection';

/**
 * Local-storage connector — wraps `LocalStorageConnection`.
 */
@Injectable()
export class LocalStorageConnector implements IQueueConnector {
  /**
   * Build a `LocalStorageConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use local-storage connection.
   */
  public async connect(config: QueueConnectionConfig): Promise<IQueueConnection> {
    if (config.driver !== 'local-storage') {
      throw new Error(`LocalStorageConnector received non-local-storage driver: ${config.driver}`);
    }

    const name = (config as { name?: string }).name ?? 'local-storage';
    return new LocalStorageConnection(name, config.prefix ?? '');
  }
}
