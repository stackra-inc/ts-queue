/**
 * IndexedDB connector.
 *
 * Resolves an `IIndexedDBQueueConnectionConfig` into a live
 * `IndexedDBConnection`.
 *
 * @module @stackra/ts-queue/connectors/indexeddb
 */

import { Injectable } from '@stackra/ts-container';
import type { IQueueConnection, IQueueConnector, QueueConnectionConfig } from '@stackra/contracts';

import { IndexedDBConnection } from '@/connections/indexeddb.connection';

/**
 * IndexedDB connector — wraps `IndexedDBConnection`.
 */
@Injectable()
export class IndexedDBConnector implements IQueueConnector {
  /**
   * Build an `IndexedDBConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use IndexedDB connection.
   */
  public async connect(config: QueueConnectionConfig): Promise<IQueueConnection> {
    if (config.driver !== 'indexeddb') {
      throw new Error(`IndexedDBConnector received non-indexeddb driver: ${config.driver}`);
    }

    const name = (config as { name?: string }).name ?? 'indexeddb';
    return new IndexedDBConnection(
      name,
      config.dbName ?? 'stackra-queue',
      config.dbVersion ?? 1,
      config.prefix ?? ''
    );
  }
}
