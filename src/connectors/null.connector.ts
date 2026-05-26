/**
 * Null connector.
 *
 * Resolves a `INullQueueConnectionConfig` into a live `NullConnection`.
 *
 * @module @stackra/ts-queue/connectors/null
 */

import { Injectable } from '@stackra/ts-container';
import type { IQueueConnection, IQueueConnector, QueueConnectionConfig } from '@stackra/contracts';

import { NullConnection } from '@/connections/null.connection';

/**
 * Null connector — wraps `NullConnection`.
 */
@Injectable()
export class NullConnector implements IQueueConnector {
  /**
   * Build a `NullConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use null connection.
   */
  public async connect(config: QueueConnectionConfig): Promise<IQueueConnection> {
    if (config.driver !== 'null') {
      throw new Error(`NullConnector received non-null driver: ${config.driver}`);
    }

    const name = (config as { name?: string }).name ?? 'null';
    return new NullConnection(name);
  }
}
