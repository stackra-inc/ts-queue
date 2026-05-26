/**
 * QStash connector.
 *
 * Resolves an `IQStashQueueConnectionConfig` into a live
 * `QStashConnection`.
 *
 * @module @stackra/ts-queue/connectors/qstash
 */

import { Injectable } from '@stackra/ts-container';
import type { IQueueConnection, IQueueConnector, QueueConnectionConfig } from '@stackra/contracts';

import { QStashConnection } from '@/connections/qstash.connection';

/**
 * QStash connector — wraps `QStashConnection`.
 */
@Injectable()
export class QStashConnector implements IQueueConnector {
  /**
   * Build a `QStashConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use QStash connection.
   */
  public async connect(config: QueueConnectionConfig): Promise<IQueueConnection> {
    if (config.driver !== 'qstash') {
      throw new Error(`QStashConnector received non-qstash driver: ${config.driver}`);
    }

    const name = (config as { name?: string }).name ?? 'qstash';
    return new QStashConnection(name, {
      driver: 'qstash',
      mode: config.mode ?? 'proxy',
      ...(config.token !== undefined ? { token: config.token } : {}),
      ...(config.proxyUrl !== undefined ? { proxyUrl: config.proxyUrl } : {}),
      ...(config.defaultDestination !== undefined
        ? { defaultDestination: config.defaultDestination }
        : {}),
      ...(config.baseUrl !== undefined ? { baseUrl: config.baseUrl } : {}),
    });
  }
}
