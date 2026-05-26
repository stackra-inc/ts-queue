/**
 * Broadcast-channel connector.
 *
 * Resolves an `IBroadcastChannelQueueConnectionConfig` into a live
 * `BroadcastChannelConnection`. The optional `TabCoordinator` is
 * injected when `@stackra/ts-coordinator` is present in the host
 * application — when missing the connection runs as always-leader.
 *
 * @module @stackra/ts-queue/connectors/broadcast-channel
 */

import { Inject, Injectable, Optional } from '@stackra/ts-container';
import type { IQueueConnection, IQueueConnector, QueueConnectionConfig } from '@stackra/contracts';
import { TAB_COORDINATOR } from '@stackra/contracts';
import type { TabCoordinator } from '@stackra/ts-coordinator';

import { BroadcastChannelConnection } from '@/connections/broadcast-channel.connection';

/**
 * Broadcast-channel connector — wraps `BroadcastChannelConnection`.
 */
@Injectable()
export class BroadcastChannelConnector implements IQueueConnector {
  /**
   * @param coordinator - Optional shared `TabCoordinator` instance.
   *   When unavailable, the resulting connection acts as always-leader.
   */
  public constructor(
    @Optional() @Inject(TAB_COORDINATOR) private readonly coordinator?: TabCoordinator
  ) {}

  /**
   * Build a `BroadcastChannelConnection` from the supplied configuration.
   *
   * @param config - Driver-specific connection configuration.
   * @returns A ready-to-use broadcast-channel connection.
   */
  public async connect(config: QueueConnectionConfig): Promise<IQueueConnection> {
    if (config.driver !== 'broadcast-channel') {
      throw new Error(
        `BroadcastChannelConnector received non-broadcast-channel driver: ${config.driver}`
      );
    }

    const name = (config as { name?: string }).name ?? 'broadcast-channel';
    return new BroadcastChannelConnection(
      name,
      this.coordinator ?? null,
      config.dbName ?? 'stackra-queue',
      1,
      config.prefix ?? ''
    );
  }
}
