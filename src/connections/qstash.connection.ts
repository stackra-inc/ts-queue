/**
 * @fileoverview Upstash QStash driver.
 *
 * Browser-producer, server-consumer. Publishes messages to QStash which
 * durably stores them and delivers to an HTTP destination you control.
 *
 * ## Two modes
 *
 * - **proxy** (default, recommended for public apps): POST to your own
 *   backend endpoint, which then calls the QStash API server-side using
 *   the real `QSTASH_TOKEN`. This keeps the token out of browser bundles.
 *   Use this for POS/ticketing/anywhere anonymous users can reach the app.
 *
 * - **direct**: Use `@upstash/qstash` from the browser with the token
 *   baked into the client. Only safe for internal tools behind SSO.
 *
 * ## Consumer-side methods are no-ops
 *
 * QStash is a producer-only broker from the browser's perspective — it
 * delivers to server endpoints, not back to the browser. The worker-side
 * methods (`pop`, `size`, `remove`, …) throw/no-op because they have no
 * meaning here. The backend that receives QStash callbacks is the
 * consumer; it uses its own persistence (database, Redis, etc.) rather
 * than this driver.
 *
 * @module connections/qstash
 * @category Connections
 */

import { QueueDriverError } from '@/errors/queue-driver.error';
import type { IJobOptions } from '@stackra/contracts';
import type { IQueuedJob } from '@stackra/contracts';
import type { IQStashQueueConnectionConfig } from '@stackra/contracts';
import { BaseConnection } from './base.connection';

/**
 * Payload shape sent to a proxy endpoint.
 *
 * Keeping this explicit (rather than sending options verbatim) lets the
 * backend validate and rewrite them before forwarding to QStash.
 */
interface ProxyPublishRequest {
  /**
   * Application-level job name.
   *
   * Used by the receiving server to dispatch to the correct handler.
   */
  name: string;
  /**
   * Application payload.
   *
   * Forwarded as-is to the QStash destination.
   */
  data: unknown;
  /**
   * Destination URL or URL Group name.
   *
   * Optional override — when omitted, the proxy backend uses its own
   * default destination configured server-side.
   */
  destination?: string;
  /**
   * Delay before delivery, expressed in seconds.
   *
   * Translated from the caller-supplied `delayMs` so the backend
   * doesn't have to know about milliseconds.
   */
  delaySec?: number;
  /**
   * Retry count for the delivery attempt.
   *
   * Forwarded to QStash via the `Upstash-Retries` header.
   */
  retries?: number;
  /**
   * Deduplication id.
   *
   * Set when the caller opted into `uniqueFor`. QStash treats messages
   * with matching ids as duplicates and ignores subsequent submissions.
   */
  deduplicationId?: string;
  /**
   * Free-form tag array.
   *
   * Used for observability/filtering on the receiving server.
   */
  tags?: string[];
}

/**
 * QStash-backed producer driver.
 *
 * @example
 * Proxy mode — publish via your own server.
 * ```typescript
 * const conn = new QStashConnection('qstash', {
 *   driver: 'qstash',
 *   mode: 'proxy',
 *   proxyUrl: '/api/queue/publish',
 *   defaultDestination: 'https://api.example.com/webhooks/queue',
 * });
 * await conn.push('send-reminder', { ticketId: '42' }, { delayMs: 3600_000 });
 * ```
 */
export class QStashConnection extends BaseConnection {
  /**
   * Lazy-loaded QStash client for `mode: 'direct'`.
   *
   * `null` until the first `pushDirect()` call. The dynamic import of
   * `@upstash/qstash` is deferred so apps using proxy mode never ship
   * the QStash SDK.
   */
  private directClient: unknown | null = null;

  constructor(
    name: string,
    private readonly config: IQStashQueueConnectionConfig
  ) {
    super(name);
  }

  /**
   * Publish a job to QStash.
   *
   * Routes to either {@link pushProxy} (recommended for public apps,
   * keeps the QStash token server-side) or {@link pushDirect} (for
   * trusted internal tools where the token can ship to browsers).
   *
   * @typeParam T - Type of the job payload.
   * @param jobName - Application-level job name.
   * @param data    - The job payload.
   * @param options - Optional dispatch options. `driverOptions.destination`
   *   may override the default destination at call time.
   * @returns The QStash messageId on success.
   * @throws {QueueDriverError} When the destination cannot be resolved or
   *   the HTTP/SDK call fails.
   */
  public async push<T = unknown>(jobName: string, data: T, options?: IJobOptions): Promise<string> {
    const mode = this.config.mode ?? 'proxy';
    const destination =
      (options?.driverOptions?.destination as string | undefined) ?? this.config.defaultDestination;

    if (!destination) {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] push() requires a destination. Pass 'defaultDestination' in config or 'driverOptions.destination' at push time.`
      );
    }

    return mode === 'direct'
      ? this.pushDirect(jobName, data, destination, options)
      : this.pushProxy(jobName, data, destination, options);
  }

  /**
   * Delayed dispatch — forwards to push() with the delay option set.
   *
   * QStash natively supports delayed delivery via the `Upstash-Delay`
   * header (proxy mode) or the `delay` option (direct mode); the driver
   * translates `delayMs` into the correct unit for each path.
   */
  public override async later<T = unknown>(
    delayMs: number,
    jobName: string,
    data: T,
    options?: IJobOptions
  ): Promise<string> {
    return this.push(jobName, data, { ...options, delayMs });
  }

  // ── Producer-only contract stubs ──────────────────────────────────────

  /**
   * No-op pop.
   *
   * QStash delivers to HTTP endpoints; the browser never polls it. The
   * server-side consumer uses its own persistence and is the actual
   * worker for these jobs.
   *
   * @param _queue - Queue tube name (ignored).
   * @returns Always `null`.
   */
  public async pop(_queue?: string): Promise<IQueuedJob | null> {
    return null;
  }

  /**
   * Always-zero size.
   *
   * Producer-only driver — the browser does not track in-flight jobs.
   *
   * @returns `0`.
   */
  public async size(): Promise<number> {
    return 0;
  }

  /**
   * Always-zero pending count.
   *
   * @returns `0`.
   */
  public async pendingSize(): Promise<number> {
    return 0;
  }

  /**
   * Always-zero delayed count.
   *
   * @returns `0`.
   */
  public async delayedSize(): Promise<number> {
    return 0;
  }

  /**
   * Always-zero reserved count.
   *
   * @returns `0`.
   */
  public async reservedSize(): Promise<number> {
    return 0;
  }

  /**
   * No-op remove.
   *
   * Lifecycle is owned by the server-side consumer that receives the
   * QStash callback.
   *
   * @param _jobId - Job id (ignored).
   */
  public async remove(_jobId: string): Promise<void> {
    /* Handled by the server-side consumer, not the browser producer. */
  }

  /**
   * No-op release.
   *
   * Lifecycle is owned by the server-side consumer.
   *
   * @param _jobId - Job id (ignored).
   */
  public async release(_jobId: string): Promise<void> {
    /* Handled by the server-side consumer. */
  }

  /**
   * No-op fail.
   *
   * Lifecycle is owned by the server-side consumer.
   *
   * @param _jobId - Job id (ignored).
   */
  public async fail(_jobId: string): Promise<void> {
    /* Handled by the server-side consumer. */
  }

  /**
   * Unsupported clear.
   *
   * QStash doesn't expose a "clear queue" primitive over its HTTP API
   * for browsers — use the QStash console or the API server-side.
   *
   * @throws {QueueDriverError} Always.
   */
  public async clear(): Promise<void> {
    throw new QueueDriverError(
      `[QStashConnection:${this.name}] clear() is not supported. Use the QStash console or API from your backend to clear messages.`
    );
  }

  /**
   * Drop the lazy QStash client reference.
   *
   * Called on `OnModuleDestroy` so subsequent calls re-import the SDK
   * cleanly during hot reloads.
   */
  public async close(): Promise<void> {
    this.directClient = null;
  }

  // ── Private mode implementations ──────────────────────────────────────

  /**
   * Publish via the server proxy endpoint.
   *
   * The request body is intentionally declarative — the backend stays
   * in control of which QStash options are honoured and which are
   * ignored, so a compromised browser cannot request, say, unbounded
   * retry counts.
   */
  private async pushProxy<T>(
    name: string,
    data: T,
    destination: string,
    options: IJobOptions | undefined
  ): Promise<string> {
    if (!this.config.proxyUrl) {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] mode: 'proxy' requires 'proxyUrl' in config.`
      );
    }

    const body: ProxyPublishRequest = {
      name,
      data,
      destination,
      delaySec: options?.delayMs !== undefined ? Math.floor(options.delayMs / 1000) : undefined,
      retries: options?.tries,
      deduplicationId: options?.uniqueId,
      tags: options?.tags,
    };

    const response = await fetch(this.config.proxyUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] Proxy returned HTTP ${response.status}: ${await response.text()}`
      );
    }

    // Backend responds with `{ messageId: string }` on success.
    const result = (await response.json()) as { messageId?: string; scheduleId?: string };
    return result.messageId ?? result.scheduleId ?? '';
  }

  /**
   * Publish directly from the browser using @upstash/qstash.
   *
   * Only safe in trusted contexts. The client is lazy-loaded so apps
   * using proxy mode don't ship the QStash SDK at all.
   */
  private async pushDirect<T>(
    name: string,
    data: T,
    destination: string,
    options: IJobOptions | undefined
  ): Promise<string> {
    if (!this.config.token) {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] mode: 'direct' requires 'token' in config. ` +
          `Do NOT ship this token in a public client — prefer 'mode: proxy' instead.`
      );
    }

    const client = await this.getDirectClient();
    const delaySec =
      options?.delayMs !== undefined ? Math.floor(options.delayMs / 1000) : undefined;

    const res = await (client as any).publishJSON({
      url: destination,
      body: { name, data },
      retries: options?.tries,
      delay: delaySec,
      deduplicationId: options?.uniqueId,
    });

    return (res as { messageId: string }).messageId;
  }

  /**
   * Lazy-load the QStash client. Throws a clear error if the peer dep
   * isn't installed so the failure mode is obvious.
   */
  private async getDirectClient(): Promise<unknown> {
    if (this.directClient) return this.directClient;

    try {
      // Dynamic import keeps @upstash/qstash fully optional.
      const mod = await import(/* @vite-ignore */ '@upstash/qstash');
      const Client = (mod as { Client: new (opts: { token: string; baseUrl?: string }) => unknown })
        .Client;
      this.directClient = new Client({
        token: this.config.token!,
        baseUrl: this.config.baseUrl,
      });
      return this.directClient;
    } catch {
      throw new QueueDriverError(
        `[QStashConnection:${this.name}] mode: 'direct' requires '@upstash/qstash' to be installed. ` +
          `Run 'pnpm add @upstash/qstash' or switch to 'mode: proxy'.`
      );
    }
  }
}
