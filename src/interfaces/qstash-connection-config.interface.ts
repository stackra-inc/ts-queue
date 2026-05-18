/**
 * @fileoverview QStash driver configuration.
 *
 * @module interfaces/qstash-connection-config
 * @category Interfaces
 */

import type { QueueDriverName } from "@stackra/contracts";

/**
 * Configuration for the Upstash QStash driver.
 *
 * QStash is an HTTP-based managed message broker. The browser is a
 * **producer only** — QStash delivers messages to an HTTP endpoint you
 * control (usually your own backend).
 *
 * ## Two operating modes
 *
 * - `mode: 'proxy'` (recommended for public apps):
 *   The driver POSTs `{ jobName, data, options }` to `proxyUrl` on your
 *   own backend. Your backend calls the real QStash API with its server-
 *   side token. This avoids exposing `QSTASH_TOKEN` to the browser.
 *
 * - `mode: 'direct'` (trusted environments only):
 *   The driver imports `@upstash/qstash` and calls QStash directly from
 *   the browser using `token`. Only safe when the bundle is behind SSO
 *   (internal admin tools), never in public-facing apps.
 *
 * @example
 * Proxy mode — safe for public apps.
 * ```typescript
 * {
 *   driver: 'qstash',
 *   mode: 'proxy',
 *   proxyUrl: '/api/queue/publish',
 *   defaultDestination: 'https://api.example.com/webhooks/queue',
 * }
 * ```
 *
 * @example
 * Direct mode — internal tools only.
 * ```typescript
 * {
 *   driver: 'qstash',
 *   mode: 'direct',
 *   token: import.meta.env.VITE_QSTASH_TOKEN,
 *   defaultDestination: 'https://api.example.com/webhooks/queue',
 * }
 * ```
 */
export interface QStashConnectionConfig {
  /** Driver discriminator. */
  driver: QueueDriverName.QStash | "qstash";

  /**
   * Execution mode.
   *
   * - `'proxy'` — publish via your own server (no token in browser).
   * - `'direct'` — call QStash directly (token must be injected).
   *
   * @default "proxy"
   */
  mode?: "proxy" | "direct";

  /**
   * QStash API token. Required only for `mode: 'direct'`.
   *
   * **Security warning:** never ship this value in a public client
   * bundle. When in doubt, use `mode: 'proxy'`.
   */
  token?: string;

  /**
   * Backend endpoint that fronts QStash. Required for `mode: 'proxy'`.
   *
   * The driver POSTs each publish request here as
   * `{ jobName, data, options }` and expects a JSON response of
   * `{ messageId: string }` or `{ scheduleId: string }`.
   */
  proxyUrl?: string;

  /**
   * Default destination for published messages.
   *
   * Can be an absolute URL (`https://api.example.com/webhook`) or a
   * QStash URL Group name (`"primary-api"`). Overridable per-push via
   * `options.driverOptions.destination`.
   */
  defaultDestination?: string;

  /**
   * Base URL for QStash's REST API. Only relevant in `mode: 'direct'`
   * when targeting non-default endpoints.
   *
   * @default "https://qstash.upstash.io"
   */
  baseUrl?: string;
}
