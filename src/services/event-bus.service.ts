/**
 * @fileoverview Thin wrapper around `@stackra/ts-events` for queue lifecycle.
 *
 * The queue package publishes every lifecycle transition through the
 * event bus so instrumentation code can subscribe without coupling to
 * queue internals. This service isolates that dependency — if
 * `@stackra/ts-events` isn't registered, all emits become no-ops.
 *
 * @module services/event-bus
 * @category Services
 */

import { Injectable, Inject, Optional } from '@stackra/ts-container';
import { EVENT_EMITTER } from '@stackra/contracts';
import type { IEventEmitter, QueueEventName } from '@stackra/contracts';

/**
 * Publishes queue lifecycle events on the default event connection.
 *
 * The `EventManager` dep is `@Optional()` — consumers that don't install
 * `@stackra/ts-events` still get a working queue; they just don't get
 * lifecycle events. Use the facade or event manager directly if you
 * want to subscribe.
 */
@Injectable()
export class QueueEventBus {
  constructor(
    @Optional()
    @Inject(EVENT_EMITTER)
    private readonly events?: IEventEmitter
  ) {}

  /**
   * Emit a queue lifecycle event.
   *
   * @param event   - One of the {@link QUEUE_EVENTS} constants from
   *                  `@stackra/contracts`.
   * @param payload - Event payload (`{ job, error?, … }`).
   */
  public emit(event: QueueEventName, payload: unknown): void {
    if (!this.events) return;
    try {
      this.events.emit(event, payload);
    } catch {
      // Fail open — lifecycle events should never abort processing.
    }
  }
}
