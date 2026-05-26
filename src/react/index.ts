/**
 * `@stackra/ts-queue/react` — React entry point.
 *
 * Optional React surface for the queue package. Exports nothing that
 * touches the DOM directly — every hook composes
 * `@stackra/ts-container/react`'s `useInject` so it works equally
 * well in web and native consumers.
 *
 * Web/native consumers must also import the root `@stackra/ts-queue`
 * to register the module.
 *
 * @module @stackra/ts-queue/react
 */

// ============================================================================
// React Hooks
// ============================================================================
export { useQueue, useQueueConnection, useQueueManager } from './hooks';
export type { IUseQueueResult, IUseQueueConnectionResult } from './hooks';
