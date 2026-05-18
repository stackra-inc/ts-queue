/**
 * @fileoverview `@Processor(queue, options?)` class decorator.
 *
 * Marks a class as the handler for a named queue tube. Stores metadata
 * that the bootstrap loader reads at `onApplicationBootstrap` to wire
 * `instance.process(job)` as the worker for `(connection, queue)`.
 *
 * The decorator composes `@Injectable()` internally by way of the
 * metadata key — the loader discovers the class through the decorator
 * discovery plugin (build-time) and resolves its instance through the DI
 * container at bootstrap.
 *
 * @module decorators/processor
 * @category Decorators
 */

import { defineMetadata } from "@vivtel/metadata";
import { Injectable } from "@stackra/ts-container";

import { PROCESSOR_METADATA } from "@/constants/tokens.constant";
import type { ProcessorMetadata } from "@/interfaces/processor-metadata.interface";

/**
 * Mark a class as a queue processor.
 *
 * Accepts either a queue name string (shorthand) or an options object.
 * The class must extend {@link WorkerHost} — the loader uses the
 * `instanceof` check to catch misuse early.
 *
 * @example
 * Shorthand — queue name only.
 * ```typescript
 * @Processor('tracking')
 * class PixelProcessor extends WorkerHost {
 *   async process(job: QueuedJob) { ... }
 * }
 * ```
 *
 * @example
 * Full options.
 * ```typescript
 * @Processor({ queue: 'scans', connection: 'indexeddb', concurrency: 2 })
 * class ScanProcessor extends WorkerHost { ... }
 * ```
 */
export function Processor(queue: string): ClassDecorator;
export function Processor(options: ProcessorMetadata): ClassDecorator;
export function Processor(arg: string | ProcessorMetadata): ClassDecorator {
  const options: ProcessorMetadata = typeof arg === "string" ? { queue: arg } : arg;

  return (target: Function) => {
    // Mark the class `@Injectable()` so the DI container registers it
    // when the decorator discovery scan picks it up.
    Injectable()(target as any);

    // Store the processor metadata under our known key.
    defineMetadata(PROCESSOR_METADATA, options, target as object);
  };
}
