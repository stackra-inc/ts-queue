/**
 * @fileoverview `@Job({...})` class decorator.
 *
 * Attaches default {@link JobOptions} to a job class. When the class is
 * dispatched via `JobDispatcher.dispatch(JobClass, data)`, the dispatcher
 * merges these defaults with per-dispatch overrides before enqueuing.
 *
 * Mirrors Laravel's PHP 8 attribute pattern (`#[Tries(3)]`, `#[Queue]`,
 * `#[Backoff]`) but consolidated into a single decorator since
 * TypeScript doesn't have a native concept of multi-attribute decoration
 * the way PHP does.
 *
 * Composes `@Injectable()` internally so consumers don't need to apply
 * both decorators — the job can be resolved from the DI container with
 * its declared dependencies.
 *
 * @module decorators/job
 * @category Decorators
 */

import { Injectable } from '@stackra/ts-container';
import { defineMetadata } from '@vivtel/metadata';

import { JOB_METADATA } from '@/constants/tokens.constant';
import type { IJobMetadata } from '@stackra/contracts';

/**
 * Configure class-level defaults for a job.
 *
 * Marks the class as `@Injectable()` and attaches the provided
 * {@link IJobMetadata} via the `JOB_METADATA` key.
 *
 * @param options - Default dispatch options for every instance of this job.
 * @returns A class decorator that wires both `@Injectable()` and the
 *   job-metadata side-effect onto the target.
 *
 * @example
 * ```typescript
 * @Job({
 *   name: 'send-receipt',
 *   queue: 'emails',
 *   tries: 3,
 *   backoffMs: 2000,
 *   uniqueFor: 60_000,
 * })
 * class SendReceiptJob {
 *   constructor(
 *     @Inject(MAILER) private readonly mailer: Mailer,
 *     public readonly saleId: string,
 *   ) {}
 * }
 *
 * dispatcher.dispatch(SendReceiptJob, { saleId: '123' });
 * ```
 */
export function Job(options: IJobMetadata): ClassDecorator {
  // Build the @Injectable() decorator once so we can reuse it across
  // every `Job(options)` invocation without re-creating the closure.
  const injectableDecorator = Injectable();

  return (target: Function) => {
    // Step 1 — apply @Injectable so the DI container recognises the class.
    injectableDecorator(target as never);
    // Step 2 — attach the job metadata for the dispatcher.
    defineMetadata(JOB_METADATA, options, target as object);
  };
}
