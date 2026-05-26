/**
 * @fileoverview Reads `@Processor` and `@OnJobEvent` metadata.
 *
 * Tiny helper separated from the loader so each metadata lookup is
 * testable in isolation (and the loader stays readable).
 *
 * @module services/processor-metadata-accessor
 * @category Services
 */

import { Injectable } from "@stackra/ts-container";
import { getMetadata } from "@vivtel/metadata";

import { ON_JOB_EVENT_METADATA, PROCESSOR_METADATA } from "@/constants/tokens.constant";
import type { IOnJobEventMetadata } from "@/interfaces/on-job-event-metadata.interface";
import type { IProcessorMetadata } from "@stackra/contracts";

/**
 * Reads queue decorator metadata from classes and methods.
 */
@Injectable()
export class ProcessorMetadataAccessor {
  /**
   * Return the `@Processor` metadata for a class, or `undefined` if the
   * class isn't decorated.
   */
  public getProcessorMetadata(target: unknown): IProcessorMetadata | undefined {
    if (!target || typeof target !== "function") return undefined;
    return getMetadata<IProcessorMetadata>(PROCESSOR_METADATA, target as object);
  }

  /**
   * Return the `@OnJobEvent` metadata array for a method, or `undefined`
   * if the method isn't decorated. Always returns an array — decorators
   * can stack, so a method may carry more than one listener entry.
   */
  public getOnJobEventMetadata(target: unknown): IOnJobEventMetadata[] | undefined {
    if (!target || (typeof target !== "function" && typeof target !== "object")) {
      return undefined;
    }
    const meta = getMetadata<IOnJobEventMetadata | IOnJobEventMetadata[]>(
      ON_JOB_EVENT_METADATA,
      target as object,
    );
    if (!meta) return undefined;
    return Array.isArray(meta) ? meta : [meta];
  }
}
