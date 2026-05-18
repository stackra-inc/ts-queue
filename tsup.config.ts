/**
 * @fileoverview tsup build configuration for @stackra/ts-queue.
 *
 * Uses the shared `basePreset` from `@stackra/tsup-config` so all packages
 * in the monorepo build with identical output conventions (ESM + CJS + .d.ts,
 * `src/index.ts` as entry, external peer deps, no code splitting).
 *
 * @module @stackra/ts-queue
 * @see https://tsup.egoist.dev/
 */

import { basePreset as preset } from "@stackra/tsup-config";

export default preset;
