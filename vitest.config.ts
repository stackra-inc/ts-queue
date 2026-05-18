/**
 * Vitest Configuration
 *
 * Extends the shared monorepo preset from @stackra/testing.
 * Only package-specific overrides belong here.
 *
 * @see @stackra/testing/preset
 */

import { defineConfig, mergeConfig } from "vitest/config";
import path from "path";
import preset from "@stackra/testing/preset";

export default mergeConfig(preset, defineConfig({
  test: {
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
}));
