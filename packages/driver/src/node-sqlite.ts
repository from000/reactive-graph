/**
 * Lazy loader for the Node `node:sqlite` builtin (Task 9/6).
 *
 * `node:sqlite` is a runtime module that vite-node cannot statically externalize,
 * so both the durability log and the SQLite storage backends load it lazily via
 * `createRequire`. This module is the single source of that lazy load.
 */

import { createRequire } from "node:module";
import type { DatabaseSync as DatabaseSyncT } from "node:sqlite";

const require = createRequire(import.meta.url);

/** Return the `DatabaseSync` constructor, loading `node:sqlite` on first use. */
export function loadDatabaseSync(): typeof DatabaseSyncT {
  return (require("node:sqlite") as { DatabaseSync: typeof DatabaseSyncT }).DatabaseSync;
}
