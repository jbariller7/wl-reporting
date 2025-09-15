import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { q } from "../lib/db.js";

// Determine directory of this script when using ES modules
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Read SQL files
const schema = fs.readFileSync(path.join(__dirname, "../lib/sql/schema.sql"), "utf8");
const indices = fs.readFileSync(path.join(__dirname, "../lib/sql/indices.sql"), "utf8");

// Immediately invoke migration to create tables and indices
(async () => {
  try {
    await q(schema);
    await q(indices);
    console.log("DB migrated.");
    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
