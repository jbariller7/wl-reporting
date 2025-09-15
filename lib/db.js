// Simple Postgres helper using a single Pool.
// Set DB_URL in Netlify environment (Netlify DB or any Postgres).
import pg from "pg";

const { Pool } = pg;

// Create a pool using the connection string from environment
const pool = new Pool({
  connectionString: process.env.DB_URL
});

/**
 * Execute a query on the database and return the result.
 * @param {string} text SQL text
 * @param {Array} params Prepared statement parameters
 */
export async function q(text, params = []) {
  const client = await pool.connect();
  try {
    const res = await client.query(text, params);
    return res;
  } finally {
    client.release();
  }
}

/**
 * Generic upsert helper for inserting multiple rows.
 * Uses ON CONFLICT to update specified columns when the conflict columns match.
 *
 * @param {string} table Name of the table
 * @param {string[]} conflictCols Columns that trigger a conflict
 * @param {string[]} dataCols All columns present in the row objects
 * @param {Array<Object>} rows Array of row objects to upsert
 */
export async function upsert(table, conflictCols, dataCols, rows) {
  if (!rows.length) return { rowCount: 0 };

  // Determine full set of columns to insert (union of conflict and data columns)
  const cols = dataCols;
  const allCols = [...new Set([...conflictCols, ...cols])];

  // Build list of values and placeholders
  const values = [];
  const chunks = rows.map((row, i) => {
    const base = i * allCols.length;
    allCols.forEach((c) => values.push(row[c] ?? null));
    const placeholders = allCols.map((_, j) => `$${base + j + 1}`).join(",");
    return `(${placeholders})`;
  });

  // Build update clause: update only data columns that are not part of conflict key
  const updates = cols
    .filter((c) => !conflictCols.includes(c))
    .map((c) => `${c}=EXCLUDED.${c}`)
    .join(",");

  const sql = `
    INSERT INTO ${table} (${allCols.join(",")})
    VALUES ${chunks.join(",")}
    ON CONFLICT (${conflictCols.join(",")})
    DO UPDATE SET ${updates}
  `;

  return q(sql, values);
}
