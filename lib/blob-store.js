import { getStore } from "@netlify/blobs";

const storeName = process.env.BLOB_STORE_NAME || "etl-history";

function safeKeyPart(value) {
  return value.replace(/[:.]/g, "-");
}

/**
 * Persist fetched rows into Netlify Blob Storage for later reuse/inspection.
 * Returns the fully qualified blob key when something was written.
 *
 * @param {string} source
 * @param {{ sinceUtc: string, untilUtc: string }} range
 * @param {any[]} records
 */
export async function archiveRange(source, range, records) {
  if (!records || records.length === 0) return null;

  try {
    const store = getStore({ name: storeName });
    const key = `${source}/${safeKeyPart(range.sinceUtc)}_to_${safeKeyPart(
      range.untilUtc
    )}.json`;
    const payload = {
      source,
      range,
      savedAt: new Date().toISOString(),
      rows: records
    };
    await store.set(key, JSON.stringify(payload), {
      contentType: "application/json"
    });
    return `${storeName}/${key}`;
  } catch (err) {
    console.error(`Failed to archive ${source} data to blobs`, err);
    return null;
  }
}
