import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';

let docPromise = null;
let authClient = null; // Store the auth client to use for raw Bulk API requests

export async function getDoc() {
  if (!docPromise) {
    docPromise = (async () => {
      authClient = new JWT({
        email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
        key: (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
      });

      const doc = new GoogleSpreadsheet(process.env.GOOGLE_SHEET_ID, authClient);
      await doc.loadInfo(); 
      return doc;
    })();
  }
  return docPromise;
}

const TAB_MAPPING = {
  'stripe_orders': 'Stripe',
  'meta_insights': 'Meta',
  'tiktok_insights': 'TikTok',
  'mailerlite_subscribers': 'MailerLite',
  'mailerlite_group_memberships': 'MailerLite_Groups',
  'steam_sales': 'Steam_Sales',
  'telemetry': 'Telemetry' 
};

export async function q(text, params = []) {
  console.log("SQL q() bypassed for Google Sheets.");
  return { rowCount: 0, rows: [] };
}

/**
 * Erases existing data for a given date range inside the specified tab.
 * USES BULK API REQUEST TO PREVENT TIMEOUTS.
 */
export async function clearDateRange(table, dateCol, since, until) {
  const doc = await getDoc();
  const tabName = TAB_MAPPING[table] || table;
  const sheet = doc.sheetsByTitle[tabName];
  
  if (!sheet) return 0;

  const rows = await sheet.getRows();
  const s = since.substring(0, 10);
  const u = until.substring(0, 10);

  // 1. Gather all 0-based indices of rows that fall in the date range
  const indicesToDelete = [];
  for (let i = 0; i < rows.length; i++) {
    const rowDateStr = rows[i].get(dateCol);
    if (rowDateStr) {
      const d = rowDateStr.substring(0, 10);
      if (d >= s && d <= u) {
        // BUGFIX: google-spreadsheet v4 uses .rowNumber (1-based index).
        // Subtracting 1 gives us the true 0-based index required by Google's API.
        indicesToDelete.push(rows[i].rowNumber - 1);
      }
    }
  }

  if (indicesToDelete.length === 0) return 0;

  // 2. Sort descending. This is CRITICAL. 
  // By deleting rows from the bottom up, we prevent lower index shifts from breaking the batch update.
  indicesToDelete.sort((a, b) => b - a);

  // 3. Group contiguous indices into dimension ranges for minimum payload size
  const requests = [];
  let currentStart = null;
  let currentEnd = null;

  for (const idx of indicesToDelete) {
    if (currentStart === null) {
      currentStart = idx;
      currentEnd = idx + 1;
    } else if (idx === currentStart - 1) {
      currentStart = idx; // Extend range backward
    } else {
      // Break in continuity, push the current range and start a new one
      requests.push({
        deleteDimension: {
          range: {
            sheetId: sheet.sheetId,
            dimension: "ROWS",
            startIndex: currentStart,
            endIndex: currentEnd
          }
        }
      });
      currentStart = idx;
      currentEnd = idx + 1;
    }
  }
  
  if (currentStart !== null) {
    requests.push({
      deleteDimension: {
        range: {
          sheetId: sheet.sheetId,
          dimension: "ROWS",
          startIndex: currentStart,
          endIndex: currentEnd
        }
      }
    });
  }

  // 4. Send a single batch request to delete all ranges instantly
  if (requests.length > 0) {
    try {
      await authClient.request({
        url: `https://sheets.googleapis.com/v4/spreadsheets/${process.env.GOOGLE_SHEET_ID}:batchUpdate`,
        method: 'POST',
        data: { requests }
      });
      
      console.log(`Force Refresh: Bulk deleted ${indicesToDelete.length} rows from ${tabName} (${s} to ${u})`);
      
      // CRITICAL FIX: Destroy cache and wait 2 full seconds for Google's internal replica servers to sync
      // so the subsequent Upsert function doesn't pull stale rows.
      docPromise = null;
      await new Promise(resolve => setTimeout(resolve, 2000));
      
    } catch (e) {
      throw new Error(`Batch delete failed: ${e.response?.data?.error?.message || e.message}`);
    }
  }

  return indicesToDelete.length;
}

export async function upsert(table, conflictCols, dataCols, rows) {
  if (!rows || rows.length === 0) return { rowCount: 0 };

  const doc = await getDoc();
  const tabName = TAB_MAPPING[table] || table;
  const sheet = doc.sheetsByTitle[tabName];

  if (!sheet) {
    console.error(`Tab named "${tabName}" not found in your Google Sheet!`);
    return { rowCount: 0 };
  }

  try {
    await sheet.loadHeaderRow();
  } catch (e) {
    await sheet.setHeaderRow(dataCols);
  }

  const existingRows = await sheet.getRows();
  const existingKeys = new Set();
  
  existingRows.forEach(row => {
    const key = conflictCols.map(col => row.get(col)).join('||');
    existingKeys.add(key);
  });

  const newRows = [];
  for (const row of rows) {
    const key = conflictCols.map(col => row[col]).join('||');
    
    if (!existingKeys.has(key)) {
      const sheetRow = {};
      for (const col of dataCols) {
        sheetRow[col] = typeof row[col] === 'object' && row[col] !== null 
          ? JSON.stringify(row[col]) 
          : row[col];
      }
      newRows.push(sheetRow);
      existingKeys.add(key); 
    }
  }

  if (newRows.length > 0) {
    await sheet.addRows(newRows);
    console.log(`Added ${newRows.length} new rows to ${tabName}`);
  }

  return { rowCount: newRows.length };
}
