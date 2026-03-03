import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';

let docPromise = null;

export async function getDoc() {
  if (!docPromise) {
    docPromise = (async () => {
      const serviceAccountAuth = new JWT({
        email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
        key: (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
      });

      const doc = new GoogleSpreadsheet(process.env.GOOGLE_SHEET_ID, serviceAccountAuth);
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
  'steam_sales': 'Steam_Sales'
};

export async function q(text, params = []) {
  console.log("SQL q() bypassed for Google Sheets.");
  return { rowCount: 0, rows: [] };
}

/**
 * Erases existing data for a given date range inside the specified tab.
 */
export async function clearDateRange(table, dateCol, since, until) {
  const doc = await getDoc();
  const tabName = TAB_MAPPING[table] || table;
  const sheet = doc.sheetsByTitle[tabName];
  
  if (!sheet) return 0;

  let deletedCount = 0;
  const rows = await sheet.getRows();
  
  const s = since.substring(0, 10);
  const u = until.substring(0, 10);

  // We MUST iterate backward so deleting a row doesn't shift the indices of the ones before it
  for (let i = rows.length - 1; i >= 0; i--) {
    const rowDateStr = rows[i].get(dateCol);
    if (rowDateStr) {
      const d = rowDateStr.substring(0, 10);
      if (d >= s && d <= u) {
        await rows[i].delete();
        deletedCount++;
      }
    }
  }
  
  if (deletedCount > 0) {
    console.log(`Force Refresh: Deleted ${deletedCount} rows from ${tabName} (${s} to ${u})`);
  }
  return deletedCount;
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
  }

  return { rowCount: newRows.length };
}
