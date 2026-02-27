import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';

// We use a Promise here to prevent race conditions during Netlify cold starts
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
      await doc.loadInfo(); // This ensures the sheet metadata is fully loaded
      return doc;
    })();
  }
  return docPromise;
}

// We map your old Postgres table names to the new Google Sheet Tab names
const TAB_MAPPING = {
  'stripe_orders': 'Stripe',
  'meta_insights': 'Meta',
  'tiktok_insights': 'TikTok',
  'mailerlite_subscribers': 'MailerLite',
  'mailerlite_group_memberships': 'MailerLite_Groups',
  'steam_sales': 'Steam_Sales'
};

// Mock the 'q' function for compatibility
export async function q(text, params = []) {
  console.log("SQL q() bypassed for Google Sheets.");
  return { rowCount: 0, rows: [] };
}

/**
 * Replaces the Postgres upsert. 
 * Checks Google Sheets for existing records and appends new ones.
 */
export async function upsert(table, conflictCols, dataCols, rows) {
  if (!rows || rows.length === 0) return { rowCount: 0 };

  const doc = await getDoc(); // This now waits for docPromise to resolve
  const tabName = TAB_MAPPING[table] || table;
  const sheet = doc.sheetsByTitle[tabName];

  if (!sheet) {
    console.error(`Tab named "${tabName}" not found in your Google Sheet!`);
    return { rowCount: 0 };
  }

  // Ensure header row exists
  try {
    await sheet.loadHeaderRow();
  } catch (e) {
    await sheet.setHeaderRow(dataCols);
  }

  // Fetch existing rows to prevent duplicates
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
