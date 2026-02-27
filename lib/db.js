import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';

let docInstance = null;

export async function getDoc() {
  if (docInstance) return docInstance;

  const serviceAccountAuth = new JWT({
    email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  docInstance = new GoogleSpreadsheet(process.env.GOOGLE_SHEET_ID, serviceAccountAuth);
  await docInstance.loadInfo();
  return docInstance;
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

// Mock the 'q' function so other files that import it don't crash
export async function q(text, params = []) {
  console.log("SQL q() bypassed for Google Sheets.");
  return { rowCount: 0, rows: [] };
}

/**
 * Replaces the Postgres upsert. 
 * Checks Google Sheets for existing records (based on conflictCols) and appends new ones.
 */
export async function upsert(table, conflictCols, dataCols, rows) {
  if (!rows || rows.length === 0) return { rowCount: 0 };

  const doc = await getDoc();
  const tabName = TAB_MAPPING[table] || table;
  const sheet = doc.sheetsByTitle[tabName];

  if (!sheet) {
    console.error(`Tab named "${tabName}" not found in your Google Sheet!`);
    return { rowCount: 0 };
  }

  // If the sheet is totally empty, set up the header row automatically
  try {
    await sheet.loadHeaderRow();
  } catch (e) {
    await sheet.setHeaderRow(dataCols);
  }

  // Fetch existing rows to prevent duplicates
  const existingRows = await sheet.getRows();
  const existingKeys = new Set();
  
  existingRows.forEach(row => {
    // Build a unique key (e.g., "date||ad_id") to track what's already in the sheet
    const key = conflictCols.map(col => row.get(col)).join('||');
    existingKeys.add(key);
  });

  // Filter out data that is already in the sheet
  const newRows = [];
  for (const row of rows) {
    const key = conflictCols.map(col => row[col]).join('||');
    
    if (!existingKeys.has(key)) {
      // Prepare row for insertion (stringify objects/arrays so Sheets doesn't crash)
      const sheetRow = {};
      for (const col of dataCols) {
        sheetRow[col] = typeof row[col] === 'object' && row[col] !== null 
          ? JSON.stringify(row[col]) 
          : row[col];
      }
      newRows.push(sheetRow);
      // Add to set so we don't duplicate within the same batch
      existingKeys.add(key); 
    }
  }

  // Append new rows to the sheet
  if (newRows.length > 0) {
    await sheet.addRows(newRows);
    console.log(`Added ${newRows.length} new rows to ${tabName}`);
  }

  return { rowCount: newRows.length };
}
