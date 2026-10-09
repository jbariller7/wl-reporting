import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';

let docPromise = null;
let authClient = null; // Store the auth client to use for raw Bulk API requests
const headerLoadedSheets = new WeakSet();

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
  'meta_adsets': 'Meta_AdSets',
  'meta_ad_insights': 'Meta_Ads',
  'tiktok_insights': 'TikTok',
  'mailerlite_subscribers': 'MailerLite',
  'mailerlite_dashboard_subscribers': 'MailerLite_Dashboard',
  'mailerlite_group_memberships': 'MailerLite_Groups',
  'steam_sales': 'Steam_Sales',
  'steam_wishlist': 'Steam_Wishlist',
  'google_play_sales': 'Google_Play',
  'google_play_installs': 'Google_Play_Installs',
  'telemetry': 'Telemetry' 
};

export async function q(text, params = []) {
  console.log("SQL q() bypassed for Google Sheets.");
  return { rowCount: 0, rows: [] };
}

export async function readSheetColumns(table, columns, options = {}) {
  const doc = await getDoc();
  const tabName = TAB_MAPPING[table] || table;
  let sheet = doc.sheetsByTitle[tabName];
  if (!sheet && options.unbounded) {
    await doc.loadInfo();
    sheet = doc.sheetsByTitle[tabName];
  }
  if (!sheet) throw new Error(`Tab named "${tabName}" not found in your Google Sheet`);

  if (!headerLoadedSheets.has(sheet)) {
    await sheet.loadHeaderRow();
    headerLoadedSheets.add(sheet);
  }
  const headers = sheet.headerValues || [];
  const columnIndices = columns.map(column => headers.indexOf(column));
  const missing = columns.filter((_, index) => columnIndices[index] < 0);
  if (missing.length > 0) throw new Error(`${tabName} is missing columns: ${missing.join(", ")}`);

  const startRow = Math.max(2, Number(options.startRow || 2));
  const endRow = options.unbounded && options.endRow == null
    ? null
    : Math.min(sheet.rowCount, Number(options.endRow || sheet.rowCount));
  if (endRow !== null && endRow < startRow) return [];

  const columnName = (index) => {
    let value = index + 1;
    let name = '';
    while (value > 0) {
      const remainder = (value - 1) % 26;
      name = String.fromCharCode(65 + remainder) + name;
      value = Math.floor((value - 1) / 26);
    }
    return name;
  };
  const escapedTitle = tabName.replace(/'/g, "''");
  const ranges = columnIndices.map(index =>
    `'${escapedTitle}'!${columnName(index)}${startRow}:${columnName(index)}${endRow ?? ''}`
  );
  const query = ranges.map(range => `ranges=${encodeURIComponent(range)}`).join('&');
  const response = await authClient.request({
    url: `https://sheets.googleapis.com/v4/spreadsheets/${process.env.GOOGLE_SHEET_ID}/values:batchGet?${query}&majorDimension=COLUMNS&valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=SERIAL_NUMBER`,
    method: 'GET'
  });
  const valueRanges = response.data?.valueRanges || [];
  const valuesByColumn = valueRanges.map(valueRange => valueRange.values?.[0] || []);
  const rowCount = Math.max(0, ...valuesByColumn.map(values => values.length));

  return Array.from({ length: rowCount }, (_, rowOffset) => {
    const row = { __rowNumber: startRow + rowOffset };
    columns.forEach((column, columnIndex) => {
      row[column] = valuesByColumn[columnIndex]?.[rowOffset] ?? '';
    });
    return row;
  }).filter(row => columns.some(column => String(row[column] ?? '').trim() !== ''));
}

export function normalizeSheetDateValue(value) {
  const text = String(value || "").trim();
  const iso = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;

  const slashDate = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (slashDate) {
    const first = Number(slashDate[1]);
    const second = Number(slashDate[2]);
    const monthFirst = second > 12 && first <= 12;
    const day = monthFirst ? second : first;
    const month = monthFirst ? first : second;
    return `${slashDate[3]}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
  }

  const serial = Number(text);
  if (Number.isFinite(serial) && serial >= 1 && serial < 100000) {
    return new Date((Math.floor(serial) - 25569) * 86400000).toISOString().slice(0, 10);
  }
  return text.slice(0, 10);
}

export function coalesceVerticalUpdates(updates) {
  const sorted = [...updates].sort((a, b) =>
    a.startColumn - b.startColumn
    || a.endColumn - b.endColumn
    || a.rowNumber - b.rowNumber
  );
  const result = [];
  for (const update of sorted) {
    const previous = result[result.length - 1];
    if (previous
      && previous.startColumn === update.startColumn
      && previous.endColumn === update.endColumn
      && previous.endRow + 1 === update.rowNumber) {
      previous.endRow = update.rowNumber;
      previous.values.push(update.values);
    } else {
      result.push({
        startColumn: update.startColumn,
        endColumn: update.endColumn,
        startRow: update.rowNumber,
        endRow: update.rowNumber,
        values: [update.values]
      });
    }
  }
  return result;
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
  const s = normalizeSheetDateValue(since);
  const u = normalizeSheetDateValue(until);

  // 1. Gather all 0-based indices of rows that fall in the date range
  const indicesToDelete = [];
  for (let i = 0; i < rows.length; i++) {
    const rowDateStr = rows[i].get(dateCol);
    if (rowDateStr) {
      const d = normalizeSheetDateValue(rowDateStr);
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

export async function retypeSheetColumns(table, columns) {
  const doc = await getDoc();
  const tabName = TAB_MAPPING[table] || table;
  const sheet = doc.sheetsByTitle[tabName];
  if (!sheet) return 0;

  await sheet.loadHeaderRow();
  const headers = sheet.headerValues;
  const indices = columns.map(column => headers.indexOf(column));
  if (indices.some(index => index < 0)) throw new Error(`Cannot repair ${tabName}: missing one of ${columns.join(", ")}`);

  const rows = await sheet.getRows();
  if (rows.length === 0) return 0;
  const columnName = (index) => {
    let value = index + 1;
    let name = '';
    while (value > 0) {
      const remainder = (value - 1) % 26;
      name = String.fromCharCode(65 + remainder) + name;
      value = Math.floor((value - 1) / 26);
    }
    return name;
  };

  const sortedIndices = [...indices].sort((a, b) => a - b);
  const groups = [];
  for (const index of sortedIndices) {
    const previous = groups[groups.length - 1];
    if (previous && previous.end + 1 === index) previous.end = index;
    else groups.push({ start: index, end: index });
  }

  const escapedTitle = tabName.replace(/'/g, "''");
  const data = groups.map(group => ({
    range: `'${escapedTitle}'!${columnName(group.start)}${rows[0].rowNumber}:${columnName(group.end)}${rows[rows.length - 1].rowNumber}`,
    values: rows.map(row => headers.slice(group.start, group.end + 1).map(header => row.get(header) ?? ''))
  }));
  await authClient.request({
    url: `https://sheets.googleapis.com/v4/spreadsheets/${process.env.GOOGLE_SHEET_ID}/values:batchUpdate`,
    method: 'POST',
    data: { valueInputOption: 'USER_ENTERED', data }
  });
  console.log(`Re-typed ${columns.join(', ')} for ${rows.length} rows in ${tabName}`);
  return rows.length;
}

export async function ensureSheetColumns(table, columns, options = {}) {
  const doc = await getDoc();
  const tabName = TAB_MAPPING[table] || table;
  let sheet = doc.sheetsByTitle[tabName];
  if (!sheet) {
    if (!options.createSheet) throw new Error(`Tab named "${tabName}" not found in your Google Sheet`);
    sheet = await doc.addSheet({ title: tabName, headerValues: columns });
    console.log(`Created Google Sheet tab "${tabName}".`);
    return sheet;
  }

  let headers = [];
  try {
    await sheet.loadHeaderRow();
    headers = sheet.headerValues || [];
  } catch {
    headers = [];
  }
  if (columns.every(column => headers.includes(column))) return sheet;

  const rows = headers.length > 0 ? await sheet.getRows() : [];
  if (rows.length > 0) {
    throw new Error(`${tabName} has an incompatible non-empty schema. Preserve or rename that tab before running the new sync.`);
  }
  await sheet.setHeaderRow(columns);
  console.log(`Initialized ${tabName} with the required schema.`);
  return sheet;
}

export async function upsert(table, conflictCols, dataCols, rows, options = {}) {
  if (!rows || rows.length === 0) return { rowCount: 0 };

  const doc = await getDoc();
  const tabName = TAB_MAPPING[table] || table;
  let sheet = doc.sheetsByTitle[tabName];

  if (!sheet) {
    if (!options.createSheet) {
      console.error(`Tab named "${tabName}" not found in your Google Sheet!`);
      return { rowCount: 0 };
    }
    sheet = await doc.addSheet({ title: tabName, headerValues: dataCols });
    console.log(`Created Google Sheet tab "${tabName}".`);
  }

  try {
    await sheet.loadHeaderRow();
  } catch (e) {
    await sheet.setHeaderRow(dataCols);
  }

  const existingRows = await sheet.getRows();
  const existingKeys = new Map();
  
  existingRows.forEach(row => {
    const key = conflictCols.map(col => row.get(col)).join('||');
    existingKeys.set(key, row);
  });

  const newRows = [];
  const rawCellUpdates = [];
  const userEnteredCellUpdates = [];
  const updatedRowNumbers = new Set();
  const serializeCell = (value) => {
    if (typeof value === 'object' && value !== null) return JSON.stringify(value);
    return value ?? '';
  };
  const headers = sheet.headerValues || dataCols;
  const userEnteredColumns = new Set(options.userEnteredColumns || []);
  const columnName = (index) => {
    let value = index + 1;
    let name = '';
    while (value > 0) {
      const remainder = (value - 1) % 26;
      name = String.fromCharCode(65 + remainder) + name;
      value = Math.floor((value - 1) / 26);
    }
    return name;
  };

  for (const row of rows) {
    const key = conflictCols.map(col => row[col]).join('||');
    const existingRow = existingKeys.get(key);

    if (!existingRow) {
      const sheetRow = {};
      for (const col of dataCols) {
        sheetRow[col] = serializeCell(row[col]);
      }
      newRows.push(sheetRow);
      existingKeys.set(key, sheetRow);
    } else if (options.updateExisting) {
      const changedIndices = [];
      for (let index = 0; index < headers.length; index++) {
        const header = headers[index];
        if (!dataCols.includes(header) || userEnteredColumns.has(header)) continue;
        const value = serializeCell(row[header]);
        if (String(existingRow.get(header) ?? '') !== String(value)) changedIndices.push(index);
      }

      for (let i = 0; i < changedIndices.length;) {
        const startColumn = changedIndices[i];
        let endColumn = startColumn;
        while (i + 1 < changedIndices.length && changedIndices[i + 1] === endColumn + 1) {
          i++;
          endColumn = changedIndices[i];
        }
        rawCellUpdates.push({
          rowNumber: existingRow.rowNumber,
          startColumn,
          endColumn,
          values: headers.slice(startColumn, endColumn + 1).map(header => serializeCell(row[header]))
        });
        updatedRowNumbers.add(existingRow.rowNumber);
        i++;
      }

      const typedIndices = headers
        .map((header, index) => userEnteredColumns.has(header) && dataCols.includes(header) ? index : -1)
        .filter(index => index >= 0);
      for (let i = 0; i < typedIndices.length;) {
        const startColumn = typedIndices[i];
        let endColumn = startColumn;
        while (i + 1 < typedIndices.length && typedIndices[i + 1] === endColumn + 1) {
          i++;
          endColumn = typedIndices[i];
        }
        userEnteredCellUpdates.push({
          rowNumber: existingRow.rowNumber,
          startColumn,
          endColumn,
          values: headers.slice(startColumn, endColumn + 1).map(header => serializeCell(row[header]))
        });
        updatedRowNumbers.add(existingRow.rowNumber);
        i++;
      }
    }
  }

  if (newRows.length > 0) {
    // Append as RAW first so identifiers and JSON are never coerced. Columns
    // explicitly listed in userEnteredColumns are re-written immediately below
    // so dates and numeric report fields retain a consistent Sheet type.
    const addedRows = await sheet.addRows(newRows, { raw: true });
    if (userEnteredColumns.size > 0) {
      const typedIndices = headers
        .map((header, index) => userEnteredColumns.has(header) && dataCols.includes(header) ? index : -1)
        .filter(index => index >= 0);
      for (const addedRow of addedRows) {
        for (let i = 0; i < typedIndices.length;) {
          const startColumn = typedIndices[i];
          let endColumn = startColumn;
          while (i + 1 < typedIndices.length && typedIndices[i + 1] === endColumn + 1) {
            i++;
            endColumn = typedIndices[i];
          }
          userEnteredCellUpdates.push({
            rowNumber: addedRow.rowNumber,
            startColumn,
            endColumn,
            values: headers.slice(startColumn, endColumn + 1).map(header => serializeCell(addedRow.get(header)))
          });
          i++;
        }
      }
    }
    console.log(`Added ${newRows.length} new rows to ${tabName}`);
  }

  const escapedTitle = tabName.replace(/'/g, "''");
  const toSheetUpdates = (updates) => coalesceVerticalUpdates(updates).map(update => ({
    range: `'${escapedTitle}'!${columnName(update.startColumn)}${update.startRow}:${columnName(update.endColumn)}${update.endRow}`,
    values: update.values
  }));
  const rawUpdates = toSheetUpdates(rawCellUpdates);
  const userEnteredUpdates = toSheetUpdates(userEnteredCellUpdates);

  if (rawUpdates.length > 0) {
    for (let i = 0; i < rawUpdates.length; i += 200) {
      await authClient.request({
        url: `https://sheets.googleapis.com/v4/spreadsheets/${process.env.GOOGLE_SHEET_ID}/values:batchUpdate`,
        method: 'POST',
        data: { valueInputOption: 'RAW', data: rawUpdates.slice(i, i + 200) }
      });
    }
  }

  if (userEnteredUpdates.length > 0) {
    for (let i = 0; i < userEnteredUpdates.length; i += 200) {
      await authClient.request({
        url: `https://sheets.googleapis.com/v4/spreadsheets/${process.env.GOOGLE_SHEET_ID}/values:batchUpdate`,
        method: 'POST',
        data: { valueInputOption: 'USER_ENTERED', data: userEnteredUpdates.slice(i, i + 200) }
      });
    }
  }

  if (updatedRowNumbers.size > 0) console.log(`Updated ${updatedRowNumbers.size} existing rows in ${tabName}`);

  return { rowCount: newRows.length + updatedRowNumbers.size, addedRows: newRows.length, updatedRows: updatedRowNumbers.size };
}
