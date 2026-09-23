/**
 * ACN Requests Sync — Apps Script port of acn-requests-sync.py
 *
 * Setup:
 * 1. Extensions > Apps Script on the target Google Sheet.
 * 2. Paste this file in as Code.gs.
 * 3. Project Settings > Script Properties, add:
 *      FIREBASE_PROJECT_ID
 *      FIREBASE_CLIENT_EMAIL
 *      FIREBASE_PRIVATE_KEY      (full PEM, keep \n as literal \n — script unescapes it)
 *    Pick ONE Firestore project's creds (the source .env's NEW_-prefixed
 *    values for the "new" DB, unprefixed for "old") and paste those three
 *    fields in here — this script targets whichever set you choose.
 * 4. Run `syncAcnRequests` once to authorize (Firestore + Sheets scopes).
 * 5. Optional: run `installSyncTrigger` once to schedule an automatic sync
 *    (every 6 hours — change the interval in the function if needed).
 */

var IST_OFFSET_MINUTES = 5 * 60 + 30;

var LOAN_COLLECTION = 'acnLoanRequests';
var LOAN_SHEET_NAME = 'Loan';
var LOAN_HEADERS = [
  'Request ID', 'CP ID', 'Agent Name', 'Agent Phone', 'Bank Key', 'Loan Amount',
  'Loan Amount Label', 'Bank Payout', 'ACN Payout', 'Extra Percent',
  'Status', 'Source', 'Created At', 'Updated At'
];

var SERVICE_COLLECTION = 'acnServiceRequests';
var SERVICE_SHEET_NAME = 'Legal';
var SERVICE_HEADERS = [
  'Request ID', 'CP ID', 'Agent Name', 'Agent Phone', 'Service Key',
  'Payout Label', 'Status', 'Source', 'Created At', 'Updated At'
];

function syncAcnRequests() {
  var startTime = new Date().getTime();
  var token = getFirestoreAccessToken_();
  var projectId = getConfig_('FIREBASE_PROJECT_ID');

  var loanRows = fetchRows_(token, projectId, LOAN_COLLECTION, mapLoanRow_);
  writeToSheet_(LOAN_SHEET_NAME, LOAN_HEADERS, loanRows);

  var serviceRows = fetchRows_(token, projectId, SERVICE_COLLECTION, mapServiceRow_);
  writeToSheet_(SERVICE_SHEET_NAME, SERVICE_HEADERS, serviceRows);

  var elapsed = (new Date().getTime() - startTime) / 1000;
  Logger.log('Done in ' + elapsed.toFixed(2) + ' seconds.');
}

function installSyncTrigger() {
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === 'syncAcnRequests') ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger('syncAcnRequests').timeBased().everyHours(6).create();
}

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('ACN Sync')
    .addItem('Sync now', 'syncAcnRequests')
    .addToUi();
}

// ---------------------------
// Row mappers
// ---------------------------
function mapLoanRow_(data) {
  return [
    sanitizeStr_(data.requestId),
    sanitizeStr_(data.cpId),
    sanitizeStr_(data.agentName),
    sanitizeStr_(data.agentPhone),
    sanitizeStr_(data.bankKey),
    data.loanAmount != null ? data.loanAmount : '',
    sanitizeStr_(data.loanAmountLabel),
    data.bankPayout != null ? data.bankPayout : '',
    data.acnPayout != null ? data.acnPayout : '',
    data.extraPercent != null ? data.extraPercent : '',
    sanitizeStr_(data.status),
    sanitizeStr_(data.source),
    formatDate_(data.createdAt) + ' ' + formatTime_(data.createdAt),
    formatDate_(data.updatedAt) + ' ' + formatTime_(data.updatedAt)
  ];
}

function mapServiceRow_(data) {
  return [
    sanitizeStr_(data.requestId),
    sanitizeStr_(data.cpId),
    sanitizeStr_(data.agentName),
    sanitizeStr_(data.agentPhone),
    sanitizeStr_(data.serviceKey),
    sanitizeStr_(data.payoutLabel),
    sanitizeStr_(data.status),
    sanitizeStr_(data.source),
    formatDate_(data.createdAt) + ' ' + formatTime_(data.createdAt),
    formatDate_(data.updatedAt) + ' ' + formatTime_(data.updatedAt)
  ];
}

// ---------------------------
// Helpers
// ---------------------------
function sanitizeStr_(value) {
  if (typeof value === 'string') return value.replace(/^'+/, '');
  if (value === null || value === undefined) return '';
  return String(value);
}

function istDateFromUnix_(unixTimestamp) {
  var seconds = Number(unixTimestamp);
  if (!seconds) return null;
  var utcMs = seconds * 1000;
  return new Date(utcMs + IST_OFFSET_MINUTES * 60 * 1000);
}

function pad2_(n) { return (n < 10 ? '0' : '') + n; }

function formatDate_(unixTimestamp) {
  var d = istDateFromUnix_(unixTimestamp);
  if (!d) return '';
  return pad2_(d.getUTCMonth() + 1) + '/' + pad2_(d.getUTCDate()) + '/' + d.getUTCFullYear();
}

function formatTime_(unixTimestamp) {
  var d = istDateFromUnix_(unixTimestamp);
  if (!d) return '';
  return pad2_(d.getUTCHours()) + ':' + pad2_(d.getUTCMinutes()) + ':' + pad2_(d.getUTCSeconds());
}

function getConfig_(key) {
  var value = PropertiesService.getScriptProperties().getProperty(key);
  if (!value) throw new Error('Missing script property: ' + key);
  return value;
}

// ---------------------------
// Firestore auth (service account JWT -> OAuth2 access token)
// ---------------------------
function getFirestoreAccessToken_() {
  var projectId = getConfig_('FIREBASE_PROJECT_ID');
  var clientEmail = getConfig_('FIREBASE_CLIENT_EMAIL');
  var privateKey = getConfig_('FIREBASE_PRIVATE_KEY').replace(/\\n/g, '\n');

  var now = Math.floor(new Date().getTime() / 1000);
  var header = { alg: 'RS256', typ: 'JWT' };
  var claimSet = {
    iss: clientEmail,
    scope: 'https://www.googleapis.com/auth/datastore',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now
  };

  var toSign = base64UrlEncode_(JSON.stringify(header)) + '.' + base64UrlEncode_(JSON.stringify(claimSet));
  var signatureBytes = Utilities.computeRsaSha256Signature(toSign, privateKey);
  var jwt = toSign + '.' + base64UrlEncodeBytes_(signatureBytes);

  var response = UrlFetchApp.fetch('https://oauth2.googleapis.com/token', {
    method: 'post',
    contentType: 'application/x-www-form-urlencoded',
    payload: {
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt
    },
    muteHttpExceptions: true
  });

  var body = JSON.parse(response.getContentText());
  if (!body.access_token) {
    throw new Error('Failed to get access token: ' + response.getContentText());
  }
  return body.access_token;
}

function base64UrlEncode_(str) {
  return base64UrlEncodeBytes_(Utilities.newBlob(str).getBytes());
}

function base64UrlEncodeBytes_(bytes) {
  return Utilities.base64Encode(bytes)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

// ---------------------------
// Firestore REST fetch
// ---------------------------
function fetchRows_(token, projectId, collectionName, rowMapper) {
  Logger.log('Fetching documents from ' + collectionName + '...');
  var rows = [];
  var pageToken = null;

  do {
    var url = 'https://firestore.googleapis.com/v1/projects/' + projectId +
      '/databases/(default)/documents/' + collectionName + '?pageSize=300' +
      (pageToken ? '&pageToken=' + encodeURIComponent(pageToken) : '');

    var response = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: { Authorization: 'Bearer ' + token },
      muteHttpExceptions: true
    });

    var body = JSON.parse(response.getContentText());
    if (body.error) {
      throw new Error('Firestore error on ' + collectionName + ': ' + JSON.stringify(body.error));
    }

    (body.documents || []).forEach(function (doc) {
      var data = firestoreFieldsToObject_(doc.fields || {});
      rows.push([data.createdAt || 0, rowMapper(data)]);
    });

    pageToken = body.nextPageToken || null;
  } while (pageToken);

  rows.sort(function (a, b) { return (b[0] || 0) - (a[0] || 0); });
  Logger.log('Fetched ' + rows.length + ' docs from ' + collectionName + '.');
  return rows.map(function (r) { return r[1]; });
}

function firestoreFieldsToObject_(fields) {
  var out = {};
  Object.keys(fields).forEach(function (key) {
    out[key] = firestoreValueToJs_(fields[key]);
  });
  return out;
}

function firestoreValueToJs_(value) {
  if (value.stringValue !== undefined) return value.stringValue;
  if (value.integerValue !== undefined) return Number(value.integerValue);
  if (value.doubleValue !== undefined) return value.doubleValue;
  if (value.booleanValue !== undefined) return value.booleanValue;
  if (value.timestampValue !== undefined) return Math.floor(new Date(value.timestampValue).getTime() / 1000);
  if (value.nullValue !== undefined) return null;
  if (value.mapValue !== undefined) return firestoreFieldsToObject_(value.mapValue.fields || {});
  if (value.arrayValue !== undefined) {
    return (value.arrayValue.values || []).map(firestoreValueToJs_);
  }
  return null;
}

// ---------------------------
// Write to the bound Google Sheet
// ---------------------------
function writeToSheet_(sheetName, headers, rows) {
  if (!rows.length) {
    Logger.log("No rows for '" + sheetName + "', skipping write.");
    return;
  }

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
    Logger.log("Created new sheet '" + sheetName + "'.");
  } else {
    Logger.log("Found existing sheet '" + sheetName + "'.");
  }

  sheet.clear();
  var payload = [headers].concat(rows);
  sheet.getRange(1, 1, payload.length, headers.length).setValues(payload);
  Logger.log("'" + sheetName + "' write complete: " + payload.length + ' rows.');
}
