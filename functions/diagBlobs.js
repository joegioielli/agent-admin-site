// netlify/functions/diagBlobs.js
import { getStore } from '@netlify/blobs';

const { NETLIFY_SITE_ID, NETLIFY_BLOBS_TOKEN, LEADS_ADMIN_TOKEN } = process.env;

const headers = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Max-Age": "86400",
  "Content-Type": "application/json"
};

function tryAuto() { try { return getStore('leads'); } catch { return null; } }
function tryManual() {
  try {
    if (NETLIFY_SITE_ID && NETLIFY_BLOBS_TOKEN) {
      return getStore('leads', { siteID: NETLIFY_SITE_ID, token: NETLIFY_BLOBS_TOKEN });
    }
  } catch {}
  return null;
}

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 204, headers, body: '' };

  // protect with your admin token (same as listLeads)
  const auth = event.headers?.authorization || event.headers?.Authorization || '';
  if (!auth.startsWith('Bearer ') || auth.slice(7) !== (LEADS_ADMIN_TOKEN || '')) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const out = {
    hasSiteId: !!NETLIFY_SITE_ID,
    tokenPresent: !!NETLIFY_BLOBS_TOKEN,
    tokenLen: NETLIFY_BLOBS_TOKEN ? NETLIFY_BLOBS_TOKEN.length : 0,
  };

  // Try site context
  try {
    const s = tryAuto();
    if (s) { await s.list({ prefix: 'diag/' }); out.autoStoreOk = true; } else out.autoStoreOk = false;
  } catch (e) { out.autoStoreOk = false; out.autoError = String(e); }

  // Try manual token
  try {
    const s = tryManual();
    if (s) { await s.list({ prefix: 'diag/' }); out.fallbackStoreOk = true; } else out.fallbackStoreOk = !!(NETLIFY_SITE_ID && NETLIFY_BLOBS_TOKEN) ? false : null;
  } catch (e) { out.fallbackStoreOk = false; out.fallbackError = String(e); }

  // Write/read test using whichever works
  try {
    const s = out.autoStoreOk ? tryAuto() : out.fallbackStoreOk ? tryManual() : null;
    if (s) {
      const key = `diag/${Date.now()}-${Math.random().toString(36).slice(2)}.json`;
      await s.setJSON(key, { diag: true, at: new Date().toISOString() });
      const got = await s.getJSON(key).catch(()=>null);
      out.writeTest = !!got; out.writeKey = key;
    } else {
      out.writeTest = false; out.writeNote = 'no store available';
    }
  } catch (e) { out.writeTest = false; out.writeError = String(e); }

  return { statusCode: 200, headers, body: JSON.stringify(out, null, 2) };
};
