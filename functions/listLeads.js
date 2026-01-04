// netlify/functions/listLeads.js
import { getStore } from '@netlify/blobs';

const ALLOW_ORIGIN = "https://mls-drop2.netlify.app"; // adjust if needed
const cors = {
  "Access-Control-Allow-Origin": ALLOW_ORIGIN,
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Max-Age": "86400",
  "Content-Type": "application/json"
};

function leadsStore() {
  try { return getStore('leads'); }
  catch {
    return getStore('leads', {
      siteID: process.env.NETLIFY_SITE_ID,
      token: process.env.NETLIFY_BLOBS_TOKEN
    });
  }
}

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 204, headers: cors, body: '' };
  if (event.httpMethod !== 'GET') return { statusCode: 405, headers: cors, body: JSON.stringify({ error: 'Method not allowed' }) };

  // bearer auth
  const token = (event.headers.authorization || '').replace(/^Bearer\s+/i,'').trim();
  if (!process.env.LEADS_ADMIN_TOKEN || token !== process.env.LEADS_ADMIN_TOKEN) {
    return { statusCode: 401, headers: cors, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const day = (event.queryStringParameters?.date || new Date().toISOString().slice(0,10)).replace(/[^0-9-]/g,'');
  const kind = (event.queryStringParameters?.kind || '').trim();

  try {
    const store = leadsStore();
    const { blobs = [] } = await store.list({ prefix: `${day}/` });
    const items = [];
    for (const b of blobs) {
      const data = await store.getJSON(b.key).catch(()=>null);
      if (!data) continue;
      if (kind && data.kind !== kind) continue;
      items.push({ key: b.key, ...data });
    }
    items.sort((a,b)=>(b.submittedAt || b.at || '').localeCompare(a.submittedAt || a.at || ''));
    return { statusCode: 200, headers: cors, body: JSON.stringify({ day, count: items.length, items }) };
  } catch (e) {
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error: 'List failed', message: String(e) }) };
  }
};
