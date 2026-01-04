// netlify/functions/lenders.js — Lambda-compatible with explicit Netlify Blobs config

const { getStore } = require('@netlify/blobs');

/* ------------------------ Store bootstrap ------------------------ */
// Uses explicit siteID + token from environment
function makeStore() {
  const siteID = process.env.NETLIFY_SITE_ID;
  const token =
    process.env.NETLIFY_BLOBS_TOKEN ||
    process.env.NETLIFY_API_TOKEN ||
    process.env.NETLIFY_AUTH_TOKEN;

  if (!siteID || !token) {
    throw new Error(
      'Netlify Blobs not configured. Set NETLIFY_SITE_ID and NETLIFY_BLOBS_TOKEN (or API/AUTH token).'
    );
  }

  // Explicit config avoids MissingBlobsEnvironmentError
  return getStore({ name: 'preferred-lenders', siteID, token });
}

/* ------------------------ Small helpers ------------------------ */
const CORS_HEADERS = {
  'access-control-allow-origin': '*',
  'access-control-allow-headers': 'content-type',
  'access-control-allow-methods': 'GET,PUT,OPTIONS',
  'content-type': 'application/json',
};

function json(body, init = {}) {
  return {
    statusCode: init.status || 200,
    headers: { ...CORS_HEADERS, ...(init.headers || {}) },
    body: JSON.stringify(body),
  };
}

function readJSON(event) {
  if (!event.body) return null;
  try {
    return JSON.parse(event.body);
  } catch {
    return null;
  }
}

/**
 * Accepts:
 *  {
 *    lender?: { name, phone?, nmls?, email?, link? },
 *    offer?:  string | { title?, details?, expiresOn? },
 *    revision?: string
 *  }
 */
function validatePerPropertyPayload(payload) {
  if (!payload || typeof payload !== 'object') {
    return { ok: false, msg: 'Invalid JSON body' };
  }

  let lender = null;
  let offer = null;

  if (payload.lender) {
    const l = payload.lender;
    if (!l || typeof l !== 'object' || !l.name || typeof l.name !== 'string') {
      return { ok: false, msg: 'lender.name (string) is required when lender is provided' };
    }
    lender = {
      name: String(l.name || ''),
      phone: String(l.phone || ''),
      nmls: String(l.nmls || ''),
      email: String(l.email || ''),
      link: String(l.link || ''),
    };
  }

  if (payload.offer != null) {
    if (typeof payload.offer === 'string') {
      // simple/legacy: store into details
      offer = { title: '', details: payload.offer, expiresOn: '' };
    } else if (typeof payload.offer === 'object') {
      const o = payload.offer;
      offer = {
        title: String(o?.title || ''),
        details: String(o?.details || ''),
        expiresOn: String(o?.expiresOn || ''),
      };
    } else {
      return { ok: false, msg: 'offer must be a string or an object' };
    }
  }

  const revision = payload.revision ? String(payload.revision) : null;
  return { ok: true, lender, offer, revision };
}

/* ------------------------ Handler ------------------------ */
const handler = async (event) => {
  console.log('lenders function start', {
    method: event.httpMethod,
    path: event.path,
    qs: event.queryStringParameters,
  });

  // CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return json({}, { status: 200 });
  }

  let store;
  try {
    store = makeStore();
  } catch (e) {
    console.error('makeStore error', e);
    return json(
      { error: 'Blobs not configured', detail: String(e.message || e) },
      { status: 500 }
    );
  }

  const propertyId = event.queryStringParameters?.propertyId || null;

  // GET: per-listing or global
  if (event.httpMethod === 'GET') {
    const key = propertyId ? `${propertyId}.json` : 'global.json';

    try {
      const { data, etag } = await store
        .getWithMetadata(key, { type: 'json' })
        .catch(() => ({ data: null, etag: null }));

      if (propertyId) {
        // Per-listing doc
        return json({
          propertyId,
          lender: data?.lender || null,
          offer: data?.offer || null,
          updatedAt: data?.updatedAt || null,
          revision: etag || null,
        });
      }

      // Global doc
      return json({
        lenders: Array.isArray(data?.lenders) ? data.lenders : [],
        updatedAt: data?.updatedAt || null,
        revision: etag || null,
      });
    } catch (e) {
      console.error('GET error', e);
      return json({ error: 'Read failed', detail: String(e.message || e) }, { status: 500 });
    }
  }

  // PUT: per-listing or global (no admin key required)
  if (event.httpMethod === 'PUT') {
    const body = readJSON(event);
    console.log('PUT body', body);

    // Global update: expects { lenders: [...], revision? }
    if (!propertyId) {
      if (!body || !Array.isArray(body.lenders)) {
        return json(
          { error: 'Expected { lenders: [...] } for global update' },
          { status: 400 }
        );
      }

      const payload = {
        lenders: body.lenders,
        updatedAt: new Date().toISOString(),
      };

      const putOpts = {};
      if (body.revision) putOpts.onlyIfMatch = String(body.revision); // ETag check

      try {
        const { etag } = await store.setJSON('global.json', payload, putOpts);
        console.log('Global PUT ok', { etag });
        return json({ ok: true, revision: etag });
      } catch (e) {
        console.error('Global PUT error', e);
        const msg = String(e?.message || e || '');
        const status = msg.includes('412') ? 412 : 500; // 412 = ETag mismatch
        return json({ error: 'Write failed', detail: msg }, { status });
      }
    }

    // Per-listing update
    const parsed = validatePerPropertyPayload(body);
    if (!parsed.ok) return json({ error: parsed.msg }, { status: 400 });

    const key = `${propertyId}.json`;
    const payload = {
      lender: parsed.lender ?? null,
      offer: parsed.offer ?? null,
      updatedAt: new Date().toISOString(),
    };

    const putOpts = {};
    if (parsed.revision) putOpts.onlyIfMatch = parsed.revision; // ETag check

    try {
      const { etag } = await store.setJSON(key, payload, putOpts);
      console.log('Per-listing PUT ok', { key, etag });
      return json({ ok: true, revision: etag });
    } catch (e) {
      console.error('Per-listing PUT error', e);
      const msg = String(e?.message || e || '');
      const status = msg.includes('412') ? 412 : 500;
      return json({ error: 'Write failed', detail: msg }, { status });
    }
  }

  return json({ error: 'Method not allowed' }, { status: 405 });
};

module.exports = { handler };
