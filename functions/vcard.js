// netlify/functions/vcard.js
// Generate a vCard for a lender, from global store (by lenderId) or direct query params.

export async function handler(event) {
  try {
    // Parse query
    const qs = new URLSearchParams(event.rawQuery || event.queryStringParameters ? '' : '');
    // Netlify sometimes gives both; normalize:
    const params = event.queryStringParameters || Object.fromEntries(qs.entries());

    const proto = event.headers["x-forwarded-proto"] || "https";
    const host  = event.headers.host;
    const base  = `${proto}://${host}`;

    // Helper to fetch a lender by ID from the global store
    async function getGlobalLender(id) {
      try {
        const res = await fetch(`${base}/.netlify/functions/lenders`, { headers: { "cache-control": "no-store" } });
        if (!res.ok) return null;
        const doc = await res.json();
        const list = Array.isArray(doc.lenders) ? doc.lenders : [];
        const found = list.find(l => String(l.lenderId || '').toLowerCase() === String(id || '').toLowerCase());
        return found || null;
      } catch { return null; }
    }

    // Prefer lenderId from global store
    let lender = null;
    const lenderId = params?.lenderId;
    if (lenderId) lender = await getGlobalLender(lenderId);

    // Or build from direct fields
    if (!lender) {
      lender = {
        company: params.company || '',
        repName: params.repName || '',
        phone:   params.phone   || '',
        email:   params.email   || '',
        url:     params.url     || '',
        title:   params.title   || 'Loan Officer',
      };
      // If nothing meaningful provided:
      if (!lender.company && !lender.repName && !lender.phone) {
        return {
          statusCode: 400,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ error: 'Missing lender parameters.' })
        };
      }
    }

    // Utilities
    const onlyDigits = (p) => String(p || '').replace(/[^0-9]/g, '');
    const e164 = (p) => {
      const d = onlyDigits(p);
      if (d.length === 11 && d.startsWith('1')) return `+${d}`;
      if (d.length === 10) return `+1${d}`;
      return d ? `+${d}` : '';
    };
    function splitName(full) {
      const s = String(full || '').trim();
      if (!s) return { first: '', last: '' };
      const parts = s.split(/\s+/);
      if (parts.length === 1) return { first: parts[0], last: '' };
      const last = parts.pop();
      return { first: parts.join(' '), last };
    }

    const org   = lender.company || '';
    const fn    = lender.repName || org || 'Preferred Lender';
    const { first, last } = splitName(lender.repName);
    const tel   = e164(lender.phone);
    const email = lender.email || '';
    const url   = lender.url || '';
    const title = lender.title || '';

    // vCard 3.0 (widely supported by iOS/Android)
    const lines = [
      'BEGIN:VCARD',
      'VERSION:3.0',
      `N:${last};${first};;;`,
      `FN:${fn}`,
      org ? `ORG:${org}` : null,
      title ? `TITLE:${title}` : null,
      tel ? `TEL;TYPE=CELL,VOICE:${tel}` : null,
      email ? `EMAIL;TYPE=INTERNET:${email}` : null,
      url ? `URL:${url}` : null,
      'END:VCARD'
    ].filter(Boolean);

    const vcf = lines.join('\r\n');

    // filename
    const safe = (s) => String(s || '').replace(/[^a-z0-9._-]+/gi, '_').slice(0, 60) || 'contact';
    const fname = safe(fn + (org ? `-${org}` : '')).replace(/_+/g,'_');

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'text/vcard; charset=utf-8',
        'Content-Disposition': `attachment; filename="${fname}.vcf"`,
        'Cache-Control': 'no-store'
      },
      body: vcf
    };
  } catch (e) {
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: e.message || 'server error' })
    };
  }
}
