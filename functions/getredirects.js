// netlify/functions/getredirects.js
import { makestore } from "./store.js";

export async function handler(event) {
  // 🔍 FULL REQUEST + QR DEBUG
  const qrCode = event.pathParameters?.code || 
                event.queryStringParameters?.code || 
                event.path.split('/').pop()?.split('?')[0];

  console.log('🔍 QR REQUEST DEBUG:', {
    path: event.path,
    pathParameters: event.pathParameters,
    queryStringParameters: event.queryStringParameters,
    qrCode: qrCode || 'MISSING'
  });

  try {
    const store = makestore();
    const raw = (await store.getJSON()) || [];

    console.log('📦 S3 Result:', {
      rawLength: raw.length,
      firstItem: raw[0] || 'empty',
      bucketUsed: process.env.MY_AWS_BUCKET_NAME || process.env.SMARTSIGNS_BUCKET,
      allNames: raw.map(r => r.name || r.slug || r.id || r.label)
    });

    // 🔥 ADMIN MODE
    if (event.queryStringParameters?.pin === 'admin') {
      console.log('✅ ADMIN LIST MODE');
      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ redirects: raw, count: raw.length })
      };
    }

    // 🔍 FIND SPECIFIC QR CODE MATCH (case-insensitive)
    const match = raw.find(r => {
      const name = (r.name || r.slug || r.id || r.label || '').toLowerCase().trim();
      return name === (qrCode || '').toLowerCase().trim();
    });

    console.log('🔍 QR MATCH:', {
      qrCode: qrCode,
      matchFound: !!match,
      matchName: match?.name || 'NO MATCH'
    });

    if (match && match.url) {
      // 🔥 LOG HIT TO ANALYTICS (non-blocking)
      try {
        const siteUrl = process.env.URL;
        if (siteUrl) {
          const ip = event.headers["x-nf-client-connection-ip"] || 
                     event.headers["client-ip"] || 
                     event.headers["x-forwarded-for"] || null;
          
          console.log('📊 Logging hit:', { qrCode, ipHash: ip ? '...' : null });
          
          await fetch(`${siteUrl}/.netlify/functions/logHit`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              name: qrCode,
              url: match.url,
              userAgent: event.headers["user-agent"] || null,
              referer: event.headers["referer"] || null,
              ip
            }),
          });
        } else {
          console.warn('No process.env.URL - skipping analytics');
        }
      } catch (logErr) {
        console.error("logHit failed (redirect continues):", logErr);
      }

      // ✅ QR FOUND - 302 NO-CACHE REDIRECT!
      return {
        statusCode: 302,
        headers: { 
          Location: match.url,
          "Cache-Control": "no-cache, no-store, must-revalidate",
          "Netlify-CDN-Cache-Control": "no-cache, no-store, must-revalidate",
          "Pragma": "no-cache",
          "Expires": "0"
        },
        body: ''
      };
    } else {
      // ❌ QR NOT FOUND
      console.log('❌ QR Not Found:', qrCode);
      return {
        statusCode: 404,
        headers: { "Content-Type": "text/html" },
        body: `
          <!DOCTYPE html>
          <html>
            <head><title>QR Not Found</title></head>
            <body>
              <h1>QR Code Not Found</h1>
              <p>No redirect found for: <code>${qrCode || 'unknown'}</code></p>
              <p><a href="/">Back to Home</a></p>
            </body>
          </html>
        `
      };
    }
  } catch (err) {
    console.error("getredirect error:", err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "QR redirect service error"
    };
  }
}
