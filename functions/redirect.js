// netlify/functions/redirect.js
// NO-CACHE REDIRECTS - PHONE + NETLIFY FIXED!

export const handler = async (event) => {
  try {
    // Extract name: /r/<name> or /qr/<name>
    let name = event.path.split("/").pop();

    const S3_URL = process.env.S3_REDIRECTS_URL;
    if (!S3_URL) {
      return {
        statusCode: 500,
        body: "Missing S3_REDIRECTS_URL",
      };
    }

    const res = await fetch(S3_URL);
    if (!res.ok) {
      return {
        statusCode: 500,
        body: `S3 fetch failed: ${res.status}`,
      };
    }

    const redirects = await res.json();
    const match = redirects.find((r) => r.name === name);
    
    if (!match?.url) {
      return {
        statusCode: 404,
        body: `No redirect: ${name}`,
      };
    }

    const location = normalizeRedirectUrl(match.url, event);

    // LOG (non-blocking)
    try {
      const siteUrl = process.env.URL;
      if (siteUrl) {
        const ip = event.headers["x-nf-client-connection-ip"] || 
                   event.headers["client-ip"] || 
                   event.headers["x-forwarded-for"] || null;
        
        await fetch(`${siteUrl}/.netlify/functions/logHit`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            name, url: match.url,
            userAgent: event.headers["user-agent"] || null,
            referer: event.headers["referer"] || null,
            ip
          }),
        });
      }
    } catch (logErr) {
      console.error("logHit failed:", logErr);
    }

    // 🔥 PHONE + NETLIFY ZERO CACHE!
    return {
      statusCode: 302,
      headers: {
        Location: location,
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Netlify-CDN-Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      },
    };
  } catch (err) {
    console.error("redirect.js:", err);
    return {
      statusCode: 500,
      body: "Error: " + err.message,
    };
  }
};

function normalizeRedirectUrl(rawUrl, event) {
  const value = String(rawUrl || "").trim();
  if (!value) return value;

  if (/^https?:\/\//i.test(value)) return value;

  const proto = event.headers["x-forwarded-proto"] || "https";
  const host =
    event.headers.host ||
    event.headers.Host ||
    process.env.URL?.replace(/^https?:\/\//, "");

  if (!host) return value;
  if (value.startsWith("//")) return `${proto}:${value}`;
  if (value.startsWith("/")) return `${proto}://${host}${value}`;
  if (/^[a-z0-9.-]+\.[a-z]{2,}(?:[/:?#]|$)/i.test(value)) return `https://${value}`;
  return `${proto}://${host}/${value.replace(/^\/+/, "")}`;
}
