// netlify/functions/track.js
import { getStore } from "@netlify/blobs";

function getAnalyticsStore() {
  const { NETLIFY_BLOBS_TOKEN, NETLIFY_SITE_ID } = process.env;

  // Prefer explicit creds if present
  if (NETLIFY_BLOBS_TOKEN && NETLIFY_SITE_ID) {
    return getStore({
      name: "analytics",
      consistency: "strong",
      siteID: NETLIFY_SITE_ID,
      token: NETLIFY_BLOBS_TOKEN,
    });
  }

  // Fallback to auto binding
  return getStore({ name: "analytics", consistency: "strong" });
}

async function appendJsonl(store, key, obj) {
  const line = JSON.stringify(obj) + "\n";

  // If the runtime supports .append, use it; otherwise use set(..., { append:true })
  if (typeof store.append === "function") {
    // Newer SDKs
    await store.append(key, line);
  } else {
    // Back-compat path
    await store.set(key, line, { append: true });
  }
}

export async function handler(event) {
  // Simple diag on GET
  if (event.httpMethod === "GET") {
    const { NETLIFY_BLOBS_TOKEN, NETLIFY_SITE_ID } = process.env;
    const store = getAnalyticsStore();
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ok: true,
        diag: {
          hasSiteId: !!NETLIFY_SITE_ID,
          hasToken: !!NETLIFY_BLOBS_TOKEN,
          hasAppendMethod: typeof store.append === "function",
          note: "POST to write an event; GET is diagnostics only.",
        },
      }),
    };
  }

  if (event.httpMethod !== "POST") {
    return { statusCode: 405, body: "Method Not Allowed" };
  }

  try {
    const store = getAnalyticsStore();

    const { type, listingId, lenderId, ui, extra } = JSON.parse(event.body || "{}");

    const payload = {
      type: type || "unknown",
      listingId: listingId ?? null,
      lenderId: lenderId ?? null,
      ui: ui ?? null,            // e.g. "lender-cta", "vcard-download", "schedule-submit"
      extra: extra ?? null,      // any free-form object
      ts: new Date().toISOString(),
      ip: event.headers["x-nf-client-connection-ip"] || null,
      ua: event.headers["user-agent"] || null,
      ref: event.headers["referer"] || null,
    };

    await appendJsonl(store, "events.jsonl", payload);

    return { statusCode: 200, body: JSON.stringify({ ok: true }) };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        ok: false,
        error: err?.message || String(err),
      }),
    };
  }
}
