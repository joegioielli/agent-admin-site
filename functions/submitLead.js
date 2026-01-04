// netlify/functions/submitLead.js
import { getStore } from '@netlify/blobs';

const {
  SENDGRID_API_KEY,
  SENDGRID_FROM,
  LEAD_NOTIFY_TO,
  NETLIFY_SITE_ID,
  NETLIFY_BLOBS_TOKEN
} = process.env;

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Max-Age": "86400",
  "Content-Type": "application/json"
};

/* ---------- safe logging (no-crash) ---------- */
function tryGetLeadsStore() {
  // 1) Built-in site context (when Blobs is enabled for the site)
  try { return getStore('leads'); } catch {}
  // 2) Manual token fallback (works on all plans if env vars are set)
  try {
    if (NETLIFY_SITE_ID && NETLIFY_BLOBS_TOKEN) {
      return getStore('leads', { siteID: NETLIFY_SITE_ID, token: NETLIFY_BLOBS_TOKEN });
    }
  } catch {}
  // 3) Not available -> return null; caller must no-op
  return null;
}
async function safeLog(record) {
  try {
    const store = tryGetLeadsStore();
    if (!store) return false;
    const day = new Date().toISOString().slice(0,10);
    const id = (globalThis.crypto?.randomUUID?.() || Math.random().toString(36).slice(2));
    const key = `${day}/${id}-${record.kind || 'lead'}.json`;
    await store.setJSON(key, record);
    return true;
  } catch { return false; }
}
/* -------------------------------------------- */

const cleanPhone = (p="") => String(p).replace(/[^0-9]/g,"");
const isEmail   = (e="") => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(e).trim());
const nowIso    = () => new Date().toISOString();

export const handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: cors, body: "" };
    if (event.httpMethod !== "POST") return { statusCode: 405, headers: cors, body: JSON.stringify({ error:"Method not allowed" }) };

    if (!SENDGRID_API_KEY || !SENDGRID_FROM || !LEAD_NOTIFY_TO) {
      return { statusCode: 500, headers: cors, body: JSON.stringify({ error:"Email is not configured" }) };
    }

    let body;
    try { body = JSON.parse(event.body || "{}"); }
    catch { return { statusCode: 400, headers: cors, body: JSON.stringify({ error:"Invalid JSON" }) }; }

    const {
      name = "", email = "", phone = "", message = "",
      propertyId = "", propertyAddress = "", propertyCity = "", propertyState = "", propertyZip = "",
      source = "web", utm = {}, website = "" // honeypot
    } = body;

    // Honeypot: log as spam (best-effort) and exit OK
    if (website && String(website).trim() !== "") {
      await safeLog({ kind:"spam", submittedAt: nowIso(), bodySnapshot: body, meta: metaFromEvent(event) });
      return { statusCode: 200, headers: cors, body: JSON.stringify({ ok:true, spamFiltered:true, logged:true }) };
    }

    const phoneDigits = cleanPhone(phone);
    const emailOk = isEmail(email);
    if (!name && !phoneDigits && !emailOk) {
      return { statusCode: 400, headers: cors, body: JSON.stringify({ error:"Provide at least name plus (email or phone)." }) };
    }

    // Build subject/address
    const subjectParts = [];
    if (propertyAddress) subjectParts.push(propertyAddress);
    if (propertyCity || propertyState) subjectParts.push([propertyCity, propertyState].filter(Boolean).join(", "));
    const subjectProperty = subjectParts.filter(Boolean).join(" • ");
    const emailSubject = `New Lead${subjectProperty ? ` – ${subjectProperty}` : ""}`;
    const humanPhone = phoneDigits ? `(${phoneDigits.slice(0,3)}) ${phoneDigits.slice(3,6)}-${phoneDigits.slice(6)}` : "";

    const html = `
      <div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.5;color:#222">
        <h2 style="margin:0 0 8px">New Lead</h2>
        <p style="margin:0 0 16px;color:#666">${nowIso()}</p>
        <table cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse">
          <tr><td><strong>Name</strong></td><td>${escapeHtml(name || "-")}</td></tr>
          <tr><td><strong>Email</strong></td><td>${escapeHtml(email || "-")}</td></tr>
          <tr><td><strong>Phone</strong></td><td>${escapeHtml(humanPhone || "-")}</td></tr>
          <tr><td><strong>Message</strong></td><td>${nl2br(escapeHtml(message || "-"))}</td></tr>
          <tr><td><strong>Property ID</strong></td><td>${escapeHtml(propertyId || "-")}</td></tr>
          <tr><td><strong>Address</strong></td><td>${escapeHtml(subjectProperty || "-")}</td></tr>
          <tr><td><strong>Zip</strong></td><td>${escapeHtml(propertyZip || "-")}</td></tr>
          <tr><td><strong>Source</strong></td><td>${escapeHtml(source || "-")}</td></tr>
        </table>
        ${utm && Object.keys(utm).length
          ? `<h3 style="margin-top:18px">UTM</h3>
             <pre style="background:#f6f6f6;padding:10px;border-radius:6px">${escapeHtml(JSON.stringify(utm, null, 2))}</pre>`
          : ""
        }
      </div>
    `;

    const recipients = String(LEAD_NOTIFY_TO).split(",").map(s=>s.trim()).filter(Boolean);
    const from = parseFrom(SENDGRID_FROM);
    if (!from?.email) return { statusCode: 500, headers: cors, body: JSON.stringify({ error:"SENDGRID_FROM is invalid" }) };

    const sgPayload = {
      personalizations: [{ to: recipients.map(e=>({ email:e })) }],
      from,
      subject: emailSubject,
      content: [
        { type: "text/plain", value: plainTextFrom({ name, email, humanPhone, message, propertyId, subjectProperty, propertyZip, source, utm }) },
        { type: "text/html", value: html }
      ],
      ...(emailOk ? { reply_to: { email: email.trim() } } : {})
    };

    // Send email
    let emailOkFlag = false, emailStatus = 0, emailErr = "";
    try {
      const res = await fetch("https://api.sendgrid.com/v3/mail/send", {
        method: "POST",
        headers: { Authorization:`Bearer ${SENDGRID_API_KEY}`, "Content-Type":"application/json" },
        body: JSON.stringify(sgPayload)
      });
      emailStatus = res.status;
      emailOkFlag = res.ok;
      if (!res.ok) emailErr = await res.text();
    } catch (e) { emailStatus = -1; emailErr = String(e); }

    // Best-effort log (won't crash if Blobs not configured)
    const logged = await safeLog({
      kind: "lead",
      submittedAt: nowIso(),
      emailAttempt: { ok: emailOkFlag, status: emailStatus, error: emailErr?.slice(0,1000) || "" },
      bodySnapshot: {
        name, email, phone: humanPhone, message,
        propertyId, propertyAddress: subjectProperty, propertyZip, source, utm
      },
      meta: metaFromEvent(event),
      recipients
    });

    if (!emailOkFlag) {
      return { statusCode: 502, headers: cors, body: JSON.stringify({ error:"SendGrid API error", status: emailStatus, body: emailErr, logged }) };
    }

    return { statusCode: 200, headers: cors, body: JSON.stringify({ ok:true, notified: recipients.length, logged }) };
  } catch (e) {
    // Last-ditch: try to record the error, but never crash because of logging
    await safeLog({ kind:"error", at: nowIso(), message: String(e) });
    return { statusCode: 500, headers: cors, body: JSON.stringify({ error:"Unhandled error", message:String(e) }) };
  }
};

/* ---------- helpers ---------- */
function escapeHtml(s){ return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
function nl2br(s){ return String(s).replace(/\n/g,"<br/>"); }
function plainTextFrom({ name, email, humanPhone, message, propertyId, subjectProperty, propertyZip, source, utm }){
  return [
    `New Lead`,``,
    `Name: ${name || "-"}`,
    `Email: ${email || "-"}`,
    `Phone: ${humanPhone || "-"}`,
    `Message: ${message || "-"}`,
    `Property ID: ${propertyId || "-"}`,
    `Address: ${subjectProperty || "-"}`,
    `Zip: ${propertyZip || "-"}`,
    `Source: ${source || "-"}`,
    utm && Object.keys(utm).length ? `UTM: ${JSON.stringify(utm)}` : ``,
    ``, `Sent at: ${nowIso()}`
  ].filter(Boolean).join("\n");
}
function parseFrom(input){
  const s = String(input || "").trim();
  const m = s.match(/^(.*)<([^>]+)>$/);
  if (m) { const name = m[1].trim().replace(/(^"|"$)/g,""); const email = m[2].trim(); return name ? { email, name } : { email }; }
  return { email: s };
}
function metaFromEvent(event){
  const ua  = event.headers?.['user-agent'] || event.headers?.['User-Agent'] || '';
  const ref = event.headers?.referer || event.headers?.Referer || '';
  return { userAgent: ua, referer: ref, path: event.path };
}
