// netlify/functions/chat.js — ESM + global/per-listing lenders + vCard links + rich quick answers + lender extras + correct Days on Market (America/Chicago)
// Uses getListingDetails first (source of truth), then falls back to listListings.
// Writes compact JSONL to logs-jsonl/ for Athena + pretty JSON to logs/ for humans.
//
// NEW: Supports "text mode" for SMS so users see ONLY the reply string (not JSON).
//   - Add ?format=text  OR  header Accept: text/plain
// Web chat (chat.html) can keep using JSON (default).

import OpenAI from "openai";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

// IMPORTANT: instantiate OpenAI INSIDE handler so missing env doesn't crash module init
function getOpenAIClient() {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return null;
  return new OpenAI({ apiKey: key });
}

/* ------------------------- Response formatting (JSON vs plain text) ------------------------- */
function wantsPlainText(event) {
  const qs = event?.queryStringParameters || {};
  if (String(qs.format || "").toLowerCase() === "text") return true;

  const h = event?.headers || {};
  const accept = String(h.accept || h.Accept || "").toLowerCase();
  return accept.includes("text/plain");
}

function corsHeaders(extra = {}) {
  return {
    "Access-Control-Allow-Origin": "*",
    ...extra,
  };
}

function respond(event, payload, statusCode = 200) {
  // payload can be:
  //   - string (reply only)
  //   - object like { reply, listingId, extras }
  const plain = wantsPlainText(event);

  // If payload is string: always plain
  if (typeof payload === "string") {
    return {
      statusCode,
      headers: corsHeaders({ "Content-Type": "text/plain; charset=utf-8" }),
      body: payload,
    };
  }

  // If payload is object:
  if (plain) {
    const reply = payload?.reply ?? "";
    return {
      statusCode,
      headers: corsHeaders({ "Content-Type": "text/plain; charset=utf-8" }),
      body: String(reply),
    };
  }

  return {
    statusCode,
    headers: corsHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(payload ?? {}),
  };
}

/* ------------------------- S3 logger ------------------------- */
const s3 =
  process.env.MY_AWS_REGION && process.env.SMARTSIGNS_BUCKET
    ? new S3Client({
        region: process.env.MY_AWS_REGION,
        credentials:
          process.env.MY_AWS_ACCESS_KEY_ID && process.env.MY_AWS_SECRET_ACCESS_KEY
            ? {
                accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID,
                secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY,
              }
            : undefined,
      })
    : null;

async function logChatEvent({ event, propertyId, message, reply, intent, meta = {} }) {
  if (!s3) return;

  const bucket = process.env.SMARTSIGNS_BUCKET;
  const ts = new Date();
  const day = ts.toISOString().slice(0, 10);
  const shortTs = ts.toISOString().replace(/[:.]/g, "-");
  const pid = String(propertyId || "unknown").trim() || "unknown";
  const headers = event?.headers || {};
  const ip =
    headers["x-nf-client-connection-ip"] ||
    headers["x-forwarded-for"] ||
    headers["client-ip"] ||
    "";
  const ua = headers["user-agent"] || "";

  const record = {
    type: "chat",
    timestamp: ts.toISOString(),
    propertyId: pid,
    question: message,
    reply,
    intent,
    ip,
    ua,
    meta,
  };

  const prettyKey = `logs/${day}/${shortTs}_${pid}.json`;
  const jsonlKey = `logs-jsonl/${day}/${shortTs}_${pid}.jsonl`;

  try {
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: jsonlKey,
        Body: JSON.stringify(record) + "\n",
        ContentType: "application/json; charset=utf-8",
      })
    );
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: prettyKey,
        Body: JSON.stringify(record, null, 2) + "\n",
        ContentType: "application/json; charset=utf-8",
      })
    );
  } catch {
    // never break chat on logging errors
  }
}

/* ------------------------- utilities ------------------------- */
function parseBody(event) {
  let raw = event.body || "";
  if (event.isBase64Encoded) {
    try {
      raw = Buffer.from(raw, "base64").toString("utf8");
    } catch {}
  }

  // JSON first
  try {
    return JSON.parse(raw);
  } catch {}

  // form / query style
  try {
    const params = new URLSearchParams(raw);
    const obj = {};
    for (const [k, v] of params.entries()) obj[k] = v;
    if (Object.keys(obj).length) return obj;
  } catch {}

  throw new Error("Invalid request body (expected JSON).");
}

function pick(obj, keys, fallback = undefined) {
  for (const k of keys) {
    const v = obj?.[k];
    if (v === 0 || v === false) return v;
    if (v !== undefined && v !== null && String(v).trim?.() !== "") return v;
  }
  return fallback;
}

function fmtUSD(x) {
  if (typeof x === "number")
    return x.toLocaleString("en-US", { style: "currency", currency: "USD" });
  const n = Number(String(x ?? "").replace(/[^\d.]/g, ""));
  return Number.isFinite(n)
    ? n.toLocaleString("en-US", { style: "currency", currency: "USD" })
    : String(x ?? "");
}

const toNum = (x) => {
  if (typeof x === "number") return x;
  const n = Number(String(x ?? "").replace(/[^\d.]/g, ""));
  return Number.isFinite(n) ? n : undefined;
};

const fmtNum = (x) => toNum(x) ?? x;

const onlyDigits = (p) => String(p || "").replace(/[^0-9]/g, "");
const fmtPhoneInline = (p) => {
  const d = onlyDigits(p);
  if (d.length === 11 && d.startsWith("1")) return `+${d}`;
  if (d.length === 10) return `+1${d}`;
  return d || "";
};

/* ------------------ Days on Market helpers (Chicago) ------------------ */
const CHI_FMT = new Intl.DateTimeFormat("en-CA", {
  timeZone: "America/Chicago",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

function zero2(n) {
  return String(n).padStart(2, "0");
}

function chicagoYMDFromDate(d) {
  const parts = CHI_FMT.formatToParts(d);
  const y = parts.find((p) => p.type === "year").value;
  const m = parts.find((p) => p.type === "month").value;
  const dd = parts.find((p) => p.type === "day").value;
  return `${y}-${m}-${dd}`;
}

function parseActiveDateToChicagoYMD(activeDateStr) {
  if (!activeDateStr) return null;
  const s = String(activeDateStr).trim();

  // YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  // MM/DD/YYYY or MM-DD-YY/YY
  if (/^\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}$/.test(s)) {
    const parts = s.split(/[\/-]/);
    let m = Number(parts[0]);
    let d = Number(parts[1]);
    let yRaw = parts[2];
    let y;
    if (yRaw.length === 2) {
      const two = Number(yRaw);
      y = two >= 70 ? 1900 + two : 2000 + two;
    } else {
      y = Number(yRaw);
    }
    if (!y || !m || !d) return null;
    return `${String(y)}-${zero2(m)}-${zero2(d)}`;
  }

  // fallback Date parse
  const asDate = new Date(s);
  if (isNaN(asDate)) return null;
  return chicagoYMDFromDate(asDate);
}

function todayChicagoYMD() {
  return chicagoYMDFromDate(new Date());
}

// Active day counts as 0.
function daysOnMarketFromActiveDate(activeDateStr) {
  const startYMD = parseActiveDateToChicagoYMD(activeDateStr);
  if (!startYMD) return null;
  const todayYMD = todayChicagoYMD();
  const start = new Date(`${startYMD}T00:00:00Z`).getTime();
  const today = new Date(`${todayYMD}T00:00:00Z`).getTime();
  const diffDays = Math.floor((today - start) / 86400000);
  return diffDays < 0 ? 0 : diffDays;
}

/* ------------------ Listing loader (getListingDetails first) ------------------ */
function baseFromEvent(event) {
  const headers = event?.headers || {};
  const proto = headers["x-forwarded-proto"] || "https";
  const host = headers.host;
  return `${proto}://${host}`;
}

async function loadListingViaGetListingDetails(event, propertyId) {
  const base = baseFromEvent(event);
  const id = String(propertyId || "").trim();
  if (!id) return { listing: {}, foundCard: null, fullText: "" };

  const url = `${base}/.netlify/functions/getListingDetails?listingId=${encodeURIComponent(id)}`;

  const res = await fetch(url, { headers: { "cache-control": "no-store" } });
  if (!res.ok) return { listing: {}, foundCard: null, fullText: "" };

  const json = await res.json().catch(() => ({}));
  const details = json?.details || json?.listing || json?.item || {};
  return { listing: details || {}, foundCard: null, fullText: "" };
}

/* ------------------ Listing loader fallback via listListings ------------------ */
async function loadListingViaListListings(event, propertyId) {
  const base = baseFromEvent(event);

  const resp = await fetch(`${base}/.netlify/functions/listListings`, {
    headers: { "cache-control": "no-store" },
  });
  if (!resp.ok) throw new Error(`listListings HTTP ${resp.status}`);

  const raw = await resp.json();
  const items = Array.isArray(raw) ? raw : raw.items || [];

  const id = String(propertyId).trim();

  const found = items.find(
    (it) =>
      String(it.listingId || "").trim() === id ||
      String(it.mls || "").trim() === id ||
      String(it.id || "").trim() === id ||
      String(it.slug || "").trim() === id
  );

  if (!found) return { foundCard: null, listing: {}, fullText: "" };

  const out = { foundCard: found, listing: {}, fullText: "" };

  // Try detailsUrl if present
  if (found.detailsUrl) {
    try {
      const dRes = await fetch(found.detailsUrl, { headers: { "cache-control": "no-store" } });
      if (dRes.ok) {
        const detailsResp = await dRes.json();
        out.listing = detailsResp.details || detailsResp || {};
      }
    } catch {}
  }

  const textUrl = found.rawUrl || found.fullTextUrl;
  if (textUrl) {
    try {
      const tRes = await fetch(textUrl, { headers: { "cache-control": "no-store" } });
      if (tRes.ok) out.fullText = await tRes.text();
    } catch {}
  }

  return out;
}

async function loadListingAny(event, propertyId) {
  // 1) Prefer getListingDetails (your verified source of truth)
  try {
    const a = await loadListingViaGetListingDetails(event, propertyId);
    if (a?.listing && Object.keys(a.listing).length) return a;
  } catch {}

  // 2) Fallback to listListings
  try {
    return await loadListingViaListListings(event, propertyId);
  } catch {
    return { listing: {}, foundCard: null, fullText: "" };
  }
}

/* ------------------------- global lenders ------------------------- */
async function loadGlobalLenders(event) {
  try {
    const base = baseFromEvent(event);
    const res = await fetch(`${base}/.netlify/functions/lenders`, {
      headers: { "cache-control": "no-store" },
    });
    if (!res.ok) return [];
    const doc = await res.json();
    const list = Array.isArray(doc.lenders) ? doc.lenders : [];
    return list.filter((l) => l.active !== false).slice(0, 3);
  } catch {
    return [];
  }
}

/* -------------- lender intent + formatting helpers -------------- */
function isLenderIntent(text = "") {
  const s = String(text || "").toLowerCase();
  return /\b(preferred\s+lenders?|lenders?|lender|mortgage|loan|pre[-\s]?approval|preapproval|rate|rates|prequal|pre[-\s]?qual)\b/.test(
    s
  );
}
function isCardIntent(text = "") {
  const s = String(text || "").toLowerCase();
  return /\b(vcard|contact\s*card|save\s+to\s+(?:my|your)\s+phone|download\s+card|add\s+contact)\b/.test(
    s
  );
}

function formatLenderLine(l) {
  const phone = l.phone ? fmtPhoneInline(l.phone) : "";
  const bits = [];
  if (l.company) bits.push(l.company);
  if (l.repName) bits.push(`— ${l.repName}`);
  if (phone) bits.push(`· ${phone}`);
  if (l.url) bits.push(`· ${l.url}`);
  return bits.join(" ");
}

/* ---------- build structured lender extras for UI CTA ---------- */
function normalizeLenderId(v) {
  const s = String(v ?? "").trim();
  return s || "";
}

function buildVcardUrl(base, lender) {
  const id = normalizeLenderId(lender.lenderId || lender.id);
  if (id) {
    return `${base}/.netlify/functions/vcard?lenderId=${encodeURIComponent(id)}`;
  }
  const q = new URLSearchParams({
    company: lender.company || "",
    repName: lender.repName || "",
    phone: lender.phone || "",
    email: lender.email || "",
    url: lender.url || "",
    title: lender.title || "Loan Officer",
  }).toString();
  return `${base}/.netlify/functions/vcard?${q}`;
}

function normalizeLenderRecord(raw) {
  const id = normalizeLenderId(raw?.lenderId || raw?.id);
  const company = raw?.company || raw?.name || raw?.org || "";
  const repName = raw?.repName || raw?.contact || raw?.person || raw?.name2 || "";
  const phone = raw?.phone || raw?.tel || raw?.mobile || "";
  const email = raw?.email || "";
  const url = raw?.url || raw?.website || "";
  const title = raw?.title || "Loan Officer";
  return { id, lenderId: id, company, repName, phone, email, url, title };
}

function resolvePreferredFromListing(listing) {
  const simpleId = pick(listing, ["preferredLenderId", "PreferredLenderId", "preferred_lender_id"]);
  if (simpleId) return { type: "id", lenderId: String(simpleId).trim() };

  const prefRef = pick(listing, ["preferredLenderRef", "PreferredLenderRef", "preferred_lender_ref"]);
  if (prefRef && typeof prefRef === "object" && (prefRef.lenderId || prefRef.id)) {
    return { type: "ref", ...prefRef, lenderId: prefRef.lenderId || prefRef.id };
  }

  const prefCustom = pick(listing, ["preferredLenderCustom", "PreferredLenderCustom", "preferred_lender_custom"]);
  if (prefCustom && typeof prefCustom === "object") {
    return { type: "custom", ...prefCustom };
  }

  return null;
}

function mergeAndPrepareLenders({ base, globals = [], listing, limit = 3 }) {
  const preferredRef = resolvePreferredFromListing(listing);
  const g = Array.isArray(globals) ? globals.map(normalizeLenderRecord) : [];

  let preferredObj = null;

  if (preferredRef?.type === "id" || preferredRef?.type === "ref") {
    const id = normalizeLenderId(preferredRef.lenderId);
    const match = g.find((x) => x.lenderId && x.lenderId.toLowerCase() === id.toLowerCase());
    if (match) {
      preferredObj = {
        ...match,
        phone: preferredRef.phoneOverride || match.phone,
        url: preferredRef.urlOverride || match.url,
        offer: preferredRef.offer || "",
        note: preferredRef.note || "",
      };
    }
  } else if (preferredRef?.type === "custom") {
    preferredObj = normalizeLenderRecord(preferredRef);
  }

  const list = [];
  if (preferredObj) list.push(preferredObj);
  for (const x of g) {
    if (
      preferredObj &&
      x.lenderId &&
      preferredObj.lenderId &&
      x.lenderId.toLowerCase() === preferredObj.lenderId.toLowerCase()
    ) {
      continue;
    }
    list.push(x);
  }

  const trimmed = list.slice(0, limit);

  const extras = trimmed.map((l, idx) => {
    const name = [l.company, l.repName].filter(Boolean).join(" — ");
    const preferred = idx === 0 && preferredObj != null;
    return {
      id: l.lenderId || l.id || name || `lender_${idx}`,
      name,
      phone: l.phone || "",
      email: l.email || "",
      vcardUrl: buildVcardUrl(base, l),
      preferred,
    };
  });

  const preferredLine = preferredObj ? formatLenderLine(preferredObj) : "";

  return { extras, preferredLine };
}

/* --------------------------- handler -------------------------- */
export async function handler(event) {
  // CORS preflight
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type, Accept",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
      },
      body: "",
    };
  }

  let intent = "unknown";

  try {
    const body = parseBody(event);

    // Accept old + new param names
    const propertyId =
      body.propertyId || body.listingId || body.pid || body.propertyID || body.ListingId;

    const message = body.message || body.text || body.question;

    const address1 = body.address1 || body.a1 || "";
    const address2 = body.address2 || body.a2 || "";

    if (!propertyId || !message) {
      const reply = "Missing propertyId/listingId (pid) or message.";
      await logChatEvent({ event, propertyId, message, reply, intent: "bad_request" });
      return respond(event, { reply }, 400);
    }

    const base = baseFromEvent(event);

    /* ---------- LENDER / VCARD FLOW (works even without listing) ---------- */
    if (isLenderIntent(message) || isCardIntent(message)) {
      const globals = await loadGlobalLenders(event);

      let preferredLine = "";
      let extrasLenders = [];

      try {
        const data = await loadListingAny(event, propertyId);
        const listing = data?.listing || {};
        const merge = mergeAndPrepareLenders({ base, globals, listing, limit: 3 });
        preferredLine = merge.preferredLine;
        extrasLenders = merge.extras;
      } catch {
        const merge = mergeAndPrepareLenders({ base, globals, listing: {}, limit: 3 });
        preferredLine = merge.preferredLine;
        extrasLenders = merge.extras;
      }

      if (isCardIntent(message)) {
        const links = extrasLenders.map((l) => `${l.name || "Lender"}: ${l.vcardUrl}`);

        const reply = links.length
          ? `Here are contact cards you can save:\n${links.map((x) => `• ${x}`).join("\n")}`
          : `I couldn’t build a contact card right now.`;

        intent = "lender_vcard";
        await logChatEvent({
          event,
          propertyId,
          message,
          reply,
          intent,
          meta: { address1, address2, lenders: extrasLenders.length },
        });

        return respond(event, {
          reply,
          listingId: String(propertyId),
          extras: { listingId: String(propertyId), lenders: extrasLenders },
        });
      }

      let lines = [];
      if (preferredLine) {
        lines.push("This property has a preferred lender:");
        lines.push(preferredLine);
      }

      if (globals.length) {
        if (preferredLine) lines.push("\nYou can also talk with any of our preferred lenders:");
        else lines.push("Here are our preferred lenders:");
        const globalLabels = extrasLenders.map((e) => e.name).filter(Boolean);
        globalLabels.forEach((label, idx) => lines.push(`${idx + 1}. ${label}`));
      }

      if (!preferredLine && !globals.length) {
        lines = ["I don’t have lender info yet."];
      } else {
        lines.push("\nWould you like the contact info here, or a contact card you can save to your phone?");
      }

      const reply = lines.join("\n");
      intent = "lender_info";

      await logChatEvent({
        event,
        propertyId,
        message,
        reply,
        intent,
        meta: { address1, address2, lenders: extrasLenders.length },
      });

      return respond(event, {
        reply,
        listingId: String(propertyId),
        extras: { listingId: String(propertyId), lenders: extrasLenders },
      });
    }

    /* ---------- Load listing for all other intents ---------- */
    const data = await loadListingAny(event, propertyId);
    const listing = data?.listing || {};
    const fullText = data?.fullText || "";
    const foundCard = data?.foundCard || {};

    if (!Object.keys(listing).length && !fullText && !foundCard?.price) {
      const reply = "I couldn't find listing data for this property yet.";
      await logChatEvent({ event, propertyId, message, reply, intent: "no_listing_data" });

      return respond(event, { reply, listingId: String(propertyId) });
    }

    /* ------------------ Compute correct Days on Market ------------------ */
    // Prefer explicit activeDate overrides, but fall back to ListDate if that's all we have.
    const activeDate = pick(listing, [
      "activeDate",
      "ActiveDate",
      "listActiveDate",
      "ListActiveDate",
      "dateActive",
      "DateActive",
      "listingDate",
      "ListingDate",
      "DateListed",
      "date_listed",
      "ListDate",
      "listDate",
    ]);
    const computedDom = daysOnMarketFromActiveDate(activeDate);
    if (computedDom !== null) listing.daysOnMarket = computedDom;

    /* -------------------- QUICK ANSWERS -------------------- */
    const q = String(message).toLowerCase();
    const reAny = (arr) => arr.some((rx) => rx.test(q));

    // IMPORTANT: include your CSV keys like TotalBedrooms and SqFtTotal
    const price = pick(listing, ["listPrice", "ListPrice", "price", "Price", "list_price"]);
    const bedsRaw = pick(listing, [
      "beds",
      "Beds",
      "bedrooms",
      "Bedrooms",
      "bedroomsTotal",
      "BedroomsTotal",
      "totalBedrooms",
      "TotalBedrooms",
      "BedsTotal",
      "br",
      "BR",
    ]);
    const bathsTotal = pick(listing, ["totalBaths", "TotalBaths", "baths", "Baths", "BathroomsTotalInteger"]);
    const bathsFull = pick(listing, ["fullBaths", "FullBaths", "TotalFullBaths"]);
    const bathsHalf = pick(listing, ["halfBaths", "HalfBaths", "TotalHalfBaths"]);
    const sqftRaw = pick(listing, [
      "SqFtTotal",
      "SqFtMainFloor",
      "sqft",
      "SqFt",
      "squareFeet",
      "SquareFeet",
      "totalSqft",
      "TotalSqft",
      "livingArea",
      "LivingArea",
      "buildingAreaTotal",
      "BuildingAreaTotal",
    ]);
    const acresRaw = pick(listing, ["Acres", "acres", "lotSizeAcres", "LotSizeAcres", "lotSize", "LotSize"]);
    const domRaw = pick(listing, ["daysOnMarket", "DaysOnMarket", "DOM", "dom"]);
    const yearBuiltRaw = pick(listing, ["YearBuilt", "yearBuilt", "builtYear", "BuiltYear"]);
    const garageSpacesRaw = pick(listing, ["GarageSpaces", "garageSpaces", "ParkingTotal", "parkingTotal"]);

    const isBeds = reAny([/\bbeds?\b/i, /\bbedrooms?\b/i, /\bhow\s+many\s+beds?\b/i]);
    const isBaths = reAny([/\bbaths?\b/i, /\bbathrooms?\b/i, /\bhow\s+many\s+baths?\b/i]);
    const isSqft = reAny([/\bsq\s?ft\b/i, /\bsquare\s?feet\b/i, /\bsquare\s?footage\b/i, /\bliving\s+area\b/i]);
    const isAcres = reAny([/\bacres?\b/i, /\blot\s*size\b/i]);
    const isDom = reAny([/\bdays?\s+on\s+market\b/i, /\bdom\b/i, /\bhow\s+long\b/i, /\blisted\b/i]);
    const isYearBuilt = reAny([/\byear\s*built\b/i, /\bwhen\s+was\s+.*built\b/i]);
    const isGarage = reAny([/\bgarage\b/i, /\bparking\b/i]);

    const totalBathsNum = toNum(bathsTotal);
    const fullNum = toNum(bathsFull);
    const halfNum = toNum(bathsHalf);

    let bathAnswer = null;
    if ((fullNum ?? 0) > 0 || (halfNum ?? 0) > 0) {
      const parts = [];
      if (fullNum != null) parts.push(`${fullNum} full`);
      if (halfNum != null) parts.push(`${halfNum} half`);
      bathAnswer = parts.join(" / ");
    } else if (totalBathsNum != null && totalBathsNum > 0) {
      bathAnswer = `${totalBathsNum} bathroom${totalBathsNum === 1 ? "" : "s"}`;
    }

    let quick = null;

    if (isBeds && bedsRaw !== undefined) {
      const b = toNum(bedsRaw);
      quick = `This property has ${fmtNum(bedsRaw)} bedroom${b === 1 ? "" : "s"}.`;
      intent = "beds";
    } else if (isBaths && bathAnswer) {
      quick = `This property has ${bathAnswer}.`;
      intent = "baths";
    } else if (isSqft && sqftRaw !== undefined) {
      quick = `The home is ${fmtNum(sqftRaw)} sq ft.`;
      intent = "sqft";
    } else if (isAcres && acresRaw !== undefined) {
      quick = `The lot size is ${fmtNum(acresRaw)} acres.`;
      intent = "acres";
    } else if (isDom) {
      const domToReport = computedDom !== null ? computedDom : toNum(domRaw);
      if (domToReport != null) {
        quick = `It’s been on the market for ${domToReport} day${domToReport === 1 ? "" : "s"}.`;
        intent = "dom";
      }
    } else if (isYearBuilt && yearBuiltRaw !== undefined) {
      quick = `It was built in ${fmtNum(yearBuiltRaw)}.`;
      intent = "year_built";
    } else if (isGarage && garageSpacesRaw !== undefined) {
      const g = fmtNum(garageSpacesRaw);
      quick = toNum(g) != null ? `${g}-car garage.` : `Garage/Parking: ${g}.`;
      intent = "garage";
    } else if (
      /\b(price|asking|cost|list(?:ing)?\s+price)\b/i.test(q) ||
      (/\bhow much\b/i.test(q) &&
        !/\b(square|sq|acres?|bed|bath|land|lot|area|garage|hoa|dues|association)\b/i.test(q))
    ) {
      if (price !== undefined) {
        const pNum = toNum(price);
        quick = `The current list price is ${pNum != null ? fmtUSD(pNum) : String(price)}.`;
        intent = "price";
      }
    } else if (/\baddress\b/i.test(q)) {
      const addr = pick(listing, ["address", "Address"]);
      if (addr || address1 || address2) {
        quick = `The address is ${[addr, address1, address2].filter(Boolean).join(" ")}`.trim();
        intent = "address";
      }
    }

    if (quick) {
      const meta = { address1, address2, daysOnMarket: computedDom, source: "quick" };
      const activeYMD = parseActiveDateToChicagoYMD(activeDate);
      meta.todayChicagoYMD = todayChicagoYMD();
      if (activeYMD) meta.activeDateChicagoYMD = activeYMD;

      await logChatEvent({ event, propertyId, message, reply: quick, intent, meta });

      return respond(event, { reply: quick, listingId: String(propertyId) });
    }

    /* -------------------- LLM fallback -------------------- */
    const client = getOpenAIClient();
    if (!client) {
      const reply =
        "AI is not configured yet (missing OPENAI_API_KEY). I can answer basic questions like price/beds/baths/sqft, but I can’t do deeper AI responses right now.";
      await logChatEvent({
        event,
        propertyId,
        message,
        reply,
        intent: "missing_openai_key",
        meta: { address1, address2, daysOnMarket: computedDom },
      });
      return respond(event, { reply, listingId: String(propertyId) });
    }

    const system = [
      "You are a helpful real-estate assistant.",
      "Answer ONLY using the provided listing JSON and MLS/full text.",
      "If the information is not present, say you do not have that info.",
      "Be concise and factual.",
    ].join(" ");

    const domLine =
      computedDom !== null
        ? `Days on Market (computed, America/Chicago, active day = 0): ${computedDom}`
        : "";

    const addrLine = pick(listing, ["address", "Address"]);
    const context = [
      addrLine ? `Address: ${addrLine}` : "",
      domLine,
      `Listing JSON:\n${JSON.stringify(listing)}`,
      fullText ? `MLS Text:\n${fullText}` : "",
    ]
      .filter(Boolean)
      .join("\n\n");

    intent = "llm";
    await logChatEvent({
      event,
      propertyId,
      message,
      reply: "[LLM RESPONSE PENDING]",
      intent,
      meta: { address1, address2, daysOnMarket: computedDom, source: "llm" },
    });

    const completion = await client.chat.completions.create({
      model: "gpt-4o-mini",
      temperature: 0.2,
      messages: [
        { role: "system", content: system },
        { role: "user", content: context },
        { role: "user", content: message },
      ],
    });

    const llmReply =
      completion.choices?.[0]?.message?.content?.trim() ||
      "I’m not sure how to answer that from the listing data.";

    await logChatEvent({
      event,
      propertyId,
      message,
      reply: llmReply,
      intent,
      meta: { address1, address2, daysOnMarket: computedDom, source: "llm" },
    });

    return respond(event, { reply: llmReply, listingId: String(propertyId) });
  } catch (err) {
    const reply = `Server error: ${err.message || "unknown"}`;
    await logChatEvent({
      event,
      propertyId: "unknown",
      message: "parse/handler error",
      reply,
      intent: "server_error",
    });
    // Keep status 200 so frontends don't choke; still return readable reply
    return respond(event, { reply });
  }
}
