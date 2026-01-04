// netlify/functions/updateListing.js
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  DeleteObjectCommand, 
  ListObjectsV2Command
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { domInZone } from "./domInZone.js";



/* -------------------- ENV / S3 CLIENT -------------------- */
const REGION =
  process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";

const BUCKET =
  process.env.SMARTSIGNS_BUCKET ||
  process.env.S3_BUCKET || // legacy fallback
  "gioi-real-estate-bucket";

const DEFAULT_LISTING_TZ = process.env.DEFAULT_LISTING_TZ || "America/Chicago";
const SIGN_EXPIRES = 900; // 15 min presign

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId:
      process.env.MY_AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey:
      process.env.MY_AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY,
  },
});

/* -------------------- HELPERS -------------------- */
const streamToString = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
  });

async function headExists(Key) {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key }));
    return true;
  } catch {
    return false;
  }
}
async function getJson(Key) {
  if (!(await headExists(Key))) return {};
  const o = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key }));
  return JSON.parse(await streamToString(o.Body));
}
async function putJson(Key, obj) {
  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key,
      Body: JSON.stringify(obj, null, 2),
      ContentType: "application/json",
    })
  );
}
async function deleteJson(Key) {  // ← NEW DELETE HELPER
  console.log("🗑️ DELETING S3:", Key);
  await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key }));
  console.log("✅ DELETED S3:", Key);
}

function normalizeTimezoneMaybe(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return s || null;
}

/* ---- date normalization ---- */
function parseActiveYMD(s) {
  if (!s) return null;
  const str = String(s).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const [y, m, d] = str.split("-").map(Number);
    return { y, m, d };
  }
  if (/^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$/.test(str)) {
    const [mm, dd, yy] = str.split(/[/-]/);
    const m = Number(mm);
    const d = Number(dd);
    let y = Number(yy);
    if (yy.length === 2) y = y >= 70 ? 1900 + y : 2000 + y;
    return { y, m, d };
  }
  const ad = new Date(str);
  if (isNaN(ad)) return null;
  return { y: ad.getFullYear(), m: ad.getMonth() + 1, d: ad.getDate() };
}
function normalizeActiveDateISO(s) {
  const ymd = parseActiveYMD(s);
  if (!ymd) return null;
  const y = ymd.y;
  const m = String(ymd.m).padStart(2, "0");
  const d = String(ymd.d).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function pickActiveDateAlias(obj) {
  if (!obj || typeof obj !== "object") return null;
  const keys = [
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
  ];
  for (const k of keys) {
    const v = obj[k];
    if (v !== undefined && v !== null && String(v).trim?.() !== "") {
      return { key: k, value: v };
    }
  }
  return null;
}

function applyActiveDateNormalization(next, incomingCandidate) {
  const found = pickActiveDateAlias(incomingCandidate);
  if (!found) return { iso: null };

  const iso = normalizeActiveDateISO(found.value);
  if (!iso) return { iso: null };

  next.activeDate = iso;

  const aliases = [
    "ActiveDate",
    "listActiveDate",
    "ListActiveDate",
    "dateActive",
    "DateActive",
    "listingDate",
    "ListingDate",
    "DateListed",
    "date_listed",
  ];
  for (const a of aliases) delete next[a];

  if ("daysOnMarket" in next) delete next.daysOnMarket;

  return { iso };
}

/* -------------------- BODY SHAPE NORMALIZATION (back-compat) -------------------- */
function extractIdentifier(body) {
  return String(body.listingId || body.slug || body.id || "").trim();
}
function extractUpdates(body) {
  const u = { ...(body.overrides || {}), ...(body.updates || {}) };

  if (body.activeDate != null && u.activeDate == null) u.activeDate = body.activeDate;
  if (body.timezone != null && u.timezone == null) u.timezone = body.timezone;

  if ("daysOnMarket" in u) delete u.daysOnMarket;
  if ("daysOnMarket" in body) delete body.daysOnMarket;

  return u;
}

/* -------------------- CORS -------------------- */
function cors() {
  return {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, x-admin-key",
  };
}
const ok = (body) => ({ statusCode: 200, headers: cors(), body: JSON.stringify(body) });
const bad = (code, body) => ({ statusCode: code, headers: cors(), body: JSON.stringify(body) });

/* -------------------- DETAILS PATCH HELPERS -------------------- */
function orNullString(v) {
  if (v === null) return null;
  const s = String(v ?? "").trim();
  return s === "" ? null : s;
}
function numberOrPass(v) {
  if (v === null) return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  const n = Number(String(v ?? "").replace(/[^0-9.]/g, ""));
  return Number.isFinite(n) ? n : orNullString(v);
}

/** Build a conservative patch for details.json from overrides we just saved */
function deriveCoreDetailsPatch(fromOverrides = {}) {
  if (!fromOverrides || typeof fromOverrides !== "object") return {};
  const o = fromOverrides;

  const activeIso =
    typeof o.activeDate === "string" ? normalizeActiveDateISO(o.activeDate) : null;

  const patch = {};

  // Address
  if ("address" in o) patch.address = orNullString(o.address);
  if ("city" in o)    patch.city    = orNullString(o.city);
  if ("state" in o)   patch.state   = orNullString(o.state);
  if ("zip" in o)     patch.zip     = orNullString(o.zip);

  // Price
  if ("price" in o)     patch.price     = numberOrPass(o.price);
  if ("listPrice" in o) patch.listPrice = numberOrPass(o.listPrice);

  // Beds / baths / sqft / year
  if ("beds" in o)       patch.beds       = numberOrPass(o.beds);
  if ("bedrooms" in o)   patch.bedrooms   = numberOrPass(o.bedrooms);
  if ("baths" in o)      patch.baths      = numberOrPass(o.baths);
  if ("totalBaths" in o) patch.totalBaths = numberOrPass(o.totalBaths);
  if ("sqft" in o)       patch.sqft       = numberOrPass(o.sqft);
  if ("squareFeet" in o) patch.squareFeet = numberOrPass(o.squareFeet);
  if ("yearBuilt" in o)  patch.yearBuilt  = numberOrPass(o.yearBuilt);

  // Status
  if ("status" in o) patch.status = orNullString(o.status);

  // Dates / TZ
  if ("activeDate" in o) patch.activeDate = activeIso ?? orNullString(o.activeDate);
  if ("timezone" in o)   patch.timezone   = orNullString(o.timezone);

  // Remarks / notes
  if ("publicRemarks" in o) patch.publicRemarks = orNullString(o.publicRemarks);
  if ("remarks" in o)       patch.remarks       = orNullString(o.remarks);
  if ("agentNotes" in o)    patch.agentNotes    = orNullString(o.agentNotes);

  // Photo
  if ("primaryPhoto" in o) patch.primaryPhoto = orNullString(o.primaryPhoto);
  if ("photo" in o)        patch.photo        = orNullString(o.photo);

  // Lender
  if ("preferredLenderId" in o)    patch.preferredLenderId    = orNullString(o.preferredLenderId);
  if ("preferredLender" in o)      patch.preferredLender      = orNullString(o.preferredLender);
  if ("preferredLenderOffer" in o) patch.preferredLenderOffer = orNullString(o.preferredLenderOffer);

  return patch;
}

function applyPatchMutable(target, patch) {
  if (!patch || typeof patch !== "object") return target;
  for (const [k, v] of Object.entries(patch)) {
    if (v === null || (typeof v === "string" && v.trim() === "")) {
      delete target[k];
    } else {
      target[k] = v;
    }
  }
  return target;
}

/* ---- Bath normalization helpers ---- */
function numOrNull(v) {
  if (v === null || v === undefined) return null;
  const n = Number(String(v).replace(/[^0-9.]/g, ""));
  return Number.isFinite(n) ? n : null;
}

/**
 * Normalize bath fields without ever overwriting per-level full bath counts
 * from totalBaths/baths.
 */
function normalizeBaths(details) {
  if (!details || typeof details !== "object") return details;

  const fullMain   = numOrNull(details.FullBathsMain);
  const fullSecond = numOrNull(details.FullBathsSecond);
  const fullThird  = numOrNull(details.FullBathsThird);
  const halfMain   = numOrNull(details.HalfBathsMain);
  const halfSecond = numOrNull(details.HalfBathsSecond);
  const halfThird  = numOrNull(details.HalfBathsThird);

  const fullParts = [fullMain, fullSecond, fullThird].filter((n) => n != null);
  const halfParts = [halfMain, halfSecond, halfThird].filter((n) => n != null);

  if (fullParts.length) {
    const totalFull = fullParts.reduce((a, b) => a + b, 0);
    details.TotalFullBaths = totalFull;
  }

  if (fullParts.length || halfParts.length) {
    const fullCount = fullParts.reduce((a, b) => a + b, 0);
    const halfCount = halfParts.reduce((a, b) => a + b, 0);
    const total = fullCount + halfCount;
    details.totalBaths = total;
  }

  return details;
}

/* -------------------- HANDLER -------------------- */
export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 200, headers: cors(), body: "" };
    }
    if (event.httpMethod !== "POST") {
      return bad(405, { ok: false, error: "Method Not Allowed" });
    }

    await s3.send(new HeadBucketCommand({ Bucket: BUCKET }));

    let body = {};
    try {
      body = JSON.parse(event.body || "{}");
    } catch {
      return bad(400, { ok: false, error: "Invalid JSON body" });
    }

   // 🗑️ DELETE HANDLING - FULL LISTING + PHOTOS
if (body.delete === true) {
  const id = extractIdentifier(body);
  if (!id) return bad(400, { ok: false, error: "slug/id required for delete" });
  
  const overridesKey = `listings/${id}/overrides.json`;
  const detailsKey = `listings/${id}/details.json`;
  const photosPrefix = `photos/${id}/`;
  
  console.log("🗑️ DELETING FULL LISTING:", id);
  
  // 1. Delete listing files
  await deleteJson(overridesKey);
  if (await headExists(detailsKey)) {
    await deleteJson(detailsKey);
  }
  
  // 2. Delete ALL photos for this MLS
  let photosContinuationToken;
  let photosDeleted = 0;
  do {
    const listPhotos = await s3.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: photosPrefix,
      ContinuationToken: photosContinuationToken
    }));
    
    for (const obj of listPhotos.Contents || []) {
      console.log("🗑️ Deleting photo:", obj.Key);
      await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: obj.Key }));
      photosDeleted++;
    }
    
    photosContinuationToken = listPhotos.IsTruncated ? listPhotos.NextContinuationToken : undefined;
  } while (photosContinuationToken);
  
  console.log(`✅ DELETED listing + ${photosDeleted} photos for MLS: ${id}`);
  
  return ok({
    ok: true,
    deleted: id,
    photosDeleted,
    message: "Listing and photos deleted"
  });
}


    // ← EXISTING UPDATE CODE CONTINUES HERE (unchanged)
    const id = extractIdentifier(body);
    if (!id) return bad(400, { ok: false, error: "listingId/slug/id is required" });

    const overridesKey = `listings/${id}/overrides.json`;
    const detailsKey   = `listings/${id}/details.json`;

    console.log("[updateListing] id:", id, "overridesKey:", overridesKey, "region:", REGION, "bucket:", BUCKET);

    const currentOverrides = (await getJson(overridesKey)) || {};

    const { overrides, replace, detailsPatch: explicitDetailsPatch } = body;
    let updates = extractUpdates(body);
    console.log("[updateListing] updates from body:", updates);
    let nextOverrides;
    let normalizedISO = null;

    if (overrides && typeof overrides === "object" && replace) {
      nextOverrides = { ...overrides };
      ({ iso: normalizedISO } = applyActiveDateNormalization(nextOverrides, overrides));
      if ("timezone" in overrides) {
        const tz = normalizeTimezoneMaybe(overrides.timezone);
        if (tz) nextOverrides.timezone = tz;
        else delete nextOverrides.timezone;
      }
    } else {
      if (!updates || Object.keys(updates).length === 0) {
        const tz = currentOverrides.timezone || DEFAULT_LISTING_TZ;
        const dom = currentOverrides.activeDate ? domInZone(currentOverrides.activeDate, tz) : null;

        const url = await getSignedUrl(
          s3,
          new GetObjectCommand({ Bucket: BUCKET, Key: overridesKey }),
          { expiresIn: SIGN_EXPIRES }
        ).catch(() => null);
        const detailsUrl0 = await getSignedUrl(
          s3,
          new GetObjectCommand({ Bucket: BUCKET, Key: detailsKey }),
          { expiresIn: SIGN_EXPIRES }
        ).catch(() => null);

        return ok({
          ok: true,
          key: overridesKey,
          overrides: currentOverrides,
          overridesUrl: url,
          detailsKey,
          detailsUrl: detailsUrl0,
          daysOnMarket: dom,
          timezone: tz,
          noChange: true,
        });
      }

      nextOverrides = { ...currentOverrides };
      for (const [k, v] of Object.entries(updates)) {
        if (v === null || (typeof v === "string" && v.trim() === "")) {
          delete nextOverrides[k];
        } else {
          nextOverrides[k] = v;
        }
      }
      ({ iso: normalizedISO } = applyActiveDateNormalization(nextOverrides, updates));
      if ("timezone" in updates) {
        const tz = normalizeTimezoneMaybe(updates.timezone);
        if (tz) nextOverrides.timezone = tz;
        else delete nextOverrides.timezone;
      }
    }

    if (!normalizedISO && typeof nextOverrides.activeDate === "string" && nextOverrides.activeDate) {
      const iso = normalizeActiveDateISO(nextOverrides.activeDate);
      if (iso) nextOverrides.activeDate = iso;
      if ("daysOnMarket" in nextOverrides) delete nextOverrides.daysOnMarket;
    }

    if (!nextOverrides.timezone) {
      nextOverrides.timezone = currentOverrides.timezone || DEFAULT_LISTING_TZ;
    }

    nextOverrides.updatedAt = new Date().toISOString();
    nextOverrides.updatedBy = "admin-dashboard";

    await putJson(overridesKey, nextOverrides);
    const savedOverrides = await getJson(overridesKey);

    const detailsCurr = (await getJson(detailsKey)) || {};
    const autoPatch = deriveCoreDetailsPatch(savedOverrides);
    let detailsPatch =
      explicitDetailsPatch && typeof explicitDetailsPatch === "object"
        ? explicitDetailsPatch
        : autoPatch;

    if (savedOverrides.activeDate) {
      if (!detailsPatch || typeof detailsPatch !== "object") detailsPatch = {};
      detailsPatch.activeDate = savedOverrides.activeDate;
    }
    if (savedOverrides.timezone) {
      if (!detailsPatch || typeof detailsPatch !== "object") detailsPatch = {};
      detailsPatch.timezone = savedOverrides.timezone;
    }

    for (const [k, v] of Object.entries(savedOverrides || {})) {
      if (!detailsPatch || typeof detailsPatch !== "object") detailsPatch = {};
      if (k === "updatedAt" || k === "updatedBy") continue;
      detailsPatch[k] = v;
    }

    let detailsNext = null;

    if (detailsPatch && Object.keys(detailsPatch).length > 0) {
      if (typeof detailsPatch.activeDate === "string") {
        const iso = normalizeActiveDateISO(detailsPatch.activeDate);
        if (iso) detailsPatch.activeDate = iso;
      }
      detailsNext = { ...detailsCurr };
      applyPatchMutable(detailsNext, detailsPatch);

      normalizeBaths(detailsNext);

      detailsNext.updatedAt = new Date().toISOString();
      detailsNext._lastEditedBy = "admin-dashboard";

      await putJson(detailsKey, detailsNext);
    }

    console.log("[updateListing] BATH DEBUG", {
      FullBathsMain: detailsNext?.FullBathsMain,
      FullBathsSecond: detailsNext?.FullBathsSecond,
      TotalFullBaths: detailsNext?.TotalFullBaths,
      totalBaths: detailsNext?.totalBaths,
      baths: detailsNext?.baths,
    });

    const overridesUrl = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: BUCKET, Key: overridesKey }),
      { expiresIn: SIGN_EXPIRES }
    ).catch(() => null);
    const detailsUrl = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: BUCKET, Key: detailsKey }),
      { expiresIn: SIGN_EXPIRES }
    ).catch(() => null);

    const tzForDom = savedOverrides.timezone || DEFAULT_LISTING_TZ;
    const dom = savedOverrides.activeDate ? domInZone(savedOverrides.activeDate, tzForDom) : null;

    return ok({
      ok: true,
      key: overridesKey,
      overrides: savedOverrides,
      overridesUrl,
      detailsKey,
      detailsUrl,
      detailsPatched: !!detailsPatch && Object.keys(detailsPatch).length > 0,
      daysOnMarket: dom,
      timezone: tzForDom,
    });
  } catch (e) {
    console.error("[updateListing] ERROR:", e);
    return bad(502, {
      ok: false,
      error: "updateListing failed",
      diag: {
        region: REGION,
        bucket: BUCKET,
        message: e.message,
        name: e.name,
      },
    });
  }
}
