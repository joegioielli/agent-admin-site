// netlify/functions/updateListing.js
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { domInZone } from "./domInZone.js";

/* -------------------- ENV / S3 CLIENT -------------------- */
const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";

const BUCKET =
  process.env.SMARTSIGNS_BUCKET ||
  process.env.S3_BUCKET || // legacy fallback
  "gioi-real-estate-bucket";

const DEFAULT_LISTING_TZ = process.env.DEFAULT_LISTING_TZ || "America/Chicago";
const SIGN_EXPIRES = 900; // 15 min presign

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY,
  },
});

/* -------------------- TENANT PATH HELPERS (non-breaking) -------------------- */
/**
 * Optional tenant id for future agent separation.
 * If omitted, everything uses legacy keys (current behavior).
 */
function extractTenantId(body) {
  const t = body && (body.tenantId || body.tenant || body.accountId);
  const s = String(t ?? "").trim();
  return s ? s : null;
}

/**
 * Returns tenant keys when tenantId present; always includes legacy fallback.
 */
function keysForListing(id, tenantId) {
  const legacy = {
    detailsKey: `listings/${id}/details.json`,
    photosPrefix: `photos/${id}/`,
  };

  if (!tenantId) return { tenantId: null, ...legacy, legacy };

  return {
    tenantId,
    detailsKey: `tenants/${tenantId}/listings/${id}/details.json`,
    photosPrefix: `tenants/${tenantId}/photos/${id}/`,
    legacy,
  };
}

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

async function deleteJson(Key) {
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

/* -------------------- BODY SHAPE NORMALIZATION (back-compat) -------------------- */
function extractIdentifier(body) {
  return String(body.listingId || body.slug || body.id || "").trim();
}

/**
 * Convert legacy body shapes into a single "details patch".
 * - body.details (preferred new)
 * - body.detailsPatch (legacy-ish)
 * - body.updates / body.overrides (legacy)
 *
 * Flags:
 * - body.replaceDetails === true -> replace the whole details file with provided object
 * - body.replace === true w/ overrides -> legacy mapping to replaceDetails
 */
function extractDetailsIntent(body) {
  const replaceDetails =
    body.replaceDetails === true ||
    (body.replace === true && body.overrides && typeof body.overrides === "object");

  if (body.details && typeof body.details === "object") {
    return { patch: body.details, replaceDetails };
  }

  if (body.detailsPatch && typeof body.detailsPatch === "object") {
    return { patch: body.detailsPatch, replaceDetails: false };
  }

  const patch = {
    ...(body.overrides && typeof body.overrides === "object" ? body.overrides : {}),
    ...(body.updates && typeof body.updates === "object" ? body.updates : {}),
  };

  if (body.activeDate != null && patch.activeDate == null) patch.activeDate = body.activeDate;
  if (body.timezone != null && patch.timezone == null) patch.timezone = body.timezone;

  return { patch, replaceDetails };
}

/* -------------------- DOM POLICY (HARD BLOCK + AUTO CLEAN) -------------------- */
/**
 * DOM truth = computed from activeDate + timezone.
 * We NEVER persist any DOM-like fields (including CSV DOM).
 * We also remove them from existing files on any save.
 */
const DOM_KEYS_EXACT = new Set([
  "daysOnMarket",
  "computedDaysOnMarket",
  "DaysOnMarket",
  "DOM",
  "dom",
  "CsvDom",
  "CSVDOM",
  "csvDOM",
  "csvDom",
  "csvDaysOnMarket",
  "CsvDaysOnMarket",
  "computedDom",
]);

function shouldStripDomKey(key) {
  if (!key) return false;
  const k = String(key).trim();
  if (!k) return false;

  if (DOM_KEYS_EXACT.has(k)) return true;

  const norm = k.replace(/[^\w]/g, "").toLowerCase();
  if (
    norm === "daysonmarket" ||
    norm === "computeddaysonmarket" ||
    norm === "csvdaysonmarket" ||
    norm === "csvdom" ||
    norm === "dom"
  ) return true;

  if (/csv.*days.*on.*market/i.test(k)) return true;
  if (/computed.*days.*on.*market/i.test(k)) return true;
  if (/days.*on.*market/i.test(k)) return true;

  return false;
}

function stripDomFieldsMutable(obj) {
  if (!obj || typeof obj !== "object") return obj;
  for (const k of Object.keys(obj)) {
    if (shouldStripDomKey(k)) delete obj[k];
  }
  return obj;
}

function sanitizeDetailsPatchMutable(patch) {
  if (!patch || typeof patch !== "object") return patch;

  // normalize activeDate + timezone
  if (typeof patch.activeDate === "string") {
    const iso = normalizeActiveDateISO(patch.activeDate);
    if (iso) patch.activeDate = iso;
  }
  if ("timezone" in patch) {
    const tz = normalizeTimezoneMaybe(patch.timezone);
    if (tz) patch.timezone = tz;
    else delete patch.timezone;
  }

  // strip details.* keys
  for (const k of Object.keys(patch)) {
    if (String(k).startsWith("details.")) delete patch[k];
  }

  // kill the two troublemakers explicitly
  delete patch.bedrooms;
  delete patch.squareFeet;

  // never persist any DOM fields (csv or computed)
  stripDomFieldsMutable(patch);

  // never persist server-ish keys
  const SERVER_ONLY_KEYS = [
    "updatedAt",
    "_lastEditedBy",
    "hasNote",
    "lastModified",
    "ok",
    "id",
    "slug",
    "detailsUrl",
  ];
  for (const k of SERVER_ONLY_KEYS) {
    if (k in patch) delete patch[k];
  }

  return patch;
}

/**
 * IMPORTANT: Deletions are honored.
 * - v === null => delete
 * - v === "" (blank string) => delete
 * This matches your preference: if an edit is deleted, it should NOT be preserved.
 */
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

function normalizeBaths(details) {
  if (!details || typeof details !== "object") return details;

  const fullMain = numOrNull(details.FullBathsMain);
  const fullSecond = numOrNull(details.FullBathsSecond);
  const fullThird = numOrNull(details.FullBathsThird);
  const halfMain = numOrNull(details.HalfBathsMain);
  const halfSecond = numOrNull(details.HalfBathsSecond);
  const halfThird = numOrNull(details.HalfBathsThird);

  const fullParts = [fullMain, fullSecond, fullThird].filter((n) => n != null);
  const halfParts = [halfMain, halfSecond, halfThird].filter((n) => n != null);

  if (fullParts.length) {
    details.TotalFullBaths = fullParts.reduce((a, b) => a + b, 0);
  }

  if (fullParts.length || halfParts.length) {
    const fullCount = fullParts.reduce((a, b) => a + b, 0);
    const halfCount = halfParts.reduce((a, b) => a + b, 0);
    details.totalBaths = fullCount + halfCount;
  }

  return details;
}

/**
 * Keep details.json clean:
 * - Do NOT write updatedAt / _lastEditedBy into details.json
 * - But optionally remove any legacy copies if they exist
 */
function stripServerMetaMutable(obj) {
  if (!obj || typeof obj !== "object") return obj;
  delete obj.updatedAt;
  delete obj._lastEditedBy;
  delete obj.hasNote;
  delete obj.lastModified;
  delete obj.ok;
  delete obj.id;
  delete obj.slug;
  delete obj.detailsUrl;
  return obj;
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

    const tenantId = extractTenantId(body);

    // 🗑️ DELETE HANDLING - FULL LISTING + PHOTOS (NO overrides.json)
    if (body.delete === true) {
      const id = extractIdentifier(body);
      if (!id) return bad(400, { ok: false, error: "slug/id required for delete" });

      const k = keysForListing(id, tenantId);

      const detailsKeyToDelete = (await headExists(k.detailsKey)) ? k.detailsKey : k.legacy.detailsKey;
      const prefixesToDelete = tenantId ? [k.photosPrefix, k.legacy.photosPrefix] : [k.photosPrefix];

      console.log("🗑️ DELETING FULL LISTING:", id, tenantId ? `(tenant: ${tenantId})` : "(legacy)");

      if (await headExists(detailsKeyToDelete)) await deleteJson(detailsKeyToDelete);

      let photosDeleted = 0;

      for (const prefix of prefixesToDelete) {
        let photosContinuationToken;
        do {
          const listPhotos = await s3.send(
            new ListObjectsV2Command({
              Bucket: BUCKET,
              Prefix: prefix,
              ContinuationToken: photosContinuationToken,
            })
          );

          for (const obj of listPhotos.Contents || []) {
            console.log("🗑️ Deleting photo:", obj.Key);
            await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: obj.Key }));
            photosDeleted++;
          }

          photosContinuationToken = listPhotos.IsTruncated ? listPhotos.NextContinuationToken : undefined;
        } while (photosContinuationToken);
      }

      console.log(`✅ DELETED listing + ${photosDeleted} photos for MLS: ${id}`);

      return ok({
        ok: true,
        deleted: id,
        tenantId: tenantId || undefined,
        detailsKey: detailsKeyToDelete,
        photosDeleted,
        message: "Listing and photos deleted",
      });
    }

    // UPDATE / GET
    const id = extractIdentifier(body);
    if (!id) return bad(400, { ok: false, error: "listingId/slug/id is required" });

    const k = keysForListing(id, tenantId);

    // Read: tenant path if exists, else legacy path
    const readKey = (await headExists(k.detailsKey)) ? k.detailsKey : k.legacy.detailsKey;

    console.log(
      "[updateListing] id:",
      id,
      "tenant:",
      tenantId || "(none)",
      "readKey:",
      readKey,
      "region:",
      REGION,
      "bucket:",
      BUCKET
    );

    const currentDetailsRaw = (await getJson(readKey)) || {};

    // Always keep responses clean (no CSV DOM / no server-meta)
    const currentDetails = stripServerMetaMutable(stripDomFieldsMutable({ ...currentDetailsRaw }));

    const { patch: rawPatch, replaceDetails } = extractDetailsIntent(body);
    const noPatch = !rawPatch || typeof rawPatch !== "object" || Object.keys(rawPatch).length === 0;

    // "No change" snapshot
    if (noPatch) {
      const tz = currentDetails.timezone || DEFAULT_LISTING_TZ;
      const dom = currentDetails.activeDate ? domInZone(currentDetails.activeDate, tz) : null;

      const detailsUrl = await getSignedUrl(
        s3,
        new GetObjectCommand({ Bucket: BUCKET, Key: readKey }),
        { expiresIn: SIGN_EXPIRES }
      ).catch(() => null);

      return ok({
        ok: true,
        tenantId: tenantId || undefined,
        detailsKey: readKey,
        details: currentDetails,
        detailsUrl,
        daysOnMarket: dom, // computed only
        timezone: tz,
        noChange: true,
      });
    }

    // sanitize patch (includes DOM stripping + removing server-ish keys)
    const patch = sanitizeDetailsPatchMutable({ ...rawPatch });

    // Build next details
    let nextDetails;
    if (replaceDetails) {
      // Replace mode: you are setting the entire details object.
      // Still enforce: timezone default, no DOM fields, no server-meta.
      nextDetails = { ...patch };
    } else {
      // Patch mode: honor deletions (null/blank => delete)
      nextDetails = applyPatchMutable(
        stripServerMetaMutable(stripDomFieldsMutable({ ...(currentDetailsRaw || {}) })),
        patch
      );
    }

    // Ensure timezone defaults
    if (!nextDetails.timezone) nextDetails.timezone = currentDetailsRaw.timezone || DEFAULT_LISTING_TZ;

    // FINAL HARD CLEAN: no DOM fields + no server meta stored in S3
    stripDomFieldsMutable(nextDetails);
    stripServerMetaMutable(nextDetails);

    // Normalize derived baths (ok to persist if you want)
    normalizeBaths(nextDetails);

    // Write: if tenantId supplied, write to tenant path; otherwise keep legacy.
    const writeKey = tenantId ? k.detailsKey : k.legacy.detailsKey;
    await putJson(writeKey, nextDetails);

    const detailsUrl = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: BUCKET, Key: writeKey }),
      { expiresIn: SIGN_EXPIRES }
    ).catch(() => null);

    const tzForDom = nextDetails.timezone || DEFAULT_LISTING_TZ;
    const dom = nextDetails.activeDate ? domInZone(nextDetails.activeDate, tzForDom) : null;

    // Return UI meta in response only (NOT stored in details.json)
    const responseMeta = {
      updatedAt: new Date().toISOString(),
      _lastEditedBy: "admin-dashboard",
    };

    return ok({
      ok: true,
      tenantId: tenantId || undefined,
      detailsKey: writeKey,
      detailsUrl,
      details: nextDetails, // cleaned + persisted
      detailsPatched: true,
      daysOnMarket: dom, // computed only
      timezone: tzForDom,
      ...responseMeta,
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