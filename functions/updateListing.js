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
 *
 * Supported inputs:
 * - body.details (preferred new)
 * - body.detailsPatch (legacy-ish)
 * - body.updates (legacy)
 * - body.overrides (legacy)  <-- treated as details patch (NOT saved separately)
 * - body.activeDate / body.timezone (legacy)
 *
 * Flags:
 * - body.replaceDetails === true -> replace the whole details file with provided details object
 * - body.replace === true with body.overrides -> treat as replaceDetails (legacy mapping)
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

  if ("daysOnMarket" in patch) delete patch.daysOnMarket;

  return { patch, replaceDetails };
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

    // 🗑️ DELETE HANDLING - FULL LISTING + PHOTOS (NO overrides.json)
    if (body.delete === true) {
      const id = extractIdentifier(body);
      if (!id) return bad(400, { ok: false, error: "slug/id required for delete" });

      const detailsKey = `listings/${id}/details.json`;
      const photosPrefix = `photos/${id}/`;

      console.log("🗑️ DELETING FULL LISTING:", id);

      if (await headExists(detailsKey)) await deleteJson(detailsKey);

      let photosContinuationToken;
      let photosDeleted = 0;
      do {
        const listPhotos = await s3.send(
          new ListObjectsV2Command({
            Bucket: BUCKET,
            Prefix: photosPrefix,
            ContinuationToken: photosContinuationToken,
          })
        );

        for (const obj of listPhotos.Contents || []) {
          console.log("🗑️ Deleting photo:", obj.Key);
          await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: obj.Key }));
          photosDeleted++;
        }

        photosContinuationToken = listPhotos.IsTruncated
          ? listPhotos.NextContinuationToken
          : undefined;
      } while (photosContinuationToken);

      console.log(`✅ DELETED listing + ${photosDeleted} photos for MLS: ${id}`);

      return ok({ ok: true, deleted: id, photosDeleted, message: "Listing and photos deleted" });
    }

    // UPDATE / GET
    const id = extractIdentifier(body);
    if (!id) return bad(400, { ok: false, error: "listingId/slug/id is required" });

    const detailsKey = `listings/${id}/details.json`;

    console.log("[updateListing] id:", id, "detailsKey:", detailsKey, "region:", REGION, "bucket:", BUCKET);

    const currentDetails = (await getJson(detailsKey)) || {};

    const { patch: rawPatch, replaceDetails } = extractDetailsIntent(body);
    const noPatch =
      !rawPatch || typeof rawPatch !== "object" || Object.keys(rawPatch).length === 0;

    if (noPatch) {
      const tz = currentDetails.timezone || DEFAULT_LISTING_TZ;
      const dom = currentDetails.activeDate ? domInZone(currentDetails.activeDate, tz) : null;

      const detailsUrl = await getSignedUrl(
        s3,
        new GetObjectCommand({ Bucket: BUCKET, Key: detailsKey }),
        { expiresIn: SIGN_EXPIRES }
      ).catch(() => null);

      return ok({
        ok: true,
        detailsKey,
        details: currentDetails,
        detailsUrl,
        daysOnMarket: dom,
        timezone: tz,
        noChange: true,
      });
    }

    // sanitize patch
    const patch = sanitizeDetailsPatchMutable({ ...rawPatch });

    // Build next details
    const nextDetails = replaceDetails
      ? { ...patch }
      : applyPatchMutable({ ...currentDetails }, patch);

    if (!nextDetails.timezone) nextDetails.timezone = currentDetails.timezone || DEFAULT_LISTING_TZ;

    normalizeBaths(nextDetails);

    nextDetails.updatedAt = new Date().toISOString();
    nextDetails._lastEditedBy = "admin-dashboard";

    await putJson(detailsKey, nextDetails);

    const detailsUrl = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: BUCKET, Key: detailsKey }),
      { expiresIn: SIGN_EXPIRES }
    ).catch(() => null);

    const tzForDom = nextDetails.timezone || DEFAULT_LISTING_TZ;
    const dom = nextDetails.activeDate ? domInZone(nextDetails.activeDate, tzForDom) : null;

    return ok({
      ok: true,
      detailsKey,
      detailsUrl,
      details: nextDetails,
      detailsPatched: true,
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
