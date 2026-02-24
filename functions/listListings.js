// netlify/functions/listListings.js
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  HeadObjectCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { domInZone } from "./domInZone.js";

/** ===== Config ===== */
const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";

const BUCKET =
  process.env.SMARTSIGNS_BUCKET ||
  process.env.S3_BUCKET ||
  process.env.MY_AWS_BUCKET_NAME ||
  "gioi-real-estate-bucket";

const DEFAULT_LISTING_TZ = process.env.DEFAULT_LISTING_TZ || "America/Chicago";
const SIGN_EXPIRES = 3600; // 1 hour

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId:
      process.env.SMARTSIGNS_ACCESS_KEY_ID ||
      process.env.MY_AWS_ACCESS_KEY_ID ||
      process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey:
      process.env.SMARTSIGNS_SECRET_ACCESS_KEY ||
      process.env.MY_AWS_SECRET_ACCESS_KEY ||
      process.env.AWS_SECRET_ACCESS_KEY,
  },
});

/** ===== Small utils ===== */
const streamToString = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
  });

const baseName = (key) => (key || "").split("/").pop() || "";

async function readJSON(Key) {
  const out = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key }));
  const body = await streamToString(out.Body);
  return JSON.parse(body);
}

async function headExists(Key) {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key }));
    return true;
  } catch {
    return false;
  }
}

async function presign(Key) {
  return getSignedUrl(s3, new GetObjectCommand({ Bucket: BUCKET, Key }), {
    expiresIn: SIGN_EXPIRES,
  });
}

function cleanNumberOrString(v) {
  if (typeof v === "number") return v;
  if (v == null) return null;
  const str = String(v).trim();
  if (!str) return null;
  const n = Number(str.replace(/[^\d.]/g, ""));
  return Number.isFinite(n) && n > 0 ? n : str;
}

function firstNonEmpty(...vals) {
  for (const v of vals) {
    if (v == null) continue;
    if (typeof v === "string" && !v.trim()) continue;
    return v;
  }
  return null;
}

function normalizeISODate(s) {
  if (!s) return null;
  const str = String(s).trim();

  // ISO already
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;

  // M/D/YY or M/D/YYYY or with dashes
  if (/^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$/.test(str)) {
    const [mm, dd, yy] = str.split(/[/-]/);
    let y = Number(yy);
    if (yy.length === 2) y = y >= 70 ? 1900 + y : 2000 + y;
    const m = String(Number(mm)).padStart(2, "0");
    const d = String(Number(dd)).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  // Last resort
  const d = new Date(str);
  if (isNaN(d)) return null;
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(
    d.getDate()
  ).padStart(2, "0")}`;
}

/** Choose best photo key for a listing, preferring 'primaryPhoto' if present. */
async function resolvePhotoKey(listingId, details) {
  if (details?.primaryPhoto && typeof details.primaryPhoto === "string") {
    const k = details.primaryPhoto.replace(/^https?:\/\/[^/]+\//, "");
    if (await headExists(k)) return k;
  }

  const candidates = [
    `photos/${listingId}/${listingId}.jpg`,
    `photos/${listingId}/${listingId}.jpeg`,
    `photos/${listingId}/${listingId}.png`,
    `photos/${listingId}/${listingId}.webp`,
  ];
  for (const k of candidates) {
    if (await headExists(k)) return k;
  }

  try {
    const page = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: `photos/${listingId}/`,
        MaxKeys: 50,
      })
    );
    for (const obj of page.Contents ?? []) {
      const bn = baseName(obj.Key || "");
      if (/\.(jpe?g|png|webp|gif)$/i.test(bn)) return obj.Key;
    }
  } catch {}

  const flat = [
    `photos/${listingId}.jpg`,
    `photos/${listingId}.jpeg`,
    `photos/${listingId}.png`,
    `photos/${listingId}.webp`,
  ];
  for (const k of flat) {
    if (await headExists(k)) return k;
  }

  return null;
}

/** Concurrency limiter */
function pLimit(limit) {
  const queue = [];
  let active = 0;
  const next = () => {
    active--;
    if (queue.length) queue.shift()();
  };
  return (fn) =>
    new Promise((resolve, reject) => {
      const run = async () => {
        active++;
        try {
          const val = await fn();
          resolve(val);
        } catch (e) {
          reject(e);
        } finally {
          next();
        }
      };
      if (active < limit) run();
      else queue.push(run);
    });
}

/** ===== Main handler ===== */
export async function handler() {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: BUCKET }));

    // 1) Find all details.json objects
    const detailObjects = [];
    let token;
    do {
      const page = await s3.send(
        new ListObjectsV2Command({
          Bucket: BUCKET,
          Prefix: "listings/",
          ContinuationToken: token,
        })
      );

      for (const obj of page.Contents ?? []) {
        const key = obj.Key || "";
        if (key.endsWith("/details.json")) {
          detailObjects.push({
            Key: key,
            LastModified: obj.LastModified ? new Date(obj.LastModified).toISOString() : null,
          });
        }
      }

      token = page.IsTruncated ? page.NextContinuationToken : undefined;
    } while (token);

    // 2) For each details.json, read & enrich (NO overrides.json)
    const limit = pLimit(8);

    const items = await Promise.all(
      detailObjects.map((o) =>
        limit(async () => {
          const key = o.Key; // listings/{id}/details.json
          const listingId = key.split("/")[1];

          let details = {};
          try {
            details = await readJSON(key);
          } catch {}

          // --- Canonical fields for cards ---
          const mls = firstNonEmpty(details.mlsNumber, details.MlsNumber, details.mls, details.MLS, listingId);

          const address = firstNonEmpty(details.address, details.Address);

          const price = cleanNumberOrString(firstNonEmpty(details.price, details.listPrice, details.ListPrice));

          // --- Active Date truth ---
          // Prefer explicit activeDate; fall back to ListDate/listDate.
          const activeDate = normalizeISODate(
            firstNonEmpty(details.activeDate, details.ActiveDate, details.listDate, details.ListDate)
          );

          const timezone = firstNonEmpty(details.timezone, details.Timezone, DEFAULT_LISTING_TZ);

          // --- DOM MUST be computed from activeDate ---
          const computedDaysOnMarket = activeDate ? domInZone(activeDate, timezone) : null;

          // CSV DOM is allowed only as debug, never as truth
          const csvDaysOnMarket = cleanNumberOrString(firstNonEmpty(details.DaysOnMarket, details.daysOnMarket));

          const hasNote = typeof details.agentNotes === "string" && details.agentNotes.trim().length > 0;

          const photoKey = await resolvePhotoKey(listingId, details);
          const photoUrl = photoKey ? await presign(photoKey) : null;

          // detailsUrl stays Netlify function (not presigned S3)
          const detailsUrl = `/.netlify/functions/getListingDetails?listingId=${encodeURIComponent(listingId)}`;

          return {
            slug: listingId,
            id: listingId,
            listingId,

            mls,
            address: address || null,
            price: price ?? null,

            activeDate,
            timezone,

            // ✅ always the one the UI should use
            computedDaysOnMarket,

            // optional debug only
            csvDaysOnMarket: csvDaysOnMarket ?? null,

            photoUrl,
            detailsUrl,

            lastModified: o.LastModified,
            hasNote,

            key,
          };
        })
      )
    );

    items.sort((a, b) => {
      const ad = a.lastModified ? Date.parse(a.lastModified) : 0;
      const bd = b.lastModified ? Date.parse(b.lastModified) : 0;
      if (ad !== bd) return bd - ad;
      return String(b.key).localeCompare(String(a.key));
    });

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        Pragma: "no-cache",
        Expires: "0",
      },
      body: JSON.stringify(items),
    };
  } catch (e) {
    return {
      statusCode: 502,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        Pragma: "no-cache",
        Expires: "0",
      },
      body: JSON.stringify({
        error: "listListings failed",
        diag: { region: REGION, bucket: BUCKET, message: e.message, name: e.name },
      }),
    };
  }
}