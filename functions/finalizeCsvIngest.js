// netlify/functions/finalizeCsvIngest.js
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  PutObjectCommand,
  CopyObjectCommand,
  DeleteObjectCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";

/** ===== Config ===== */
const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";
const BUCKET =
  process.env.S3_BUCKET ||
  process.env.SMARTSIGNS_BUCKET ||
  "gioi-real-estate-bucket";

// Default TZ for all listings unless CSV provides something else
const DEFAULT_LISTING_TZ = process.env.DEFAULT_LISTING_TZ || "America/Chicago";

// Delete the CSV from csv-incoming/ after a successful ingest:
const DELETE_CSV_AFTER_SUCCESS = true;

// (Optional) safety: refuse to overwrite an existing details.json unless explicitly allowed.
// Set to "1" to require allowOverwrite=true in request body.
const REQUIRE_ALLOW_OVERWRITE = process.env.REQUIRE_ALLOW_OVERWRITE === "1";

/** ===== S3 client (prefer MY_* creds on Netlify) ===== */
function makeS3() {
  const accessKeyId = process.env.MY_AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID;
  const secretAccessKey = process.env.MY_AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) {
    throw new Error("Missing AWS creds. Set MY_AWS_ACCESS_KEY_ID and MY_AWS_SECRET_ACCESS_KEY in Netlify.");
  }
  return new S3Client({ region: REGION, credentials: { accessKeyId, secretAccessKey } });
}

/** ===== Small utils ===== */
const streamToString = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
  });

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function isEmptyValue(v) {
  if (v === null || v === undefined) return true;
  if (typeof v === "string") {
    const s = v.trim();
    if (!s) return true;
    const tombstones = new Set(["n/a", "na", "none", "-", "—", "null", "undefined"]);
    return tombstones.has(s.toLowerCase());
  }
  if (Array.isArray(v)) return v.length === 0;
  if (typeof v === "object") return Object.keys(v).length === 0;
  return false;
}

function deepClean(obj) {
  if (Array.isArray(obj)) {
    const arr = obj.map(deepClean).filter((v) => !isEmptyValue(v));
    return arr.length ? arr : undefined;
  }
  if (obj && typeof obj === "object") {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      const cleaned = deepClean(v);
      if (!isEmptyValue(cleaned)) out[k] = cleaned;
    }
    return Object.keys(out).length ? out : undefined;
  }
  return obj;
}

/** ===== CSV parsing ===== */
function parseCSV(text) {
  const rows = [];
  let row = [];
  let field = "";
  let i = 0;
  let inQuotes = false;

  while (i < text.length) {
    const ch = text[i];

    if (inQuotes) {
      if (ch === '"') {
        const next = text[i + 1];
        if (next === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i += 1;
        continue;
      }
      field += ch;
      i += 1;
      continue;
    }

    // not in quotes
    if (ch === '"') {
      inQuotes = true;
      i += 1;
      continue;
    }
    if (ch === ",") {
      row.push(field);
      field = "";
      i += 1;
      continue;
    }
    if (ch === "\r") {
      i += 1;
      continue;
    }
    if (ch === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      i += 1;
      continue;
    }
    field += ch;
    i += 1;
  }

  row.push(field);
  rows.push(row);
  return rows;
}

function csvToObjects(text) {
  const rows = parseCSV(text).filter((r) => !(r.length === 1 && r[0].trim() === ""));
  if (!rows.length) return [];
  const header = rows[0].map((h) => h.trim());
  const out = [];
  for (let r = 1; r < rows.length; r++) {
    const vals = rows[r];
    const obj = {};
    for (let c = 0; c < header.length; c++) {
      const key = header[c] || `col_${c}`;
      obj[key] = vals[c] !== undefined ? vals[c] : "";
    }
    out.push(obj);
  }
  return out;
}

/** ===== Field helpers ===== */
function normKey(k) {
  return String(k).toLowerCase().replace(/[^a-z0-9]/g, "");
}

function pickFirst(obj, candidates) {
  for (const key of candidates) {
    for (const [k, v] of Object.entries(obj)) {
      if (normKey(k) === normKey(key)) {
        if (v !== undefined && String(v).trim() !== "") return v;
      }
    }
  }
  return null;
}

function readMLS(row) {
  return pickFirst(row, ["MLS Number", "MLS#", "mls", "MLS", "Listing ID", "ListingId"]) || null;
}

function readPrice(row) {
  const raw = pickFirst(row, ["List Price", "Price", "ListPrice", "Asking Price"]) || null;
  if (raw == null) return null;
  if (typeof raw === "number") return raw;
  const n = Number(String(raw).replace(/[^\d.]/g, ""));
  return Number.isFinite(n) && n > 0 ? n : String(raw).trim();
}

function readAddress(row) {
  const direct = pickFirst(row, [
    "Address",
    "Street Address",
    "Full Address",
    "Property Address",
    "Site Address",
    "StreetAddress",
    "Unparsed Address",
  ]);
  if (direct) return String(direct).trim();

  const street =
    pickFirst(row, ["Street", "Street Name", "StreetName"]) ||
    pickFirst(row, ["Street Address", "StreetAddress"]);
  const number = pickFirst(row, ["Street Number", "StreetNumber", "Address Number"]) || "";
  const unit = pickFirst(row, ["Unit", "Unit Number", "UnitNumber", "Apt"]) || "";
  const city = pickFirst(row, ["City", "Municipality"]) || "";
  const state = pickFirst(row, ["State", "State Or Province", "StateOrProvince"]) || "";
  const zip = pickFirst(row, ["Zip", "Zip Code", "Postal Code", "PostalCode"]) || "";

  const streetLine = [number, street].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
  const line1 = [streetLine, unit].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
  const line2 = [city, state, zip].filter(Boolean).join(", ").replace(/,\s*,/g, ",").trim();
  const addr = [line1, line2].filter(Boolean).join(", ").replace(/,\s+,/g, ", ").trim();
  return addr || null;
}

function deriveListingId(row, idx) {
  const mls = readMLS(row);
  if (mls) return String(mls).trim();
  const address = readAddress(row);
  if (address) return slugify(address);
  return `row-${idx + 1}`;
}

/** ---- Active date normalization (DOM computed from activeDate elsewhere) ---- */
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

function readActiveDateISO(row) {
  const raw =
    pickFirst(row, ["Active Date", "ActiveDate", "Date Active", "DateActive"]) ||
    pickFirst(row, ["List Date", "ListDate", "Listing Date", "ListingDate", "Date Listed", "DateListed"]) ||
    null;

  const iso = normalizeActiveDateISO(raw);
  return iso || null;
}

function readTimezone(row) {
  const raw = pickFirst(row, ["Time Zone", "TimeZone", "timezone", "TZ", "TimeZoneLocal"]);
  const tz = raw ? String(raw).trim() : "";
  return tz || null;
}

/** ---- Strip CSV DOM fields (never persist CSV DOM) ---- */
const DOM_KEYS_TO_STRIP = new Set([
  // Common variations
  "DaysOnMarket",
  "Days on Market",
  "Days On Market",
  "daysOnMarket",
  "DOM",
  "Dom",
  "dom",
  "CSVDOM",
  "CsvDom",
  "csvDom",
  "csvDaysOnMarket",
  "computedDaysOnMarket",
  // Some exports / vendor variants
  "DOMTotal",
  "DOM Total",
  "CumulativeDaysOnMarket",
  "Cumulative Days On Market",
]);

function stripDomFromRow(row) {
  if (!row || typeof row !== "object") return row;
  const out = { ...row };

  for (const k of Object.keys(out)) {
    const norm = String(k).replace(/[^a-z0-9]/gi, "").toLowerCase();

    const isDom =
      DOM_KEYS_TO_STRIP.has(k) ||
      norm === "daysonmarket" ||
      norm === "computeddaysonmarket" ||
      norm === "csvdaysonmarket" ||
      norm === "csvdom" ||
      norm === "dom" ||
      norm === "domtotal" ||
      norm === "cumulativedaysonmarket" ||
      norm === "daysonmarketcumulative";

    if (isDom) delete out[k];
  }

  return out;
}

/** ===== S3 helpers ===== */
async function listNewestCSV(s3) {
  let newest = null;
  let token;
  do {
    const page = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: "csv-incoming/",
        ContinuationToken: token,
      })
    );
    for (const obj of page.Contents ?? []) {
      if (!obj.Key || !obj.Key.toLowerCase().endsWith(".csv")) continue;
      if (!newest || new Date(obj.LastModified) > new Date(newest.LastModified)) newest = obj;
    }
    token = page.IsTruncated ? page.NextContinuationToken : undefined;
  } while (token);
  return newest?.Key || null;
}

async function getTextObject(s3, Key) {
  const out = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key }));
  return await streamToString(out.Body);
}

async function putJSON(s3, Key, dataObj) {
  const Body = JSON.stringify(dataObj, null, 2);
  await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key, Body, ContentType: "application/json" }));
}

async function copyThenDelete(s3, srcKey, destKey) {
  const CopySource = encodeURIComponent(`${BUCKET}/${srcKey}`);
  await s3.send(new CopyObjectCommand({ Bucket: BUCKET, Key: destKey, CopySource, MetadataDirective: "COPY" }));
  await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: srcKey })); // remove incoming photo
}

async function headExists(s3, Key) {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: BUCKET })); // cheap-ish sanity; ok for Netlify
    await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key, Range: "bytes=0-0" }));
    return true;
  } catch {
    return false;
  }
}

/** ===== Photo discovery (flat keys + folder variant) ===== */
const IMG_EXT = [".jpg", ".jpeg", ".png", ".webp", ".gif", ".JPG", ".JPEG", ".PNG", ".WEBP", ".GIF"];
const baseName = (key) => key.split("/").pop() || "";

async function listPhotoCandidates(s3, listingId) {
  const keys = [];

  // Folder-style: photos-incoming/{id}/**
  let token;
  do {
    const page = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: `photos-incoming/${listingId}/`,
        ContinuationToken: token,
      })
    );
    for (const obj of page.Contents ?? []) {
      const k = obj.Key;
      if (!k) continue;
      const bn = baseName(k);
      if (IMG_EXT.some((ext) => bn.endsWith(ext))) keys.push(k);
    }
    token = page.IsTruncated ? page.NextContinuationToken : undefined;
  } while (token);

  // Flat files: photos-incoming/{id}.{ext}
  for (const ext of IMG_EXT) {
    const k = `photos-incoming/${listingId}${ext}`;
    const page = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: k }));
    if ((page.Contents || []).some((o) => o.Key === k)) keys.push(k);
  }

  return Array.from(new Set(keys));
}

function choosePrimary(destKeys, listingId) {
  const scores = destKeys.map((k) => {
    const bn = baseName(k).toLowerCase();
    let score = 0;
    if (bn === `${String(listingId).toLowerCase()}.jpg`) score += 100;
    if (bn === `${String(listingId).toLowerCase()}.jpeg`) score += 99;
    if (bn.includes(String(listingId).toLowerCase())) score += 50;
    if (/\/(main|cover)\./i.test(k)) score += 40;
    if (/\/(1|front)\./i.test(k)) score += 30;
    if (/\.(jpg|jpeg)$/.test(bn)) score += 10;
    return { k, score };
  });
  scores.sort((a, b) => b.score - a.score);
  return scores[0]?.k || destKeys[0] || null;
}

/** ===== Main handler ===== */
export async function handler(event) {
  const s3 = makeS3();

  try {
    await s3.send(new HeadBucketCommand({ Bucket: BUCKET }));

    // Body can include: { csvKey?, fileNames?, dryRun?, allowOverwrite? }
    let body = {};
    try {
      body = JSON.parse(event.body || "{}");
    } catch {}
    const dryRun = !!body.dryRun;
    const allowOverwrite = !!body.allowOverwrite;

    // Resolve CSV key
    let csvKey = body.csvKey || null;
    if (!csvKey && Array.isArray(body.fileNames)) {
      const csvName = body.fileNames.find((n) => /\.csv$/i.test(n));
      if (csvName) csvKey = `csv-incoming/${csvName}`;
    }
    if (!csvKey) csvKey = await listNewestCSV(s3);
    if (!csvKey) {
      return { statusCode: 400, body: JSON.stringify({ ok: false, error: "No CSV found to ingest." }) };
    }

    const csvText = await getTextObject(s3, csvKey);
    const rows = csvToObjects(csvText);

    let processed = 0;
    let written = 0;
    let skippedExisting = 0;

    const detailKeys = [];
    const photoMoves = [];

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      // Core fields
      const listingId = deriveListingId(row, i);
      const mls = readMLS(row);
      const listPrice = readPrice(row);
      const address = readAddress(row);

      // Plan: activeDate ingested from CSV when possible
      const activeDate = readActiveDateISO(row) || undefined;

      // Plan: timezone defaults unless CSV overrides it
      const timezone = readTimezone(row) || DEFAULT_LISTING_TZ;

      // Details key
      const detailsKey = `listings/${listingId}/details.json`;

      // Optional: skip overwrite protection
      if (REQUIRE_ALLOW_OVERWRITE && !allowOverwrite && !dryRun) {
        // Lightweight existence check: list objects under exact key prefix
        const page = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: detailsKey, MaxKeys: 1 }));
        const exists = (page.Contents || []).some((o) => o.Key === detailsKey);
        if (exists) {
          skippedExisting++;
          processed++;
          continue;
        }
      }

      // Discover photos
      const candidates = await listPhotoCandidates(s3, listingId);

      // Move photos to photos/{listingId}/...
      const movedDestKeys = [];
      for (const src of candidates) {
        const dest = `photos/${listingId}/${baseName(src)}`;
        if (!dryRun) {
          await copyThenDelete(s3, src, dest);
        }
        photoMoves.push({ from: src, to: dest });
        movedDestKeys.push(dest);
      }

      // Choose primary photo path (if any moved)
      const primaryPhoto = movedDestKeys.length ? choosePrimary(movedDestKeys, listingId) : undefined;

      // Clean raw row AND strip any DOM columns
      const cleanedRaw = deepClean(stripDomFromRow(row)) ?? {};

      // Build and write details.json (Plan rules: never persist CSV DOM)
      const details =
        deepClean({
          mlsNumber: mls ?? undefined,
          listPrice: listPrice ?? undefined,
          address: address ?? undefined,
          primaryPhoto: primaryPhoto ?? undefined,

          activeDate, // ISO if found
          timezone, // always present

          source: { csvKey, ingestedAt: new Date().toISOString() },

          ...cleanedRaw,
        }) ?? {};

      if (!dryRun) {
        await putJSON(s3, detailsKey, details);
      }

      detailKeys.push(detailsKey);
      written++;
      processed++;
    }

    // Optionally delete the CSV after success
    let csvDeleted = false;
    if (DELETE_CSV_AFTER_SUCCESS && !dryRun) {
      try {
        await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: csvKey }));
        csvDeleted = true;
      } catch {
        /* non-fatal */
      }
    }

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ok: true,
        csvKey,
        dryRun,
        processed,
        written,
        skippedExisting,
        details: detailKeys,
        photosMoved: photoMoves.length,
        photoMoves,
        csvDeleted,
        plan: {
          domFromActiveDateOnly: true,
          csvDomPersisted: false,
          timezoneDefault: DEFAULT_LISTING_TZ,
          overwriteGuard: REQUIRE_ALLOW_OVERWRITE ? "REQUIRE_ALLOW_OVERWRITE=1 (use allowOverwrite=true)" : "off",
        },
      }),
    };
  } catch (err) {
    return {
      statusCode: 502,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ok: false,
        error: "finalizeCsvIngest failed",
        diag: {
          region: REGION,
          bucket: BUCKET,
          message: err?.message,
          name: err?.name,
          code: err?.$metadata?.httpStatusCode || err?.code,
        },
      }),
    };
  }
}