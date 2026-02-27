// netlify/functions/getListingDetails.js
import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { domInZone } from "./domInZone.js";

const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";

const BUCKET =
  process.env.SMARTSIGNS_BUCKET ||
  process.env.S3_BUCKET ||
  "gioi-real-estate-bucket";

const DEFAULT_LISTING_TZ = process.env.DEFAULT_LISTING_TZ || "America/Chicago";

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY,
  },
});

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

function cors() {
  return {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, x-admin-key",
  };
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

function stripCsvDomFieldsMutable(details) {
  if (!details || typeof details !== "object") return details;

  // Hard removes (common variants)
  delete details.DaysOnMarket;
  delete details.daysOnMarket;
  delete details.csvDaysOnMarket;
  delete details.CSVDOM;
  delete details.CsvDom;

  // Also remove any weird nested variants you may have stored later
  // (kept conservative; we’re not flattening here)
  return details;
}

export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 200, headers: cors(), body: "" };
    }
    if (event.httpMethod !== "GET") {
      return {
        statusCode: 405,
        headers: cors(),
        body: JSON.stringify({ ok: false, error: "Method Not Allowed" }),
      };
    }

    const id = (event.queryStringParameters?.listingId || "").trim();
    if (!id) {
      return {
        statusCode: 400,
        headers: cors(),
        body: JSON.stringify({ ok: false, error: "listingId is required" }),
      };
    }

    const key = `listings/${id}/details.json`;

    if (!(await headExists(key))) {
      return {
        statusCode: 200,
        headers: cors(),
        body: JSON.stringify({
          ok: true,
          listingId: id,
          details: {},
          activeDate: null,
          timezone: DEFAULT_LISTING_TZ,
          daysOnMarket: null,
        }),
      };
    }

    const obj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    const raw = JSON.parse(await streamToString(obj.Body));

    // Make a shallow copy so we can safely delete fields
    const details = { ...(raw && typeof raw === "object" ? raw : {}) };

    // ✅ remove CSV DOM so it never becomes “truth”
    stripCsvDomFieldsMutable(details);

    // ✅ activeDate truth (activeDate > ActiveDate > listDate/ListDate)
    const activeDate = normalizeISODate(
      firstNonEmpty(details.activeDate, details.ActiveDate, details.listDate, details.ListDate)
    );

    // ✅ timezone default
    const timezone = String(firstNonEmpty(details.timezone, details.Timezone, DEFAULT_LISTING_TZ) || DEFAULT_LISTING_TZ);

    // ✅ computed DOM
    const daysOnMarket = activeDate ? domInZone(activeDate, timezone) : null;

    // (optional but helpful): include these at top-level so UI/AI can rely on them
    return {
      statusCode: 200,
      headers: cors(),
      body: JSON.stringify({
        ok: true,
        listingId: id,
        details,
        activeDate,
        timezone,
        daysOnMarket, // computed truth
      }),
    };
  } catch (e) {
    console.error("[getListingDetails] ERROR", e);
    return {
      statusCode: 502,
      headers: cors(),
      body: JSON.stringify({
        ok: false,
        error: "getListingDetails failed",
        message: e.message,
      }),
    };
  }
}