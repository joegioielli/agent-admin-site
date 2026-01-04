// netlify/functions/deleteListing.js
import {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";

/** ---- Env config ---- */
const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";
const BUCKET =
  process.env.S3_BUCKET ||
  process.env.SMARTSIGNS_BUCKET||
  process.env.MY_AWS_BUCKET ||
  "gioi-real-estate-bucket";

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function json(statusCode, body) {
  return { statusCode, headers: { "Content-Type": "application/json", ...CORS }, body: JSON.stringify(body) };
}

async function listKeys(prefix) {
  const keys = [];
  let ContinuationToken;
  do {
    const page = await s3.send(
      new ListObjectsV2Command({ Bucket: BUCKET, Prefix: prefix, ContinuationToken })
    );
    for (const obj of page.Contents ?? []) if (obj.Key) keys.push(obj.Key);
    ContinuationToken = page.IsTruncated ? page.NextContinuationToken : undefined;
  } while (ContinuationToken);
  return keys;
}

async function batchDelete(keys) {
  if (!keys.length) return 0;
  let deleted = 0;
  for (let i = 0; i < keys.length; i += 1000) {
    const chunk = keys.slice(i, i + 1000).map((Key) => ({ Key }));
    await s3.send(
      new DeleteObjectsCommand({ Bucket: BUCKET, Delete: { Objects: chunk, Quiet: true } })
    );
    deleted += chunk.length;
  }
  return deleted;
}

export async function handler(event) {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { ok: false, error: "Method Not Allowed" });

  try {
    await s3.send(new HeadBucketCommand({ Bucket: BUCKET }));

    const body = JSON.parse(event.body || "{}");
    const listingId = body.listingId ?? body.listingKey; // support both
    const dryRun = !!body.dryRun;

    if (!listingId) return json(400, { ok: false, error: "Missing listingId" });
    if (!/^[A-Za-z0-9_-]+$/.test(String(listingId))) {
      return json(400, { ok: false, error: "Invalid listingId format" });
    }

    // Collect keys under listings/{id}/ and photos/{id}/
    const keys = [
      ...(await listKeys(`listings/${listingId}/`)),
      ...(await listKeys(`photos/${listingId}/`)),
    ];

    // Also attempt to delete flat photo variants
    for (const ext of ["jpg", "jpeg", "png", "webp", "gif"]) {
      keys.push(`photos/${listingId}.${ext}`);
    }

    // De-duplicate
    const uniq = Array.from(new Set(keys));

    if (dryRun) {
      return json(200, { ok: true, listingId, wouldDelete: uniq.length, keys: uniq });
    }

    const deletedCount = await batchDelete(uniq);
    return json(200, { ok: true, listingId, deletedCount, attempted: uniq.length });
  } catch (e) {
    return json(502, { ok: false, error: e.message });
  }
}
