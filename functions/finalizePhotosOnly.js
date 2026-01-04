// netlify/functions/finalizePhotosOnly.js
import {
  S3Client,
  ListObjectsV2Command,
  CopyObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";

const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";
// FIXED:
const BUCKET =
  process.env.SMARTSIGNS_BUCKET ||   // ← #1 CORRECT (gioi-real-estate-bucket)
  process.env.S3_BUCKET ||          // ← #2 (qr-redirects-config fallback)
  process.env.MY_AWS_BUCKET_NAME || // ← #3
  "gioi-real-estate-bucket";        // ← #4 final

console.log("🪣 finalizePhotosOnly BUCKET:", BUCKET);  // ← DEBUG


const IMG_EXT = /\.(jpe?g|png|webp|gif)$/i;

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const baseName = (k) => (k || "").split("/").pop() || "";

// Heuristic: choose nicest primary among a set of destination keys
function choosePrimary(destKeys, listingId) {
  const id = String(listingId).toLowerCase();
  const scores = destKeys.map(k => {
    const bn = baseName(k).toLowerCase();
    let s = 0;
    if (bn === `${id}.jpg`) s += 100;
    if (bn === `${id}.jpeg`) s += 99;
    if (bn.includes(id)) s += 50;
    if (/\/(main|cover)\./i.test(k)) s += 40;
    if (/\/(1|front)\./i.test(k)) s += 30;
    if (/\.(jpg|jpeg)$/.test(bn)) s += 10;
    return { k, s };
  });
  scores.sort((a,b) => b.s - a.s);
  return scores[0]?.k || destKeys[0] || null;
}

async function headExists(Key) {
  try { await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key })); return true; }
  catch { return false; }
}

async function getJSON(Key) {
  const obj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key }));
  const text = await new Promise((res, rej) => {
    const chunks = [];
    obj.Body.on("data", c => chunks.push(c));
    obj.Body.on("end", () => res(Buffer.concat(chunks).toString("utf-8")));
    obj.Body.on("error", rej);
  });
  return JSON.parse(text);
}

async function putJSON(Key, data) {
  await s3.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key,
    Body: JSON.stringify(data, null, 2),
    ContentType: "application/json",
  }));
}

async function copyThenDelete(srcKey, destKey) {
  await s3.send(new CopyObjectCommand({
    Bucket: BUCKET,
    Key: destKey,
    CopySource: encodeURIComponent(`${BUCKET}/${srcKey}`),
    MetadataDirective: "COPY",
  }));
  await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: srcKey }));
}

function listingIdFromKey(k) {
  // Accept: photos-incoming/1234567.jpg  OR photos-incoming/1234567/anything.jpg
  const parts = (k || "").split("/");
  // If folder variant
  if (parts.length >= 3 && parts[0] === "photos-incoming") return parts[1];
  // If flat file variant
  const bn = baseName(k);
  const m = bn.match(/^([A-Za-z0-9_-]+)\./);
  return m ? m[1] : null;
}

export async function handler(event) {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: BUCKET }));

    let body = {};
    try { body = JSON.parse(event.body || "{}"); } catch {}
    const fileNames = Array.isArray(body.fileNames) ? body.fileNames : null;
    const dryRun = !!body.dryRun;

    // Build candidate source keys list
    const srcKeys = new Set();

    if (fileNames && fileNames.length) {
      // Restrict to provided names (flat variant)
      for (const name of fileNames) {
        if (!IMG_EXT.test(name)) continue;
        srcKeys.add(`photos-incoming/${name}`);
      }
    } else {
      // Scan all incoming photos (safety: cap pages)
      let token, pages = 0;
      do {
        const page = await s3.send(new ListObjectsV2Command({
          Bucket: BUCKET,
          Prefix: "photos-incoming/",
          ContinuationToken: token,
          MaxKeys: 1000,
        }));
        for (const obj of page.Contents ?? []) {
          const k = obj.Key || "";
          if (IMG_EXT.test(k)) srcKeys.add(k);
        }
        token = page.IsTruncated ? page.NextContinuationToken : undefined;
        pages += 1;
      } while (token && pages < 10); // guard
    }

    // Partition by listingId
    const byListing = new Map();
    for (const k of srcKeys) {
      const id = listingIdFromKey(k);
      if (!id) continue;
      if (!byListing.has(id)) byListing.set(id, []);
      byListing.get(id).push(k);
    }

    const results = [];
    let moved = 0;

    for (const [listingId, keys] of byListing.entries()) {
      const dests = [];

      // Move each photo
      for (const src of keys) {
        const bn = baseName(src);
        const destKey = `photos/${listingId}/${bn}`;
        if (!dryRun) await copyThenDelete(src, destKey);
        dests.push(destKey);
        moved++;
      }

      // Ensure details.json exists and has primaryPhoto
      const detailsKey = `listings/${listingId}/details.json`;
      let details = {};
      if (await headExists(detailsKey)) {
        try { details = await getJSON(detailsKey); } catch {}
      } else {
        // Minimal stub if no details yet
        details = { mlsNumber: listingId };
      }

      if (!details.primaryPhoto || typeof details.primaryPhoto !== "string") {
        const chosen = choosePrimary(dests, listingId);
        if (chosen) details.primaryPhoto = chosen;
      }

      if (!dryRun) {
        await putJSON(detailsKey, details);
      }

      results.push({ listingId, moved: dests.length, primaryPhoto: details.primaryPhoto || null });
    }

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ok: true, moved, listings: results.length, results }),
    };
  } catch (e) {
    return {
      statusCode: 502,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: "finalizePhotosOnly failed",
        diag: { region: REGION, bucket: BUCKET, message: e.message, name: e.name },
      }),
    };
  }
}
