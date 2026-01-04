// netlify/functions/getPresignedUrls.js
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

/** ---- Config ---- */
const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";
const BUCKET =
  process.env.SMARTSIGNS_BUCKET ||    // ← #1 NEW (Smart Signs)
  process.env.S3_BUCKET ||           // ← #2 (legacy QR)
  process.env.MY_AWS_BUCKET_NAME ||  // ← #3
  "gioi-real-estate-bucket";         // ← #4 (final)

console.log("🪣 getPresignedUrls BUCKET:", BUCKET);  // ← DEBUG LOG

// Prefer MY_* creds (Netlify-safe), fall back to AWS_* if present
const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// Upload URL lifetime (seconds)
const EXPIRES_IN = 600;

function mimeFor(name) {
  if (/\.csv$/i.test(name)) return "text/csv";
  if (/\.jpe?g$/i.test(name)) return "image/jpeg";
  if (/\.png$/i.test(name)) return "image/png";
  if (/\.webp$/i.test(name)) return "image/webp";
  return "application/octet-stream";
}

function baseName(name = "") {
  const just = String(name).split("/").pop().split("\\").pop();
  return just;
}

function cors() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
}

export async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: cors(), body: "" };
  }
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers: cors(), body: "Method Not Allowed" };
  }

  try {
    const payload = JSON.parse(event.body || "{}");
    const fileNames = Array.isArray(payload.fileNames) ? payload.fileNames : [];
    if (!fileNames.length) {
      return { statusCode: 400, headers: cors(), body: "Provide { fileNames: [...] }" };
    }

    // Keep a sessionId in the response to satisfy the frontend,
    // even though we no longer include it in the S3 key paths.
    const sessionId =
      (typeof crypto !== "undefined" && crypto.randomUUID)
        ? crypto.randomUUID()
        : `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

    const uploads = [];
    for (const rawName of fileNames) {
      const name = baseName(rawName);
      const isCsv = /\.csv$/i.test(name);

      // ✨ Flat keys (NO session folder) so finalizeCsvIngest can find them:
      // csv-incoming/<file.csv> and photos-incoming/<file.jpg>
      const key = `${isCsv ? "csv-incoming" : "photos-incoming"}/${name}`;

      const cmd = new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        ContentType: mimeFor(name),
        ServerSideEncryption: "AES256",
      });

      const url = await getSignedUrl(s3, cmd, { expiresIn: EXPIRES_IN });

      uploads.push({
        name,
        key,
        url,
        headers: { "x-amz-server-side-encryption": "AES256" },
      });
    }

    // Return the shape your UI expects
    return {
      statusCode: 200,
      headers: { ...cors(), "Content-Type": "application/json" },
      body: JSON.stringify({ sessionId, uploads }),
    };
  } catch (e) {
    console.error("getPresignedUrls ERROR:", e);
    return {
      statusCode: 500,
      headers: cors(),
      body: JSON.stringify({ error: e.message }),
    };
  }
}
