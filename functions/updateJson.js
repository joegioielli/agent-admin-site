// netlify/functions/updateJson.js — ESM + AWS SDK v3

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

/* -------------------- Env -------------------- */
const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";
const BUCKET =
  process.env.SMARTSIGNS_BUCKET || // preferred
  process.env.MY_AWS_BUCKET ||      // legacy fallback
  "gioi-real-estate-bucket";

/* -------------------- S3 Client -------------------- */
const s3 = new S3Client({
  region: REGION,
  credentials:
    process.env.MY_AWS_ACCESS_KEY_ID && process.env.MY_AWS_SECRET_ACCESS_KEY
      ? {
          accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID,
          secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY,
        }
      : undefined,
});

/* -------------------- HTTP helpers -------------------- */
const CORS = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const json = (statusCode, body) => ({
  statusCode,
  headers: CORS,
  body: JSON.stringify(body),
});

/* -------------------- Handler -------------------- */
export async function handler(event) {
  try {
    // CORS preflight
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 200, headers: CORS, body: "" };
    }

    if (event.httpMethod !== "POST") {
      return json(405, { ok: false, error: "Method Not Allowed" });
    }

    let payload = {};
    try {
      payload = JSON.parse(event.body || "{}");
    } catch {
      return json(400, { ok: false, error: "Invalid JSON body" });
    }

    const { key, updatedJson } = payload || {};
    if (!key || !updatedJson) {
      return json(400, { ok: false, error: "Provide key and updatedJson" });
    }

    // Safety: only allow writing details.json under listings/{listingKey}/
    const m = key.match(/^listings\/([^/]+)\/details\.json$/);
    if (!m) {
      return json(400, {
        ok: false,
        error: "Key must be listings/{listingKey}/details.json",
      });
    }

    // Stamp update time (non-destructive)
    try {
      updatedJson.updatedAt = new Date().toISOString();
    } catch {
      // ignore if updatedJson is not a plain object
    }

    // Write to S3
    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: JSON.stringify(updatedJson, null, 2),
        ContentType: "application/json",
        ServerSideEncryption: "AES256",
      })
    );

    return json(200, { ok: true, message: "JSON file updated successfully" });
  } catch (error) {
    console.error("updateJson error:", error);
    return json(500, {
      ok: false,
      error: error?.message || String(error),
      region: REGION,
      bucket: BUCKET,
    });
  }
}
