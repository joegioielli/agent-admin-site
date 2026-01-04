// netlify/functions/logHit.js
// Logs QR hits with timestamp, slug, and hashed IP (privacy-safe)

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import crypto from "crypto";

const REGION = process.env.MY_AWS_REGION;
const BUCKET = process.env.MY_AWS_BUCKET_NAME;
const PREFIX = process.env.QR_ANALYTICS_PREFIX || "qr-analytics/";

const s3 = new S3Client({
  region: REGION,
  credentials:
    process.env.MY_AWS_ACCESS_KEY_ID &&
    process.env.MY_AWS_SECRET_ACCESS_KEY
      ? {
          accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID,
          secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY,
        }
      : undefined,
});

// Hash IP for privacy while still tracking unique users
function hashIP(ip) {
  if (!ip) return null;
  return crypto.createHash("sha256").update(ip).digest("hex").slice(0, 16);
}

export const handler = async (event) => {
  try {
    const body = JSON.parse(event.body || "{}");

    const slug = body.name || null;
    const ip = body.ip || null;

    const logEntry = {
      slug,
      ts: Date.now(),
      ipHash: hashIP(ip),
    };

    const line = JSON.stringify(logEntry) + "\n";
    const key = `${PREFIX}${new Date().toISOString()}.json`;

    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: line,
      })
    );

    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true }),
    };
  } catch (err) {
    console.error("logHit error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message }),
    };
  }
};
