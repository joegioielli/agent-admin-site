// functions/presignReplacePhoto.js
// Presigns a PUT to overwrite the single canonical photo:
//   photos/{listingId}/{listingId}.jpg
// Used by the dashboard "Replace Photo" button.

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const REGION =
  process.env.MY_AWS_REGION ||
  process.env.AWS_REGION ||
  "us-east-2";

const BUCKET =
  process.env.MY_AWS_BUCKET_NAME ||
  process.env.MY_AWS_BUCKET ||
  "gioi-real-estate-bucket";

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY,
  },
});

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    },
    body: JSON.stringify(body),
  };
}

export const handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
    if (event.httpMethod !== "POST")
      return json(405, { ok: false, error: "Method Not Allowed" });

    const { listingId, contentType } = JSON.parse(event.body || "{}");
    if (!listingId) return json(400, { ok: false, error: "Provide listingId" });

    // Basic guard so nobody can inject slashes into the key path
    const safeId = String(listingId);
    if (safeId.includes("/") || safeId.includes("\\")) {
      return json(400, { ok: false, error: "Invalid listingId" });
    }

    const ct = contentType || "image/jpeg";
    const key = `photos/${safeId}/${safeId}.jpg`;

    const cmd = new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      ContentType: ct,
      ServerSideEncryption: "AES256",
    });

    // 10 minutes is plenty for a single image upload
    const url = await getSignedUrl(s3, cmd, { expiresIn: 10 * 60 });

    return json(200, {
      ok: true,
      key,
      url,
      headers: {
        "x-amz-server-side-encryption": "AES256",
        "Content-Type": ct,
      },
    });
  } catch (err) {
    console.error("presignReplacePhoto error:", err);
    return json(500, { ok: false, error: err?.message || String(err) });
  }
};
