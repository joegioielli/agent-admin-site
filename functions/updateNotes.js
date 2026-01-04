// netlify/functions/updateNotes.js
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";

/** Config from Netlify env */
const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";
const BUCKET = process.env.S3_BUCKET || process.env.SMARTSIGNS_BUCKET || "gioi-real-estate-bucket";

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

const streamToString = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
  });

export async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "" };
  }
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers: CORS, body: "Method Not Allowed" };
  }

  try {
    await s3.send(new HeadBucketCommand({ Bucket: BUCKET }));

    const body = JSON.parse(event.body || "{}");
    const listingId = body.listingId;
    const providedKey = body.key; // legacy param
    const note = (body.note ?? body.agentNotes ?? "").toString();

    // Resolve and validate the target key
    let detailsKey;
    if (listingId) {
      if (!/^[A-Za-z0-9_-]+$/.test(listingId)) {
        return { statusCode: 400, headers: CORS, body: "Invalid listingId" };
      }
      detailsKey = `listings/${listingId}/details.json`;
    } else if (providedKey) {
      // Only allow updates to listings/{id}/details.json
      if (!/^listings\/[A-Za-z0-9_-]+\/details\.json$/.test(providedKey)) {
        return { statusCode: 400, headers: CORS, body: "Invalid key format" };
      }
      detailsKey = providedKey;
    } else {
      return { statusCode: 400, headers: CORS, body: "Missing listingId or key" };
    }

    // Fetch existing details (create if missing)
    let details = {};
    try {
      const got = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: detailsKey }));
      details = JSON.parse(await streamToString(got.Body)) || {};
    } catch {
      // if details.json doesn't exist yet, we’ll create it
      details = {};
    }

    // Merge note
    details.agentNotes = note;
    details.agentNotesUpdatedAt = new Date().toISOString();

    // Save back
    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: detailsKey,
        Body: JSON.stringify(details, null, 2),
        ContentType: "application/json",
      })
    );

    return {
      statusCode: 200,
      headers: { ...CORS, "Content-Type": "application/json" },
      body: JSON.stringify({ ok: true, key: detailsKey, agentNotes: details.agentNotes }),
    };
  } catch (e) {
    return {
      statusCode: 502,
      headers: CORS,
      body: JSON.stringify({ error: e.message }),
    };
  }
}
