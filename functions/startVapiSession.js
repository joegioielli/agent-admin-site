// netlify/functions/startVapiSession.js — ESM

import { S3Client, GetObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";

/* -------------------- Env & Clients -------------------- */
const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";
const BUCKET =
  process.env.MY_AWS_BUCKET_NAME ||
  process.env.S3_BUCKET || // legacy fallback
  "gioi-real-estate-bucket";

const VAPI_BASE_URL = process.env.VAPI_BASE_URL || "https://api.vapi.ai/v1";
const VAPI_API_KEY = process.env.VAPI_API_KEY;
const BASE_URL = process.env.BASE_URL; // your site base (e.g., https://mls-drop2.netlify.app)

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

/* -------------------- Helpers -------------------- */
const corsHeaders = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function json(statusCode, body) {
  return { statusCode, headers: corsHeaders, body: JSON.stringify(body) };
}

async function keyExists(Key) {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key }));
    return true;
  } catch {
    return false;
  }
}

async function readJsonFromS3(Key) {
  const obj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key }));
  const text = await obj.Body.transformToString();
  return JSON.parse(text);
}

/**
 * Try multiple likely S3 key patterns to locate the property JSON.
 */
async function loadPropertyJson(propertyId) {
  const candidates = [
    `listings/${propertyId}/details.json`,
    `listings/${propertyId}.json`,
    `details/${propertyId}.json`,
    `mvp-in/${propertyId}.json`,
    `${propertyId}.json`,
  ];
  for (const Key of candidates) {
    if (await keyExists(Key)) {
      const json = await readJsonFromS3(Key);
      return { json, key: Key };
    }
  }
  throw new Error(`Property JSON not found for id "${propertyId}" in expected paths.`);
}

/* -------------------- Handler -------------------- */
export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: corsHeaders, body: "" };
    if (event.httpMethod !== "POST") return json(405, { error: "Method Not Allowed" });

    if (!VAPI_API_KEY) return json(500, { error: "Missing VAPI_API_KEY env" });

    let body;
    try {
      body = JSON.parse(event.body || "{}");
    } catch {
      return json(400, { error: "Invalid JSON body" });
    }

    const { propertyId, callerId } = body || {};
    if (!propertyId) return json(400, { error: "Missing propertyId" });

    // 1) Load property JSON from S3 (try several key patterns)
    const { json: propertyData, key: s3Key } = await loadPropertyJson(propertyId);

    // 2) Build webhook URL (prefer env BASE_URL)
    const webhookUrl =
      BASE_URL
        ? `${BASE_URL}/.netlify/functions/vapiWebhook`
        : `https://${process.env.URL || process.env.DEPLOY_URL || "mls-drop2.netlify.app"}/.netlify/functions/vapiWebhook`;

    // 3) Start Vapi session
    const resp = await fetch(`${VAPI_BASE_URL}/session`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${VAPI_API_KEY}`,
      },
      body: JSON.stringify({
        callerId,
        context: {
          system: `Property data: ${JSON.stringify(propertyData)}`,
          s3Key,
        },
        webhookUrl,
      }),
    });

    const respText = await resp.text();
    if (!resp.ok) {
      console.error("Vapi session error:", resp.status, respText);
      return json(resp.status, { error: "Failed to start Vapi session", detail: respText });
    }

    let payload = {};
    try {
      payload = JSON.parse(respText);
    } catch {
      // If Vapi returns non-JSON for any reason, still return raw
      payload = { raw: respText };
    }

    // Return session payload (keep original shape with sessionId if present)
    return json(200, {
      ok: true,
      sessionId: payload.sessionId || payload.id || null,
      payload,
    });
  } catch (err) {
    console.error("startVapiSession error:", err);
    return json(500, {
      error: "Internal server error",
      message: err.message,
      bucket: BUCKET,
      region: REGION,
    });
  }
}
