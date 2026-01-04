// netlify/functions/vapiWebhook.js — ESM + AWS SDK v3 + Node 20 fetch

import { S3Client, GetObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";

/* -------------------- Env -------------------- */
const REGION = process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";
const BUCKET =
  process.env.SMARTSIGNS_BUCKET ||
  process.env.S3_BUCKET || // legacy fallback you used before
  "gioi-real-estate-bucket";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_URL = "https://api.openai.com/v1/chat/completions";

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

/* -------------------- Helpers -------------------- */
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

/** Try multiple likely S3 key patterns to locate the property JSON */
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
  throw new Error(`Property JSON not found for id "${propertyId}"`);
}

/* -------------------- Handler -------------------- */
export async function handler(event) {
  let body;
  try {
    body = JSON.parse(event.body || "{}");
  } catch (err) {
    console.error("vapiWebhook: invalid JSON:", err);
    return {
      statusCode: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Invalid JSON" }),
    };
  }

  const vapiEvent = body.event;
  const transcript = (body.transcript || "").trim();
  const fromNumber = body.from || "";
  const metadata = body.metadata || {};
  let responsePayload;

  console.log(
    `🔔 vapiWebhook event="${vapiEvent}" from=${fromNumber} metadata=${JSON.stringify(
      metadata
    )}`
  );

  try {
    if (vapiEvent === "call_initiated") {
      responsePayload = {
        say: {
          text:
            "Hello! You requested information on a property. Please say the property number now.",
        },
        listen: true,
      };
    } else if (vapiEvent === "transcription") {
      let userPrompt = transcript;

      // Determine listing ID from metadata or speech
      const idMatch = userPrompt.match(/\b\d{2,}\b/);
      const propertyId = metadata.listingId || (idMatch && idMatch[0]);

      if (!propertyId) {
        responsePayload = {
          say: {
            text:
              "I did not catch a valid property number. Please say just the digits of the property ID.",
          },
          listen: true,
        };
      } else {
        // 1) Fetch property JSON from S3 (try several known locations)
        const { json: contextData, key: s3Key } = await loadPropertyJson(propertyId);

        // 2) Build OpenAI messages with the property context
        const messages = [
          {
            role: "system",
            content:
              "You are a helpful real-estate assistant. Use the following property data to answer user questions.",
          },
          { role: "system", content: JSON.stringify(contextData) },
          { role: "user", content: userPrompt },
        ];

        if (!OPENAI_API_KEY) {
          console.warn("OPENAI_API_KEY not set; returning fallback response");
          responsePayload = {
            say: {
              text:
                "I have your property context, but AI is not configured right now.",
            },
            listen: true,
          };
        } else {
          // 3) Call OpenAI (Node 20 fetch)
          const aiRes = await fetch(OPENAI_URL, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${OPENAI_API_KEY}`,
            },
            body: JSON.stringify({
              model: "gpt-4o-mini",
              messages,
            }),
          });

          if (!aiRes.ok) {
            const errText = await aiRes.text();
            throw new Error(`OpenAI error ${aiRes.status}: ${errText}`);
          }

          const data = await aiRes.json();
          const aiText = data?.choices?.[0]?.message?.content || "I’m here.";
          responsePayload = { say: { text: aiText }, listen: true, s3Key };
        }
      }
    } else if (vapiEvent === "call_ended") {
      console.log(`📞 [${fromNumber}] Call ended.`);
      return { statusCode: 200, body: "" };
    } else {
      responsePayload = {
        say: {
          text:
            "I’m sorry, I didn’t understand that. Please say the property number now.",
        },
        listen: true,
      };
    }

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(responsePayload),
    };
  } catch (err) {
    console.error("vapiWebhook error:", err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: "Internal server error",
        message: err?.message || String(err),
        region: REGION,
        bucket: BUCKET,
      }),
    };
  }
}
