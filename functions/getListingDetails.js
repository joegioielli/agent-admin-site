// netlify/functions/getListingDetails.js
import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";

const REGION =
  process.env.MY_AWS_REGION || process.env.AWS_REGION || "us-east-2";

const BUCKET =
  process.env.SMARTSIGNS_BUCKET ||
  process.env.S3_BUCKET ||
  "gioi-real-estate-bucket";

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId:
      process.env.MY_AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey:
      process.env.MY_AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY,
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

export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 200, headers: cors(), body: "" };
    }
    if (event.httpMethod !== "GET") {
      return { statusCode: 405, headers: cors(), body: JSON.stringify({ ok: false, error: "Method Not Allowed" }) };
    }

    const id = (event.queryStringParameters?.listingId || "").trim();
    if (!id) {
      return { statusCode: 400, headers: cors(), body: JSON.stringify({ ok: false, error: "listingId is required" }) };
    }

    const key = `listings/${id}/details.json`;

    if (!(await headExists(key))) {
      return { statusCode: 200, headers: cors(), body: JSON.stringify({ ok: true, details: {} }) };
    }

    const obj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    const json = JSON.parse(await streamToString(obj.Body));

    return {
      statusCode: 200,
      headers: cors(),
      body: JSON.stringify({ ok: true, details: json }),
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
