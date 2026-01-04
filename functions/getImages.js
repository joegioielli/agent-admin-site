// netlify/functions/getImages.js — ESM version

import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";

/* -------------------- S3 Setup -------------------- */
const REGION = process.env.MY_AWS_REGION || "us-east-2";
const BUCKET = process.env.MY_AWS_BUCKET_NAME;

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

/* -------------------- Helper for CORS -------------------- */
function corsHeaders() {
  return {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
}

/* -------------------- Handler -------------------- */
export async function handler(event) {
  try {
    // Handle CORS preflight
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 200, headers: corsHeaders(), body: "" };
    }

    if (event.httpMethod !== "GET") {
      return {
        statusCode: 405,
        headers: corsHeaders(),
        body: JSON.stringify({ error: "Method Not Allowed" }),
      };
    }

    const command = new ListObjectsV2Command({ Bucket: BUCKET });
    const data = await s3.send(command);

    const jpgs =
      data.Contents?.filter((obj) => obj.Key.toLowerCase().endsWith(".jpg"))
        .map((obj) => obj.Key) || [];

    return {
      statusCode: 200,
      headers: corsHeaders(),
      body: JSON.stringify({ ok: true, images: jpgs }),
    };
  } catch (err) {
    console.error("getImages error:", err);
    return {
      statusCode: 500,
      headers: corsHeaders(),
      body: JSON.stringify({
        ok: false,
        error: err.message,
        region: REGION,
        bucket: BUCKET,
      }),
    };
  }
}
