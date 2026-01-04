// netlify/functions/analytics.js
// Returns aggregated analytics for the last 90 days.

import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
} from "@aws-sdk/client-s3";

const BUCKET = process.env.MY_AWS_BUCKET_NAME;
const PREFIX = process.env.QR_ANALYTICS_PREFIX || "qr-analytics/";
const REGION = process.env.MY_AWS_REGION;

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

// Convert stream → string
async function streamToString(stream) {
  return await new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
  });
}

export const handler = async () => {
  try {
    if (!BUCKET) {
      return {
        statusCode: 500,
        body: "Missing MY_AWS_BUCKET_NAME env var",
      };
    }

    const now = new Date();
    const cutoff = new Date(now);
    cutoff.setDate(cutoff.getDate() - 90); // lookback: 90 days

    let allRecords = [];

    // ---- STEP 1: List all keys in prefix ----
    let ContinuationToken = undefined;

    do {
      const listCmd = new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: PREFIX,
        ContinuationToken,
      });

      const listed = await s3.send(listCmd);

      if (listed.Contents) {
        for (const item of listed.Contents) {
          // Parse timestamp from key
          const tsMatch = item.Key.match(/(\d{4}-\d{2}-\d{2}T.+)\.json$/);
          if (!tsMatch) continue;
          const ts = new Date(tsMatch[1]);

          if (ts >= cutoff) {
            // Load file
            const getCmd = new GetObjectCommand({
              Bucket: BUCKET,
              Key: item.Key,
            });

            const file = await s3.send(getCmd);
            const txt = await streamToString(file.Body);
            try {
              const json = JSON.parse(txt);
              allRecords.push(json);
            } catch (e) {}
          }
        }
      }

      ContinuationToken = listed.NextContinuationToken;
    } while (ContinuationToken);

    // ---- STEP 2: Aggregate ----
    const totals = {
      totalHits: allRecords.length,
      perRedirect: {},
      perDay: {},
      userAgents: {},
      referers: {},
      ips: {},
    };

    for (const r of allRecords) {
      const day = r.ts.split("T")[0];
      const name = r.name || "unknown";

      totals.perRedirect[name] = (totals.perRedirect[name] || 0) + 1;
      totals.perDay[day] = (totals.perDay[day] || 0) + 1;
      totals.userAgents[r.userAgent || "Unknown"] =
        (totals.userAgents[r.userAgent || "Unknown"] || 0) + 1;
      totals.referers[r.referer || "Direct"] =
        (totals.referers[r.referer || "Direct"] || 0) + 1;
      totals.ips[r.ip || "Unknown"] =
        (totals.ips[r.ip || "Unknown"] || 0) + 1;
    }

    return {
      statusCode: 200,
      body: JSON.stringify(totals),
    };
  } catch (err) {
    console.error("analytics.js error", err);
    return {
      statusCode: 500,
      body: "Error generating analytics",
    };
  }
};
