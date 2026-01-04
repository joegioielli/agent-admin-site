// netlify/functions/updateredirects.js
import { makestore } from "./store.js";
import { 
  S3Client, 
  ListObjectsV2Command, 
  DeleteObjectCommand, 
  GetObjectCommand 
} from "@aws-sdk/client-s3";

const s3 = new S3Client({
  region: process.env.MY_AWS_REGION,
  credentials: process.env.MY_AWS_ACCESS_KEY_ID ? {
    accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY
  } : undefined
});

// Convert S3 stream to text
async function streamToString(stream) {
  return await new Response(stream).text();
}

export async function handler(event) {
  const qs = event.queryStringParameters || {};
  const mode = qs.mode;
  const name = qs.name;
  const url = qs.url;

  if (!mode) {
    return { statusCode: 400, body: "Missing mode" };
  }

  const store = makestore();

  let raw;
  try {
    raw = (await store.getJSON()) || [];
  } catch (err) {
    console.error("updateredirects getJSON error:", err);
    return { statusCode: 500, body: "Error reading redirects" };
  }

  // Normalize existing data to { name, url }
  const redirects = raw.map((r) => ({
    name: r.name || r.slug || r.id || r.label,
    url: r.url,
  }));

  // ADD
  if (mode === "add") {
    if (!name || !url) {
      return { statusCode: 400, body: "Missing name or url" };
    }

    const filtered = redirects.filter((r) => r.name !== name);
    filtered.push({ name, url });

    await store.setJSON(filtered);
    return json({ ok: true, message: "Added", name, url });
  }

  // REMOVE (w/ content-aware analytics cleanup)
  if (mode === "remove") {
    if (!name) {
      return { statusCode: 400, body: "Missing name" };
    }

    const filtered = redirects.filter((r) => r.name !== name);
    await store.setJSON(filtered);

    // 🔥 SCAN & DELETE ANALYTICS CONTAINING THIS SLUG
    try {
      const analyticsPrefix = process.env.QR_ANALYTICS_PREFIX || "qr-analytics/";
      const listed = await s3.send(new ListObjectsV2Command({
        Bucket: process.env.MY_AWS_BUCKET_NAME,
        Prefix: analyticsPrefix
      }));
      
      let deletedCount = 0;
      let scannedCount = 0;
      
      // Only scan recent files (90d window)
      for (const obj of listed.Contents || []) {
        const fileAgeMs = Date.now() - new Date(obj.LastModified).getTime();
        if (fileAgeMs > 90 * 24 * 60 * 60 * 1000) continue; // Skip old
        
        scannedCount++;
        const file = await s3.send(new GetObjectCommand({
          Bucket: process.env.MY_AWS_BUCKET_NAME,
          Key: obj.Key
        }));
        
        const text = await streamToString(file.Body);
        const lines = text.trim().split('\n');
        
        // Check if ANY line contains our slug
        const hasSlug = lines.some(line => {
          try {
            return JSON.parse(line)?.slug === name;
          } catch {
            return false;
          }
        });
        
        if (hasSlug) {
          await s3.send(new DeleteObjectCommand({
            Bucket: process.env.MY_AWS_BUCKET_NAME,
            Key: obj.Key
          }));
          deletedCount++;
          console.log(`🗑️ Deleted ${obj.Key} (contained ${name})`);
        }
      }
      
      console.log(`📊 Scanned ${scannedCount} files, deleted ${deletedCount} for ${name}`);
    } catch (cleanupErr) {
      console.warn(`Analytics cleanup failed for ${name}:`, cleanupErr);
    }

    return json({ 
      ok: true, 
      message: `Removed ${name} + scanned ${scannedCount || 0} analytics files (deleted ${deletedCount || 0})` 
    });
  }

  // EDIT
  if (mode === "edit") {
    if (!name || !url) {
      return { statusCode: 400, body: "Missing name or url" };
    }

    const updated = redirects.map((r) =>
      r.name === name ? { ...r, url } : r
    );

    await store.setJSON(updated);
    return json({ ok: true, message: "Updated", name, url });
  }

  return { statusCode: 400, body: "Invalid mode" };
}

function json(obj) {
  return {
    statusCode: 200,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(obj),
  };
}
