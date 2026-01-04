// netlify/functions/store.js
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

// Convert S3 stream to JSON
async function streamToJson(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString("utf-8") || "[]");
}

export function makestore() {
  const region = process.env.MY_AWS_REGION;
  const BUCKET = process.env.MY_AWS_BUCKET_NAME;  // ✔ correct variable
  const key = "redirects.json";

  if (!region) throw new Error("Missing MY_AWS_REGION env variable!");
  if (!BUCKET) throw new Error("Missing MY_AWS_BUCKET_NAME env variable!");

  const client = new S3Client({
    region,
    credentials: {
      accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY,
    },
  });

  return {
    async getJSON() {
      try {
        const res = await client.send(
          new GetObjectCommand({ Bucket: BUCKET, Key: key })
        );
        return await streamToJson(res.Body);
      } catch (err) {
        console.log("getJSON fallback:", err.message);
        return []; // return empty array if file doesn't exist
      }
    },

    async setJSON(value) {
      await client.send(
        new PutObjectCommand({
          Bucket: BUCKET,
          Key: key,
          ContentType: "application/json",
          Body: JSON.stringify(value, null, 2),
        })
      );
      return true;
    },
  };
}
