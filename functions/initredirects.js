// netlify/functions/initRedirects.js
import { makestore } from "./store.js";

export async function handler(event) {
  if (event.queryStringParameters?.pin !== process.env.ADMIN_PIN) {
    return { statusCode: 403, body: "Invalid PIN" };
  }

  const store = makestore();
  await store.setJSON([]);

  return {
    statusCode: 200,
    body: JSON.stringify({ ok: true, message: "Initialized redirects.json" })
  };
}
