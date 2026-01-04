// netlify/functions/listredirects.js
import { makestore } from "./store.js";

export async function handler(event) {
  try {
    const store = makestore();
    const raw = (await store.getJSON()) || [];
    
    console.log('📋 LIST REDIRECTS:', {
      count: raw.length,
      names: raw.map(r => r.name || r.slug || r.id || r.label),
      bucket: process.env.MY_AWS_BUCKET_NAME || process.env.SMARTSIGNS_BUCKET
    });

    // Transform to clean list format
    const redirects = raw.map(item => ({
      name: item.name || item.slug || item.id || item.label || 'unnamed',
      url: item.url || '#',
      created: item.created || 'unknown'
    })).sort((a, b) => a.name.localeCompare(b.name));

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ 
        redirects, 
        count: redirects.length,
        bucket: process.env.MY_AWS_BUCKET_NAME || process.env.SMARTSIGNS_BUCKET 
      })
    };
  } catch (err) {
    console.error("listredirects error:", err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: err.message })
    };
  }
}
