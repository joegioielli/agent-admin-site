// netlify/functions/cma.js
// Proxy RentCast sale listing data for the CMA tool.
// TODO: Expand this to merge valuation, property, and comparable endpoints for the full report.

const RENTCAST_BASE_URL = "https://api.rentcast.io/v1/listings/sale";

function corsHeaders(extra = {}) {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    ...extra,
  };
}

function json(statusCode, payload) {
  return {
    statusCode,
    headers: corsHeaders({ "Content-Type": "application/json; charset=utf-8" }),
    body: JSON.stringify(payload),
  };
}

function clean(value) {
  return String(value ?? "").trim();
}

export async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: corsHeaders(),
      body: "",
    };
  }

  const apiKey = clean(process.env.RENTCAST_API_KEY);
  if (!apiKey) {
    return json(500, {
      error: "Missing RENTCAST_API_KEY",
      details: "Add RENTCAST_API_KEY to the Netlify environment before using the CMA function.",
    });
  }

  const params = event.queryStringParameters || {};
  const address = clean(params.address || params.address1 || params.street);
  const city = clean(params.city);
  const state = clean(params.state);
  const zipCode = clean(params.zip || params.zipCode || params.postalCode);
  const limit = clean(params.limit || "10");
  const propertyType = clean(params.propertyType);

  if (!address && !(city && state)) {
    return json(400, {
      error: "Missing search input",
      details: "Provide at least an address, or send both city and state.",
    });
  }

  try {
    const url = new URL(RENTCAST_BASE_URL);

    if (address) url.searchParams.set("address", address);
    if (city) url.searchParams.set("city", city);
    if (state) url.searchParams.set("state", state);
    if (zipCode) url.searchParams.set("zipCode", zipCode);
    if (limit) url.searchParams.set("limit", limit);
    if (propertyType) url.searchParams.set("propertyType", propertyType);

    const response = await fetch(url, {
      headers: {
        Accept: "application/json",
        "X-Api-Key": apiKey,
      },
    });

    const rawText = await response.text();
    let data;

    try {
      data = rawText ? JSON.parse(rawText) : null;
    } catch {
      data = { raw: rawText };
    }

    if (!response.ok) {
      return json(response.status, {
        error: "RentCast request failed",
        details: data?.message || data?.error || rawText || "Unknown upstream error",
        request: {
          address,
          city,
          state,
          zipCode,
          limit,
          propertyType,
        },
      });
    }

    return json(200, {
      source: "rentcast",
      request: {
        address,
        city,
        state,
        zipCode,
        limit,
        propertyType,
      },
      data,
    });
  } catch (error) {
    return json(500, {
      error: "Failed to fetch CMA data",
      details: error?.message || "Unknown error",
    });
  }
}
