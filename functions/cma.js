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

function normalizeState(value) {
  return clean(value).toUpperCase();
}

function extractItems(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.listings)) return payload.listings;
  if (Array.isArray(payload?.data)) return payload.data;
  return [];
}

function dedupeListings(items) {
  const seen = new Set();
  return items.filter((item) => {
    const key = clean(
      item?.id ||
      item?.listingId ||
      item?.propertyId ||
      item?.mlsNumber ||
      item?.formattedAddress ||
      item?.address ||
      `${item?.addressLine1 || ""}|${item?.city || ""}|${item?.state || ""}|${item?.zipCode || item?.postalCode || ""}`
    );
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function fetchRentcast(url, apiKey) {
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

  return { response, data, rawText };
}

function buildAttemptConfigs({ address, city, state, zipCode, limit, propertyType }) {
  const attempts = [];

  function addAttempt(label, params) {
    const normalized = Object.fromEntries(
      Object.entries(params).filter(([, value]) => clean(value) !== "")
    );
    const key = JSON.stringify(normalized);
    if (!attempts.some((attempt) => attempt.key === key)) {
      attempts.push({ label, params: normalized, key });
    }
  }

  addAttempt("street+city+state+zip", { address, city, state, zipCode, limit, propertyType });
  addAttempt("city+state+zip", { city, state, zipCode, limit, propertyType });
  addAttempt("city+state", { city, state, limit, propertyType });
  addAttempt("zip only", { zipCode, limit, propertyType });
  addAttempt("street+city+state", { address, city, state, limit, propertyType });
  addAttempt("city+state without type", { city, state, limit });
  addAttempt("zip only without type", { zipCode, limit });

  return attempts.filter((attempt) => {
    const hasAddress = clean(attempt.params.address);
    const hasCityState = clean(attempt.params.city) && clean(attempt.params.state);
    const hasZip = clean(attempt.params.zipCode);
    return hasAddress || hasCityState || hasZip;
  });
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
  const state = normalizeState(params.state);
  const zipCode = clean(params.zip || params.zipCode || params.postalCode);
  const limit = clean(params.limit || "60");
  const propertyType = clean(params.propertyType);

  if (!address && !(city && state) && !zipCode) {
    return json(400, {
      error: "Missing search input",
      details: "Provide an address, city and state, or a zip code.",
    });
  }

  try {
    const attempts = buildAttemptConfigs({ address, city, state, zipCode, limit, propertyType });
    const diagnostics = [];
    const combined = [];
    let lastError = null;

    for (const attempt of attempts) {
      const url = new URL(RENTCAST_BASE_URL);
      Object.entries(attempt.params).forEach(([key, value]) => {
        if (clean(value)) url.searchParams.set(key, value);
      });

      const { response, data, rawText } = await fetchRentcast(url, apiKey);
      const items = response.ok ? extractItems(data) : [];

      diagnostics.push({
        label: attempt.label,
        request: attempt.params,
        statusCode: response.status,
        resultCount: items.length,
      });

      if (!response.ok) {
        lastError = {
          statusCode: response.status,
          details: data?.message || data?.error || rawText || "Unknown upstream error",
        };
        continue;
      }

      combined.push(...items);
      if (combined.length >= Number(limit || 60)) break;
    }

    const data = dedupeListings(combined).slice(0, Number(limit || 60));

    if (!data.length && lastError) {
      return json(lastError.statusCode, {
        error: "RentCast request failed",
        details: lastError.details,
        request: {
          address,
          city,
          state,
          zipCode,
          limit,
          propertyType,
        },
        attempts: diagnostics,
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
      attempts: diagnostics,
      data,
    });
  } catch (error) {
    return json(500, {
      error: "Failed to fetch CMA data",
      details: error?.message || "Unknown error",
    });
  }
}
