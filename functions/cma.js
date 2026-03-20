// netlify/functions/cma.js
// Hybrid CMA data source:
// - RentCast for AVM + live listing coverage
// - ATTOM for exact subject-property detail + sold-sale fallback

const RENTCAST_AVM_URL = "https://api.rentcast.io/v1/avm/value";
const RENTCAST_LISTINGS_URL = "https://api.rentcast.io/v1/listings/sale";

const ATTOM_BASE_URL = "https://api.gateway.attomdata.com/propertyapi/v1.0.0";
const ATTOM_PROPERTY_DETAIL_URL = `${ATTOM_BASE_URL}/property/detail`;
const ATTOM_SALE_SNAPSHOT_URL = `${ATTOM_BASE_URL}/sale/snapshot`;
const ATTOM_MAX_RADIUS = 20;

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

function toFiniteNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function normalizeKey(value) {
  return clean(value).toLowerCase().replace(/[^\w\s]/g, " ").replace(/\s+/g, " ").trim();
}

function extractItems(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.comparables)) return payload.comparables;
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.listings)) return payload.listings;
  if (Array.isArray(payload?.data)) return payload.data;
  return [];
}

function getPathValue(obj, path) {
  return String(path)
    .split(".")
    .reduce((current, part) => (current == null ? undefined : current[part]), obj);
}

function pickFirstValue(obj, paths, fallback = null) {
  for (const path of paths) {
    const value = getPathValue(obj, path);
    if (value !== undefined && value !== null && clean(value) !== "") return value;
  }
  return fallback;
}

function comparableKey(item) {
  const direct = normalizeKey(
    item?.formattedAddress ||
    item?.address ||
    item?.fullAddress ||
    item?.oneLine
  );
  if (direct) return direct;

  const line1 = normalizeKey(item?.addressLine1 || item?.line1);
  const city = normalizeKey(item?.city || item?.locality);
  const state = normalizeKey(item?.state || item?.countrySubd);
  const zipCode = normalizeKey(item?.zipCode || item?.postalCode || item?.postal1);

  return [line1, city, state, zipCode].filter(Boolean).join("|");
}

function dedupeListings(items) {
  const seen = new Set();
  return items.filter((item) => {
    const key = comparableKey(item) || clean(
      item?.id ||
      item?.listingId ||
      item?.propertyId ||
      item?.mlsNumber
    );
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function mergeDefined(existing, incoming) {
  const merged = { ...existing };
  for (const [key, value] of Object.entries(incoming || {})) {
    if (value == null) continue;
    if (typeof value === "string" && clean(value) === "") continue;
    merged[key] = value;
  }
  return merged;
}

function mergeComparablePools(...pools) {
  const merged = new Map();

  for (const item of pools.flat()) {
    const identity = comparableKey(item) || clean(item?.id || item?.listingId || item?.propertyId);
    if (!identity) continue;
    const current = merged.get(identity);
    merged.set(identity, current ? mergeDefined(current, item) : item);
  }

  return Array.from(merged.values());
}

async function fetchJson(url, headers) {
  const response = await fetch(url, { headers });
  const rawText = await response.text();
  let data;

  try {
    data = rawText ? JSON.parse(rawText) : null;
  } catch {
    data = { raw: rawText };
  }

  return { response, data, rawText };
}

function fetchRentcast(url, apiKey) {
  return fetchJson(url, {
    Accept: "application/json",
    "X-Api-Key": apiKey,
  });
}

function fetchAttom(url, apiKey) {
  return fetchJson(url, {
    Accept: "application/json",
    APIKey: apiKey,
  });
}

function buildAttemptList(builders) {
  const attempts = [];

  for (const builder of builders) {
    const params = Object.fromEntries(
      Object.entries(builder.params || {}).filter(([, value]) => clean(value) !== "")
    );
    const key = JSON.stringify(params);
    if (!params || !Object.keys(params).length || attempts.some((attempt) => attempt.key === key)) continue;
    attempts.push({ ...builder, params, key });
  }

  return attempts;
}

function buildListingAttempts({ address, city, state, zipCode, limit, propertyType }) {
  return buildAttemptList([
    { label: "street+city+state+zip", params: { address, city, state, zipCode, limit, propertyType } },
    { label: "city+state+zip", params: { city, state, zipCode, limit, propertyType } },
    { label: "city+state", params: { city, state, limit, propertyType } },
    { label: "zip only", params: { zipCode, limit, propertyType } },
    { label: "street+city+state", params: { address, city, state, limit, propertyType } },
    { label: "city+state without type", params: { city, state, limit } },
    { label: "zip only without type", params: { zipCode, limit } },
  ]).filter((attempt) => {
    const hasAddress = clean(attempt.params.address);
    const hasCityState = clean(attempt.params.city) && clean(attempt.params.state);
    const hasZip = clean(attempt.params.zipCode);
    return hasAddress || hasCityState || hasZip;
  });
}

function normalizeAttomAddress2(city, state, zipCode) {
  const locality = clean(city);
  const region = normalizeState(state);
  const postal = clean(zipCode);
  if (!locality && !region && !postal) return "";
  return [locality, [region, postal].filter(Boolean).join(" ")].filter(Boolean).join(", ");
}

function buildFullAddress(address, city, state, zipCode) {
  return [clean(address), normalizeAttomAddress2(city, state, zipCode)].filter(Boolean).join(", ");
}

function mapPropertyIndicator(propertyType) {
  const normalized = normalizeKey(propertyType);
  if (!normalized) return "";
  if (/(single|detached|sfh|house|townhome|townhouse)/.test(normalized)) return "10";
  if (/condo|condominium/.test(normalized)) return "11";
  if (/duplex|triplex|quad|multi/.test(normalized)) return "21";
  if (/apartment/.test(normalized)) return "22";
  if (/commercial/.test(normalized)) return "20";
  if (/vacant|lot|land/.test(normalized)) return "80";
  return "";
}

function formatAttomDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}/${month}/${day}`;
}

function monthsAgo(date, months) {
  const copy = new Date(date.getTime());
  copy.setMonth(copy.getMonth() - months);
  return copy;
}

function extractAttomRecords(payload) {
  if (Array.isArray(payload?.property)) return payload.property;
  if (Array.isArray(payload?.properties)) return payload.properties;
  if (Array.isArray(payload?.data?.property)) return payload.data.property;
  if (Array.isArray(payload?.data?.properties)) return payload.data.properties;
  if (Array.isArray(payload?.response?.property)) return payload.response.property;
  if (Array.isArray(payload?.response?.properties)) return payload.response.properties;
  if (payload?.property && typeof payload.property === "object") return [payload.property];
  if (payload?.data?.property && typeof payload.data.property === "object") return [payload.data.property];
  return [];
}

function summarizeAttomPayload(response, data) {
  const code = toFiniteNumber(data?.status?.code, null);
  const message = clean(data?.status?.msg || data?.status?.description);
  const total = toFiniteNumber(data?.status?.total, null);
  const records = extractAttomRecords(data);
  const noResults = code === 400 || /successwithoutresult/i.test(message);
  const hasResults = response.ok && !noResults && records.length > 0;

  return {
    code,
    message,
    total,
    hasResults,
    noResults,
    resultCount: records.length,
  };
}

function coerceBathrooms(value) {
  const numeric = toFiniteNumber(value, null);
  return numeric != null ? numeric : clean(value);
}

function buildGarageSummary(record) {
  const spaces = toFiniteNumber(pickFirstValue(record, [
    "building.parking.prkgSpaces",
    "building.parking.spaces",
  ]), null);
  const garageType = clean(pickFirstValue(record, [
    "building.parking.garagetype",
    "building.parking.prkgType",
    "building.parking.garageType",
  ]));

  return [Number.isFinite(spaces) ? `${spaces}-space` : "", garageType].filter(Boolean).join(" ").trim();
}

function mapAttomRecord(record, { fallbackStatus = "Sold", source = "attom-sale" } = {}) {
  const line1 = clean(pickFirstValue(record, [
    "address.line1",
    "address.lineOne",
  ]));
  const city = clean(pickFirstValue(record, [
    "address.locality",
    "address.city",
  ]));
  const state = normalizeState(pickFirstValue(record, [
    "address.countrySubd",
    "address.state",
  ]));
  const zipCode = clean(pickFirstValue(record, [
    "address.postal1",
    "address.zip",
    "address.zipCode",
  ]));
  const formattedAddress = clean(pickFirstValue(record, [
    "address.oneLine",
    "address.fullAddress",
  ], [line1, [city, state, zipCode].filter(Boolean).join(" ")].filter(Boolean).join(", ")));
  const garageSummary = buildGarageSummary(record);
  const lotSizeAcres = toFiniteNumber(pickFirstValue(record, ["lot.lotsize1", "lot.lotSize1"]), null);
  const lotSizeSquareFeet = toFiniteNumber(pickFirstValue(record, ["lot.lotsize2", "lot.lotSize2"]), null);

  return {
    id: clean(pickFirstValue(record, [
      "identifier.attomId",
      "identifier.Id",
      "identifier.id",
      "identifier.obPropId",
    ], formattedAddress)),
    propertyId: clean(pickFirstValue(record, [
      "identifier.attomId",
      "identifier.obPropId",
    ])),
    formattedAddress,
    addressLine1: line1,
    city,
    state,
    zipCode,
    status: clean(pickFirstValue(record, ["status"], fallbackStatus)) || fallbackStatus,
    soldPrice: toFiniteNumber(pickFirstValue(record, [
      "sale.amount.saleamt",
      "sale.saleAmount",
      "sale.amount.amount",
    ]), null),
    squareFootage: toFiniteNumber(pickFirstValue(record, [
      "building.size.universalsize",
      "building.size.livingsize",
      "building.size.grosssizeadjusted",
      "building.size.grosssize",
      "building.size.sizeLiving",
    ]), null),
    bedrooms: toFiniteNumber(pickFirstValue(record, [
      "building.rooms.beds",
      "building.rooms.bedrooms",
    ]), null),
    bathrooms: coerceBathrooms(pickFirstValue(record, [
      "building.rooms.bathsTotal",
      "building.rooms.bathstotal",
      "building.rooms.bathsfull",
    ], null)),
    yearBuilt: toFiniteNumber(pickFirstValue(record, [
      "summary.yearbuilt",
      "building.summary.yearbuilt",
      "building.summary.yearBuilt",
    ]), null),
    lotSizeAcres,
    lotSizeSquareFeet,
    distance: toFiniteNumber(pickFirstValue(record, [
      "location.distance",
      "location.dist",
    ]), null),
    subdivision: clean(pickFirstValue(record, [
      "area.subdname",
      "area.neighborhood",
      "area.munName",
    ])),
    propertyType: clean(pickFirstValue(record, [
      "summary.proptype",
      "summary.propsubtype",
      "summary.propertyType",
      "summary.propclass",
    ])),
    stories: pickFirstValue(record, [
      "building.summary.levels",
      "building.summary.storyDesc",
      "building.summary.storydescription",
    ], ""),
    architecturalStyle: clean(pickFirstValue(record, [
      "building.parking.archStyle",
      "building.summary.bldgType",
      "building.summary.imprType",
    ])),
    garageSpaces: toFiniteNumber(pickFirstValue(record, [
      "building.parking.prkgSpaces",
      "building.parking.spaces",
    ]), null),
    garage: garageSummary,
    parking: garageSummary,
    soldDate: clean(pickFirstValue(record, [
      "sale.saleTransDate",
      "sale.amount.salerecdate",
      "sale.recordingDate",
    ])),
    latitude: toFiniteNumber(pickFirstValue(record, [
      "location.latitude",
      "location.lat",
    ]), null),
    longitude: toFiniteNumber(pickFirstValue(record, [
      "location.longitude",
      "location.lon",
      "location.lng",
    ]), null),
    remarks: clean(pickFirstValue(record, [
      "summary.absenteeInd",
      "sale.salesearchdate",
    ])),
    source,
  };
}

function estimateSoldComparableCount(items) {
  return items.filter((item) => {
    const soldPrice = toFiniteNumber(
      item?.soldPrice ||
      item?.lastSalePrice ||
      item?.salePrice ||
      item?.closedPrice,
      null
    );
    const status = normalizeKey(
      item?.status ||
      item?.listingStatus ||
      item?.propertyStatus ||
      item?.saleType
    );
    return soldPrice != null || /\b(sold|closed|settled|off market)\b/.test(status);
  }).length;
}

async function fetchRentcastBundle({
  address,
  city,
  state,
  zipCode,
  limit,
  propertyType,
  searchRadius,
  compCount,
  apiKey,
}) {
  const diagnostics = [];

  const avmUrl = new URL(RENTCAST_AVM_URL);
  if (address) avmUrl.searchParams.set("address", address);
  if (city) avmUrl.searchParams.set("city", city);
  if (state) avmUrl.searchParams.set("state", state);
  if (zipCode) avmUrl.searchParams.set("zipCode", zipCode);
  avmUrl.searchParams.set("compCount", String(compCount));
  avmUrl.searchParams.set("maxRadius", String(searchRadius));

  const avmFetch = await fetchRentcast(avmUrl, apiKey);
  const avmComparables = dedupeListings(extractItems(avmFetch.data?.comparables || avmFetch.data));
  const avmSubject = avmFetch.data?.subjectProperty || null;

  diagnostics.push({
    source: "rentcast-avm",
    statusCode: avmFetch.response.status,
    request: {
      address,
      city,
      state,
      zipCode,
      compCount,
      maxRadius: searchRadius,
    },
    subjectFound: Boolean(avmSubject),
    resultCount: avmComparables.length,
  });

  const combined = [...avmComparables];
  const listingAttempts = buildListingAttempts({ address, city, state, zipCode, limit, propertyType });
  let responseSource = avmComparables.length ? "rentcast-avm" : "rentcast-listings";
  let lastError = avmFetch.response.ok
    ? null
    : {
        statusCode: avmFetch.response.status,
        details: avmFetch.data?.message || avmFetch.data?.error || avmFetch.rawText || "Unknown upstream error",
      };

  for (const attempt of listingAttempts) {
    if (combined.length >= Number(limit || 60)) break;

    const url = new URL(RENTCAST_LISTINGS_URL);
    Object.entries(attempt.params).forEach(([key, value]) => {
      if (clean(value)) url.searchParams.set(key, value);
    });

    const listingFetch = await fetchRentcast(url, apiKey);
    const items = listingFetch.response.ok ? extractItems(listingFetch.data) : [];

    diagnostics.push({
      source: "rentcast-listings",
      label: attempt.label,
      request: attempt.params,
      statusCode: listingFetch.response.status,
      resultCount: items.length,
    });

    if (!listingFetch.response.ok) {
      lastError = {
        statusCode: listingFetch.response.status,
        details: listingFetch.data?.message || listingFetch.data?.error || listingFetch.rawText || "Unknown upstream error",
      };
      continue;
    }

    combined.push(...items);
    if (items.length) responseSource = avmSubject ? "rentcast-avm+listings" : "rentcast-listings";
  }

  return {
    diagnostics,
    source: responseSource,
    subjectProperty: avmSubject,
    comparables: dedupeListings(combined).slice(0, Number(limit || 60)),
    avm: avmFetch.response.ok ? avmFetch.data : null,
    lastError,
  };
}

function buildAttomSubjectAttempts({ fullAddress, address, address2 }) {
  return buildAttemptList([
    { label: "full-address", params: { address: fullAddress } },
    { label: "address1+address2", params: { address1: address, address2 } },
  ]);
}

function buildAttomSaleAttempts({
  fullAddress,
  address,
  address2,
  zipCode,
  radius,
  pageSize,
  propertyIndicator,
  startSaleTransDate,
  endSaleTransDate,
}) {
  return buildAttemptList([
    {
      label: "full-address+radius+indicator",
      params: { address: fullAddress, radius, pageSize, propertyIndicator, startSaleTransDate, endSaleTransDate },
    },
    {
      label: "address1+address2+radius+indicator",
      params: { address1: address, address2, radius, pageSize, propertyIndicator, startSaleTransDate, endSaleTransDate },
    },
    {
      label: "full-address+radius",
      params: { address: fullAddress, radius, pageSize, startSaleTransDate, endSaleTransDate },
    },
    {
      label: "address1+address2+radius",
      params: { address1: address, address2, radius, pageSize, startSaleTransDate, endSaleTransDate },
    },
    {
      label: "zip+indicator",
      params: { postalCode: zipCode, pageSize, propertyIndicator, startSaleTransDate, endSaleTransDate },
    },
    {
      label: "zip only",
      params: { postalCode: zipCode, pageSize, startSaleTransDate, endSaleTransDate },
    },
  ]).filter((attempt) => {
    const hasAddress = clean(attempt.params.address) || (clean(attempt.params.address1) && clean(attempt.params.address2));
    const hasZip = clean(attempt.params.postalCode);
    return hasAddress || hasZip;
  });
}

function buildAttomError(fetchResult, summary) {
  return {
    statusCode: fetchResult.response.status,
    details: summary.message || fetchResult.data?.status?.description || fetchResult.rawText || "Unknown upstream error",
  };
}

async function fetchAttomBundle({
  address,
  city,
  state,
  zipCode,
  propertyType,
  searchRadius,
  compCount,
  limit,
  lookbackMonths,
  apiKey,
}) {
  const diagnostics = [];
  const fullAddress = buildFullAddress(address, city, state, zipCode);
  const address2 = normalizeAttomAddress2(city, state, zipCode);
  const propertyIndicator = mapPropertyIndicator(propertyType);
  const saleHistoryMonths = clamp(Math.max(toFiniteNumber(lookbackMonths, 6) * 2, 24), 24, 36);
  const radius = clamp(searchRadius || 3, 0.25, ATTOM_MAX_RADIUS);
  const pageSize = clamp(Math.max(Number(limit || 60), compCount * 3), 20, 100);
  const startSaleTransDate = formatAttomDate(monthsAgo(new Date(), saleHistoryMonths));
  const endSaleTransDate = formatAttomDate(new Date());

  let subjectProperty = null;
  let lastError = null;

  for (const attempt of buildAttomSubjectAttempts({ fullAddress, address, address2 })) {
    const url = new URL(ATTOM_PROPERTY_DETAIL_URL);
    Object.entries(attempt.params).forEach(([key, value]) => url.searchParams.set(key, value));

    const result = await fetchAttom(url, apiKey);
    const summary = summarizeAttomPayload(result.response, result.data);

    diagnostics.push({
      source: "attom-property",
      label: attempt.label,
      request: attempt.params,
      statusCode: result.response.status,
      attomStatusCode: summary.code,
      attomMessage: summary.message,
      resultCount: summary.resultCount,
    });

    if (summary.hasResults) {
      subjectProperty = mapAttomRecord(extractAttomRecords(result.data)[0], {
        fallbackStatus: "Subject",
        source: "attom-property",
      });
      break;
    }

    if (!summary.noResults || !result.response.ok) {
      lastError = buildAttomError(result, summary);
    }
  }

  const comparables = [];

  for (const attempt of buildAttomSaleAttempts({
    fullAddress,
    address,
    address2,
    zipCode,
    radius,
    pageSize,
    propertyIndicator,
    startSaleTransDate,
    endSaleTransDate,
  })) {
    if (comparables.length >= Math.min(pageSize, Number(limit || 60))) break;

    const url = new URL(ATTOM_SALE_SNAPSHOT_URL);
    Object.entries(attempt.params).forEach(([key, value]) => url.searchParams.set(key, value));

    const result = await fetchAttom(url, apiKey);
    const summary = summarizeAttomPayload(result.response, result.data);
    const records = summary.hasResults ? extractAttomRecords(result.data) : [];

    diagnostics.push({
      source: "attom-sale",
      label: attempt.label,
      request: attempt.params,
      statusCode: result.response.status,
      attomStatusCode: summary.code,
      attomMessage: summary.message,
      resultCount: summary.resultCount,
    });

    if (!summary.hasResults) {
      if (!summary.noResults || !result.response.ok) {
        lastError = buildAttomError(result, summary);
      }
      continue;
    }

    comparables.push(
      ...records.map((record) => mapAttomRecord(record, {
        fallbackStatus: "Sold",
        source: "attom-sale",
      }))
    );
  }

  const dedupedComparables = dedupeListings(comparables).slice(0, Number(limit || 60));
  const sourceParts = [];
  if (subjectProperty) sourceParts.push("attom-property");
  if (dedupedComparables.length) sourceParts.push("attom-sale");

  return {
    diagnostics,
    source: sourceParts.join("+"),
    subjectProperty,
    comparables: dedupedComparables,
    lastError,
  };
}

export async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: corsHeaders(),
      body: "",
    };
  }

  const rentcastApiKey = clean(process.env.RENTCAST_API_KEY);
  const attomApiKey = clean(process.env.ATTOM_API_KEY);

  if (!rentcastApiKey && !attomApiKey) {
    return json(500, {
      error: "Missing CMA provider credentials",
      details: "Add RENTCAST_API_KEY, ATTOM_API_KEY, or both to the Netlify environment before using the CMA function.",
    });
  }

  const params = event.queryStringParameters || {};
  const address = clean(params.address || params.address1 || params.street);
  const city = clean(params.city);
  const state = normalizeState(params.state);
  const zipCode = clean(params.zip || params.zipCode || params.postalCode);
  const limit = clean(params.limit || "60");
  const propertyType = clean(params.propertyType);
  const searchRadius = toFiniteNumber(params.searchRadius, 3);
  const compCount = Math.max(4, Math.min(toFiniteNumber(params.compCount, 20), 50));
  const lookbackMonths = clamp(toFiniteNumber(params.lookbackMonths, 6), 1, 24);

  if (!address && !(city && state) && !zipCode) {
    return json(400, {
      error: "Missing search input",
      details: "Provide an address, city and state, or a zip code.",
    });
  }

  try {
    const diagnostics = [];
    let subjectProperty = null;
    let comparables = [];
    let avm = null;
    let lastError = null;
    const sourceParts = [];

    if (rentcastApiKey) {
      const rentcastBundle = await fetchRentcastBundle({
        address,
        city,
        state,
        zipCode,
        limit,
        propertyType,
        searchRadius,
        compCount,
        apiKey: rentcastApiKey,
      });

      diagnostics.push(...rentcastBundle.diagnostics);
      if (rentcastBundle.source) sourceParts.push(rentcastBundle.source);
      if (rentcastBundle.subjectProperty) subjectProperty = rentcastBundle.subjectProperty;
      if (rentcastBundle.comparables.length) comparables = mergeComparablePools(comparables, rentcastBundle.comparables);
      if (rentcastBundle.avm) avm = rentcastBundle.avm;
      if (rentcastBundle.lastError) lastError = rentcastBundle.lastError;
    }

    if (attomApiKey) {
      const attomBundle = await fetchAttomBundle({
        address,
        city,
        state,
        zipCode,
        propertyType,
        searchRadius,
        compCount,
        limit,
        lookbackMonths,
        apiKey: attomApiKey,
      });

      diagnostics.push(...attomBundle.diagnostics);
      if (attomBundle.source) sourceParts.push(attomBundle.source);
      if (attomBundle.subjectProperty) {
        subjectProperty = subjectProperty
          ? mergeDefined(subjectProperty, attomBundle.subjectProperty)
          : attomBundle.subjectProperty;
      }
      if (attomBundle.comparables.length) comparables = mergeComparablePools(comparables, attomBundle.comparables);
      if (attomBundle.lastError) lastError = attomBundle.lastError;
    }

    const listingData = dedupeListings(comparables).slice(0, Number(limit || 60));
    const soldCount = estimateSoldComparableCount(listingData);

    if (!listingData.length) {
      const fallbackError = lastError?.details || "No comparable listings were returned by the configured CMA providers.";
      return json(lastError?.statusCode || 404, {
        error: "No comparable data found",
        details: fallbackError,
        request: {
          address,
          city,
          state,
          zipCode,
          propertyType,
          limit,
          compCount,
          searchRadius,
          lookbackMonths,
        },
        attempts: diagnostics,
      });
    }

    return json(200, {
      source: sourceParts.filter(Boolean).join("+") || (attomApiKey ? "attom-sale" : "rentcast-listings"),
      request: {
        address,
        city,
        state,
        zipCode,
        propertyType,
        limit,
        compCount,
        searchRadius,
        lookbackMonths,
      },
      attempts: diagnostics,
      data: {
        subjectProperty,
        comparables: listingData,
        avm,
        stats: {
          totalComparables: listingData.length,
          soldComparableSignals: soldCount,
        },
      },
    });
  } catch (error) {
    return json(500, {
      error: "Failed to fetch CMA data",
      details: error?.message || "Unknown error",
    });
  }
}
