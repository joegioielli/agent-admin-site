// netlify/functions/cma.js
// CMA data sources (switchable with the providerMode query param):
// - RentCast for AVM + live listing coverage
// - ATTOM for exact subject-property detail + sold-sale fallback

const RENTCAST_AVM_URL = "https://api.rentcast.io/v1/avm/value";
const RENTCAST_LISTINGS_URL = "https://api.rentcast.io/v1/listings/sale";

const ATTOM_BASE_URL = "https://api.gateway.attomdata.com/propertyapi/v1.0.0";
const ATTOM_PROPERTY_ADDRESS_URL = `${ATTOM_BASE_URL}/property/address`;
const ATTOM_PROPERTY_BASIC_PROFILE_URL = `${ATTOM_BASE_URL}/property/basicprofile`;
const ATTOM_PROPERTY_DETAIL_URL = `${ATTOM_BASE_URL}/property/detail`;
const ATTOM_SALE_SNAPSHOT_URL = `${ATTOM_BASE_URL}/sale/snapshot`;
const ATTOM_AVM_SNAPSHOT_URL = `${ATTOM_BASE_URL}/avm/snapshot`;
const ATTOM_AVM_DETAIL_URL = `${ATTOM_BASE_URL}/attomavm/detail`;
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
  if (value == null) return fallback;
  if (typeof value === "string" && clean(value) === "") return fallback;
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
    apikey: apiKey,
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

function normalizeAttomAddress2CityState(city, state) {
  const locality = clean(city);
  const region = normalizeState(state);
  if (!locality && !region) return "";
  return [locality, region].filter(Boolean).join(", ");
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

function looksLikeAttomPropertyRecord(value) {
  return Boolean(
    value &&
    typeof value === "object" &&
    (
      value.identifier ||
      value.address ||
      value.summary ||
      value.building ||
      value.sale ||
      value.location ||
      value.area
    )
  );
}

function unwrapAttomPropertyRecord(record) {
  if (looksLikeAttomPropertyRecord(record)) return record;
  if (looksLikeAttomPropertyRecord(record?.property)) return record.property;
  if (looksLikeAttomPropertyRecord(record?.data?.property)) return record.data.property;
  return record;
}

function normalizeAttomRecordList(value) {
  if (Array.isArray(value)) {
    return value
      .map((record) => unwrapAttomPropertyRecord(record))
      .filter((record) => looksLikeAttomPropertyRecord(record));
  }

  if (looksLikeAttomPropertyRecord(value)) return [unwrapAttomPropertyRecord(value)];
  if (looksLikeAttomPropertyRecord(value?.property)) return [unwrapAttomPropertyRecord(value.property)];
  return [];
}

function extractAttomRecords(payload) {
  const candidatePaths = [
    payload?.property,
    payload?.properties,
    payload?.data?.property,
    payload?.data?.properties,
    payload?.response?.property,
    payload?.response?.properties,
    payload?.result?.property,
    payload?.result?.properties,
  ];

  for (const candidate of candidatePaths) {
    const records = normalizeAttomRecordList(candidate);
    if (records.length) return records;
  }

  if (looksLikeAttomPropertyRecord(payload)) return [unwrapAttomPropertyRecord(payload)];
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

function isDebugEnabled(value) {
  return /^(1|true|yes|on)$/i.test(clean(value));
}

function collectScalarPaths(value, prefix = "", paths = [], depth = 0, maxDepth = 4, maxItems = 120) {
  if (paths.length >= maxItems || value == null) return paths;

  if (Array.isArray(value)) {
    if (!value.length) {
      if (prefix) paths.push(`${prefix}[]`);
      return paths;
    }

    if (depth >= maxDepth) {
      if (prefix) paths.push(`${prefix}[]`);
      return paths;
    }

    collectScalarPaths(value[0], `${prefix}[0]`, paths, depth + 1, maxDepth, maxItems);
    return paths;
  }

  if (typeof value === "object") {
    if (depth >= maxDepth) {
      if (prefix) paths.push(prefix);
      return paths;
    }

    for (const [key, child] of Object.entries(value)) {
      if (paths.length >= maxItems) break;
      const nextPrefix = prefix ? `${prefix}.${key}` : key;
      collectScalarPaths(child, nextPrefix, paths, depth + 1, maxDepth, maxItems);
    }
    return paths;
  }

  if (prefix) paths.push(prefix);
  return paths;
}

function buildAttomRecordPreview(record, options = {}) {
  const propertyRecord = unwrapAttomPropertyRecord(record);
  if (!propertyRecord || typeof propertyRecord !== "object") return null;

  return {
    topLevelKeys: Object.keys(propertyRecord),
    availableScalarPaths: collectScalarPaths(propertyRecord),
    mappedPreview: mapAttomRecord(propertyRecord, options),
    rawPreview: {
      identifier: propertyRecord.identifier || null,
      address: propertyRecord.address || null,
      summary: propertyRecord.summary || null,
      building: propertyRecord.building || null,
      lot: propertyRecord.lot || null,
      area: propertyRecord.area || null,
      location: propertyRecord.location || null,
      sale: propertyRecord.sale || null,
      avm: propertyRecord.avm || null,
    },
  };
}

function buildAttomPayloadPreview(data, options = {}) {
  const records = extractAttomRecords(data);
  return {
    topLevelKeys: data && typeof data === "object" ? Object.keys(data) : [],
    status: data?.status || null,
    recordCount: records.length,
    firstRecordPreview: records.length ? buildAttomRecordPreview(records[0], options) : null,
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
    "building.parking.garageSpaces",
    "building.parking.numGarageSpaces",
  ]), null);
  const garageType = clean(pickFirstValue(record, [
    "building.parking.garagetype",
    "building.parking.prkgType",
    "building.parking.garageType",
    "building.parking.garageDesc",
    "building.parking.description",
    "building.parking.type",
  ]));

  return [Number.isFinite(spaces) ? `${spaces}-space` : "", garageType].filter(Boolean).join(" ").trim();
}

function mapAttomAddressMatch(record) {
  const propertyRecord = unwrapAttomPropertyRecord(record);
  if (!propertyRecord || typeof propertyRecord !== "object") return null;

  const line1 = clean(pickFirstValue(propertyRecord, [
    "address.line1",
    "address.lineOne",
  ]));
  const city = clean(pickFirstValue(propertyRecord, [
    "address.locality",
    "address.city",
  ]));
  const state = normalizeState(pickFirstValue(propertyRecord, [
    "address.countrySubd",
    "address.state",
  ]));
  const zipCode = clean(pickFirstValue(propertyRecord, [
    "address.postal1",
    "address.zip",
    "address.zipCode",
  ]));
  const address2 = clean(pickFirstValue(propertyRecord, [
    "address.line2",
  ], normalizeAttomAddress2(city, state, zipCode)));
  const fullAddress = clean(pickFirstValue(propertyRecord, [
    "address.oneLine",
    "address.fullAddress",
  ], [line1, address2].filter(Boolean).join(", ")));

  return {
    attomId: clean(pickFirstValue(propertyRecord, [
      "identifier.attomId",
    ])),
    id: clean(pickFirstValue(propertyRecord, [
      "identifier.Id",
      "identifier.id",
      "identifier.obPropId",
    ])),
    address: fullAddress,
    address1: line1,
    address2,
    matchCode: clean(pickFirstValue(propertyRecord, [
      "address.matchCode",
    ])),
  };
}

function buildAttomResolvedSubjectAttempts(match) {
  if (!match) return [];

  return buildAttemptList([
    { label: "resolved-attomId", params: { attomId: match.attomId } },
    { label: "resolved-id", params: { id: match.id } },
    { label: "resolved-address", params: { address: match.address } },
    { label: "resolved-address1+address2", params: { address1: match.address1, address2: match.address2 } },
  ]);
}

function subjectPropertyNeedsAttomEnrichment(subjectProperty) {
  return !clean(subjectProperty?.subdivision)
    || !clean(subjectProperty?.stories)
    || (!clean(subjectProperty?.garage) && !Number.isFinite(subjectProperty?.garageSpaces));
}

function mapRentcastAvmResponse(data, subjectProperty = null) {
  if (!data || typeof data !== "object") return null;

  const value = toFiniteNumber(pickFirstValue(data, [
    "price",
    "value",
    "estimate",
    "estimatedValue",
    "avm",
    "avm.value",
  ]), null);
  const low = toFiniteNumber(pickFirstValue(data, [
    "priceRangeLow",
    "valueRangeLow",
    "range.low",
    "low",
  ]), null);
  const high = toFiniteNumber(pickFirstValue(data, [
    "priceRangeHigh",
    "valueRangeHigh",
    "range.high",
    "high",
  ]), null);
  const confidenceScore = toFiniteNumber(pickFirstValue(data, [
    "confidenceScore",
    "confidence",
    "score",
  ]), null);
  const comparablesCount = extractItems(data?.comparables || data).length || null;

  if (!Number.isFinite(value) && !Number.isFinite(low) && !Number.isFinite(high)) return null;

  return {
    provider: "RentCast AVM",
    source: "rentcast-avm",
    address: clean(subjectProperty?.formattedAddress || data?.subjectProperty?.formattedAddress),
    value,
    low,
    high,
    confidenceScore,
    comparablesCount,
  };
}

function mapAttomAvmRecord(record, source = "attom-avm") {
  const propertyRecord = unwrapAttomPropertyRecord(record);
  if (!propertyRecord || typeof propertyRecord !== "object") return null;

  const line1 = clean(pickFirstValue(propertyRecord, [
    "address.line1",
    "address.lineOne",
  ]));
  const city = clean(pickFirstValue(propertyRecord, [
    "address.locality",
    "address.city",
  ]));
  const state = normalizeState(pickFirstValue(propertyRecord, [
    "address.countrySubd",
    "address.state",
  ]));
  const zipCode = clean(pickFirstValue(propertyRecord, [
    "address.postal1",
    "address.zip",
    "address.zipCode",
  ]));
  const formattedAddress = clean(pickFirstValue(propertyRecord, [
    "address.oneLine",
    "address.fullAddress",
  ], [line1, [city, state, zipCode].filter(Boolean).join(" ")].filter(Boolean).join(", ")));
  const value = toFiniteNumber(pickFirstValue(propertyRecord, [
    "avm.amount.value",
  ]), null);
  const low = toFiniteNumber(pickFirstValue(propertyRecord, [
    "avm.amount.low",
  ]), null);
  const high = toFiniteNumber(pickFirstValue(propertyRecord, [
    "avm.amount.high",
  ]), null);
  const confidenceScore = toFiniteNumber(pickFirstValue(propertyRecord, [
    "avm.amount.scr",
  ]), null);
  const pricePerSqft = toFiniteNumber(pickFirstValue(propertyRecord, [
    "avm.calculations.perSizeUnit",
  ]), null);
  const eventDate = clean(pickFirstValue(propertyRecord, [
    "avm.eventDate",
  ]));

  if (!Number.isFinite(value) && !Number.isFinite(low) && !Number.isFinite(high)) return null;

  return {
    provider: "ATTOM AVM",
    source,
    address: formattedAddress,
    value,
    low,
    high,
    confidenceScore,
    pricePerSqft,
    eventDate,
    avmId: clean(pickFirstValue(propertyRecord, [
      "avm.avmID",
      "avm.avmId",
      "avm.avmid",
    ])),
  };
}

function mapAttomRecord(record, { fallbackStatus = "Sold", source = "attom-sale" } = {}) {
  const propertyRecord = unwrapAttomPropertyRecord(record);
  const line1 = clean(pickFirstValue(propertyRecord, [
    "address.line1",
    "address.lineOne",
  ]));
  const city = clean(pickFirstValue(propertyRecord, [
    "address.locality",
    "address.city",
  ]));
  const state = normalizeState(pickFirstValue(propertyRecord, [
    "address.countrySubd",
    "address.state",
  ]));
  const zipCode = clean(pickFirstValue(propertyRecord, [
    "address.postal1",
    "address.zip",
    "address.zipCode",
  ]));
  const formattedAddress = clean(pickFirstValue(propertyRecord, [
    "address.oneLine",
    "address.fullAddress",
  ], [line1, [city, state, zipCode].filter(Boolean).join(" ")].filter(Boolean).join(", ")));
  const garageSummary = buildGarageSummary(propertyRecord);
  const lotSizeAcres = toFiniteNumber(pickFirstValue(propertyRecord, ["lot.lotsize1", "lot.lotSize1"]), null);
  const lotSizeSquareFeet = toFiniteNumber(pickFirstValue(propertyRecord, ["lot.lotsize2", "lot.lotSize2"]), null);

  return {
    id: clean(pickFirstValue(propertyRecord, [
      "identifier.attomId",
      "identifier.Id",
      "identifier.id",
      "identifier.obPropId",
    ], formattedAddress)),
    propertyId: clean(pickFirstValue(propertyRecord, [
      "identifier.attomId",
      "identifier.obPropId",
    ])),
    formattedAddress,
    addressLine1: line1,
    city,
    state,
    zipCode,
    status: clean(pickFirstValue(propertyRecord, ["status"], fallbackStatus)) || fallbackStatus,
    soldPrice: toFiniteNumber(pickFirstValue(propertyRecord, [
      "sale.amount.saleamt",
      "sale.saleAmount",
      "sale.amount.amount",
    ]), null),
    squareFootage: toFiniteNumber(pickFirstValue(propertyRecord, [
      "building.size.universalsize",
      "building.size.livingsize",
      "building.size.grosssizeadjusted",
      "building.size.grosssize",
      "building.size.sizeLiving",
    ]), null),
    bedrooms: toFiniteNumber(pickFirstValue(propertyRecord, [
      "building.rooms.beds",
      "building.rooms.bedrooms",
    ]), null),
    bathrooms: coerceBathrooms(pickFirstValue(propertyRecord, [
      "building.rooms.bathsTotal",
      "building.rooms.bathstotal",
      "building.rooms.bathsfull",
    ], null)),
    yearBuilt: toFiniteNumber(pickFirstValue(propertyRecord, [
      "summary.yearbuilt",
      "building.summary.yearbuilt",
      "building.summary.yearBuilt",
    ]), null),
    lotSizeAcres,
    lotSizeSquareFeet,
    distance: toFiniteNumber(pickFirstValue(propertyRecord, [
      "location.distance",
      "location.dist",
    ]), null),
    subdivision: clean(pickFirstValue(propertyRecord, [
      "area.subdname",
      "area.subdName",
      "area.subdivision",
      "area.subdivisionName",
      "area.neighborhood",
      "area.neighborhoodName",
      "area.munName",
    ])),
    propertyType: clean(pickFirstValue(propertyRecord, [
      "summary.proptype",
      "summary.propsubtype",
      "summary.propertyType",
      "summary.propclass",
    ])),
    stories: pickFirstValue(propertyRecord, [
      "building.summary.levels",
      "building.summary.levelsCount",
      "building.summary.storyDesc",
      "building.summary.storyCount",
      "building.summary.storydescription",
      "building.summary.storyDescription",
      "summary.levels",
    ], ""),
    architecturalStyle: clean(pickFirstValue(propertyRecord, [
      "building.summary.archStyle",
      "building.parking.archStyle",
      "building.summary.bldgType",
      "building.summary.imprType",
    ])),
    garageSpaces: toFiniteNumber(pickFirstValue(propertyRecord, [
      "building.parking.prkgSpaces",
      "building.parking.spaces",
      "building.parking.garageSpaces",
      "building.parking.numGarageSpaces",
    ]), null),
    garage: garageSummary,
    parking: garageSummary,
    soldDate: clean(pickFirstValue(propertyRecord, [
      "sale.saleTransDate",
      "sale.amount.salerecdate",
      "sale.recordingDate",
    ])),
    latitude: toFiniteNumber(pickFirstValue(propertyRecord, [
      "location.latitude",
      "location.lat",
    ]), null),
    longitude: toFiniteNumber(pickFirstValue(propertyRecord, [
      "location.longitude",
      "location.lon",
      "location.lng",
    ]), null),
    remarks: clean(pickFirstValue(propertyRecord, [
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
    rentcastAvm: avmFetch.response.ok ? mapRentcastAvmResponse(avmFetch.data, avmSubject) : null,
    lastError,
  };
}

function buildAttomSubjectAttempts({ fullAddress, address, address2, address2CityState }) {
  return buildAttemptList([
    { label: "address1+address2", params: { address1: address, address2 } },
    { label: "address1+address2-city-state", params: { address1: address, address2: address2CityState } },
    { label: "full-address", params: { address: fullAddress } },
  ]).filter((attempt) =>
    clean(attempt.params.address) || (clean(attempt.params.address1) && clean(attempt.params.address2))
  );
}

function buildAttomSaleAttempts({
  fullAddress,
  address,
  address2,
  address2CityState,
  zipCode,
  radius,
  pageSize,
  propertyIndicator,
  startSaleTransDate,
  endSaleTransDate,
}) {
  return buildAttemptList([
    {
      label: "address1+address2+radius+indicator",
      params: { address1: address, address2, radius, pageSize, propertyIndicator, startSaleTransDate, endSaleTransDate },
    },
    {
      label: "address1+address2-city-state+radius+indicator",
      params: { address1: address, address2: address2CityState, radius, pageSize, propertyIndicator, startSaleTransDate, endSaleTransDate },
    },
    {
      label: "full-address+radius+indicator",
      params: { address: fullAddress, radius, pageSize, propertyIndicator, startSaleTransDate, endSaleTransDate },
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
      label: "address1+address2-city-state+radius",
      params: { address1: address, address2: address2CityState, radius, pageSize, startSaleTransDate, endSaleTransDate },
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
  debug = false,
}) {
  const diagnostics = [];
  const debugAttempts = [];
  const fullAddress = buildFullAddress(address, city, state, zipCode);
  const address2 = normalizeAttomAddress2(city, state, zipCode);
  const address2CityState = normalizeAttomAddress2CityState(city, state);
  const baseSubjectAttempts = buildAttomSubjectAttempts({ fullAddress, address, address2, address2CityState });
  const propertyIndicator = mapPropertyIndicator(propertyType);
  const saleHistoryMonths = clamp(Math.max(toFiniteNumber(lookbackMonths, 6) * 2, 24), 24, 36);
  const radius = clamp(searchRadius || 3, 0.25, ATTOM_MAX_RADIUS);
  const pageSize = clamp(Math.max(Number(limit || 60), compCount * 3), 20, 100);
  const startSaleTransDate = formatAttomDate(monthsAgo(new Date(), saleHistoryMonths));
  const endSaleTransDate = formatAttomDate(new Date());

  let subjectProperty = null;
  let attomAvm = null;
  let resolvedAddressMatch = null;
  let lastError = null;

  for (const attempt of baseSubjectAttempts) {
    const url = new URL(ATTOM_PROPERTY_ADDRESS_URL);
    Object.entries(attempt.params).forEach(([key, value]) => url.searchParams.set(key, value));

    const result = await fetchAttom(url, apiKey);
    const summary = summarizeAttomPayload(result.response, result.data);
    const records = summary.hasResults ? extractAttomRecords(result.data) : [];
    const addressMatch = records.length ? mapAttomAddressMatch(records[0]) : null;
    const attemptDiagnostics = {
      source: "attom-address",
      label: attempt.label,
      request: attempt.params,
      statusCode: result.response.status,
      attomStatusCode: summary.code,
      attomMessage: summary.message,
      resultCount: summary.resultCount,
      matchCode: addressMatch?.matchCode || "",
    };

    diagnostics.push(attemptDiagnostics);
    if (debug) {
      debugAttempts.push({
        ...attemptDiagnostics,
        url: url.toString(),
        addressMatch,
        payloadPreview: buildAttomPayloadPreview(result.data, {
          fallbackStatus: "Subject",
          source: "attom-address",
        }),
      });
    }

    if (summary.hasResults && addressMatch) {
      resolvedAddressMatch = addressMatch;
      break;
    }

    if (!summary.noResults || !result.response.ok) {
      lastError = buildAttomError(result, summary);
    }
  }

  const subjectAttempts = buildAttemptList([
    ...buildAttomResolvedSubjectAttempts(resolvedAddressMatch),
    ...baseSubjectAttempts,
  ]);

  for (const attempt of subjectAttempts) {
    const url = new URL(ATTOM_PROPERTY_DETAIL_URL);
    Object.entries(attempt.params).forEach(([key, value]) => url.searchParams.set(key, value));

    const result = await fetchAttom(url, apiKey);
    const summary = summarizeAttomPayload(result.response, result.data);
    const records = summary.hasResults ? extractAttomRecords(result.data) : [];
    const attemptDiagnostics = {
      source: "attom-property",
      label: attempt.label,
      request: attempt.params,
      statusCode: result.response.status,
      attomStatusCode: summary.code,
      attomMessage: summary.message,
      resultCount: summary.resultCount,
    };

    diagnostics.push(attemptDiagnostics);
    if (debug) {
      debugAttempts.push({
        ...attemptDiagnostics,
        url: url.toString(),
        payloadPreview: buildAttomPayloadPreview(result.data, {
          fallbackStatus: "Subject",
          source: "attom-property",
        }),
      });
    }

    if (summary.hasResults) {
      subjectProperty = mapAttomRecord(records[0], {
        fallbackStatus: "Subject",
        source: "attom-property",
      });
      break;
    }

    if (!summary.noResults || !result.response.ok) {
      lastError = buildAttomError(result, summary);
    }
  }

  for (const attempt of subjectAttempts) {
    const url = new URL(ATTOM_AVM_DETAIL_URL);
    Object.entries(attempt.params).forEach(([key, value]) => url.searchParams.set(key, value));

    const result = await fetchAttom(url, apiKey);
    const summary = summarizeAttomPayload(result.response, result.data);
    const records = summary.hasResults ? extractAttomRecords(result.data) : [];
    const avmRecord = records[0] || null;
    const avmMapped = avmRecord ? mapAttomAvmRecord(avmRecord, "attom-avm") : null;
    const attemptDiagnostics = {
      source: "attom-avm",
      label: attempt.label,
      request: attempt.params,
      statusCode: result.response.status,
      attomStatusCode: summary.code,
      attomMessage: summary.message,
      resultCount: summary.resultCount,
    };

    diagnostics.push(attemptDiagnostics);
    if (debug) {
      debugAttempts.push({
        ...attemptDiagnostics,
        url: url.toString(),
        avmMapped,
        payloadPreview: buildAttomPayloadPreview(result.data, {
          fallbackStatus: "Subject",
          source: "attom-avm",
        }),
      });
    }

    if (summary.hasResults) {
      attomAvm = avmMapped;
      if (avmRecord) {
        const avmSubjectRecord = mapAttomRecord(avmRecord, {
          fallbackStatus: "Subject",
          source: "attom-avm",
        });
        subjectProperty = subjectProperty
          ? mergeDefined(subjectProperty, avmSubjectRecord)
          : avmSubjectRecord;
      }
      break;
    }

    if (!summary.noResults || !result.response.ok) {
      lastError = buildAttomError(result, summary);
    }
  }

  if (!attomAvm) {
    for (const attempt of subjectAttempts) {
      const url = new URL(ATTOM_AVM_SNAPSHOT_URL);
      Object.entries(attempt.params).forEach(([key, value]) => url.searchParams.set(key, value));

      const result = await fetchAttom(url, apiKey);
      const summary = summarizeAttomPayload(result.response, result.data);
      const records = summary.hasResults ? extractAttomRecords(result.data) : [];
      const avmRecord = records[0] || null;
      const avmMapped = avmRecord ? mapAttomAvmRecord(avmRecord, "attom-avm-snapshot") : null;
      const attemptDiagnostics = {
        source: "attom-avm-snapshot",
        label: attempt.label,
        request: attempt.params,
        statusCode: result.response.status,
        attomStatusCode: summary.code,
        attomMessage: summary.message,
        resultCount: summary.resultCount,
      };

      diagnostics.push(attemptDiagnostics);
      if (debug) {
        debugAttempts.push({
          ...attemptDiagnostics,
          url: url.toString(),
          avmMapped,
          payloadPreview: buildAttomPayloadPreview(result.data, {
            fallbackStatus: "Subject",
            source: "attom-avm-snapshot",
          }),
        });
      }

      if (summary.hasResults) {
        attomAvm = avmMapped;
        if (avmRecord) {
          const avmSubjectRecord = mapAttomRecord(avmRecord, {
            fallbackStatus: "Subject",
            source: "attom-avm-snapshot",
          });
          subjectProperty = subjectProperty
            ? mergeDefined(subjectProperty, avmSubjectRecord)
            : avmSubjectRecord;
        }
        break;
      }

      if (!summary.noResults || !result.response.ok) {
        lastError = buildAttomError(result, summary);
      }
    }
  }

  if (!subjectProperty || subjectPropertyNeedsAttomEnrichment(subjectProperty)) {
    for (const attempt of subjectAttempts) {
      const url = new URL(ATTOM_PROPERTY_BASIC_PROFILE_URL);
      Object.entries(attempt.params).forEach(([key, value]) => url.searchParams.set(key, value));

      const result = await fetchAttom(url, apiKey);
      const summary = summarizeAttomPayload(result.response, result.data);
      const records = summary.hasResults ? extractAttomRecords(result.data) : [];
      const attemptDiagnostics = {
        source: "attom-basicprofile",
        label: attempt.label,
        request: attempt.params,
        statusCode: result.response.status,
        attomStatusCode: summary.code,
        attomMessage: summary.message,
        resultCount: summary.resultCount,
      };

      diagnostics.push(attemptDiagnostics);
      if (debug) {
        debugAttempts.push({
          ...attemptDiagnostics,
          url: url.toString(),
          payloadPreview: buildAttomPayloadPreview(result.data, {
            fallbackStatus: "Subject",
            source: "attom-basicprofile",
          }),
        });
      }

      if (summary.hasResults) {
        const basicProfileSubject = mapAttomRecord(records[0], {
          fallbackStatus: "Subject",
          source: "attom-basicprofile",
        });
        subjectProperty = subjectProperty
          ? mergeDefined(subjectProperty, basicProfileSubject)
          : basicProfileSubject;
        if (!subjectPropertyNeedsAttomEnrichment(subjectProperty)) break;
      }

      if (!summary.noResults || !result.response.ok) {
        lastError = buildAttomError(result, summary);
      }
    }
  }

  const comparables = [];

  for (const attempt of buildAttomSaleAttempts({
    fullAddress,
    address,
    address2,
    address2CityState,
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
    const attemptDiagnostics = {
      source: "attom-sale",
      label: attempt.label,
      request: attempt.params,
      statusCode: result.response.status,
      attomStatusCode: summary.code,
      attomMessage: summary.message,
      resultCount: summary.resultCount,
    };

    diagnostics.push(attemptDiagnostics);
    if (debug) {
      debugAttempts.push({
        ...attemptDiagnostics,
        url: url.toString(),
        payloadPreview: buildAttomPayloadPreview(result.data, {
          fallbackStatus: "Sold",
          source: "attom-sale",
        }),
      });
    }

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
  if (attomAvm) sourceParts.push("attom-avm");
  if (subjectProperty) sourceParts.push("attom-property");
  if (dedupedComparables.length) sourceParts.push("attom-sale");

  return {
    diagnostics,
    source: sourceParts.join("+"),
    attomAvm,
    subjectProperty,
    comparables: dedupedComparables,
    lastError,
    debug: debug ? {
      enabled: true,
      source: sourceParts.join("+"),
      resolvedAddressMatch,
      attomAvm,
      subjectPropertyMapped: subjectProperty,
      comparableCount: dedupedComparables.length,
      comparableSample: dedupedComparables.slice(0, 3),
      attempts: debugAttempts,
    } : null,
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

  const params = event.queryStringParameters || {};
  const providerModeInput = normalizeKey(params.providerMode || params.provider || "hybrid");
  const providerMode = providerModeInput === "attom"
    ? "attom"
    : providerModeInput === "rentcast"
      ? "rentcast"
      : "hybrid";
  const rentcastApiKey = clean(process.env.RENTCAST_API_KEY);
  const attomApiKey = clean(process.env.ATTOM_API_KEY);
  const useRentcast = providerMode !== "attom" && Boolean(rentcastApiKey);
  const useAttom = providerMode !== "rentcast" && Boolean(attomApiKey);
  const providers = {
    requestedMode: providerMode,
    rentcastConfigured: Boolean(rentcastApiKey),
    attomConfigured: Boolean(attomApiKey),
    rentcastEnabled: useRentcast,
    attomEnabled: useAttom,
  };

  if (providerMode === "attom" && !attomApiKey) {
    return json(500, {
      error: "Missing ATTOM credentials",
      details: "ATTOM-only mode was requested, but ATTOM_API_KEY is not configured in the Netlify environment.",
      providers,
    });
  }

  if (providerMode === "rentcast" && !rentcastApiKey) {
    return json(500, {
      error: "Missing RentCast credentials",
      details: "RentCast-only mode was requested, but RENTCAST_API_KEY is not configured in the Netlify environment.",
      providers,
    });
  }

  if (!rentcastApiKey && !attomApiKey) {
    return json(500, {
      error: "Missing CMA provider credentials",
      details: "Add RENTCAST_API_KEY, ATTOM_API_KEY, or both to the Netlify environment before using the CMA function.",
      providers,
    });
  }

  const address = clean(params.address || params.address1 || params.street);
  const city = clean(params.city);
  const state = normalizeState(params.state);
  const zipCode = clean(params.zip || params.zipCode || params.postalCode);
  const limit = clean(params.limit || "60");
  const propertyType = clean(params.propertyType);
  const searchRadius = toFiniteNumber(params.searchRadius, 3);
  const compCount = Math.max(4, Math.min(toFiniteNumber(params.compCount, 20), 50));
  const lookbackMonths = clamp(toFiniteNumber(params.lookbackMonths, 6), 1, 24);
  const debugAttom = isDebugEnabled(params.debugAttom || params.debug);

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
    const providerAvms = {
      rentcast: null,
      attom: null,
    };
    let lastError = null;
    const sourceParts = [];
    let attomDebug = debugAttom ? {
      enabled: true,
      note: useAttom
        ? "ATTOM debug mode is enabled for this response."
        : providerMode === "rentcast"
          ? "ATTOM debug mode was requested, but provider mode is set to RentCast only."
          : "ATTOM debug mode was requested, but ATTOM_API_KEY is not configured.",
      attempts: [],
    } : null;

    if (useRentcast) {
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
      if (rentcastBundle.rentcastAvm) providerAvms.rentcast = rentcastBundle.rentcastAvm;
      if (rentcastBundle.lastError) lastError = rentcastBundle.lastError;
    }

    if (useAttom) {
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
        debug: debugAttom,
      });

      diagnostics.push(...attomBundle.diagnostics);
      if (attomBundle.source) sourceParts.push(attomBundle.source);
      if (attomBundle.subjectProperty) {
        subjectProperty = subjectProperty
          ? mergeDefined(subjectProperty, attomBundle.subjectProperty)
          : attomBundle.subjectProperty;
      }
      if (attomBundle.comparables.length) comparables = mergeComparablePools(comparables, attomBundle.comparables);
      if (attomBundle.attomAvm) providerAvms.attom = attomBundle.attomAvm;
      if (attomBundle.lastError) lastError = attomBundle.lastError;
      if (debugAttom) attomDebug = attomBundle.debug;
    }

    const requestedLimit = Math.max(20, toFiniteNumber(limit, 60));
    const mergedComparableLimit = clamp(Math.max(requestedLimit * 3, 120), requestedLimit, 240);
    const listingData = dedupeListings(comparables).slice(0, mergedComparableLimit);
    const soldCount = estimateSoldComparableCount(listingData);

    if (!listingData.length) {
      const fallbackError = lastError?.details || "No comparable listings were returned by the selected CMA providers.";
      return json(lastError?.statusCode || 404, {
        error: "No comparable data found",
        details: fallbackError,
        providers,
        request: {
          providerMode,
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
        debug: debugAttom ? { attom: attomDebug } : undefined,
      });
    }

    return json(200, {
      source: sourceParts.filter(Boolean).join("+") || (useAttom ? "attom-sale" : "rentcast-listings"),
      providers,
      request: {
        providerMode,
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
        providerAvms,
        stats: {
          totalComparables: listingData.length,
          soldComparableSignals: soldCount,
        },
      },
      debug: debugAttom ? { attom: attomDebug } : undefined,
    });
  } catch (error) {
    return json(500, {
      error: "Failed to fetch CMA data",
      details: error?.message || "Unknown error",
    });
  }
}
