// admin-dashboard.js
// cards • editor • lenders • Advanced Fields • per-listing lender offer
// KEYLESS Deterministic alias strategy (Bedrooms - TotalBedrooms, Sq Ft - SqFtTotal)
// stop writing bedrooms / squareFeet

const TZ = "America/Chicago";

const ENDPOINTS = {
  list: "/.netlify/functions/listListings",
  update: "/.netlify/functions/updateListing",
  lenders: "/.netlify/functions/lenders",
};

const SELECTORS = {
  grid: "#listingsGrid",
  card: "[data-slug]",
  dateInput: ".js-activeDate",
  domOut: ".js-daysOnMarket",
  saveBtn: ".js-saveActiveDate",

  // Buttons
  btnEditListing: ".js-editListing",
  btnEditLenders: ".btnEditLenders",

  // Modals
  listingModal: "#editModal",
  lendersModal: "#lendersModal",
  closeListingX: "#btnCloseX",
  closeLendersX: "#btnCloseLenders",

  // Listing editor form fields
  listingForm: "#editForm",
  fMLS: "#f-mls",
  fAddress: "#f-address",
  fCity: "#f-city",
  fState: "#f-state",
  fZip: "#f-zip",
  fPrice: "#f-price",
  fBeds: "#f-beds",
  fBaths: "#f-baths",
  fSqft: "#f-sqft",
  fYear: "#f-year",
  fStatus: "#f-status",
  fActiveDate: "#f-activeDate",
  fDomDisplay: "#f-dom-display",
  fTimezone: "#f-timezone",
  fDesc: "#f-desc",
  fNotes: "#f-notes",
  fPhoto: "#f-photo",
  fPhotoPreviewWrap: "#f-photo-preview-wrap",
  fPhotoPreview: "#f-photo-preview",
  fPhotoHint: "#f-photo-hint",

  // Lender per listing (inside listing modal)
  fLenderSelect: "#f-lender",
  fLenderChip: "#f-lender-chip",
  fLenderChipText: "#f-lender-chip-text",
  fLenderOffer: "#f-lender-offer",
  btnManageLendersInline: "#btnManageLendersInline",

  // Listing modal actions
  btnModalSave: "#btnSave",
  btnModalCancel: "#btnCancel",
  btnDelete: "#btnDelete",

  // Lenders modal
  lendersMeta: "#lendersMeta",
  lendersList: "#lendersList",
  btnAddLender: "#btnAddLender",
  btnSaveLenders: "#btnSaveLenders",

  // Advanced Fields
  advList: "#advList",
  btnAddField: "#btnAddField",

  toast: "#globalToast",
};

const state = {
  items: new Map(),      // slug -> listing object (cards)
  details: new Map(),    // slug -> details.json
  overrides: new Map(),  // slug -> overrides.json
  currentSlug: null,
  lenders: [],
  lendersRevision: null,
};

/* ---------------- Modal helpers ---------------- */

function resolveModal(elOrSel) {
  return typeof elOrSel === "string" ? document.querySelector(elOrSel) : elOrSel;
}

function openModal(elOrSel) {
  const m = resolveModal(elOrSel);
  if (!m) return;

  m.classList.add("show");
  m.setAttribute("aria-hidden", "false");
  m.removeAttribute("inert");

  document.body.classList.add("modal-open");

  const card = m.querySelector(".modal-card");
  (card || m).focus?.();
}

function closeModal(elOrSel) {
  const m = resolveModal(elOrSel);
  if (!m) return;

  // Avoid aria-hidden warning by clearing focus first
  document.activeElement?.blur();

  m.classList.remove("show");
  m.setAttribute("aria-hidden", "true");
  m.setAttribute("inert", "");

  document.body.classList.remove("modal-open");
}

window.showModal = openModal;
window.hideModal = closeModal;

/* ---------------- Toast ---------------- */

function toast(msg, type = "info") {
  const el = document.querySelector(SELECTORS.toast);
  if (!el) {
    console[type === "error" ? "error" : "log"](msg);
    return;
  }
  el.textContent = msg;
  el.className = "";
  el.classList.add("toast", `toast--${type}`);
  el.style.opacity = "1";
  clearTimeout(el.to);
  el.to = setTimeout(() => {
    el.style.opacity = "0";
  }, 2200);
}

/* ---------------- Time helpers ---------------- */

function todayYMDInTZ(tz = TZ) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const y = parts.find((p) => p.type === "year")?.value;
  const m = parts.find((p) => p.type === "month")?.value;
  const d = parts.find((p) => p.type === "day")?.value;
  return { y: Number(y), m: Number(m), d: Number(d) };
}

function dateAtMidnightTZ(y, m, d, tz = TZ) {
  const approx = new Date(Date.UTC(y, m - 1, d, 12));
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    hourCycle: "h23",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).formatToParts(approx);
  const yy = parts.find((p) => p.type === "year")?.value;
  const mm = parts.find((p) => p.type === "month")?.value;
  const dd = parts.find((p) => p.type === "day")?.value;
  const hh = parts.find((p) => p.type === "hour")?.value;
  const mi = parts.find((p) => p.type === "minute")?.value;
  const ss = parts.find((p) => p.type === "second")?.value;
  const tzRenderedAsUTC = Date.UTC(
    Number(yy),
    Number(mm) - 1,
    Number(dd),
    Number(hh),
    Number(mi),
    Number(ss)
  );
  const offsetMs = tzRenderedAsUTC - approx.getTime();
  return new Date(Date.UTC(y, m - 1, d) - offsetMs);
}

function parseLooseDate(str) {
  if (!str) return null;
  const s = String(str).trim();

  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return { y: Number(m[1]), m: Number(m[2]), d: Number(m[3]) };

  m = s.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$/);
  if (m) return { y: Number(m[3]), m: Number(m[1]), d: Number(m[2]) };

  m = s.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{2})$/);
  if (m) {
    const yy = Number(m[3]);
    const y = yy >= 70 ? 1900 + yy : 2000 + yy;
    return { y, m: Number(m[1]), d: Number(m[2]) };
  }

  const dt = new Date(s);
  if (!Number.isNaN(dt.getTime())) {
    return {
      y: dt.getUTCFullYear(),
      m: dt.getUTCMonth() + 1,
      d: dt.getUTCDate(),
    };
  }
  return null;
}

function daysBetweenTZ(aY, aM, aD, bY, bM, bD, tz = TZ) {
  const aMid = dateAtMidnightTZ(aY, aM, aD, tz).getTime();
  const bMid = dateAtMidnightTZ(bY, bM, bD, tz).getTime();
  const diff = Math.floor((aMid - bMid) / 86400000);
  return diff < 0 ? 0 : diff;
}

function ymdToISO(y, m, d) {
  return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

/* ---------------- Small utils ---------------- */

function setValue(sel, val) {
  const el = document.querySelector(sel);
  if (!el) return;
  if (["INPUT", "TEXTAREA", "SELECT"].includes(el.tagName)) {
    el.value = val ?? "";
  } else {
    el.textContent = val ?? "";
  }
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function toNumberOrNull(v) {
  if (v === null || v === undefined) return null;
  const n = Number(String(v).replace(/[^\d.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

function isHttpUrl(s) {
  return typeof s === "string" && /^https?:\/\//i.test(s);
}

/* ---------- CSV alias map helper ---------- */

const ALIASES = {
  mls: ["mls", "MLS", "mlsNumber", "MlsNumber", "ListingId", "ListingID", "MLSNumber"],
  address: ["address", "Address", "StreetAddress", "StreetNumberNumeric", "StreetName", "FullAddress"],
  city: ["city", "City", "Town"],
  state: ["state", "State", "Province"],
  zip: ["zip", "Zip", "postalCode", "PostalCode", "ZipCode", "ParcelZip"],
  price: ["price", "listPrice", "ListPrice", "ListPriceOriginal", "OriginalListPrice", "CurrentPrice"],
  beds: ["TotalBedrooms", "BedroomsTotal", "Bedrooms", "BedsTotal", "BedroomsTotalInteger", "beds", "bedrooms"],
  baths: ["totalBaths", "BathroomsTotalInteger", "FullBaths", "BathTotal", "bathrooms", "baths", "TotalFullBaths"],
  sqft: ["SqFtTotal", "TotalSqFt", "BuildingAreaTotal", "squareFeet", "LivingArea", "livingArea", "sqft", "SqFtMainFloor", "AboveGradeFinishedArea"],
  year: ["YearBuilt", "YearBuiltDetails", "yearBuilt", "year"],
  status: ["status", "ListingStatus", "Status", "StandardStatus"],
  activeDate: ["activeDate", "listDate", "ListDate", "DateListed", "DateActive", "ListingDate"],
  timezone: ["timezone", "TimeZone", "TimeZoneLocal"],
  desc: ["publicRemarks", "remarks", "description", "PublicRemarks", "PropertyDescription", "RemarksPublic", "Remarks"],
  notes: ["agentNotes", "AgentNotes", "PrivateRemarks"],
  photo: ["PrimaryPhoto", "photo", "primaryPhoto", "PhotoUrl", "MainPhotoUrl", "mainPhotoUrl", "photoUrl"],
};

function deepFlatten(input, maxDepth = 6, prefix = "", out = {}) {
  if (!input || typeof input !== "object") return out;
  for (const [k, v] of Object.entries(input)) {
    const key = prefix ? `${prefix}.${k}` : k;
    if (v && typeof v === "object" && !Array.isArray(v) && maxDepth > 0) {
      deepFlatten(v, maxDepth - 1, key, out);
    } else {
      if (out[key] == null) out[key] = v;
    }
  }
  return out;
}

const FUZZY = {
  mls: [/mls/i, /listing\.?id/i, /mlsnumber/i],
  address: [/address/i, /street/i],
  city: [/city/i],
  state: [/state/i, /province/i],
  zip: [/zip/i, /postal/i],
  price: [/price/i, /list\.?price/i, /current\.?price/i],
  beds: [/beds?/i, /bedrooms?/i, /totalbedrooms?/i],
  baths: [/baths?/i, /bathrooms?/i, /full\b.*bath/i],
  sqft: [/sq.?ft/i, /square.?feet/i, /living.?area/i, /building.?area/i],
  year: [/year.?built/i, /built.?year/i],
  status: [/status/i, /standardstatus/i, /listingstatus/i],
  activeDate: [/active.?date/i, /list.?date/i, /date.?listed/i],
  timezone: [/time.?zone/i],
  desc: [/remarks?/i, /description/i],
  notes: [/agent.?notes?/i, /private.?remarks?/i],
  photo: [/photo/i, /image.?url/i],
};

function numberFrom(v) {
  if (v == null) return null;
  const n = Number(String(v).replace(/[^\d.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

function pickSmart(objList, aliasKeys, fuzzyGroup = null, numeric = false) {
  for (const src of objList) {
    for (const k of aliasKeys) {
      const v = src?.[k];
      if (v != null && String(v).trim() !== "") {
        return numeric ? numberFrom(v) : v;
      }
    }
  }

  const flats = objList.map((o) => deepFlatten(o || {}));
  if (fuzzyGroup && FUZZY[fuzzyGroup]) {
    for (const f of flats) {
      for (const [k, v] of Object.entries(f)) {
        if (FUZZY[fuzzyGroup].some((rx) => rx.test(k))) {
          if (v != null && String(v).trim() !== "") {
            return numeric ? numberFrom(v) : v;
          }
        }
      }
    }
  }

  if (numeric) {
    let best = null;
    for (const f of flats) {
      for (const v of Object.values(f)) {
        const n = numberFrom(v);
        if (n != null && n > 0 && best == null) best = n;
      }
    }
    return best;
  }

  return undefined;
}

/* ---- Photo preview helper ---- */

function previewUrlFrom(photoKeyOrUrl, item) {
  if (typeof photoKeyOrUrl === "string" && /^https?:\/\//i.test(photoKeyOrUrl)) {
    return photoKeyOrUrl;
  }
  if (item && typeof item.photoUrl === "string") return item.photoUrl;
  return null;
}

/* ---------------- Data ---------------- */

async function fetchListings() {
  const res = await fetch(`${ENDPOINTS.list}?ts=${Date.now()}`, {
    method: "GET",
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`listListings failed ${res.status}`);
  const json = await res.json();
  const arr = Array.isArray(json) ? json : json.items || json.files || json.listings;
  if (!Array.isArray(arr)) throw new Error("listListings expected array");
  console.log("Loaded", arr.length, "listings");
  return arr;
}

function listingIdFrom(item, fallback) {
  return item?.listingId || item?.mls || item?.MLS || item?.id || fallback;
}

function getMLSForCard(l) {
  return l.mls || l.MLS || l.listingId || l.id || "";
}

/* ---------------- updateListing calls ---------------- */

async function callUpdate(payload) {
  return fetch(ENDPOINTS.update, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    cache: "no-store",
  });
}

async function saveActiveDate(slug, isoDate) {
  const item = state.items.get(slug);
  const listingId = listingIdFrom(item, slug);
  if (!listingId) throw new Error("No listingId available for updateListing");

  const tz = item?.timezone || TZ;

  const res = await callUpdate({
    slug: String(listingId),
    activeDate: isoDate,
    timezone: tz,
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`updateListing failed ${res.status} ${t}`);
  }

  const data = await res.json().catch(() => ({}));
  const dom = typeof data.daysOnMarket === "number" ? data.daysOnMarket : null;

  if (dom != null) {
    const card = document.querySelector(`[data-slug="${CSS.escape(slug)}"]`);
    const domEl = card?.querySelector(SELECTORS.domOut);
    if (domEl) domEl.textContent = String(dom);
  }
}

/* ---------------- Advanced fields (unchanged from your logic) ---------------- */

const CANON_HIDE = new Set([
  "mls","listingid","address","streetaddress","city","state","province","zip","zipcode","postalcode",
  "price","beds","baths","sqft","yearbuilt","year","status",
  "activedate","listdate","datelisted","dateactive","listingdate",
  "timezone","publicremarks","remarks","description","agentnotes",
  "primaryphoto","photo","photourl","mainphotourl",
]);

function normalizeKey(k) {
  return String(k).replace(/[^\w]+/g, "").toLowerCase();
}

const ADVHIDE = {
  hardCI: new Set(["0.path","0.value","path","value","key","csvkey","ingestedat","listingtype","longitude","parceliddisplay","slug"].map(s => s.toLowerCase())),
  patterns: [/\.path$/i,/\.value$/i,/path$/i,/value$/i,/key$/i,/slug$/i,/csvkey/i,/ingestedat/i,/listingtype/i,/longitude/i,/parceliddisplay/i],
};

function shouldHideAdminKey(originalKey) {
  if (localStorage.getItem("showAdminKeys") === "1") return false;
  const kc = String(originalKey).toLowerCase();
  if (ADVHIDE.hardCI.has(kc)) return true;
  return ADVHIDE.patterns.some((rx) => rx.test(originalKey));
}

const GROUP_ALIAS_PRIORITY = {
  price: ["ListPrice", "CurrentPrice", "price", "listPrice", "OriginalListPrice"],
  beds: ["TotalBedrooms","BedroomsTotal","BedroomsTotalInteger","BedsTotal","Bedrooms","beds","bedrooms"],
  baths: ["totalBaths","BathroomsTotalInteger","bathrooms","baths","BathTotal"],
  sqft: ["SqFtTotal","TotalSqFt","BuildingAreaTotal","squareFeet","LivingArea","livingArea","sqft","SqFtMainFloor","AboveGradeFinishedArea"],
  year: ["YearBuilt","YearBuiltDetails","yearBuilt","year"],
  status: ["StandardStatus","ListingStatus","status"],
  desc: ["PublicRemarks","RemarksPublic","remarks","description","publicRemarks"],
  photo: ["PrimaryPhoto","PhotoUrl","photo","primaryPhoto","mainPhotoUrl","MainPhotoUrl","photoUrl"],
  address: ["FullAddress","StreetAddress","address","StreetName"],
  city: ["City","city"],
  state: ["State","Province","state","province"],
  zip: ["PostalCode","ZipCode","zip","postalCode","ParcelZip"],
};

const GROUP_CANON = {
  price: (vv) => vv.price,
  beds: (vv) => vv.beds,
  baths: (vv) => vv.baths,
  sqft: (vv) => vv.sqft,
  year: (vv) => vv.year,
  status: (vv) => vv.status,
  desc: (vv) => vv.desc,
  photo: (vv) => vv.photo,
  address: (vv) => vv.address,
  city: (vv) => vv.city,
  state: (vv) => vv.state,
  zip: (vv) => vv.zip,
};

const GROUP_MATCHERS = {
  price: [/price/i, /listprice/i, /currentprice/i, /originallistprice/i],
  beds: [/totalbedrooms/i, /bedroomstotal/i, /bedroomstotalinteger/i, /beds/i],
  baths: [
    /(^|[^A-Za-z])totalbaths([^A-Za-z]|$)/i,
    /bathroomstotalinteger/i,
    /(^|[^A-Za-z])baths([^A-Za-z]|$)/i,
    /(^|[^A-Za-z])bathrooms([^A-Za-z]|$)/i,
    /(^|[^A-Za-z])bathtotal([^A-Za-z]|$)/i,
  ],
  sqft: [/sqfttotal/i,/totalsqft/i,/buildingareatotal/i,/squarefeet/i,/livingarea/i,/sqft/i,/sqftmainfloor/i],
  year: [/yearbuilt/i,/yearbuiltdetails/i,/year/i],
  status: [/status/i,/standardstatus/i,/listingstatus/i],
  desc: [/remarks/i,/description/i,/publicremarks/i],
  photo: [/photo/i,/image.?url/i],
  address: [/address/i,/streetaddress/i,/streetname/i,/fulladdress/i],
  city: [/city/i],
  state: [/state/i,/province/i],
  zip: [/zip/i,/postalcode/i,/parcelzip/i],
};

function keyBelongsToGroup(key, group) {
  const arr = GROUP_MATCHERS[group];
  return arr && arr.some((rx) => rx.test(String(key)));
}

function pickBestAliasValueForGroup(obj, group) {
  const pri = GROUP_ALIAS_PRIORITY[group];
  if (pri) {
    for (const k of pri) {
      if (obj[k] !== undefined && obj[k] !== null && String(obj[k]).trim() !== "") {
        return obj[k];
      }
    }
  }
  for (const [k, v] of Object.entries(obj)) {
    if (keyBelongsToGroup(k, group) && v != null && String(v).trim() !== "") {
      return v;
    }
  }
  return undefined;
}

function isBlank(v) {
  return v === undefined || v === null || (typeof v === "string" && v.trim() === "");
}

function adoptCoreFromExtras(coreValues, extras) {
  for (const g of Object.keys(GROUP_CANON)) {
    const cand = pickBestAliasValueForGroup(extras, g);
    if (cand === undefined) continue;

    // ✅ Form wins: only fill if the form/core value is currently blank
    if (!isBlank(coreValues[g])) continue;

    if (["price", "beds", "baths", "sqft", "year", "zip"].includes(g)) {
      const n = toNumberOrNull(cand);
      if (n != null) coreValues[g] = n;
    } else {
      coreValues[g] = cand;
    }
  }
}

function mirrorAliasesIntoExtras(extras, coreValues, presentSources) {
  const presentFlat = Object.assign({}, ...presentSources.map((o) => deepFlatten(o || {})), extras);

  for (const [k] of Object.entries(presentFlat)) {
    if (String(k).startsWith("details.")) continue; // ✅ skip details.* keys

    for (const g of Object.keys(GROUP_CANON)) {
      if (!keyBelongsToGroup(k, g)) continue;
      const val = GROUP_CANON[g](coreValues);
      if (val !== undefined) {
        extras[k] = val;
        break;
      }
    }
  }

  if (coreValues.beds !== undefined) {
    extras.TotalBedrooms = coreValues.beds;
    delete extras.bedrooms;
  }
  if (coreValues.sqft !== undefined) {
    extras.SqFtTotal = coreValues.sqft;
    delete extras.squareFeet;
  }
}


function stripDetailsPrefix(key) {
  return String(key).startsWith("details.") ? key.slice("details.".length) : key;
}

function chooseCanonicalKeyForGroup(keys, group) {
  const pri = GROUP_ALIAS_PRIORITY[group];
  if (pri) {
    for (const preferred of pri) {
      const hit = keys.find((k) => k.split(".").pop() === preferred);
      if (hit) return hit;
    }
  }
  const nonDetails = keys.filter((k) => !k.startsWith("details."));
  if (nonDetails.length) return nonDetails[0];
  return keys[0];
}

function rowHTML(k, v) {
  const displayKey = stripDetailsPrefix(k);
  return `
    <div class="adv-row">
      <input class="adv-key" placeholder="Name" value="${escapeHtml(displayKey)}" />
      <input class="adv-val" placeholder="Value (text or JSON)" value="${escapeHtml(v)}" />
      <button type="button" class="btn danger adv-remove">Remove</button>
    </div>
  `;
}

window.renderAdvancedFieldsFromSource = renderAdvancedFieldsFromSource;
function renderAdvancedFieldsFromSource(srcObjs) {
  const list = document.querySelector(SELECTORS.advList);
  if (!list) return;

  const merged = {};
  srcObjs.forEach((obj) => {
    const flat = deepFlatten(obj || {});
    for (const [k, v] of Object.entries(flat)) {
      const nk = normalizeKey(k);
      if (CANON_HIDE.has(nk)) continue;
      if (/source\.i/i.test(k)) continue;
      if (k === "detailsUrl") continue;
      if (shouldHideAdminKey(k)) continue;
      merged[k] = v;
    }
  });

  const groupBuckets = {};
  const ungrouped = [];

  Object.keys(merged).forEach((key) => {
    let foundGroup = null;
    for (const g of Object.keys(GROUP_MATCHERS)) {
      if (keyBelongsToGroup(key, g)) { foundGroup = g; break; }
    }
    if (foundGroup) {
      if (!groupBuckets[foundGroup]) groupBuckets[foundGroup] = [];
      groupBuckets[foundGroup].push(key);
    } else {
      ungrouped.push(key);
    }
  });

  const finalEntries = [];
  for (const [g, keys] of Object.entries(groupBuckets)) {
    const chosenKey = chooseCanonicalKeyForGroup(keys, g);
    finalEntries.push([chosenKey, merged[chosenKey]]);
  }
  ungrouped.forEach((k) => finalEntries.push([k, merged[k]]));

  list.innerHTML = finalEntries.map(([k, v]) => {
    const val = v == null ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
    return rowHTML(k, val);
  }).join("");

  bindAdvList(list);
}

function bindAdvList(listEl) {
  listEl.querySelectorAll(".adv-remove").forEach((btn) => {
    btn.addEventListener("click", () => btn.closest(".adv-row")?.remove());
  });
}

function addAdvancedRow() {
  const list = document.querySelector(SELECTORS.advList);
  if (!list) return;
  const wrapper = document.createElement("div");
  wrapper.innerHTML = rowHTML("", "");
  const node = wrapper.firstElementChild;
  list.appendChild(node);
  bindAdvList(list);
  node.querySelector(".adv-key")?.focus();
}
window.addAdvancedRow = addAdvancedRow;

function collectAdvancedFieldsToObject() {
  const list = document.querySelector(SELECTORS.advList);
  if (!list) return {};
  const obj = {};
  list.querySelectorAll(".adv-row").forEach((r) => {
    let k = r.querySelector(".adv-key")?.value?.trim();
    const v = r.querySelector(".adv-val")?.value?.trim();
    if (!k) return;
    if (k.startsWith("details.")) return; // ✅ do not allow details.* to be saved into overrides

    if (!v) { obj[k] = ""; return; }

    let val = v;
    try {
      if (/^[\[{]/.test(v)) val = JSON.parse(v);
      else if (/^-?\d+(\.\d+)?$/.test(v)) val = Number(v);
      else if (/^(true|false)$/i.test(v)) val = /true/i.test(v);
    } catch { /* keep string */ }
    obj[k] = val;
  });
  return obj;
}

/* --------- Two-way sync helpers --------- */

function collectFormValues() {
  const get = (sel) => document.querySelector(sel)?.value ?? "";

  const price = toNumberOrNull(get(SELECTORS.fPrice));
  const beds = toNumberOrNull(get(SELECTORS.fBeds));
  const baths = toNumberOrNull(get(SELECTORS.fBaths));
  const sqft = toNumberOrNull(get(SELECTORS.fSqft));
  const year = toNumberOrNull(get(SELECTORS.fYear));
  const status = get(SELECTORS.fStatus) || "";
  const desc = get(SELECTORS.fDesc) || "";
  const notes = get(SELECTORS.fNotes) || "";
  const photo = get(SELECTORS.fPhoto) || "";
  const tz = get(SELECTORS.fTimezone) || TZ;

  const lenderId = get(SELECTORS.fLenderSelect) || "";
  const lenderOfferEl = document.querySelector(SELECTORS.fLenderOffer);
  const lenderOffer = lenderOfferEl ? lenderOfferEl.value : "";

  const adRaw = get(SELECTORS.fActiveDate);
  const adYMD = parseLooseDate(adRaw);
  const activeDate = adYMD ? ymdToISO(adYMD.y, adYMD.m, adYMD.d) : "";

  const address = get(SELECTORS.fAddress) || "";
  const city = get(SELECTORS.fCity) || "";
  const stateV = get(SELECTORS.fState) || "";
  const zip = get(SELECTORS.fZip) || "";
  const mls = get(SELECTORS.fMLS) || "";

  return {
    mls,
    address,
    city,
    state: stateV,
    zip,
    price,
    beds,
    baths,
    sqft,
    year,
    status,
    desc,
    notes,
    photo,
    lenderId,
    lenderOffer,
    timezone: tz,
    activeDate,
  };
}

/* ---------------- Full editor save ---------------- */

async function saveFullEdit(slug) {
  const item = state.items.get(slug);
  let listingId = listingIdFrom(item, slug);
  if (!listingId) listingId = slug;

  const v = collectFormValues();
  const extras = collectAdvancedFieldsToObject();
  adoptCoreFromExtras(v, extras);

  const overridesBase = {
    address: v.address,
    city: v.city,
    state: v.state,
    zip: v.zip,
    listPrice: v.price,
    price: v.price,

    TotalBedrooms: v.beds,
    beds: v.beds,

    totalBaths: v.baths,
    baths: v.baths,

    SqFtTotal: v.sqft,
    sqft: v.sqft,

    yearBuilt: v.year,
    status: v.status,
    activeDate: v.activeDate,
    timezone: v.timezone,

    publicRemarks: v.desc,
    remarks: v.desc,
    agentNotes: v.notes,

    primaryPhoto: v.photo,
    photo: v.photo,
  };

  const presentSources = [
    state.details.get(slug) || {},
    state.overrides.get(slug) || {},
    item || {},
  ];
  mirrorAliasesIntoExtras(extras, v, presentSources);

  delete extras.bedrooms;
  delete extras.squareFeet;

  for (const k of Object.keys(extras)) {
   if (k.startsWith("details.")) delete extras[k];
  }
 
  const overrides = { ...extras, ...overridesBase };

  console.log("==== SAVE DEBUG ====");
console.log("Slug:", listingId);

const keys = Object.keys(overrides);
console.log("Total override keys:", keys.length);

console.log("Has details.* keys:",
  keys.some(k => k.startsWith("details."))
);

console.log("Has bedrooms key:", keys.includes("bedrooms"));
console.log("Has squareFeet key:", keys.includes("squareFeet"));

console.log("Override keys list:", keys);
console.log("Full overrides object:", overrides);
console.log("====================");


  const res = await callUpdate({
    slug: String(listingId),
    overrides,
    replace: true,
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`updateListing failed ${res.status} ${t}`);
  }

  // per-property lender sync
  try {
    await upsertPerPropertyLender(listingId, v.lenderId, v.lenderOffer);
  } catch (e) {
    console.warn("Per-property lender sync failed", e);
  }

  const saved = await res.json().catch(() => ({}));
  if (saved?.overrides) state.overrides.set(slug, saved.overrides);

  const updated = { ...(item || {}) };
  if (v.address) updated.address = v.address;
  if (v.price != null) updated.price = v.price;
  if (v.activeDate !== undefined) updated.activeDate = v.activeDate;

  if (v.photo) {
    if (isHttpUrl(v.photo)) updated.photoUrl = v.photo;
    else if (!updated.photoUrl) updated.photoUrl = item?.photoUrl || "";
  }

  state.items.set(slug, updated);
  state.overrides.set(slug, { ...(state.overrides.get(slug) || {}), ...overrides });

  // Update card UI
  const card = document.querySelector(`[data-slug="${CSS.escape(slug)}"]`);
  if (card) {
    const addrEl = card.querySelector(".addr");
    if (addrEl && v.address) addrEl.textContent = v.address;

    const priceEl = card.querySelector(".price");
    if (priceEl && v.price != null) {
      priceEl.textContent = v.price
        ? new Intl.NumberFormat("en-US", {
            style: "currency",
            currency: "USD",
            maximumFractionDigits: 0,
          }).format(v.price)
        : "";
    }

    const img = card.querySelector(".thumb");
    if (img) {
      const preview = previewUrlFrom(v.photo || item?.primaryPhoto || "", updated);
      if (preview) img.src = preview;
    }

    const input = card.querySelector(SELECTORS.dateInput);
    if (input && updated.activeDate) input.value = updated.activeDate;

    updateDomForCard(card);
  }

  reflectSelectedLenderChip();
  toast("Listing saved");
}

/* ---- Per-property lender sync ---- */

async function upsertPerPropertyLender(listingId, lenderId, offerStr) {
  const hasAnything = (lenderId && lenderId.trim()) || (offerStr && offerStr.trim());
  let revision = null;

  try {
    const g = await fetch(`${ENDPOINTS.lenders}?propertyId=${encodeURIComponent(listingId)}`, { cache: "no-store" });
    if (g.ok) {
      const data = await g.json().catch(() => ({}));
      revision = data?.revision ?? null;
      if (!hasAnything && !data?.lender && !data?.offer) return;
    }
  } catch { /* ignore */ }

  let lenderObj = null;
  if (lenderId && lenderId.trim()) {
    const found = state.lenders.find((l) => l.id === lenderId || l.slug === lenderId || l.email === lenderId);
    lenderObj = found
      ? {
          name: String(found.name || ""),
          phone: String(found.phone || ""),
          nmls: String(found.nmls || found.nmlsId || ""),
          email: String(found.email || ""),
          link: String(found.link || found.url || ""),
        }
      : { name: lenderId };
  }

  const body = {
    revision: revision === null ? undefined : revision,
    lender: lenderObj || undefined,
    lenderId: lenderId || undefined,
    offer: hasAnything ? { details: String(offerStr || "") } : undefined,
  };

  if (!hasAnything) {
    body.lender = null;
    body.offer = null;
  }

  const r = await fetch(`${ENDPOINTS.lenders}?propertyId=${encodeURIComponent(listingId)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`per-property lenders PUT ${r.status} ${t}`);
  }
}

/* ---- Lenders UI helpers ---- */

function genLenderId(seed) {
  const base = seed.trim().toLowerCase().replace(/[^\w]+/g, "-").replace(/--+/g, "-");
  const stamp = Date.now().toString(36).slice(-5);
  return base ? `${base}-${stamp}` : `lender-${stamp}`;
}

function ensureLenderId(l) {
  if (l.id && String(l.id).trim()) return l;
  const seed = l.email || l.name || l.company || "";
  return { ...l, id: genLenderId(seed) };
}

async function loadLenders() {
  const r = await fetch(ENDPOINTS.lenders, { cache: "no-store" });
  if (!r.ok) throw new Error(`lenders GET ${r.status}`);
  const data = await r.json().catch(() => ({}));

  state.lenders = Array.isArray(data?.lenders) ? data.lenders.map(ensureLenderId) : [];
  state.lendersRevision = data?.revision ?? null;

  updateLendersMeta();
  renderLendersList();
  updateLenderSelectOptions();
}

async function saveLenders() {
  try {
    await collectLendersFromDOM();
    state.lenders = state.lenders.map(ensureLenderId);

    const body = { lenders: state.lenders, revision: state.lendersRevision ?? undefined };
    const r = await fetch(ENDPOINTS.lenders, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!r.ok) {
      const t = await r.text().catch(() => "");
      throw new Error(`lenders PUT ${r.status} ${t}`);
    }

    const data = await r.json().catch(() => ({}));
    state.lendersRevision = data?.revision ?? state.lendersRevision;

    toast("Lenders saved");
    updateLendersMeta();
    updateLenderSelectOptions();
    reflectSelectedLenderChip();
  } catch (e) {
    console.error(e);
    toast("Save lenders failed", "error");
  }
}

function updateLendersMeta() {
  const meta = document.querySelector(SELECTORS.lendersMeta);
  if (!meta) return;
  const count = state.lenders.length || 0;
  meta.textContent = `${count} lender${count === 1 ? "" : "s"} configured`;
}

function renderLendersList() {
  const list = document.querySelector(SELECTORS.lendersList);
  if (!list) return;

  if (!state.lenders.length) {
    list.innerHTML = `<div style="padding:8px 4px; font-size:13px; color:#666;">No lenders configured yet. Click “Add Lender” to create one.</div>`;
    return;
  }

  list.innerHTML = state.lenders.map((l, idx) => {
    const name = l.name || "";
    const company = l.company || "";
    const email = l.email || "";
    const phone = l.phone || "";
    const nmls = l.nmls || l.nmlsId || "";
    const offer = l.offer || "";
    return `
      <div class="lender-row" data-idx="${idx}">
        <div class="lender-grid">
          <div class="f col-4"><label>Name</label><input class="ln-name" type="text" value="${escapeHtml(name)}" /></div>
          <div class="f col-4"><label>Company</label><input class="ln-company" type="text" value="${escapeHtml(company)}" /></div>
          <div class="f col-4"><label>Email</label><input class="ln-email" type="email" value="${escapeHtml(email)}" /></div>
          <div class="f col-4"><label>Phone</label><input class="ln-phone" type="text" value="${escapeHtml(phone)}" /></div>
          <div class="f col-4"><label>NMLS</label><input class="ln-nmls" type="text" value="${escapeHtml(nmls)}" /></div>
          <div class="f col-12"><label>Default Offer (optional)</label><textarea class="ln-offer" placeholder="Default lender offer...">${escapeHtml(offer)}</textarea></div>
        </div>
        <div class="lender-actions">
          <button type="button" class="btn danger ln-remove">Remove</button>
        </div>
      </div>
    `;
  }).join("");

  list.querySelectorAll(".ln-remove").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest(".lender-row");
      const idx = Number(row?.dataset?.idx);
      if (Number.isNaN(idx)) return;

      state.lenders.splice(idx, 1);
      renderLendersList();
      updateLendersMeta();
      updateLenderSelectOptions();
      reflectSelectedLenderChip();
    });
  });
}

async function collectLendersFromDOM() {
  const list = document.querySelector(SELECTORS.lendersList);
  if (!list) return;
  const rows = Array.from(list.querySelectorAll(".lender-row"));

  state.lenders = rows.map((r, i) => {
    const prev = state.lenders[i] || {};
    return ensureLenderId({
      id: prev.id,
      name: r.querySelector(".ln-name")?.value?.trim() || "",
      company: r.querySelector(".ln-company")?.value?.trim() || "",
      email: r.querySelector(".ln-email")?.value?.trim() || "",
      phone: r.querySelector(".ln-phone")?.value?.trim() || "",
      nmls: r.querySelector(".ln-nmls")?.value?.trim() || "",
      offer: r.querySelector(".ln-offer")?.value?.trim() || "",
    });
  });
}

function addLenderRow() {
  state.lenders.push({ id: "", name: "", company: "", email: "", phone: "", nmls: "", offer: "" });
}

function updateLenderSelectOptions() {
  const sel = document.querySelector(SELECTORS.fLenderSelect);
  if (!sel) return;

  const current = sel.value;

  sel.innerHTML =
    `<option value="">— None —</option>` +
    state.lenders
      .filter((l) => l?.id && String(l.id).trim())
      .map((l) => {
        const id = String(l.id).trim();
        const label = [l.name, l.company].filter(Boolean).join(" • ");
        return `<option value="${escapeHtml(id)}">${escapeHtml(label || id)}</option>`;
      })
      .join("");

  if (current && Array.from(sel.options).some((o) => o.value === current)) {
    sel.value = current;
  } else {
    sel.value = "";
  }
}

function reflectSelectedLenderChip() {
  const sel = document.querySelector(SELECTORS.fLenderSelect);
  const chip = document.querySelector(SELECTORS.fLenderChip);
  const chipText = document.querySelector(SELECTORS.fLenderChipText);
  if (!sel || !chip || !chipText) return;

  const id = sel.value;
  if (!id) {
    chip.style.display = "none";
    chipText.textContent = "";
    return;
  }

  const lenderObj = state.lenders.find((l) => l.id === id || l.slug === id || l.email === id);
  if (lenderObj) {
    chip.style.display = "inline-flex";
    chipText.textContent = [lenderObj.name, lenderObj.company].filter(Boolean).join(" • ");
  } else {
    chip.style.display = "none";
    chipText.textContent = "";
  }
}

/* ---------------- Rendering cards ---------------- */

function safeSlugFrom(it) {
  if (it.slug) return String(it.slug);
  if (it.id) return String(it.id);
  if (it.listingId) return String(it.listingId);
  const addr = (typeof it.address === "string" && it.address) || (typeof it.Address === "string" && it.Address);
  return addr ? addr.toLowerCase().replace(/[^\w]+/g, "-") : "unknown";
}

function renderListingsIntoGrid(listings) {
  const grid = document.querySelector(SELECTORS.grid);
  if (!grid) return console.error("Missing #listingsGrid container");

  const html = listings.map((l) => {
    const slug = safeSlugFrom(l);
    const address = (typeof l.address === "string" && l.address) || (typeof l.title === "string" && l.title) || (typeof l.Address === "string" && l.Address) || slug || "Address unavailable";
    const iso = typeof l.activeDate === "string" && l.activeDate;

    const priceVal = l.price ?? l.listPrice ?? l.ListPrice;
    const priceNum = typeof priceVal === "string" ? Number(priceVal.replace(/[^\d.-]/g, "")) : priceVal;
    const price = priceNum != null && Number.isFinite(priceNum)
      ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(priceNum)
      : "";

    const photo = (typeof l.photoUrl === "string" && l.photoUrl) || (typeof l.primaryPhoto === "string" && l.primaryPhoto);
    const mls = getMLSForCard(l);
    const domValue = typeof l.computedDaysOnMarket === "number" ? l.computedDaysOnMarket : "";

    return `
      <div class="card-inner" data-slug="${escapeHtml(slug)}">
        ${photo ? `<img class="thumb" src="${escapeHtml(photo)}" alt="">` : `<div class="thumb"></div>`}
        <div class="content">
          <div class="price">${price ? escapeHtml(price) : ""}</div>
          <div class="addr">${escapeHtml(address)}</div>
          <div class="meta">
            <span class="mls">MLS # ${escapeHtml(String(mls || ""))}</span>
            <span class="dom-badge">DOM <span class="js-daysOnMarket">${domValue}</span></span>
          </div>
          <div class="actions actions--date">
            <label class="mls" style="min-width:auto; margin-right:6px;">Active Date</label>
            <input class="js-activeDate" type="date" value="${iso ? escapeHtml(iso) : ""}">
            <button class="js-saveActiveDate" type="button">Save Date</button>
          </div>
          <div class="actions actions--footer">
            <button class="js-editListing" type="button">Edit Listing</button>
          </div>
        </div>
      </div>
    `;
  });

  grid.innerHTML = html.join("") || `<div class="empty">No properties found.</div>`;

  listings.forEach((it) => {
    const slug = safeSlugFrom(it);
    it.slug = slug;
    state.items.set(slug, it);
  });

  updateAllDom();
}

/* ---------------- DOM calc per card ---------------- */

function updateDomForCard(card) {
  const slug = card.dataset.slug;
  const listing = state.items.get(slug);
  if (!listing) return;

  const domEl = card.querySelector(SELECTORS.domOut);
  if (!domEl) return;

  const activeISO = listing.activeDate;
  if (!activeISO) return (domEl.textContent = "0");

  const activeYMD = parseLooseDate(activeISO);
  if (!activeYMD) return (domEl.textContent = "0");

  const t = todayYMDInTZ(TZ);
  domEl.textContent = String(daysBetweenTZ(t.y, t.m, t.d, activeYMD.y, activeYMD.m, activeYMD.d, TZ));
}

function updateAllDom() {
  document.querySelectorAll(SELECTORS.card).forEach(updateDomForCard);
}

/* ---- Populate Edit Listing (ONLY ONE) ---- */

async function openListingEditor(slug, S) {
  const item = S.items.get(slug);
  if (!item) return toast("Listing not found", "error");

  // ensure lenders are loaded so select has options
  if (!state.lenders || state.lenders.length === 0) {
    try { await loadLenders(); } catch {}
  }

  // details.json
  if (!S.details.has(slug)) {
    const url = item.detailsUrl;
    if (url) {
      try {
        const r = await fetch(url, { cache: "no-store" });
        S.details.set(slug, r.ok ? await r.json() : {});
      } catch {
        S.details.set(slug, {});
      }
    } else {
      S.details.set(slug, {});
    }
  }
  const det = S.details.get(slug);

  // overrides
  let overrides = S.overrides.get(slug) || {};
  try {
    const listingId = listingIdFrom(item, slug);
    if (listingId) {
      const r = await fetch(ENDPOINTS.update, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ listingId }),
      });
      if (r.ok) {
        const j = await r.json().catch(() => ({}));
        if (j && typeof j.overrides === "object") overrides = j.overrides;
      }
    }
  } catch (e) {
    console.warn("Overrides fetch skipped", e);
  }
  state.overrides.set(slug, overrides);

  // ✅ Per-property lender loader (RESET FIRST — prevents carrying last listing)
  try {
    const listingId = listingIdFrom(item, slug);
    const sel = document.querySelector(SELECTORS.fLenderSelect);
    const offerEl = document.querySelector(SELECTORS.fLenderOffer);

    // reset UI first
    if (sel) sel.value = "";
    if (offerEl) offerEl.value = "";
    updateLenderSelectOptions();
    reflectSelectedLenderChip();

    if (listingId) {
      const lr = await fetch(`${ENDPOINTS.lenders}?propertyId=${encodeURIComponent(listingId)}`, { cache: "no-store" });
      if (lr.ok) {
        const ldata = await lr.json().catch(() => null);
        const lenderId = (ldata?.lenderId || "").trim();
        const offer = (ldata?.offer?.details || "").trim();

        updateLenderSelectOptions();

        if (sel && lenderId && Array.from(sel.options).some(o => o.value === lenderId)) {
          sel.value = lenderId;
        } else if (sel) {
          sel.value = "";
        }
        if (offerEl) offerEl.value = offer;

        reflectSelectedLenderChip();
      }
    }
  } catch (e2) {
    console.warn("Per-property lender fetch failed", e2);
  }

  const flatDet = deepFlatten(det || {});
  const src = [overrides, det, flatDet, item];

  // fill form
  setValue(SELECTORS.fMLS, pickSmart(src, ALIASES.mls, "mls") || "");
  setValue(SELECTORS.fAddress, pickSmart(src, ALIASES.address, "address") || "");
  setValue(SELECTORS.fCity, pickSmart(src, ALIASES.city, "city") || "");
  setValue(SELECTORS.fState, pickSmart(src, ALIASES.state, "state") || "");
  setValue(SELECTORS.fZip, pickSmart(src, ALIASES.zip, "zip", false) || "");

  setValue(SELECTORS.fPrice, pickSmart(src, ALIASES.price, "price", true) ?? "");
  setValue(SELECTORS.fBeds, pickSmart(src, ALIASES.beds, "beds", true) ?? "");
  setValue(SELECTORS.fBaths, pickSmart(src, ALIASES.baths, "baths", true) ?? "");
  setValue(SELECTORS.fSqft, pickSmart(src, ALIASES.sqft, "sqft", true) ?? "");
  setValue(SELECTORS.fYear, pickSmart(src, ALIASES.year, "year", true) ?? "");

  setValue(SELECTORS.fStatus, pickSmart(src, ALIASES.status, "status") || "");

  const activeRaw = pickSmart(src, ALIASES.activeDate, "activeDate") || item.activeDate;
  const activeYMD = parseLooseDate(activeRaw);
  setValue(SELECTORS.fActiveDate, activeYMD ? ymdToISO(activeYMD.y, activeYMD.m, activeYMD.d) : "");

  const tz = pickSmart(src, ALIASES.timezone, "timezone") || item.timezone || TZ;
  setValue(SELECTORS.fTimezone, tz);

  setValue(SELECTORS.fDesc, pickSmart(src, ALIASES.desc, "desc") || "");
  setValue(SELECTORS.fNotes, pickSmart(src, ALIASES.notes, "notes") || "");

  const photo = pickSmart(src, ALIASES.photo, "photo");
  setValue(SELECTORS.fPhoto, photo || "");

  // preview
  const wrap = document.querySelector(SELECTORS.fPhotoPreviewWrap);
  const imgEl = document.querySelector(SELECTORS.fPhotoPreview);
  const hintEl = document.querySelector(SELECTORS.fPhotoHint);
  if (wrap && imgEl) {
    const previewUrl = previewUrlFrom(photo || "", item);
    if (previewUrl) {
      wrap.style.display = "block";
      imgEl.src = previewUrl;
      if (hintEl) hintEl.style.display = "none";
    } else {
      wrap.style.display = "none";
    }
  }

  // DOM display
  const fDomDisplay = document.querySelector(SELECTORS.fDomDisplay);
  if (fDomDisplay) {
    if (activeYMD) {
      const t = todayYMDInTZ(tz || TZ);
      const dom = daysBetweenTZ(t.y, t.m, t.d, activeYMD.y, activeYMD.m, activeYMD.d, tz || TZ);
      fDomDisplay.textContent = `Days on Market: ${dom}`;
    } else {
      fDomDisplay.textContent = "Days on Market: —";
    }
  }

  renderAdvancedFieldsFromSource([overrides, det, item]);
}

/* ---------------- ONE set of events ---------------- */

document.addEventListener("click", async (e) => {
  // Save Active Date
  if (e.target.closest(SELECTORS.saveBtn)) {
    const card = e.target.closest(SELECTORS.card);
    if (!card) return;
    const slug = card.dataset.slug;
    const input = card.querySelector(SELECTORS.dateInput);
    const item = state.items.get(slug);
    if (!input || !item) return;

    const ymd = parseLooseDate(input.value);
    if (!ymd) return toast("Invalid date. Pick a date from the calendar.", "error");

    const iso = ymdToISO(ymd.y, ymd.m, ymd.d);
    item.activeDate = iso;
    updateDomForCard(card);

    try {
      await saveActiveDate(slug, iso);
      toast(`Saved Active Date for ${slug} → ${input.value || iso}`);
    } catch (err) {
      console.error(err);
      toast("Save failed. Check logs.", "error");
    }
    return;
  }

  // Edit Listing
  if (e.target.closest(SELECTORS.btnEditListing)) {
    const card = e.target.closest(SELECTORS.card);
    if (!card) return;
    const slug = card.dataset.slug;
    state.currentSlug = slug;
    await openListingEditor(slug, state);
    openModal(SELECTORS.listingModal);
    return;
  }

  // Edit Lenders (top button)
  if (e.target.closest(SELECTORS.btnEditLenders)) {
    try {
      if (!state.lenders || state.lenders.length === 0) await loadLenders();
    } catch (e2) {
      console.warn(e2);
      toast("Failed to load lenders", "error");
    }
    openModal(SELECTORS.lendersModal);
    return;
  }

  // Manage lenders inline (inside listing modal)
  if (e.target.closest(SELECTORS.btnManageLendersInline)) {
    try {
      if (!state.lenders || state.lenders.length === 0) await loadLenders();
    } catch (e2) {
      console.warn(e2);
      toast("Failed to load lenders", "error");
    }
    openModal(SELECTORS.lendersModal);
    return;
  }

  // Save listing modal
  if (e.target.closest(SELECTORS.btnModalSave)) {
    e.preventDefault();
    const slug = state.currentSlug;
    if (!slug) return toast("No listing selected", "error");
    try {
      await saveFullEdit(slug);
      closeModal(SELECTORS.listingModal);
    } catch (err) {
      console.error(err);
      toast("Save listing failed", "error");
    }
    return;
  }

  // Close listing modal
  if (e.target.closest(SELECTORS.closeListingX) || e.target.closest(SELECTORS.btnModalCancel)) {
    e.preventDefault();
    closeModal(SELECTORS.listingModal);
    return;
  }

  // Close lenders modal
  if (e.target.closest(SELECTORS.closeLendersX)) {
    e.preventDefault();
    closeModal(SELECTORS.lendersModal);
    return;
  }

  // Save lenders
  if (e.target.closest(SELECTORS.btnSaveLenders)) {
    e.preventDefault();
    await saveLenders();
    return;
  }

  // Add lender row
  if (e.target.closest(SELECTORS.btnAddLender)) {
    e.preventDefault();
    try { await collectLendersFromDOM(); } catch {}
    addLenderRow();
    renderLendersList();
    updateLendersMeta();
    updateLenderSelectOptions();
    reflectSelectedLenderChip();
    return;
  }

  // Add advanced field row
  if (e.target.closest(SELECTORS.btnAddField)) {
    e.preventDefault();
    addAdvancedRow();
    return;
  }

  // Delete listing
  if (e.target.closest(SELECTORS.btnDelete)) {
    const slug = state.currentSlug;
    if (!slug) return toast("No listing selected", "error");
    if (!confirm(`Delete "${slug}" permanently?`)) return;

    try {
      const res = await fetch(ENDPOINTS.update, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ slug, delete: true }),
      });

      if (!res.ok) {
        const errText = await res.text();
        throw new Error(`${res.status}: ${errText}`);
      }

      toast("✅ Deleted");
      state.items.delete(slug);
      document.querySelector(`[data-slug="${CSS.escape(slug)}"]`)?.remove();
      closeModal(SELECTORS.listingModal);
    } catch (e2) {
      console.error(e2);
      toast(`❌ ${e2.message}`, "error");
    }
    return;
  }
});

// Keep chip in sync when you change dropdown manually
document.addEventListener("change", (e) => {
  if (e.target?.matches?.(SELECTORS.fLenderSelect)) {
    reflectSelectedLenderChip();
  }
});

/* ---------------- Init ---------------- */

async function init() {
  try {
    const listings = await fetchListings();
    renderListingsIntoGrid(listings);

    // load lenders once
    try { await loadLenders(); } catch (e) { console.warn(e); }

    console.log("State items", state.items.size);
  } catch (e) {
    console.error(e);
    toast("Failed to load dashboard", "error");
  }
}

document.addEventListener("DOMContentLoaded", init);
