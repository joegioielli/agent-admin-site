/* =========================
   admin-dashboard.js (PART 1/3)
   Core config, state, modal/toast, time helpers, utils, aliases
   ========================= */

// cards • editor • lenders • Advanced Fields • per-listing lender offer
// DETAILS-FIRST strategy (NO overrides.json)
// Canonical writes only (avoid duplicate keys like mlsNumber vs MlsNumber)
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
  items: new Map(), // slug -> listing object (cards)
  details: new Map(), // slug -> details.json (loaded/saved)
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

function daysBetweenTZ(todayY, todayM, todayD, activeY, activeM, activeD, tz = TZ) {
  const aMid = dateAtMidnightTZ(todayY, todayM, todayD, tz).getTime();
  const bMid = dateAtMidnightTZ(activeY, activeM, activeD, tz).getTime();
  const diff = Math.floor((aMid - bMid) / 86400000);
  return diff < 0 ? 0 : diff; // day 0 = active day
}

function ymdToISO(y, m, d) {
  return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

/* ---------------- Small utils ---------------- */

function setValue(sel, val) {
  const el = document.querySelector(sel);
  if (!el) return;
  if (["INPUT", "TEXTAREA", "SELECT"].includes(el.tagName)) el.value = val ?? "";
  else el.textContent = val ?? "";
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

/* ---------- Flatten + smart picking ---------- */

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

function numberFrom(v) {
  if (v == null) return null;
  const n = Number(String(v).replace(/[^\d.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

const ALIASES = {
  mls: ["mlsNumber", "MlsNumber", "MLSNumber", "MLS", "mls", "ListingId", "ListingID", "listingId"],
  address: ["address", "Address", "StreetAddress", "FullAddress"],
  city: ["city", "City"],
  state: ["state", "State"],
  zip: ["zip", "Zip", "ZipCode", "PostalCode"],
  price: ["listPrice", "ListPrice", "price"],
  beds: ["TotalBedrooms", "BedroomsTotal", "beds", "bedrooms"],
  baths: ["totalBaths", "TotalFullBaths", "BathTotal", "baths"],
  sqft: ["SqFtTotal", "TotalSqFt", "sqft", "LivingArea"],
  year: ["YearBuilt", "yearBuilt", "year"],
  status: ["ListingStatus", "Status", "StandardStatus", "status"],
  activeDate: ["activeDate", "ActiveDate", "ListDate", "listDate", "ListingDate", "DateListed", "DateActive"],
  timezone: ["timezone", "TimeZone", "TimeZoneLocal"],
  desc: ["Remarks", "PublicRemarks", "remarks", "publicRemarks", "description"],
  notes: ["agentNotes", "AgentNotes", "PrivateRemarks"],
  photo: ["primaryPhoto", "PrimaryPhoto", "photoUrl", "PhotoUrl", "photo"],
};

const FUZZY = {
  mls: [/mls/i, /listing\.?id/i, /mlsnumber/i],
  address: [/address/i, /street/i],
  city: [/city/i],
  state: [/state/i],
  zip: [/zip/i, /postal/i],
  price: [/price/i],
  beds: [/beds?/i, /bedrooms?/i, /totalbedrooms?/i],
  baths: [/baths?/i, /bathrooms?/i],
  sqft: [/sq.?ft/i, /square.?feet/i, /living.?area/i],
  year: [/year.?built/i],
  status: [/status/i],
  activeDate: [/active.?date/i, /list.?date/i, /date.?listed/i],
  timezone: [/time.?zone/i],
  desc: [/remarks?/i, /description/i],
  notes: [/notes?/i, /private.?remarks?/i],
  photo: [/photo/i, /image.?url/i],
};

function pickSmart(objList, aliasKeys, fuzzyGroup = null, numeric = false) {
  for (const src of objList) {
    for (const k of aliasKeys) {
      const v = src?.[k];
      if (v != null && String(v).trim() !== "") return numeric ? numberFrom(v) : v;
    }
  }

  const flats = objList.map((o) => deepFlatten(o || {}));
  if (fuzzyGroup && FUZZY[fuzzyGroup]) {
    for (const f of flats) {
      for (const [k, v] of Object.entries(f)) {
        if (FUZZY[fuzzyGroup].some((rx) => rx.test(k))) {
          if (v != null && String(v).trim() !== "") return numeric ? numberFrom(v) : v;
        }
      }
    }
  }

  return undefined;
}

/* ---- Photo preview helper ---- */

function previewUrlFrom(photoKeyOrUrl, item) {
  if (typeof photoKeyOrUrl === "string" && /^https?:\/\//i.test(photoKeyOrUrl)) return photoKeyOrUrl;
  if (item && typeof item.photoUrl === "string") return item.photoUrl;
  return null;
}
/* =========================
   admin-dashboard.js (PART 2/3)
   Data fetch, cards render, DOM calculation, advanced fields (fixed)
   ========================= */

async function fetchListings() {
  const res = await fetch(`${ENDPOINTS.list}?ts=${Date.now()}`, { method: "GET", cache: "no-store" });
  if (!res.ok) throw new Error(`listListings failed ${res.status}`);
  const json = await res.json();
  const arr = Array.isArray(json) ? json : json.items || json.files || json.listings;
  if (!Array.isArray(arr)) throw new Error("listListings expected array");
  return arr;
}

function listingIdFrom(item, fallback) {
  return item?.listingId || item?.mls || item?.MLS || item?.id || fallback;
}

function safeSlugFrom(it) {
  if (it.slug) return String(it.slug);
  if (it.id) return String(it.id);
  if (it.listingId) return String(it.listingId);
  const addr = (typeof it.address === "string" && it.address) || (typeof it.Address === "string" && it.Address);
  return addr ? addr.toLowerCase().replace(/[^\w]+/g, "-") : "unknown";
}

function getMLSForCard(l) {
  return l.mls || l.MLS || l.listingId || l.id || "";
}

/* ---------------- DOM calc per card (FIXED timezone) ---------------- */

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

  const tz = listing.timezone || TZ;
  const t = todayYMDInTZ(tz);
  domEl.textContent = String(daysBetweenTZ(t.y, t.m, t.d, activeYMD.y, activeYMD.m, activeYMD.d, tz));
}

function updateAllDom() {
  document.querySelectorAll(SELECTORS.card).forEach(updateDomForCard);
}

/* ---------------- Render cards ---------------- */

function renderListingsIntoGrid(listings) {
  const grid = document.querySelector(SELECTORS.grid);
  if (!grid) return console.error("Missing #listingsGrid container");

  const html = listings.map((l) => {
    const slug = safeSlugFrom(l);
    const address =
      (typeof l.address === "string" && l.address) ||
      (typeof l.title === "string" && l.title) ||
      (typeof l.Address === "string" && l.Address) ||
      slug ||
      "Address unavailable";

    const iso = typeof l.activeDate === "string" && l.activeDate;

    const priceVal = l.price ?? l.listPrice ?? l.ListPrice;
    const priceNum = typeof priceVal === "string" ? Number(priceVal.replace(/[^\d.-]/g, "")) : priceVal;
    const price =
      priceNum != null && Number.isFinite(priceNum)
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

/* ---------------- updateListing calls ---------------- */

async function callUpdate(payload) {
  return fetch(ENDPOINTS.update, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    cache: "no-store",
  });
}

/* ---- Fetch details via getListingDetails OR updateListing snapshot (FIXED payload) ---- */

async function fetchDetailsForModal(item, slug) {
  const listingId = listingIdFrom(item, slug);

  // Prefer item.detailsUrl (getListingDetails)
  if (item?.detailsUrl) {
    try {
      const r = await fetch(item.detailsUrl, { cache: "no-store" });
      if (r.ok) {
        const payload = await r.json();
        // ✅ FIX: payload is { ok, details }
        if (payload?.details && typeof payload.details === "object") return payload.details;
      }
    } catch {}
  }

  // Fallback: updateListing snapshot
  if (listingId) {
    try {
      const r = await callUpdate({ listingId: String(listingId) });
      if (r.ok) {
        const j = await r.json().catch(() => ({}));
        if (j?.details && typeof j.details === "object") return j.details;
      }
    } catch {}
  }

  return {};
}

/* ---------------- Advanced fields ---------------- */

/* ---------------- Advanced fields ---------------- */

function normalizeKey(k) {
  return String(k).replace(/[^\w]+/g, "").toLowerCase();
}


// ADD THIS BLOCK HERE ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

const ALWAYS_HIDE_ADMIN_KEYS = new Set([
  "slug",
  "id",
  "listingid",
  "detailsurl",
  "lastmodified",
  "hasnote",
  "ok",
  "computeddaysonmarket",
  "csvdaysonmarket",
  "daysonmarket",
  "latitude",
  "longitude",
  "officelistcode",
  "_lasteditedby",
  "updatedat",
  "source",
  "source.csvkey",
  "source.ingestedat",
]);

function normAdminKey(k) {
  return String(k || "").trim().toLowerCase();
}


// THEN your existing code continues ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

const CANON_HIDE = new Set([
  "mls","mlsnumber","listingid",
  "address","city","state","zip","zipcode","postalcode",
  "listprice","price",
  "totalbedrooms","beds","bedrooms",
  "totalfullbaths","totalbaths","baths","bathrooms",
  "sqfttotal","sqft","squarefeet",
  "yearbuilt","year",
  "listingstatus","status",
  "activedate","listdate","datelisted","dateactive","listingdate",
  "timezone",
  "remarks","publicremarks","description","agentnotes",
  "primaryphoto","photo","photourl","mainphotourl",
]);

// Block core keys from being saved via Advanced Fields (defense-in-depth)
const CORE_SIMPLE_KEYS = new Set([
  ...Array.from(CANON_HIDE),
  "ok","id","slug","detailsurl","lastmodified",
  "computeddaysonmarket","daysonmarket","csvdaysonmarket","csvdom",
  "updatedat","_lasteditedby","hasnote",
]);

function isCoreKey(key) {
  if (!key) return false;
  const base = String(key).split(".").pop();
  const norm = base.replace(/[^\w]/g, "").toLowerCase();
  return CORE_SIMPLE_KEYS.has(norm);
}

const ADVHIDE = {
  hardCI: new Set(
    [
      "path","value","key",
      "csvkey","ingestedat","source.csvkey","source.ingestedat",
      "slug","id","listingid","detailsurl","ok","lastmodified",
      "computeddaysonmarket","csvdaysonmarket","daysonmarket","csvdom",
      "updatedat","_lasteditedby","hasnote",
      "latitude","longitude",
      "officelistcode",
      "propertyclassid",
    ].map((s) => s.toLowerCase())
  ),
  patterns: [
    /\.path$/i, /\.value$/i,
    /(^|\.)(path|value|key)$/i,
    /(^|\.)(slug|id|listingid|detailsurl|ok|lastmodified)$/i,
    /csvkey/i, /ingestedat/i,
    /computed.?days.?on.?market/i,
    /csv.?days.?on.?market/i,
    /days.?on.?market/i,
    /csvdom/i,
    /latitude/i, /longitude/i,
    /office(list)?code/i,
    /propertyclassid/i,
    /_lasteditedby/i,
    /updatedat/i,
    /hasnote/i,
  ],
};

function shouldHideAdminKey(originalKey) {
  const k = String(originalKey || "").trim();
  if (!k) return true;

  const kc = normAdminKey(k);

  // ✅ hard override: never show these keys, even when “show admin fields” is enabled
  if (ALWAYS_HIDE_ADMIN_KEYS.has(kc)) return true;

  // Optional toggle: show everything else if enabled
  if (localStorage.getItem("showAdminKeys") === "1") return false;

  // Otherwise apply normal hides
  if (ADVHIDE.hardCI.has(kc)) return true;
  return ADVHIDE.patterns.some((rx) => rx.test(k));
}

function rowHTML(k, v) {
  return `
    <div class="adv-row">
      <input class="adv-key" placeholder="Name" value="${escapeHtml(k)}" />
      <input class="adv-val" placeholder="Value (text or JSON)" value="${escapeHtml(v)}" />
      <button type="button" class="btn danger adv-remove">Remove</button>
    </div>
  `;
}

function bindAdvList(listEl) {
  listEl.querySelectorAll(".adv-remove").forEach((btn) => {
    btn.addEventListener("click", () => btn.closest(".adv-row")?.remove());
  });
}

function renderAdvancedFieldsFromSource(srcObjs) {
  const list = document.querySelector(SELECTORS.advList);
  if (!list) return;

  const merged = {};
  srcObjs.forEach((obj) => {
    const flat = deepFlatten(obj || {});
    for (const [k, v] of Object.entries(flat)) {
      const nk = normalizeKey(k);

      // Hide canonical/core
      if (CANON_HIDE.has(nk)) continue;

      // Hide source.* plumbing
      if (/^source\./i.test(k)) continue;

      if (shouldHideAdminKey(k)) continue;
      merged[k] = v;
    }
  });

  const finalEntries = Object.entries(merged);

  list.innerHTML = finalEntries
    .map(([k, v]) => {
      const val = v == null ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
      return rowHTML(k, val);
    })
    .join("");

  bindAdvList(list);
}

window.renderAdvancedFieldsFromSource = renderAdvancedFieldsFromSource;

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
    const kRaw = r.querySelector(".adv-key")?.value?.trim();
    const vRaw = r.querySelector(".adv-val")?.value?.trim();
    if (!kRaw) return;

    // block core keys + aliases
    if (isCoreKey(kRaw)) return;

    // treat blank as delete
    if (!vRaw) {
      obj[kRaw] = "";
      return;
    }

    let val = vRaw;
    try {
      if (/^[\[{]/.test(vRaw)) val = JSON.parse(vRaw);
      else if (/^-?\d+(\.\d+)?$/.test(vRaw)) val = Number(vRaw);
      else if (/^(true|false)$/i.test(vRaw)) val = /true/i.test(vRaw);
    } catch {
      /* keep string */
    }

    obj[kRaw] = val;
  });

  return obj;
}
/* =========================
   admin-dashboard.js (PART 3/3)
   Editor open/save, activeDate save, lenders, events, init
   ========================= */

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

/* ---------------- Active Date quick-save (card) ---------------- */

async function saveActiveDate(slug, isoDate) {
  const item = state.items.get(slug);
  const listingId = listingIdFrom(item, slug);
  if (!listingId) throw new Error("No listingId available for updateListing");

  const tz = item?.timezone || TZ;

  const res = await callUpdate({
    slug: String(listingId),
    details: { activeDate: isoDate, timezone: tz },
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`updateListing failed ${res.status} ${t}`);
  }

  const data = await res.json().catch(() => ({}));

  // cache latest details if server returned it
  if (data?.details && typeof data.details === "object") state.details.set(slug, data.details);

  // update card dom if server sent it
  const dom = typeof data.daysOnMarket === "number" ? data.daysOnMarket : null;
  if (dom != null) {
    const card = document.querySelector(`[data-slug="${CSS.escape(slug)}"]`);
    const domEl = card?.querySelector(SELECTORS.domOut);
    if (domEl) domEl.textContent = String(dom);
  }
}

/* ---------------- Lenders (same as your current) ---------------- */

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

  list.innerHTML = state.lenders
    .map((l, idx) => {
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
    })
    .join("");

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

  if (current && Array.from(sel.options).some((o) => o.value === current)) sel.value = current;
  else sel.value = "";
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

/* ---- Per-property lender sync (unchanged logic) ---- */

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
  } catch {}

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

/* ---------------- Open listing editor (FIXED details load) ---------------- */

async function openListingEditor(slug, S) {
  const item = S.items.get(slug);
  if (!item) return toast("Listing not found", "error");

  if (!state.lenders || state.lenders.length === 0) {
    try { await loadLenders(); } catch {}
  }

  if (!S.details.has(slug)) {
    const det = await fetchDetailsForModal(item, slug);
    S.details.set(slug, det || {});
  }

  const det = S.details.get(slug) || {};
  const flatDet = deepFlatten(det || {});
  const src = [det, flatDet, item];

  // reset per-property lender UI first
  try {
    const listingId = listingIdFrom(item, slug);
    const sel = document.querySelector(SELECTORS.fLenderSelect);
    const offerEl = document.querySelector(SELECTORS.fLenderOffer);

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
        if (sel && lenderId && Array.from(sel.options).some((o) => o.value === lenderId)) sel.value = lenderId;
        if (offerEl) offerEl.value = offer;
        reflectSelectedLenderChip();
      }
    }
  } catch (e2) {
    console.warn("Per-property lender fetch failed", e2);
  }

  // Fill form
  setValue(SELECTORS.fMLS, pickSmart(src, ALIASES.mls, "mls") || "");
  setValue(SELECTORS.fAddress, pickSmart(src, ALIASES.address, "address") || "");
  setValue(SELECTORS.fCity, pickSmart(src, ALIASES.city, "city") || "");
  setValue(SELECTORS.fState, pickSmart(src, ALIASES.state, "state") || "");
  setValue(SELECTORS.fZip, pickSmart(src, ALIASES.zip, "zip") || "");

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

  // DOM display (timezone-aware)
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

  renderAdvancedFieldsFromSource([det, item]);
}

/* ---------------- Full editor save (CANONICAL WRITES ONLY) ---------------- */

async function saveFullEdit(slug) {
  const item = state.items.get(slug);
  let listingId = listingIdFrom(item, slug);
  if (!listingId) listingId = slug;

  const v = collectFormValues();
  const extras = collectAdvancedFieldsToObject();

  // Never allow these to be written
  delete extras.bedrooms;
  delete extras.squareFeet;

  // canonical-only patch (no duplicate alias writes)
  const detailsPatch = {
    mlsNumber: v.mls || "",

    address: v.address || "",
    city: v.city || "",
    state: v.state || "",
    zip: v.zip || "",

    listPrice: v.price ?? "",

    TotalBedrooms: v.beds ?? "",
    totalBaths: v.baths ?? "",

    SqFtTotal: v.sqft ?? "",
    YearBuilt: v.year ?? "",

    ListingStatus: v.status || "",

    activeDate: v.activeDate || "",
    timezone: v.timezone || TZ,

    Remarks: v.desc || "",
    agentNotes: v.notes || "",

    primaryPhoto: v.photo || "",

    ...extras,
  };

  const res = await callUpdate({ slug: String(listingId), details: detailsPatch });

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
  if (saved?.details && typeof saved.details === "object") state.details.set(slug, saved.details);

  // Update local card model (minimal)
  const updated = { ...(item || {}) };
  if (v.address) updated.address = v.address;
  if (v.price != null) updated.price = v.price;
  if (v.activeDate !== undefined) updated.activeDate = v.activeDate;
  if (v.timezone) updated.timezone = v.timezone;

  if (v.photo) {
    if (isHttpUrl(v.photo)) updated.photoUrl = v.photo;
    else if (!updated.photoUrl) updated.photoUrl = item?.photoUrl || "";
  }

  state.items.set(slug, updated);

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
      const preview = previewUrlFrom(v.photo || "", updated);
      if (preview) img.src = preview;
    }

    const input = card.querySelector(SELECTORS.dateInput);
    if (input && updated.activeDate) input.value = updated.activeDate;

    updateDomForCard(card);
  }

  reflectSelectedLenderChip();
  toast("Listing saved");
}

/* ---------------- Events ---------------- */

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

  // Edit Lenders
  if (e.target.closest(SELECTORS.btnEditLenders) || e.target.closest(SELECTORS.btnManageLendersInline)) {
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
    state.lenders.push({ id: "", name: "", company: "", email: "", phone: "", nmls: "", offer: "" });
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
      const listing = state.items.get(slug);
      const listingId = listingIdFrom(listing, slug);

      const res = await fetch(ENDPOINTS.update, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ slug: String(listingId || slug), delete: true }),
      });

      if (!res.ok) {
        const errText = await res.text();
        throw new Error(`${res.status}: ${errText}`);
      }

      toast("✅ Deleted");
      state.items.delete(slug);
      state.details.delete(slug);
      document.querySelector(`[data-slug="${CSS.escape(slug)}"]`)?.remove();
      closeModal(SELECTORS.listingModal);
    } catch (e2) {
      console.error(e2);
      toast(`❌ ${e2.message}`, "error");
    }
    return;
  }
});

// Keep chip in sync
document.addEventListener("change", (e) => {
  if (e.target?.matches?.(SELECTORS.fLenderSelect)) reflectSelectedLenderChip();
});

/* ---------------- Init ---------------- */

async function init() {
  try {
    const listings = await fetchListings();
    renderListingsIntoGrid(listings);

    try { await loadLenders(); } catch (e) { console.warn(e); }

  } catch (e) {
    console.error(e);
    toast("Failed to load dashboard", "error");
  }
}

document.addEventListener("DOMContentLoaded", init);