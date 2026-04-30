const MODEL = process.env.OPENAI_CONTRACT_EXTRACT_MODEL || "gpt-4o";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY_CTC || process.env.OPENAI_API_KEY;

const MAX_TEXT_CHARS_PER_DOCUMENT = 32000;
const MAX_FORM_FIELDS_PER_DOCUMENT = 80;
const MAX_IMPORTANT_TERMS = 12;
const OPENAI_TIMEOUT_MS = 25000;

const FIELD_LABELS = {
  property_address: "Property Address",
  buyer_names: "Buyer Names",
  seller_names: "Seller Names",
  purchase_price: "Purchase Price",
  earnest_money_amount: "Earnest Money Amount",
  financing_type: "Financing Type",
  binding_date: "Binding Date",
  closing_date: "Closing Date",
  inspection_deadline: "Inspection Deadline",
  possession_date: "Possession Date"
};

const DATE_FIELDS = new Set([
  "binding_date",
  "closing_date",
  "inspection_deadline",
  "possession_date"
]);

const CURRENCY_FIELDS = new Set(["purchase_price", "earnest_money_amount"]);
const ARRAY_FIELDS = new Set(["buyer_names", "seller_names"]);

function corsHeaders(extra = {}) {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    ...extra
  };
}

function json(statusCode, payload) {
  return {
    statusCode,
    headers: corsHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(payload)
  };
}

function parseBody(event) {
  let raw = event.body || "";
  if (event.isBase64Encoded) {
    raw = Buffer.from(raw, "base64").toString("utf8");
  }
  return JSON.parse(raw || "{}");
}

function cleanText(value) {
  return String(value ?? "")
    .replace(/\u0000/g, " ")
    .replace(/\u00a0/g, " ")
    .trim();
}

function normalizePdfText(value) {
  return String(value ?? "")
    .replace(/\r/g, "\n")
    .replace(/\u0000/g, " ")
    .replace(/\u00a0/g, " ")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function lineMatchesAnyPattern(line, patterns) {
  const cleaned = cleanText(line);
  if (!cleaned) return false;
  return patterns.some((pattern) => pattern.test(cleaned));
}

function isBoilerplateLine(line) {
  return lineMatchesAnyPattern(line, BOILERPLATE_LINE_PATTERNS);
}

function isAgentAttributionLine(line) {
  return lineMatchesAnyPattern(line, AGENT_ATTRIBUTION_PATTERNS);
}

function stripBoilerplatePdfText(value) {
  const normalized = normalizePdfText(value);
  if (!normalized) return "";

  return normalized
    .split(/\n+/)
    .map((line) => cleanText(line))
    .filter(Boolean)
    .filter((line) => /^\[\[page\s+\d+\]\]$/i.test(line) || !isBoilerplateLine(line))
    .join("\n");
}

function slugify(input) {
  return cleanText(input)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function clampConfidence(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return Math.min(1, Math.max(0, numeric));
}

function parseCurrency(rawValue) {
  const cleaned = cleanText(rawValue).replace(/[^0-9.]/g, "");
  if (!cleaned) return null;
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeDate(rawValue) {
  const value = cleanText(rawValue).replace(/,/g, "");
  if (!value) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;

  if (/^\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}$/.test(value)) {
    const [monthRaw, dayRaw, yearRaw] = value.split(/[\/-]/);
    const month = Number.parseInt(monthRaw, 10) - 1;
    const day = Number.parseInt(dayRaw, 10);
    const year = yearRaw.length === 2 ? Number.parseInt(`20${yearRaw}`, 10) : Number.parseInt(yearRaw, 10);
    const parsed = new Date(year, month, day);
    if (Number.isNaN(parsed.getTime())) return null;
    return parsed.toISOString().slice(0, 10);
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function normalizeTime(rawValue) {
  const value = cleanText(rawValue);
  if (!value) return null;
  return value.replace(/\./g, "").replace(/\s+/g, " ").toUpperCase();
}

function formatCurrency(value) {
  if (value == null || value === "") return "Not found";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  }).format(Number(value));
}

function formatDate(value) {
  if (!value) return "Not found";
  const parsed = new Date(`${value}T12:00:00`);
  if (Number.isNaN(parsed.getTime())) return "Not found";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric"
  }).format(parsed);
}

function formatFieldValue(fieldName, value, time) {
  if (value == null || value === "" || (Array.isArray(value) && !value.length)) return "Not found";
  if (DATE_FIELDS.has(fieldName)) {
    return `${formatDate(value)}${time ? ` at ${time}` : ""}`;
  }
  if (CURRENCY_FIELDS.has(fieldName)) {
    return formatCurrency(value);
  }
  if (ARRAY_FIELDS.has(fieldName)) {
    return value.join(", ");
  }
  return String(value);
}

function normalizeNameArray(values) {
  if (!Array.isArray(values)) return [];
  return [...new Set(values.map((value) => cleanText(value)).filter(Boolean))];
}

function countDocumentMappedFields(documentName, fieldMap) {
  return Object.values(fieldMap).filter((field) => field.sourceDocumentName === documentName && field.value != null && field.value !== "" && (!Array.isArray(field.value) || field.value.length)).length;
}

function buildSourceDetails(fieldMap, importantTerms = []) {
  const fieldDetails = Object.values(fieldMap).map((field) => ({
    field: field.key,
    label: field.label,
    value: field.displayValue,
    confidence: field.confidence,
    sourceDocumentName: field.sourceDocumentName || "Not found",
    sourceDocumentType: field.sourceDocumentType || "Not found",
    evidence: field.evidence || ""
  }));

  const termDetails = importantTerms
    .filter((term) => cleanText(term?.label) && cleanText(term?.value))
    .map((term, index) => ({
      field: `important_term_${index + 1}`,
      label: term.label,
      value: term.value,
      confidence: term.confidence,
      sourceDocumentName: term.sourceDocumentName || "Not found",
      sourceDocumentType: term.sourceDocumentType || "Not found",
      evidence: term.evidence || ""
    }));

  return [...fieldDetails, ...termDetails];
}

function extractJsonFromCompletion(payload) {
  const content = payload?.choices?.[0]?.message?.content;
  if (typeof content === "string") return JSON.parse(content);
  if (Array.isArray(content)) {
    const textPart = content.find((item) => item.type === "text" && typeof item.text === "string");
    if (textPart?.text) return JSON.parse(textPart.text);
  }
  throw new Error("OpenAI response did not contain structured JSON content.");
}

function makeScalarFieldSchema(description) {
  return {
    type: "object",
    additionalProperties: false,
    properties: {
      value: { type: "string", description },
      source_document_name: { type: "string" },
      source_document_type: { type: "string" },
      source_document_index: { type: "integer" },
      confidence: { type: "number" },
      evidence: { type: "string" }
    },
    required: [
      "value",
      "source_document_name",
      "source_document_type",
      "source_document_index",
      "confidence",
      "evidence"
    ]
  };
}

function makeDateFieldSchema(description) {
  return {
    type: "object",
    additionalProperties: false,
    properties: {
      value: { type: "string", description },
      time: { type: "string" },
      source_document_name: { type: "string" },
      source_document_type: { type: "string" },
      source_document_index: { type: "integer" },
      confidence: { type: "number" },
      evidence: { type: "string" }
    },
    required: [
      "value",
      "time",
      "source_document_name",
      "source_document_type",
      "source_document_index",
      "confidence",
      "evidence"
    ]
  };
}

function makeArrayFieldSchema(description) {
  return {
    type: "object",
    additionalProperties: false,
    properties: {
      value: {
        type: "array",
        description,
        items: { type: "string" }
      },
      source_document_name: { type: "string" },
      source_document_type: { type: "string" },
      source_document_index: { type: "integer" },
      confidence: { type: "number" },
      evidence: { type: "string" }
    },
    required: [
      "value",
      "source_document_name",
      "source_document_type",
      "source_document_index",
      "confidence",
      "evidence"
    ]
  };
}

function makeImportantTermSchema() {
  return {
    type: "object",
    additionalProperties: false,
    properties: {
      label: { type: "string" },
      category: { type: "string" },
      value: { type: "string" },
      source_document_name: { type: "string" },
      source_document_type: { type: "string" },
      source_document_index: { type: "integer" },
      confidence: { type: "number" },
      evidence: { type: "string" }
    },
    required: [
      "label",
      "category",
      "value",
      "source_document_name",
      "source_document_type",
      "source_document_index",
      "confidence",
      "evidence"
    ]
  };
}

const EXTRACTION_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    documents: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          name: { type: "string" },
          likely_document_type: { type: "string" },
          classification_confidence: { type: "number" },
          note: { type: "string" }
        },
        required: ["name", "likely_document_type", "classification_confidence", "note"]
      }
    },
    fields: {
      type: "object",
      additionalProperties: false,
      properties: {
        property_address: makeScalarFieldSchema("Property address. Use empty string if unavailable."),
        buyer_names: makeArrayFieldSchema("Buyer names. Use an empty array if unavailable."),
        seller_names: makeArrayFieldSchema("Seller names. Use an empty array if unavailable."),
        purchase_price: makeScalarFieldSchema("Purchase price as a numeric string without currency symbols if possible. Use empty string if unavailable."),
        earnest_money_amount: makeScalarFieldSchema("Earnest money amount as a numeric string without currency symbols if possible. Use empty string if unavailable."),
        financing_type: makeScalarFieldSchema("Financing type like Conventional, Cash, FHA, VA, USDA. Use empty string if unavailable."),
        binding_date: makeDateFieldSchema("Binding date in YYYY-MM-DD when possible. Use empty string if unavailable."),
        closing_date: makeDateFieldSchema("Closing date in YYYY-MM-DD when possible. Use empty string if unavailable."),
        inspection_deadline: makeDateFieldSchema("Inspection deadline in YYYY-MM-DD when possible. Use empty string if unavailable."),
        possession_date: makeDateFieldSchema("Possession date in YYYY-MM-DD when possible. Use empty string if unavailable.")
      },
      required: [
        "property_address",
        "buyer_names",
        "seller_names",
        "purchase_price",
        "earnest_money_amount",
        "financing_type",
        "binding_date",
        "closing_date",
        "inspection_deadline",
        "possession_date"
      ]
    },
    important_terms: {
      type: "array",
      items: makeImportantTermSchema()
    },
    warnings: {
      type: "array",
      items: { type: "string" }
    }
  },
  required: ["documents", "fields", "important_terms", "warnings"]
};

const DATE_REGEX = /\b(?:\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}|(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\.?\s+\d{1,2},?\s+\d{2,4})\b/i;
const TIME_REGEX = /\b\d{1,2}(?::\d{2})?\s?(?:a\.?m\.?|p\.?m\.?)\b/i;
const ADDRESS_REGEX = /\b\d{1,6}\s+[A-Za-z0-9.'#-]+(?:\s+[A-Za-z0-9.'#-]+){0,7}\s+(?:Street|St|Avenue|Ave|Road|Rd|Lane|Ln|Drive|Dr|Court|Ct|Circle|Cir|Way|Boulevard|Blvd|Place|Pl|Terrace|Ter|Trail|Trl|Parkway|Pkwy)\b(?:[^\n,]*)(?:,\s*[A-Za-z .'-]+,\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?)?/i;
const NAME_STOPWORDS = /\b(closing|costs|prepaid|items|temporary|permanent|rate|buy|down|mortgage|loan|seller|buyer|pay|paid|credit|commission|broker|agent|inspection|possession|date|earnest|money|price|financing|realtors?|association|authorized|copyright|version|licensee|brokerage|office|prepared|mls)\b/i;
const BOILERPLATE_LINE_PATTERNS = [
  /\bthis form is copyrighted\b/i,
  /\bcopyright\b/i,
  /\bauthorized user\b/i,
  /\bunauthorized use\b/i,
  /\blegal sanctions\b/i,
  /\breported to\b/i,
  /\bassociation of realtors\b/i,
  /\btennessee realtors\b/i,
  /\brealtors?\b/i,
  /\bversion\s+\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}\b/i,
  /\bpage\s+\d+\s+of\s+\d+\b/i,
  /\brf\s*\d{3,}\b/i,
  /\bpurchase and sale agreement\b/i,
  /\blot\/land purchase and sale agreement\b/i
];
const AGENT_ATTRIBUTION_PATTERNS = [
  /\bagent\b/i,
  /\bbroker(?:age)?\b/i,
  /\blicensee\b/i,
  /\brealtors?\b/i,
  /\bassociation\b/i,
  /\bprepared by\b/i,
  /\bmls\b/i,
  /\boffice\b/i,
  /\bphone\b/i,
  /\bfax\b/i,
  /\bemail\b/i,
  /\bauthorized user\b/i
];
const RELEVANT_TEXT_PATTERNS = [
  /\[\[page/i,
  /\bproperty address\b/i,
  /\bstreet address\b/i,
  /\bsubject property\b/i,
  /\bbuyer\b/i,
  /\bpurchaser\b/i,
  /\bseller\b/i,
  /\bowner\b/i,
  /\bpurchase price\b/i,
  /\bsales price\b/i,
  /\bcontract price\b/i,
  /\bearnest money\b/i,
  /\bdeposit\b/i,
  /\bfinancing\b/i,
  /\bloan type\b/i,
  /\bbinding date\b/i,
  /\beffective date\b/i,
  /\bacceptance date\b/i,
  /\bclosing date\b/i,
  /\binspection\b/i,
  /\bdue diligence\b/i,
  /\boption period\b/i,
  /\boption fee\b/i,
  /\bdue diligence fee\b/i,
  /\bappraisal\b/i,
  /\bloan commitment\b/i,
  /\bloan approval\b/i,
  /\bfinancing contingency\b/i,
  /\bmortgage contingency\b/i,
  /\btitle company\b/i,
  /\bescrow\b/i,
  /\bclosing agent\b/i,
  /\bseller concession\b/i,
  /\bclosing costs paid by seller\b/i,
  /\bhome warranty\b/i,
  /\bhome sale contingency\b/i,
  /\bassociation dues\b/i,
  /\bhomeowners association\b/i,
  /\brepair\b/i,
  /\btermite\b/i,
  /\bradon\b/i,
  /\bpossession\b/i,
  /\boccupancy\b/i,
  /\bleaseback\b/i,
  /\brent back\b/i
];

const IMPORTANT_TERM_DEFINITIONS = [
  {
    key: "financing_contingency",
    label: "Financing / Loan Contingency",
    category: "Contingency",
    patterns: [/\bloan commitment\b/i, /\bloan approval\b/i, /\bfinancing contingency\b/i, /\bmortgage contingency\b/i]
  },
  {
    key: "appraisal_term",
    label: "Appraisal Term",
    category: "Contingency",
    patterns: [/\bappraisal\b/i]
  },
  {
    key: "inspection_due_diligence",
    label: "Inspection / Due Diligence Term",
    category: "Contingency",
    patterns: [/\binspection period\b/i, /\bdue diligence\b/i, /\boption period\b/i]
  },
  {
    key: "option_fee",
    label: "Option / Due Diligence Fee",
    category: "Money",
    patterns: [/\boption fee\b/i, /\bdue diligence fee\b/i, /\boption money\b/i]
  },
  {
    key: "seller_concessions",
    label: "Seller Concessions / Closing Costs",
    category: "Money",
    patterns: [/\bseller\b.*\bclosing costs?\b/i, /\bclosing costs paid by seller\b/i, /\bseller\b.*\bconcessions?\b/i, /\bseller contribution\b/i]
  },
  {
    key: "title_escrow",
    label: "Title / Escrow Company",
    category: "Closing",
    patterns: [/\btitle company\b/i, /\bescrow (?:agent|company|holder)\b/i, /\bclosing (?:agent|company)\b/i]
  },
  {
    key: "occupancy_terms",
    label: "Occupancy / Possession Terms",
    category: "Occupancy",
    patterns: [/\bpost[- ]closing occupancy\b/i, /\boccupancy\b/i, /\bleaseback\b/i, /\brent back\b/i, /\brent-back\b/i]
  },
  {
    key: "home_sale_contingency",
    label: "Home Sale Contingency",
    category: "Contingency",
    patterns: [/\bhome sale contingency\b/i, /\bsale of buyer'?s property\b/i, /\bsubject to sale\b/i]
  },
  {
    key: "home_warranty",
    label: "Home Warranty",
    category: "Closing",
    patterns: [/\bhome warranty\b/i]
  },
  {
    key: "hoa_terms",
    label: "HOA / Association",
    category: "Property",
    patterns: [/\bhoa\b/i, /\bhomeowners association\b/i, /\bassociation dues\b/i]
  },
  {
    key: "repair_terms",
    label: "Repair / Treatment Terms",
    category: "Repairs",
    patterns: [/\brepair\b/i, /\btermite\b/i, /\bwood destroying\b/i, /\bradon\b/i, /\bmold\b/i]
  }
];
const DOCUMENT_PRIORITIES = {
  purchase_contract: 100,
  counteroffer: 220,
  amendment: 240,
  addendum: 180,
  disclosure: 80,
  unknown: 10
};
const DOC_TYPE_PATTERNS = [
  { type: "purchase_contract", label: "Purchase Contract", pattern: /(purchase and sale agreement|purchase contract|sale contract|contract to buy|contract to purchase|rf\s*401|residential purchase)/i, score: 140 },
  { type: "counteroffer", label: "Counteroffer", pattern: /(counter[\s-]?offer|counterproposal)/i, score: 120 },
  { type: "amendment", label: "Amendment", pattern: /\bamend(?:ment)?\b/i, score: 110 },
  { type: "addendum", label: "Addendum", pattern: /\baddend(?:um|a)\b/i, score: 90 },
  { type: "disclosure", label: "Disclosure", pattern: /(disclosure|lead[- ]based paint|seller property disclosure|hoa disclosure)/i, score: 80 }
];

function hasFieldValue(field) {
  return field?.value != null && field?.value !== "" && (!Array.isArray(field.value) || field.value.length);
}

function buildRelevantTextExcerpt(text, maxLength) {
  const normalized = normalizePdfText(text);
  if (!normalized) return { text: "", truncated: false };
  if (normalized.length <= maxLength) {
    return { text: normalized, truncated: false };
  }

  const lines = splitLines(normalized);
  if (!lines.length) return { text: "", truncated: false };

  const relevantIndexes = new Set();
  for (let index = 0; index < Math.min(lines.length, 24); index += 1) {
    relevantIndexes.add(index);
  }
  for (let index = Math.max(0, lines.length - 18); index < lines.length; index += 1) {
    relevantIndexes.add(index);
  }

  lines.forEach((line, index) => {
    const isRelevant = RELEVANT_TEXT_PATTERNS.some((pattern) => pattern.test(line))
      || DATE_REGEX.test(line)
      || /\$\s?\d/.test(line);
    if (!isRelevant) return;

    for (let offset = -2; offset <= 3; offset += 1) {
      const targetIndex = index + offset;
      if (targetIndex >= 0 && targetIndex < lines.length) {
        relevantIndexes.add(targetIndex);
      }
    }
  });

  const excerpt = [...relevantIndexes]
    .sort((left, right) => left - right)
    .map((index) => lines[index])
    .join("\n");

  return truncateText(excerpt || normalized, maxLength);
}

function detectDocumentType(name, text) {
  const haystack = `${name || ""}\n${text || ""}`;
  const matches = DOC_TYPE_PATTERNS
    .map((entry) => {
      let score = 0;
      if (entry.pattern.test(name || "")) score += entry.score + 30;
      if (entry.pattern.test(text || "")) score += entry.score;
      return score ? { ...entry, score } : null;
    })
    .filter(Boolean)
    .sort((left, right) => right.score - left.score);

  const match = matches[0];
  if (!match) {
    return {
      type: "unknown",
      label: "Unknown",
      confidence: 0.4
    };
  }

  return {
    type: match.type,
    label: typeof match.label === "string" ? match.label : "Unknown",
    confidence: Math.min(0.97, 0.55 + match.score / 250)
  };
}

function splitLines(text) {
  return normalizePdfText(text)
    .split(/\n+/)
    .map((line) => cleanText(line))
    .filter(Boolean);
}

function cleanupCandidate(value) {
  return cleanText(value)
    .replace(/^[\s:;-]+/, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function looksLikePersonName(value) {
  const cleaned = cleanupCandidate(value)
    .replace(/\([^)]*\)/g, "")
    .trim();

  if (!cleaned || cleaned.length < 4 || cleaned.length > 120) return false;
  if (/\d/.test(cleaned)) return false;
  if (NAME_STOPWORDS.test(cleaned)) return false;
  if (!/[A-Za-z]/.test(cleaned)) return false;

  const words = cleaned.split(/\s+/).filter(Boolean);
  if (words.length < 2 || words.length > 6) return false;

  return words.every((word) => (
    /^[A-Z][A-Za-z.'-]+$/.test(word)
    || /^[A-Z]{2,}$/.test(word)
    || /^(LLC|INC|LLP|LP|TRUST)$/i.test(word)
  ));
}

function splitPartyNames(value) {
  const cleaned = cleanupCandidate(value)
    .replace(/\bas (?:joint tenants|tenants in common|husband and wife)[\s\S]*$/i, "")
    .replace(/\bmarital status[\s\S]*$/i, "")
    .replace(/\bwhose address is[\s\S]*$/i, "")
    .replace(/\b(?:agent|broker|brokerage|licensee|realtor|realtors|association|authorized user)\b[\s\S]*$/i, "")
    .trim();

  if (!cleaned) return [];

  return [...new Set(
    cleaned
      .split(/\s*;\s*|\s+\band\b\s+|\s*&\s*|\s*\/\s*|,\s*(?=[A-Z])/i)
      .map((part) => cleanupCandidate(part))
      .filter((part) => looksLikePersonName(part))
  )];
}

function getLineValueAfterLabel(lines, patterns) {
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];

    for (const pattern of patterns) {
      const match = line.match(pattern);
      if (!match) continue;

      const sameLineValue = cleanupCandidate(match[1] || "");
      if (sameLineValue && sameLineValue.length > 1) return sameLineValue;

      const nextLineValue = cleanupCandidate(lines[index + 1] || "");
      if (nextLineValue) return nextLineValue;
    }
  }

  return "";
}

function findBestFormField(formFields, patterns, validator) {
  for (const field of formFields) {
    const matches = patterns.some((pattern) => pattern.test(field.normalizedName || ""));
    if (!matches) continue;
    if (isAgentAttributionLine(field.normalizedName || "") || isBoilerplateLine(field.normalizedName || "")) continue;
    if (typeof validator === "function" && !validator(field.value, field)) continue;
    return field;
  }
  return null;
}

function findFinanceKeyword(value) {
  const match = cleanText(value).match(/\b(conventional|cash|fha|va|usda|assumption|owner financing|seller financing)\b/i);
  return match ? match[1].replace(/\b\w/g, (char) => char.toUpperCase()) : "";
}

function extractTextDateHint(text, labels) {
  for (const label of labels) {
    const regex = new RegExp(`${label}[\\s\\S]{0,120}`, "i");
    const snippetMatch = text.match(regex);
    if (!snippetMatch) continue;

    const snippet = snippetMatch[0];
    const dateMatch = snippet.match(DATE_REGEX);
    if (!dateMatch) continue;

    const date = normalizeDate(dateMatch[0]);
    if (!date) continue;

    const timeMatch = snippet.match(TIME_REGEX);
    return `${date}${timeMatch ? ` ${normalizeTime(timeMatch[0])}` : ""}`;
  }

  return "";
}

function trimSnippet(value, maxLength = 220) {
  const cleaned = cleanText(value).replace(/\s{2,}/g, " ").trim();
  if (!cleaned) return "";
  if (cleaned.length <= maxLength) return cleaned;
  return `${cleaned.slice(0, maxLength - 3).trim()}...`;
}

function buildLineSnippet(lines, index, before = 0, after = 2) {
  return trimSnippet(
    lines
      .slice(Math.max(0, index - before), Math.min(lines.length, index + after + 1))
      .join(" ")
      .replace(/\[\[page\s+\d+\]\]/ig, " ")
  );
}

function extractValueFromLabeledLine(line) {
  const match = line.match(/(?:\:\s*|\s-\s*)(.+)$/);
  return match ? trimSnippet(match[1], 180) : "";
}

function buildHeuristicImportantTermsForDocument(document) {
  const lines = splitLines(document.text);
  const importantTerms = [];

  IMPORTANT_TERM_DEFINITIONS.forEach((definition) => {
    const fieldMatch = findBestFormField(
      document.formFields,
      definition.patterns,
      (value) => cleanText(value).length > 1
    );
    if (fieldMatch) {
      importantTerms.push({
        key: definition.key,
        label: definition.label,
        category: definition.category,
        value: trimSnippet(fieldMatch.value, 180),
        confidence: 0.72,
        evidence: `form field: ${fieldMatch.name}: ${fieldMatch.value}`,
        sourceDocumentId: document.id,
        sourceDocumentName: document.name,
        sourceDocumentType: document.localDocType?.label || "Unknown",
        sourceDocumentIndex: document.sourceIndex
      });
      return;
    }

    for (let index = 0; index < lines.length; index += 1) {
      if (!definition.patterns.some((pattern) => pattern.test(lines[index]))) continue;
      const inlineValue = extractValueFromLabeledLine(lines[index]);
      const directLine = trimSnippet(lines[index], 180);
      const snippet = inlineValue || directLine || buildLineSnippet(lines, index, 0, 2);
      if (!snippet) continue;

      importantTerms.push({
        key: definition.key,
        label: definition.label,
        category: definition.category,
        value: snippet,
        confidence: 0.62,
        evidence: buildLineSnippet(lines, index, 0, 2),
        sourceDocumentId: document.id,
        sourceDocumentName: document.name,
        sourceDocumentType: document.localDocType?.label || "Unknown",
        sourceDocumentIndex: document.sourceIndex
      });
      break;
    }
  });

  return importantTerms;
}

function importantTermLabelKey(term) {
  return slugify(term?.label || "");
}

function importantTermValueKey(term) {
  return slugify(`${term?.label || ""}-${term?.value || ""}`);
}

function mergeImportantTerms(primaryTerms = [], fallbackTerms = []) {
  const merged = [];
  const seenLabels = new Set();
  const seenValues = new Set();

  [...primaryTerms, ...fallbackTerms].forEach((term) => {
    const label = cleanText(term?.label);
    const value = trimSnippet(term?.value, 220);
    if (!label || !value) return;

    const labelKey = importantTermLabelKey({ label });
    const valueKey = importantTermValueKey({ label, value });
    if (!labelKey || seenLabels.has(labelKey) || seenValues.has(valueKey)) return;

    seenLabels.add(labelKey);
    seenValues.add(valueKey);
    merged.push({
      ...term,
      label,
      value
    });
  });

  return merged.slice(0, MAX_IMPORTANT_TERMS);
}

function buildHeuristicImportantTerms(documents) {
  const prioritizedDocuments = [...documents].sort((left, right) => {
    const leftPriority = DOCUMENT_PRIORITIES[left.localDocType?.type] || DOCUMENT_PRIORITIES.unknown;
    const rightPriority = DOCUMENT_PRIORITIES[right.localDocType?.type] || DOCUMENT_PRIORITIES.unknown;
    if (rightPriority !== leftPriority) return rightPriority - leftPriority;
    return right.sourceIndex - left.sourceIndex;
  });

  return mergeImportantTerms(
    [],
    prioritizedDocuments.flatMap((document) => document.importantTermHints || [])
  );
}

function buildCandidateHints(document) {
  const lines = splitLines(document.text);
  const text = document.text || "";
  const hints = {};

  function setHint(fieldName, value, source) {
    const cleaned = cleanupCandidate(value);
    if (!cleaned || hints[fieldName]) return;
    hints[fieldName] = {
      value: cleaned,
      source
    };
  }

  const addressFormField = findBestFormField(document.formFields, [
    /\b(property|street|subject)\b.*\baddress\b/,
    /\baddress\b/
  ], (value) => ADDRESS_REGEX.test(value));
  if (addressFormField) {
    setHint("property_address", addressFormField.value, `form field: ${addressFormField.name}`);
  } else {
    const addressFromLabel = getLineValueAfterLabel(lines, [
      /(?:property address|property located at|street address|property to be conveyed)\s*[:\-]?\s*(.*)$/i
    ]);
    if (ADDRESS_REGEX.test(addressFromLabel)) {
      setHint("property_address", addressFromLabel, "text label");
    } else {
      const inlineAddress = text.match(ADDRESS_REGEX);
      if (inlineAddress) setHint("property_address", inlineAddress[0], "text pattern");
    }
  }

  const buyerField = findBestFormField(document.formFields, [/\bbuyer\b/, /\bpurchaser\b/], (value) => looksLikePersonName(value));
  if (buyerField) {
    setHint("buyer_names", buyerField.value, `form field: ${buyerField.name}`);
  } else {
    const buyerLine = getLineValueAfterLabel(lines, [
      /^(?:buyer(?:\(s\))?|purchaser(?:\(s\))?)\s*[:\-]?\s*(.*)$/i,
      /^(?:name of buyer|buyer name(?:\(s\))?)\s*[:\-]?\s*(.*)$/i
    ]);
    const names = splitPartyNames(buyerLine);
    if (names.length) setHint("buyer_names", names.join(", "), "text label");
  }

  const sellerField = findBestFormField(document.formFields, [/\bseller\b/, /\bowner\b/], (value) => looksLikePersonName(value));
  if (sellerField) {
    setHint("seller_names", sellerField.value, `form field: ${sellerField.name}`);
  } else {
    const sellerLine = getLineValueAfterLabel(lines, [
      /^(?:seller(?:\(s\))?|owner(?:\(s\))?)\s*[:\-]?\s*(.*)$/i,
      /^(?:name of seller|seller name(?:\(s\))?)\s*[:\-]?\s*(.*)$/i
    ]);
    const names = splitPartyNames(sellerLine);
    if (names.length) setHint("seller_names", names.join(", "), "text label");
  }

  const priceField = findBestFormField(document.formFields, [/\bpurchase\b.*\bprice\b/, /\bsales?\b.*\bprice\b/], (value) => parseCurrency(value) != null);
  if (priceField) {
    setHint("purchase_price", String(parseCurrency(priceField.value)), `form field: ${priceField.name}`);
  } else {
    const priceLine = getLineValueAfterLabel(lines, [
      /(?:purchase price|sales price|contract price)\s*[:\-]?\s*(.*)$/i
    ]);
    const price = parseCurrency(priceLine);
    if (price != null) setHint("purchase_price", String(price), "text label");
  }

  const earnestField = findBestFormField(document.formFields, [/\bearnest\b.*\bmoney\b/, /\bdeposit\b/], (value) => parseCurrency(value) != null);
  if (earnestField) {
    setHint("earnest_money_amount", String(parseCurrency(earnestField.value)), `form field: ${earnestField.name}`);
  } else {
    const earnestLine = getLineValueAfterLabel(lines, [
      /(?:earnest money|earnest money amount|deposit)\s*[:\-]?\s*(.*)$/i
    ]);
    const earnest = parseCurrency(earnestLine);
    if (earnest != null) setHint("earnest_money_amount", String(earnest), "text label");
  }

  const financingField = findBestFormField(document.formFields, [/\bfinancing\b/, /\bloan\b.*\btype\b/], (value) => Boolean(findFinanceKeyword(value)));
  if (financingField) {
    setHint("financing_type", findFinanceKeyword(financingField.value), `form field: ${financingField.name}`);
  } else {
    const financingLine = getLineValueAfterLabel(lines, [
      /(?:financing type|type of financing|loan type)\s*[:\-]?\s*(.*)$/i
    ]);
    const financing = findFinanceKeyword(financingLine || text);
    if (financing) setHint("financing_type", financing, financingLine ? "text label" : "text pattern");
  }

  const bindingField = findBestFormField(document.formFields, [/\bbinding\b.*\bdate\b/, /\beffective\b.*\bdate\b/, /\bacceptance\b.*\bdate\b/], (value) => Boolean(normalizeDate(value)));
  if (bindingField) {
    setHint("binding_date", normalizeDate(bindingField.value), `form field: ${bindingField.name}`);
  } else {
    const bindingDate = extractTextDateHint(text, ["binding date", "effective date", "acceptance date"]);
    if (bindingDate) setHint("binding_date", bindingDate, "text label");
  }

  const closingField = findBestFormField(document.formFields, [/\bclosing\b.*\bdate\b/, /\bdate\b.*\bclosing\b/], (value) => Boolean(normalizeDate(value)));
  if (closingField) {
    setHint("closing_date", normalizeDate(closingField.value), `form field: ${closingField.name}`);
  } else {
    const closingDate = extractTextDateHint(text, ["closing date", "date of closing", "close of escrow"]);
    if (closingDate) setHint("closing_date", closingDate, "text label");
  }

  const inspectionField = findBestFormField(document.formFields, [/\binspection\b.*\bdate\b/, /\binspection\b.*\bdeadline\b/, /\bdue diligence\b/, /\boption\b.*\bperiod\b/], (value) => Boolean(normalizeDate(value)));
  if (inspectionField) {
    setHint("inspection_deadline", normalizeDate(inspectionField.value), `form field: ${inspectionField.name}`);
  } else {
    const inspectionDate = extractTextDateHint(text, ["inspection deadline", "inspection period", "due diligence", "option period"]);
    if (inspectionDate) setHint("inspection_deadline", inspectionDate, "text label");
  }

  const possessionField = findBestFormField(document.formFields, [/\bpossession\b.*\bdate\b/, /\boccupancy\b.*\bdate\b/], (value) => Boolean(normalizeDate(value)));
  if (possessionField) {
    setHint("possession_date", normalizeDate(possessionField.value), `form field: ${possessionField.name}`);
  } else {
    const possessionDate = extractTextDateHint(text, ["possession date", "date of possession", "occupancy date"]);
    if (possessionDate) setHint("possession_date", possessionDate, "text label");
  }

  return hints;
}

function truncateText(value, maxLength) {
  const text = normalizePdfText(value);
  if (!text) return { text: "", truncated: false };
  if (text.length <= maxLength) return { text, truncated: false };

  const headLength = Math.floor(maxLength * 0.7);
  const tailLength = maxLength - headLength;
  return {
    text: `${text.slice(0, headLength).trim()}\n\n[truncated middle]\n\n${text.slice(-tailLength).trim()}`,
    truncated: true
  };
}

function normalizeFormFields(formFields) {
  if (!Array.isArray(formFields)) return [];

  return formFields
    .map((field) => ({
      name: cleanText(field?.name),
      normalizedName: cleanText(field?.normalizedName),
      value: cleanText(field?.value)
    }))
    .filter((field) => field.name && field.value)
    .slice(0, MAX_FORM_FIELDS_PER_DOCUMENT);
}

function buildDocumentContext(document) {
  const sections = [
    `Document ${document.sourceIndex}`,
    `Uploaded name: ${document.name}`,
    `Page count: ${document.pageCount || "unknown"}`,
    `Client extraction status: ${document.status || "processed"}`
  ];

  if (document.formFields.length) {
    sections.push("Form fields:");
    document.formFields.forEach((field) => {
      sections.push(`- ${field.name}: ${field.value}`);
    });
  } else {
    sections.push("Form fields: none detected.");
  }

  if (document.modelText) {
    sections.push("Extracted text:");
    sections.push(document.modelText);
  } else {
    sections.push("Extracted text: none detected.");
  }

  if (document.textTruncated) {
    sections.push(`Note: the relevant text excerpt was shortened to fit within the model-input budget of ${MAX_TEXT_CHARS_PER_DOCUMENT} characters.`);
  }

  const candidateEntries = Object.entries(document.candidateHints || {});
  if (candidateEntries.length) {
    sections.push("Field candidate hints from label/form-field parsing. Validate them against the text before using them:");
    candidateEntries.forEach(([fieldName, hint]) => {
      sections.push(`- ${FIELD_LABELS[fieldName]}: ${hint.value} (${hint.source})`);
    });
  }

  if (document.importantTermHints?.length) {
    sections.push("Additional important-term hints from heuristic parsing. Validate them against the text before using them:");
    document.importantTermHints.forEach((term) => {
      sections.push(`- ${term.label}: ${term.value}`);
    });
  }

  return sections.join("\n");
}

function buildPrompt(documents) {
  const docsList = documents.map((document) => `${document.sourceIndex}: ${document.name}`).join("\n");

  return [
    "Extract normalized contract data from the provided real estate contract packet.",
    "The packet may include a purchase contract plus counteroffers, amendments, or addenda in different formats and markets.",
    "Use only what is explicitly supported by the extracted text and form-field data below. Do not guess.",
    "Treat the field candidate hints below as non-binding suggestions only. They are helpers, not source of truth.",
    "Buyer names and seller names must be actual person or entity names only. Never use cost allocation text, financing language, or phrases like closing costs, prepaid items, to pay, rate buy down, or similar non-name content as party names.",
    "Never use agent, broker, REALTOR association, authorized-user, copyright footer, page footer, title company, or signature-block names as buyer or seller parties.",
    "If a filename or document text says Purchase and Sale Agreement or RF 401, prefer classifying it as a purchase contract unless the document clearly says amendment, counteroffer, or addendum.",
    "Only extract financing type when the contract explicitly names a financing program or says cash.",
    "If an inspection or possession term is expressed only as a relative period or condition and not an actual date, leave the date blank and mention that in warnings.",
    "If a field is not clearly present, return an empty string for scalar values, an empty array for name arrays, -1 for source_document_index, 0 for confidence, and an empty string for evidence.",
    "For date fields, normalize to YYYY-MM-DD when possible. If a time is present, return it in the time field; otherwise use an empty string.",
    "For money fields, return numeric strings without currency symbols or commas when possible.",
    "Also return up to 12 additional important_terms for materially important clauses that are not fully captured by the core fields. Prioritize contingencies, seller concessions, option or due-diligence terms, appraisal, financing deadlines, title or escrow, occupancy or rent-back, HOA, home warranty, and repair obligations.",
    "important_terms values should be short, human-readable summaries grounded in the text. They may include relative periods or conditional language if that is how the contract states the term.",
    "Classify each uploaded document by likely document type.",
    "Choose the best current value for each field across all documents. A later amendment or counteroffer can override an earlier purchase contract only if it clearly changes that field.",
    "When source_document_name is populated, it must match one of these exact uploaded names:",
    docsList,
    "",
    "Documents:",
    ...documents.map((document) => buildDocumentContext(document))
  ].join("\n");
}

async function callOpenAIContractExtraction(documents) {
  if (!OPENAI_API_KEY) {
    throw new Error("Missing OPENAI_API_KEY_CTC or OPENAI_API_KEY for server-side contract extraction.");
  }

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: MODEL,
        temperature: 0,
        messages: [
          {
            role: "system",
            content: "You extract structured data from real estate contracts. Be conservative. Prefer exact text and explicit evidence over inference."
          },
          {
            role: "user",
            content: buildPrompt(documents)
          }
        ],
        response_format: {
          type: "json_schema",
          json_schema: {
            name: "ctc_contract_extraction",
            strict: true,
            schema: EXTRACTION_SCHEMA
          }
        }
      }),
      signal: AbortSignal.timeout(OPENAI_TIMEOUT_MS)
    });

    const text = await response.text();
    if (!response.ok) {
      throw new Error(`OpenAI extraction failed (${response.status}): ${text}`);
    }

    const payload = JSON.parse(text);
    return {
      raw: payload,
      parsed: extractJsonFromCompletion(payload)
    };
  } catch (error) {
    if (error?.name === "TimeoutError" || error?.name === "AbortError") {
      throw new Error("The extractor timed out while waiting on the AI model. Try a smaller packet or a text-searchable PDF.");
    }
    throw error;
  }
}

function getHintConfidence(source) {
  const sourceText = cleanText(source).toLowerCase();
  if (sourceText.startsWith("form field")) return 0.78;
  if (sourceText === "text label") return 0.7;
  if (sourceText === "text pattern") return 0.58;
  return 0.55;
}

function buildHeuristicField(fieldName, hint, document) {
  const evidence = hint?.source ? `${hint.source}: ${hint.value}` : hint?.value || "";
  const confidence = getHintConfidence(hint?.source);
  const baseField = {
    key: fieldName,
    label: FIELD_LABELS[fieldName],
    confidence,
    sourceDocumentId: document.id,
    sourceDocumentName: document.name,
    sourceDocumentType: document.localDocType?.label || "Unknown",
    sourceDocumentIndex: document.sourceIndex,
    evidence
  };

  if (ARRAY_FIELDS.has(fieldName)) {
    const names = splitPartyNames(hint?.value);
    return {
      ...baseField,
      value: names,
      time: null,
      displayValue: formatFieldValue(fieldName, names, null)
    };
  }

  if (CURRENCY_FIELDS.has(fieldName)) {
    const amount = parseCurrency(hint?.value);
    return {
      ...baseField,
      value: amount,
      time: null,
      displayValue: formatFieldValue(fieldName, amount, null)
    };
  }

  if (DATE_FIELDS.has(fieldName)) {
    const value = cleanText(hint?.value);
    const dateMatch = value.match(/\d{4}-\d{2}-\d{2}/);
    const timeMatch = value.match(TIME_REGEX);
    const normalizedDateValue = normalizeDate(dateMatch?.[0] || value);
    const normalizedTimeValue = normalizeTime(timeMatch?.[0] || "");
    return {
      ...baseField,
      value: normalizedDateValue,
      time: normalizedTimeValue,
      displayValue: formatFieldValue(fieldName, normalizedDateValue, normalizedTimeValue)
    };
  }

  const scalarValue = cleanText(hint?.value) || null;
  return {
    ...baseField,
    value: scalarValue,
    time: null,
    displayValue: formatFieldValue(fieldName, scalarValue, null)
  };
}

function buildHeuristicFieldMap(documents) {
  const fieldMap = {};
  Object.keys(FIELD_LABELS).forEach((fieldName) => {
    fieldMap[fieldName] = buildEmptyField(fieldName);
  });

  const prioritizedDocuments = [...documents].sort((left, right) => {
    const leftPriority = DOCUMENT_PRIORITIES[left.localDocType?.type] || DOCUMENT_PRIORITIES.unknown;
    const rightPriority = DOCUMENT_PRIORITIES[right.localDocType?.type] || DOCUMENT_PRIORITIES.unknown;
    if (rightPriority !== leftPriority) return rightPriority - leftPriority;
    return right.sourceIndex - left.sourceIndex;
  });

  prioritizedDocuments.forEach((document) => {
    Object.entries(document.candidateHints || {}).forEach(([fieldName, hint]) => {
      if (!FIELD_LABELS[fieldName] || hasFieldValue(fieldMap[fieldName])) return;
      const heuristicField = buildHeuristicField(fieldName, hint, document);
      if (hasFieldValue(heuristicField)) {
        fieldMap[fieldName] = heuristicField;
      }
    });
  });

  return fieldMap;
}

function mergeFieldMaps(primaryFields, fallbackFields) {
  const merged = {};
  Object.keys(FIELD_LABELS).forEach((fieldName) => {
    merged[fieldName] = hasFieldValue(primaryFields?.[fieldName]) ? primaryFields[fieldName] : fallbackFields[fieldName];
  });
  return merged;
}

function normalizeModelField(fieldName, rawField, documentLookup) {
  const sourceDocumentName = cleanText(rawField?.source_document_name);
  const fallbackDocument = documentLookup.get(sourceDocumentName) || null;
  const sourceDocumentIndex = Number.isInteger(rawField?.source_document_index) && rawField.source_document_index >= 0
    ? rawField.source_document_index
    : fallbackDocument?.sourceIndex ?? null;
  const sourceDocumentType = cleanText(rawField?.source_document_type) || fallbackDocument?.likelyDocumentType || null;
  const confidence = clampConfidence(rawField?.confidence);
  const evidence = cleanText(rawField?.evidence) || null;

  let value = null;
  let time = null;

  if (ARRAY_FIELDS.has(fieldName)) {
    value = normalizeNameArray(rawField?.value);
  } else if (CURRENCY_FIELDS.has(fieldName)) {
    value = parseCurrency(rawField?.value);
  } else if (DATE_FIELDS.has(fieldName)) {
    value = normalizeDate(rawField?.value);
    time = normalizeTime(rawField?.time);
  } else {
    value = cleanText(rawField?.value) || null;
  }

  return {
    key: fieldName,
    label: FIELD_LABELS[fieldName],
    value,
    time,
    displayValue: formatFieldValue(fieldName, value, time),
    confidence,
    sourceDocumentId: fallbackDocument?.id || (sourceDocumentName ? slugify(`${sourceDocumentName}-${sourceDocumentIndex ?? "x"}`) : null),
    sourceDocumentName: sourceDocumentName || null,
    sourceDocumentType: sourceDocumentType || null,
    sourceDocumentIndex,
    evidence
  };
}

function normalizeImportantTerm(rawTerm, documentLookup) {
  const label = cleanText(rawTerm?.label);
  const value = trimSnippet(rawTerm?.value, 220);
  if (!label || !value) return null;

  const sourceDocumentName = cleanText(rawTerm?.source_document_name);
  const fallbackDocument = documentLookup.get(sourceDocumentName) || null;
  const sourceDocumentIndex = Number.isInteger(rawTerm?.source_document_index) && rawTerm.source_document_index >= 0
    ? rawTerm.source_document_index
    : fallbackDocument?.sourceIndex ?? null;
  const sourceDocumentType = cleanText(rawTerm?.source_document_type) || fallbackDocument?.likelyDocumentType || null;

  return {
    label,
    category: cleanText(rawTerm?.category) || "Important Term",
    value,
    confidence: clampConfidence(rawTerm?.confidence),
    sourceDocumentId: fallbackDocument?.id || (sourceDocumentName ? slugify(`${sourceDocumentName}-${sourceDocumentIndex ?? "x"}`) : null),
    sourceDocumentName: sourceDocumentName || null,
    sourceDocumentType: sourceDocumentType || null,
    sourceDocumentIndex,
    evidence: trimSnippet(rawTerm?.evidence, 260) || null
  };
}

function findLinesContainingName(documents, name) {
  const normalizedNeedle = cleanText(name).toLowerCase();
  if (!normalizedNeedle) return [];

  return documents.flatMap((document) =>
    splitLines(document.rawText || document.text)
      .filter((line) => cleanText(line).toLowerCase().includes(normalizedNeedle))
      .map((line) => ({
        line,
        documentName: document.name
      }))
  );
}

function isLikelyNoisePartyName(name, documents) {
  const cleaned = cleanupCandidate(name);
  if (!cleaned) return true;

  const matchingLines = findLinesContainingName(documents, cleaned);
  if (!matchingLines.length) return false;

  const supportingLines = matchingLines.filter(({ line }) => !isBoilerplateLine(line) && !isAgentAttributionLine(line));
  return supportingLines.length === 0;
}

function sanitizePartyField(field, documents) {
  if (!ARRAY_FIELDS.has(field?.key)) return field;

  const nextNames = normalizeNameArray(field.value).filter((name) => !isLikelyNoisePartyName(name, documents));
  if (nextNames.length === normalizeNameArray(field.value).length) {
    return {
      ...field,
      value: nextNames,
      displayValue: formatFieldValue(field.key, nextNames, null)
    };
  }

  if (!nextNames.length) {
    return buildEmptyField(field.key);
  }

  return {
    ...field,
    value: nextNames,
    displayValue: formatFieldValue(field.key, nextNames, null),
    evidence: cleanText(field.evidence) || "Party names were filtered to remove footer or agent-attribution noise."
  };
}

function resolvePartyField(primaryField, fallbackField, documents) {
  const sanitizedPrimary = sanitizePartyField(primaryField, documents);
  if (hasFieldValue(sanitizedPrimary)) return sanitizedPrimary;

  const sanitizedFallback = sanitizePartyField(fallbackField, documents);
  if (hasFieldValue(sanitizedFallback)) return sanitizedFallback;

  return buildEmptyField(primaryField?.key || fallbackField?.key || "buyer_names");
}

function buildEmptyField(fieldName) {
  return {
    key: fieldName,
    label: FIELD_LABELS[fieldName],
    value: ARRAY_FIELDS.has(fieldName) ? [] : null,
    time: null,
    displayValue: "Not found",
    confidence: null,
    sourceDocumentId: null,
    sourceDocumentName: null,
    sourceDocumentType: null,
    sourceDocumentIndex: null,
    evidence: null
  };
}

function buildFallbackPayload(documents, warnings) {
  const fields = buildHeuristicFieldMap(documents);
  fields.buyer_names = sanitizePartyField(fields.buyer_names, documents);
  fields.seller_names = sanitizePartyField(fields.seller_names, documents);
  const importantTerms = buildHeuristicImportantTerms(documents);

  return {
    schema_version: "ctc.contract.extraction.v3",
    provider: "client_pdf_text_form_extraction",
    model: null,
    extracted_at: new Date().toISOString(),
    documents: documents.map((document, index) => ({
      id: slugify(`${document.name}-${index}`),
      sourceIndex: document.sourceIndex,
      name: document.name,
      size: document.size || 0,
      likelyDocumentType: document.localDocType?.label || "Unknown",
      classificationConfidence: document.localDocType?.confidence ?? null,
      status: document.status || "needs ocr",
      note: document.note || "No usable embedded text or form fields were found.",
      extractionMethod: document.extractionMethod || "client pdf parse",
      extractedFieldCount: countDocumentMappedFields(document.name, fields)
    })),
    fields,
    important_terms: importantTerms,
    source_details: buildSourceDetails(fields, importantTerms),
    warnings,
    debug: {
      provider: "client_pdf_text_form_extraction",
      raw_document_names: documents.map((document) => document.name),
      parsed_documents: documents.map((document) => ({
        name: document.name,
        pageCount: document.pageCount,
        textLength: (document.text || "").length,
        textTruncated: document.textTruncated,
        formFieldCount: document.formFields.length,
        status: document.status,
        textPreview: (document.text || "").slice(0, 1600),
        formFieldPreview: document.formFields.slice(0, 20),
        candidateHints: document.candidateHints,
        importantTermHints: document.importantTermHints
      }))
    }
  };
}

function normalizePayload(modelParsed, documents) {
  const heuristicFields = buildHeuristicFieldMap(documents);
  const heuristicImportantTerms = buildHeuristicImportantTerms(documents);
  const documentLookupInput = documents.map((document) => ({
    id: slugify(`${document.name}-${document.sourceIndex}`),
    sourceIndex: document.sourceIndex,
    name: document.name,
    size: document.size || 0,
    likelyDocumentType: document.localDocType?.label || "Unknown"
  }));

  const normalizedDocuments = documentLookupInput.map((document) => {
    const modelDocument = (modelParsed.documents || []).find((entry) => cleanText(entry.name) === document.name) || {};
    const inputDocument = documents.find((entry) => entry.name === document.name) || {};
    return {
      ...document,
      likelyDocumentType: cleanText(modelDocument.likely_document_type) || inputDocument.localDocType?.label || "Unknown",
      classificationConfidence: clampConfidence(modelDocument.classification_confidence) ?? inputDocument.localDocType?.confidence ?? null,
      status: inputDocument.status || "processed",
      note: cleanText(modelDocument.note) || inputDocument.note || "Server-side AI extraction completed.",
      extractionMethod: inputDocument.extractionMethod || "client pdf parse + server ai"
    };
  });

  const documentLookup = new Map(normalizedDocuments.map((document) => [document.name, document]));
  const modelFields = {};

  Object.keys(FIELD_LABELS).forEach((fieldName) => {
    const rawField = modelParsed?.fields?.[fieldName];
    modelFields[fieldName] = rawField
      ? normalizeModelField(fieldName, rawField, documentLookup)
      : buildEmptyField(fieldName);
  });

  const fields = mergeFieldMaps(modelFields, heuristicFields);
  fields.buyer_names = resolvePartyField(modelFields.buyer_names, heuristicFields.buyer_names, documents);
  fields.seller_names = resolvePartyField(modelFields.seller_names, heuristicFields.seller_names, documents);
  const modelImportantTerms = Array.isArray(modelParsed?.important_terms)
    ? modelParsed.important_terms
      .map((term) => normalizeImportantTerm(term, documentLookup))
      .filter(Boolean)
    : [];
  const importantTerms = mergeImportantTerms(modelImportantTerms, heuristicImportantTerms);

  const documentsWithCounts = normalizedDocuments.map((document) => ({
    ...document,
    extractedFieldCount: countDocumentMappedFields(document.name, fields)
  }));

  return {
    schema_version: "ctc.contract.extraction.v3",
    provider: "openai_server_text_extraction",
    model: MODEL,
    extracted_at: new Date().toISOString(),
    documents: documentsWithCounts,
    fields,
    important_terms: importantTerms,
    source_details: buildSourceDetails(fields, importantTerms),
    warnings: Array.isArray(modelParsed?.warnings) ? modelParsed.warnings.map((warning) => cleanText(warning)).filter(Boolean) : [],
    debug: {
      provider: "openai_server_text_extraction",
      model: MODEL,
      raw_document_names: documents.map((document) => document.name),
      parsed_documents: documents.map((document) => ({
        name: document.name,
        pageCount: document.pageCount,
        textLength: (document.text || "").length,
        textTruncated: document.textTruncated,
        formFieldCount: document.formFields.length,
        status: document.status,
        textPreview: (document.text || "").slice(0, 1600),
        formFieldPreview: document.formFields.slice(0, 20),
        candidateHints: document.candidateHints,
        importantTermHints: document.importantTermHints
      }))
    }
  };
}

function normalizeInputDocument(document, index) {
  const rawText = normalizePdfText(document?.text);
  const normalizedText = stripBoilerplatePdfText(rawText);
  const textInfo = buildRelevantTextExcerpt(normalizedText, MAX_TEXT_CHARS_PER_DOCUMENT);
  const formFields = normalizeFormFields(document?.formFields);
  const candidateHints = buildCandidateHints({
    text: normalizedText,
    formFields
  });
  const localDocType = detectDocumentType(document?.name, normalizedText);
  const importantTermHints = buildHeuristicImportantTermsForDocument({
    id: slugify(`${document?.name || "document"}-${index}`),
    sourceIndex: Number.isInteger(document?.sourceIndex) ? document.sourceIndex : index,
    name: cleanText(document?.name),
    text: normalizedText,
    formFields,
    localDocType
  });

  return {
    id: slugify(`${document?.name || "document"}-${index}`),
    sourceIndex: Number.isInteger(document?.sourceIndex) ? document.sourceIndex : index,
    name: cleanText(document?.name),
    size: Number(document?.size) || 0,
    pageCount: Number(document?.pageCount) || null,
    rawText,
    text: normalizedText,
    modelText: textInfo.text,
    textTruncated: textInfo.truncated,
    formFields,
    candidateHints,
    importantTermHints,
    localDocType,
    status: cleanText(document?.status) || ((textInfo.text || formFields.length) ? "processed" : "needs ocr"),
    note: cleanText(document?.note) || "",
    extractionMethod: cleanText(document?.extractionMethod) || "client pdf parse"
  };
}

export async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: corsHeaders(),
      body: ""
    };
  }

  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed." });
  }

  try {
    const body = parseBody(event);
    const documents = Array.isArray(body?.documents) ? body.documents : [];

    if (!documents.length) {
      return json(400, { error: "No documents were provided." });
    }

    const normalizedDocuments = documents
      .map((document, index) => normalizeInputDocument(document, index))
      .filter((document) => document.name);

    if (!normalizedDocuments.length) {
      return json(400, { error: "Uploaded documents were empty or invalid." });
    }

    const docsWithSignals = normalizedDocuments.filter((document) => document.modelText || document.formFields.length);
    if (!docsWithSignals.length) {
      return json(200, buildFallbackPayload(normalizedDocuments, [
        "No embedded text or fillable form fields were found in the uploaded PDFs. OCR is not configured yet for this extraction path."
      ]));
    }

    try {
      const result = await callOpenAIContractExtraction(normalizedDocuments);
      return json(200, normalizePayload(result.parsed, normalizedDocuments));
    } catch (error) {
      return json(200, buildFallbackPayload(normalizedDocuments, [
        `AI normalization was skipped and heuristic fallback was used instead: ${error?.message || "Unknown AI error."}`
      ]));
    }
  } catch (error) {
    return json(500, {
      error: error?.message || "Contract extraction failed."
    });
  }
}
