const MODEL = process.env.OPENAI_CONTRACT_EXTRACT_MODEL || "gpt-4o";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY_CTC || process.env.OPENAI_API_KEY;

const MAX_TEXT_CHARS_PER_DOCUMENT = 32000;
const MAX_FORM_FIELDS_PER_DOCUMENT = 80;
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

function buildSourceDetails(fieldMap) {
  return Object.values(fieldMap).map((field) => ({
    field: field.key,
    label: field.label,
    value: field.displayValue,
    confidence: field.confidence,
    sourceDocumentName: field.sourceDocumentName || "Not found",
    sourceDocumentType: field.sourceDocumentType || "Not found",
    evidence: field.evidence || ""
  }));
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
    warnings: {
      type: "array",
      items: { type: "string" }
    }
  },
  required: ["documents", "fields", "warnings"]
};

const DATE_REGEX = /\b(?:\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}|(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\.?\s+\d{1,2},?\s+\d{2,4})\b/i;
const TIME_REGEX = /\b\d{1,2}(?::\d{2})?\s?(?:a\.?m\.?|p\.?m\.?)\b/i;
const ADDRESS_REGEX = /\b\d{1,6}\s+[A-Za-z0-9.'#-]+(?:\s+[A-Za-z0-9.'#-]+){0,7}\s+(?:Street|St|Avenue|Ave|Road|Rd|Lane|Ln|Drive|Dr|Court|Ct|Circle|Cir|Way|Boulevard|Blvd|Place|Pl|Terrace|Ter|Trail|Trl|Parkway|Pkwy)\b(?:[^\n,]*)(?:,\s*[A-Za-z .'-]+,\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?)?/i;
const NAME_STOPWORDS = /\b(closing|costs|prepaid|items|temporary|permanent|rate|buy|down|mortgage|loan|seller|buyer|pay|paid|credit|commission|broker|agent|inspection|possession|date|earnest|money|price|financing)\b/i;

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
    .trim();

  if (!cleaned) return [];

  return [...new Set(
    cleaned
      .split(/\s*;\s*|\s+\band\b\s+|\s*&\s*|\s*\/\s*/i)
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
    sections.push(`Note: extracted text was truncated to the first ${MAX_TEXT_CHARS_PER_DOCUMENT} characters for model input.`);
  }

  const candidateEntries = Object.entries(document.candidateHints || {});
  if (candidateEntries.length) {
    sections.push("Field candidate hints from label/form-field parsing. Validate them against the text before using them:");
    candidateEntries.forEach(([fieldName, hint]) => {
      sections.push(`- ${FIELD_LABELS[fieldName]}: ${hint.value} (${hint.source})`);
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
    "If a filename or document text says Purchase and Sale Agreement or RF 401, prefer classifying it as a purchase contract unless the document clearly says amendment, counteroffer, or addendum.",
    "Only extract financing type when the contract explicitly names a financing program or says cash.",
    "If an inspection or possession term is expressed only as a relative period or condition and not an actual date, leave the date blank and mention that in warnings.",
    "If a field is not clearly present, return an empty string for scalar values, an empty array for name arrays, -1 for source_document_index, 0 for confidence, and an empty string for evidence.",
    "For date fields, normalize to YYYY-MM-DD when possible. If a time is present, return it in the time field; otherwise use an empty string.",
    "For money fields, return numeric strings without currency symbols or commas when possible.",
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
  const fields = {};

  Object.keys(FIELD_LABELS).forEach((fieldName) => {
    fields[fieldName] = buildEmptyField(fieldName);
  });

  return {
    schema_version: "ctc.contract.extraction.v2",
    provider: "client_pdf_text_form_extraction",
    model: null,
    extracted_at: new Date().toISOString(),
    documents: documents.map((document, index) => ({
      id: slugify(`${document.name}-${index}`),
      sourceIndex: document.sourceIndex,
      name: document.name,
      size: document.size || 0,
      likelyDocumentType: "Unknown",
      classificationConfidence: null,
      status: document.status || "needs ocr",
      note: document.note || "No usable embedded text or form fields were found.",
      extractionMethod: document.extractionMethod || "client pdf parse",
      extractedFieldCount: 0
    })),
    fields,
    source_details: buildSourceDetails(fields),
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
        candidateHints: document.candidateHints
      }))
    }
  };
}

function normalizePayload(modelParsed, documents) {
  const documentLookupInput = documents.map((document) => ({
    id: slugify(`${document.name}-${document.sourceIndex}`),
    sourceIndex: document.sourceIndex,
    name: document.name,
    size: document.size || 0,
    likelyDocumentType: "Unknown"
  }));

  const normalizedDocuments = documentLookupInput.map((document) => {
    const modelDocument = (modelParsed.documents || []).find((entry) => cleanText(entry.name) === document.name) || {};
    const inputDocument = documents.find((entry) => entry.name === document.name) || {};
    return {
      ...document,
      likelyDocumentType: cleanText(modelDocument.likely_document_type) || "Unknown",
      classificationConfidence: clampConfidence(modelDocument.classification_confidence) ?? null,
      status: inputDocument.status || "processed",
      note: cleanText(modelDocument.note) || inputDocument.note || "Server-side AI extraction completed.",
      extractionMethod: inputDocument.extractionMethod || "client pdf parse + server ai"
    };
  });

  const documentLookup = new Map(normalizedDocuments.map((document) => [document.name, document]));
  const fields = {};

  Object.keys(FIELD_LABELS).forEach((fieldName) => {
    const rawField = modelParsed?.fields?.[fieldName];
    fields[fieldName] = rawField
      ? normalizeModelField(fieldName, rawField, documentLookup)
      : buildEmptyField(fieldName);
  });

  const documentsWithCounts = normalizedDocuments.map((document) => ({
    ...document,
    extractedFieldCount: countDocumentMappedFields(document.name, fields)
  }));

  return {
    schema_version: "ctc.contract.extraction.v2",
    provider: "openai_server_text_extraction",
    model: MODEL,
    extracted_at: new Date().toISOString(),
    documents: documentsWithCounts,
    fields,
    source_details: buildSourceDetails(fields),
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
        candidateHints: document.candidateHints
      }))
    }
  };
}

function normalizeInputDocument(document, index) {
  const textInfo = truncateText(document?.text, MAX_TEXT_CHARS_PER_DOCUMENT);
  const formFields = normalizeFormFields(document?.formFields);
  const normalizedText = normalizePdfText(document?.text);
  const candidateHints = buildCandidateHints({
    text: normalizedText,
    formFields
  });

  return {
    id: slugify(`${document?.name || "document"}-${index}`),
    sourceIndex: Number.isInteger(document?.sourceIndex) ? document.sourceIndex : index,
    name: cleanText(document?.name),
    size: Number(document?.size) || 0,
    pageCount: Number(document?.pageCount) || null,
    text: normalizedText,
    modelText: textInfo.text,
    textTruncated: textInfo.truncated,
    formFields,
    candidateHints,
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

    const result = await callOpenAIContractExtraction(normalizedDocuments);
    return json(200, normalizePayload(result.parsed, normalizedDocuments));
  } catch (error) {
    return json(500, {
      error: error?.message || "Contract extraction failed."
    });
  }
}
