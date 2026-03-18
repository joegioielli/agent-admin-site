import { PDFParse } from "pdf-parse";
import { PDFDocument } from "pdf-lib";

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

function normalizeFieldName(input) {
  return cleanText(input)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizeFormValue(value) {
  if (value == null) return "";
  if (Array.isArray(value)) return value.map((item) => cleanText(item)).filter(Boolean).join(", ");
  return cleanText(String(value));
}

function safeGetFieldValue(field) {
  try {
    switch (field?.constructor?.name) {
      case "PDFTextField":
        return field.getText?.() || "";
      case "PDFDropdown":
      case "PDFOptionList": {
        const selected = field.getSelected?.();
        return Array.isArray(selected) ? selected : selected || "";
      }
      case "PDFRadioGroup":
        return field.getSelected?.() || "";
      case "PDFCheckBox":
        return field.isChecked?.() ? "Checked" : "";
      default:
        return field.getText?.() || "";
    }
  } catch {
    return "";
  }
}

function truncateText(value, maxLength) {
  const text = normalizePdfText(value);
  if (!text) return { text: "", truncated: false };
  if (text.length <= maxLength) return { text, truncated: false };
  return {
    text: `${text.slice(0, maxLength).trim()}\n\n[truncated]`,
    truncated: true
  };
}

async function extractPdfText(buffer) {
  const parser = new PDFParse({ data: buffer });

  try {
    const result = await parser.getText();
    return {
      text: normalizePdfText(result?.text || ""),
      pageCount: Number(result?.total) || null
    };
  } finally {
    await parser.destroy().catch(() => {});
  }
}

async function extractPdfFormFields(buffer) {
  try {
    const pdfDoc = await PDFDocument.load(buffer, { ignoreEncryption: true, updateMetadata: false });
    const form = pdfDoc.getForm();
    const fields = form.getFields();

    return fields
      .map((field) => {
        const name = cleanText(field.getName?.() || "");
        const value = normalizeFormValue(safeGetFieldValue(field));
        return {
          name,
          normalizedName: normalizeFieldName(name),
          value
        };
      })
      .filter((field) => field.name && field.value)
      .slice(0, MAX_FORM_FIELDS_PER_DOCUMENT);
  } catch {
    return [];
  }
}

function buildDocumentNote(document) {
  if (!document.text && !document.formFields.length) {
    return "No embedded text or fillable form fields were found. OCR would be needed for reliable extraction.";
  }
  if (!document.text && document.formFields.length) {
    return "No embedded text layer was found, but fillable form fields were detected and sent to the extractor.";
  }
  if (document.text && !document.formFields.length) {
    return "Embedded PDF text was extracted locally and sent to the server-side AI extractor.";
  }
  return "Embedded PDF text and form fields were extracted locally before server-side AI extraction.";
}

function buildDocumentStatus(document) {
  return document.text || document.formFields.length ? "processed" : "needs ocr";
}

function buildDocumentContext(document) {
  const sections = [
    `Document ${document.sourceIndex}`,
    `Uploaded name: ${document.name}`,
    `Page count: ${document.pageCount || "unknown"}`,
    `Local extraction status: ${document.status}`
  ];

  if (document.formFields.length) {
    sections.push("Form fields:");
    document.formFields.forEach((field) => {
      sections.push(`- ${field.name}: ${field.value}`);
    });
  } else {
    sections.push("Form fields: none detected.");
  }

  if (document.text) {
    sections.push("Extracted text:");
    sections.push(document.modelText);
  } else {
    sections.push("Extracted text: none detected.");
  }

  if (document.textTruncated) {
    sections.push(`Note: extracted text was truncated to the first ${MAX_TEXT_CHARS_PER_DOCUMENT} characters for model input.`);
  }

  return sections.join("\n");
}

function buildPrompt(documents) {
  const docsList = documents.map((document) => `${document.sourceIndex}: ${document.name}`).join("\n");

  return [
    "Extract normalized contract data from the provided real estate contract packet.",
    "The packet may include a purchase contract plus counteroffers, amendments, or addenda in different formats and markets.",
    "Use only what is explicitly supported by the extracted text and form-field data below. Do not guess.",
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

function buildFallbackPayload(parsedDocuments, warnings) {
  const fields = {};

  Object.keys(FIELD_LABELS).forEach((fieldName) => {
    fields[fieldName] = buildEmptyField(fieldName);
  });

  const documents = parsedDocuments.map((document, index) => ({
    id: slugify(`${document.name}-${index}`),
    sourceIndex: index,
    name: document.name,
    size: document.size || 0,
    likelyDocumentType: "Unknown",
    classificationConfidence: null,
    status: document.status,
    note: document.note,
    extractionMethod: document.extractionMethod,
    extractedFieldCount: 0
  }));

  return {
    schema_version: "ctc.contract.extraction.v2",
    provider: "local_pdf_text_form_extraction",
    model: null,
    extracted_at: new Date().toISOString(),
    documents,
    fields,
    source_details: buildSourceDetails(fields),
    warnings,
    debug: {
      provider: "local_pdf_text_form_extraction",
      raw_document_names: parsedDocuments.map((document) => document.name),
      parsed_documents: parsedDocuments.map((document) => ({
        name: document.name,
        pageCount: document.pageCount,
        textLength: document.text.length,
        formFieldCount: document.formFields.length,
        status: document.status
      }))
    }
  };
}

function normalizePayload(modelParsed, inputDocuments, parsedDocuments) {
  const documents = inputDocuments.map((document, index) => {
    const modelDocument = (modelParsed.documents || []).find((entry) => cleanText(entry.name) === document.name) || {};
    const parsedDocument = parsedDocuments[index] || {};
    return {
      id: slugify(`${document.name}-${index}`),
      sourceIndex: index,
      name: document.name,
      size: document.size || 0,
      likelyDocumentType: cleanText(modelDocument.likely_document_type) || "Unknown",
      classificationConfidence: clampConfidence(modelDocument.classification_confidence) ?? null,
      status: parsedDocument.status || "processed",
      note: cleanText(modelDocument.note) || parsedDocument.note || "Server-side AI extraction completed.",
      extractionMethod: parsedDocument.extractionMethod || "server ai"
    };
  });

  const documentLookup = new Map(documents.map((document) => [document.name, document]));
  const fields = {};

  Object.keys(FIELD_LABELS).forEach((fieldName) => {
    const rawField = modelParsed?.fields?.[fieldName];
    fields[fieldName] = rawField
      ? normalizeModelField(fieldName, rawField, documentLookup)
      : buildEmptyField(fieldName);
  });

  const documentsWithCounts = documents.map((document) => ({
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
      raw_document_names: inputDocuments.map((document) => document.name),
      parsed_documents: parsedDocuments.map((document) => ({
        name: document.name,
        pageCount: document.pageCount,
        textLength: document.text.length,
        textTruncated: document.textTruncated,
        formFieldCount: document.formFields.length,
        status: document.status
      }))
    }
  };
}

async function parseDocument(document, index) {
  const buffer = Buffer.from(document.data, "base64");
  const [{ text, pageCount }, formFields] = await Promise.all([
    extractPdfText(buffer).catch(() => ({ text: "", pageCount: null })),
    extractPdfFormFields(buffer)
  ]);

  const truncatedText = truncateText(text, MAX_TEXT_CHARS_PER_DOCUMENT);
  const parsedDocument = {
    name: document.name,
    size: document.size || 0,
    sourceIndex: index,
    pageCount,
    text,
    modelText: truncatedText.text,
    textTruncated: truncatedText.truncated,
    formFields,
    extractionMethod: "local pdf text + form fields"
  };

  parsedDocument.status = buildDocumentStatus(parsedDocument);
  parsedDocument.note = buildDocumentNote(parsedDocument);

  return parsedDocument;
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

    const normalizedInput = documents
      .map((document) => ({
        name: cleanText(document?.name),
        data: cleanText(document?.data),
        size: Number(document?.size) || 0
      }))
      .filter((document) => document.name && document.data);

    if (!normalizedInput.length) {
      return json(400, { error: "Uploaded documents were empty or invalid." });
    }

    const parsedDocuments = await Promise.all(
      normalizedInput.map((document, index) => parseDocument(document, index))
    );

    const docsWithSignals = parsedDocuments.filter((document) => document.text || document.formFields.length);
    if (!docsWithSignals.length) {
      return json(200, buildFallbackPayload(parsedDocuments, [
        "No embedded text or fillable form fields were found in the uploaded PDFs. OCR is not configured yet for this extraction path."
      ]));
    }

    const result = await callOpenAIContractExtraction(parsedDocuments);
    const payload = normalizePayload(result.parsed, normalizedInput, parsedDocuments);
    return json(200, payload);
  } catch (error) {
    return json(500, {
      error: error?.message || "Contract extraction failed."
    });
  }
}
