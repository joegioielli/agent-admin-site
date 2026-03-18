(function () {
  const PDFJS_SCRIPT_SOURCES = [
    "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js",
    "https://unpkg.com/pdfjs-dist@3.11.174/build/pdf.min.js"
  ];
  const PDFJS_WORKER_SOURCES = [
    "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js",
    "https://unpkg.com/pdfjs-dist@3.11.174/build/pdf.worker.min.js"
  ];
  const OCR_SCRIPT_SOURCES = [
    "https://cdn.jsdelivr.net/npm/tesseract.js@5/dist/tesseract.min.js",
    "https://unpkg.com/tesseract.js@5/dist/tesseract.min.js"
  ];

  const scriptLoadPromises = new Map();
  let ocrWorkerPromise = null;

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

  const DATE_REGEX = /\b(?:\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}|(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\.?\s+\d{1,2},?\s+\d{2,4})\b/i;
  const TIME_REGEX = /\b\d{1,2}(?::\d{2})?\s?(?:a\.?m\.?|p\.?m\.?)\b/i;
  const ADDRESS_REGEX = /\b\d{1,6}\s+[A-Za-z0-9.'#-]+(?:\s+[A-Za-z0-9.'#-]+){0,7}\s+(?:Street|St|Avenue|Ave|Road|Rd|Lane|Ln|Drive|Dr|Court|Ct|Circle|Cir|Way|Boulevard|Blvd|Place|Pl|Terrace|Ter|Trail|Trl|Parkway|Pkwy)\b(?:[^\n,]*)(?:,\s*[A-Za-z .'-]+,\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?)?/i;

  const MONTH_INDEX = {
    jan: 0,
    january: 0,
    feb: 1,
    february: 1,
    mar: 2,
    march: 2,
    apr: 3,
    april: 3,
    may: 4,
    jun: 5,
    june: 5,
    jul: 6,
    july: 6,
    aug: 7,
    august: 7,
    sep: 8,
    sept: 8,
    september: 8,
    oct: 9,
    october: 9,
    nov: 10,
    november: 10,
    dec: 11,
    december: 11
  };

  const NAME_STOPWORDS = /\b(closing|costs|prepaid|items|temporary|permanent|rate|buy|down|mortgage|loan|seller|buyer|pay|paid|credit|closing costs|addendum|financing|conventional|cash|fha|va|usda|inspection|possession|date)\b/i;
  const NAMEISH_TOKEN = /^[A-Z][a-zA-Z.'-]+$/;

  function slugify(input) {
    return String(input || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");
  }

  function cleanText(input) {
    return String(input || "")
      .replace(/\u0000/g, " ")
      .replace(/\u00a0/g, " ")
      .trim();
  }

  function stripExtension(fileName) {
    return String(fileName || "").replace(/\.[^.]+$/, "");
  }

  function normalizePdfText(rawText) {
    return cleanText(rawText)
      .replace(/\r/g, "\n")
      .replace(/[ \t]+\n/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .replace(/[ \t]{2,}/g, " ")
      .trim();
  }

  function splitLines(text) {
    return text
      .split(/\n+/)
      .map((line) => cleanText(line))
      .filter(Boolean);
  }

  function loadScript(src) {
    if (scriptLoadPromises.has(src)) {
      return scriptLoadPromises.get(src);
    }

    const promise = new Promise((resolve, reject) => {
      const existing = document.querySelector(`script[src="${src}"]`);
      if (existing) {
        existing.addEventListener("load", () => resolve(), { once: true });
        existing.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), { once: true });
        if (existing.dataset.loaded === "true") resolve();
        return;
      }

      const script = document.createElement("script");
      script.src = src;
      script.async = true;
      script.crossOrigin = "anonymous";
      script.addEventListener("load", () => {
        script.dataset.loaded = "true";
        resolve();
      }, { once: true });
      script.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), { once: true });
      document.head.appendChild(script);
    });

    scriptLoadPromises.set(src, promise);
    return promise;
  }

  async function ensureGlobalLibrary(globalName, sources) {
    if (window[globalName]) return;

    let lastError = null;
    for (const src of sources) {
      try {
        await loadScript(src);
        if (window[globalName]) return;
      } catch (error) {
        lastError = error;
      }
    }

    throw lastError || new Error(`${globalName} did not load.`);
  }

  async function ensurePdfJsReady() {
    await ensureGlobalLibrary("pdfjsLib", PDFJS_SCRIPT_SOURCES);

    if (!window.pdfjsLib?.GlobalWorkerOptions?.workerSrc) {
      window.pdfjsLib.GlobalWorkerOptions.workerSrc = PDFJS_WORKER_SOURCES[0];
    }
  }

  async function ensureOcrReady() {
    await ensureGlobalLibrary("Tesseract", OCR_SCRIPT_SOURCES);
  }

  async function getOcrWorker() {
    await ensureOcrReady();
    if (!ocrWorkerPromise) {
      ocrWorkerPromise = window.Tesseract.createWorker("eng");
    }
    return ocrWorkerPromise;
  }

  async function loadPdfDocument(file) {
    await ensurePdfJsReady();
    const data = await file.arrayBuffer();
    return window.pdfjsLib.getDocument({ data }).promise;
  }

  async function extractTextRowsFromPage(page) {
    const content = await page.getTextContent();
    const rows = [];

    content.items.forEach((item) => {
      const value = cleanText(item.str);
      if (!value) return;

      const y = Math.round(item.transform?.[5] || 0);
      const currentRow = rows[rows.length - 1];

      if (currentRow && Math.abs(currentRow.y - y) <= 2) {
        currentRow.parts.push(value);
        if (item.hasEOL) currentRow.forceBreak = true;
        return;
      }

      rows.push({
        y,
        parts: [value],
        forceBreak: Boolean(item.hasEOL)
      });
    });

    return rows
      .map((row) => cleanText(row.parts.join(" ")))
      .filter(Boolean)
      .join("\n");
  }

  async function readPdfText(pdf) {
    const pageText = [];

    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
      const page = await pdf.getPage(pageNumber);
      const text = await extractTextRowsFromPage(page);
      if (text) pageText.push(text);
    }

    return normalizePdfText(pageText.join("\n"));
  }

  async function renderPageToCanvas(page, scale) {
    const viewport = page.getViewport({ scale });
    const canvas = document.createElement("canvas");
    const context = canvas.getContext("2d", { alpha: false });
    canvas.width = Math.ceil(viewport.width);
    canvas.height = Math.ceil(viewport.height);

    await page.render({
      canvasContext: context,
      viewport
    }).promise;

    return canvas;
  }

  function reportProgress(onProgress, detail) {
    if (typeof onProgress === "function") {
      onProgress(detail);
    }
  }

  async function ocrPdfText(pdf, fileName, onProgress) {
    const worker = await getOcrWorker();
    const pageText = [];

    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
      reportProgress(onProgress, {
        stage: "ocr-page",
        fileName,
        pageNumber,
        pageCount: pdf.numPages,
        message: `Running OCR on page ${pageNumber} of ${pdf.numPages} for ${fileName}...`
      });

      const page = await pdf.getPage(pageNumber);
      const canvas = await renderPageToCanvas(page, 1.75);
      const result = await worker.recognize(canvas);
      const text = normalizePdfText(result?.data?.text || "");
      if (text) pageText.push(text);
    }

    return normalizePdfText(pageText.join("\n"));
  }

  function detectDocumentType(fileName, text) {
    const topText = splitLines(text).slice(0, 20).join("\n");
    const haystack = `${fileName || ""}\n${topText}`;
    const scoredMatches = DOC_TYPE_PATTERNS
      .map((entry) => {
        let score = 0;
        if (entry.pattern.test(fileName || "")) score += entry.score + 30;
        if (entry.pattern.test(topText)) score += entry.score;
        return score ? { ...entry, score } : null;
      })
      .filter(Boolean)
      .sort((left, right) => right.score - left.score);

    const match = scoredMatches[0];
    if (!match) {
      return { type: "unknown", label: "Unknown Document", confidence: 0.4 };
    }
    return {
      type: match.type,
      label: match.label,
      confidence: Math.min(0.97, 0.55 + match.score / 250)
    };
  }

  function escapeRegExp(value) {
    return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function buildField(value, confidence, extra) {
    if (value == null || value === "") return null;
    return {
      value,
      confidence,
      ...extra
    };
  }

  function hasFieldValue(field) {
    return field?.value != null && field?.value !== "";
  }

  function countMappedFields(fieldMap) {
    return Object.values(fieldMap || {}).filter((field) => hasFieldValue(field)).length;
  }

  function scoreFieldMap(fieldMap) {
    const weights = {
      property_address: 1,
      buyer_names: 2,
      seller_names: 2,
      purchase_price: 3,
      earnest_money_amount: 2,
      financing_type: 1,
      binding_date: 2,
      closing_date: 2,
      inspection_deadline: 2,
      possession_date: 2
    };

    return Object.entries(fieldMap || {}).reduce((total, [fieldName, field]) => {
      if (!hasFieldValue(field)) return total;
      return total + (weights[fieldName] || 1);
    }, 0);
  }

  function shouldRunOcrFallback(text, extractedFields, typeInfo) {
    if (text.length < 80) return true;

    const mappedCount = countMappedFields(extractedFields);
    const weightedScore = scoreFieldMap(extractedFields);
    const isPurchaseContract = typeInfo?.type === "purchase_contract";
    const missingCoreDealTerms = !hasFieldValue(extractedFields.purchase_price)
      && !hasFieldValue(extractedFields.earnest_money_amount)
      && !hasFieldValue(extractedFields.closing_date);

    if (mappedCount <= 1) return true;
    if (weightedScore <= 3) return true;
    if (isPurchaseContract && missingCoreDealTerms) return true;

    return false;
  }

  function pickPreferredField(primaryField, secondaryField) {
    if (!hasFieldValue(primaryField)) return secondaryField || null;
    if (!hasFieldValue(secondaryField)) return primaryField || null;

    const primaryConfidence = typeof primaryField.confidence === "number" ? primaryField.confidence : 0;
    const secondaryConfidence = typeof secondaryField.confidence === "number" ? secondaryField.confidence : 0;
    return secondaryConfidence > primaryConfidence ? secondaryField : primaryField;
  }

  function mergeExtractedFields(primaryFields, secondaryFields) {
    const merged = {};
    Object.keys(FIELD_LABELS).forEach((fieldName) => {
      merged[fieldName] = pickPreferredField(primaryFields?.[fieldName], secondaryFields?.[fieldName]);
    });
    return merged;
  }

  function parseCurrency(rawValue) {
    if (!rawValue) return null;
    const cleaned = String(rawValue).replace(/[^0-9.]/g, "");
    if (!cleaned) return null;
    const parsed = Number.parseFloat(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function normalizeDate(rawValue) {
    if (!rawValue) return null;
    const value = cleanText(rawValue).replace(/,/g, "");

    if (/^\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}$/.test(value)) {
      const [monthRaw, dayRaw, yearRaw] = value.split(/[\/-]/);
      const month = Number.parseInt(monthRaw, 10) - 1;
      const day = Number.parseInt(dayRaw, 10);
      const year = yearRaw.length === 2 ? Number.parseInt(`20${yearRaw}`, 10) : Number.parseInt(yearRaw, 10);
      const parsed = new Date(year, month, day);
      if (Number.isNaN(parsed.getTime())) return null;
      return parsed.toISOString().slice(0, 10);
    }

    const monthMatch = value.match(/^([A-Za-z]+)\.?\s+(\d{1,2})\s+(\d{2,4})$/);
    if (monthMatch) {
      const month = MONTH_INDEX[monthMatch[1].toLowerCase()];
      const day = Number.parseInt(monthMatch[2], 10);
      const yearRaw = monthMatch[3];
      const year = yearRaw.length === 2 ? Number.parseInt(`20${yearRaw}`, 10) : Number.parseInt(yearRaw, 10);
      const parsed = new Date(year, month, day);
      if (Number.isNaN(parsed.getTime())) return null;
      return parsed.toISOString().slice(0, 10);
    }

    return null;
  }

  function normalizeTime(rawValue) {
    if (!rawValue) return null;
    return cleanText(rawValue)
      .replace(/\./g, "")
      .replace(/\s+/g, " ")
      .toUpperCase();
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
    if (value == null || value === "") return "Not found";
    if (DATE_FIELDS.has(fieldName)) {
      return `${formatDate(value)}${time ? ` at ${time}` : ""}`;
    }
    if (CURRENCY_FIELDS.has(fieldName)) {
      return formatCurrency(value);
    }
    if (ARRAY_FIELDS.has(fieldName)) {
      return Array.isArray(value) && value.length ? value.join(", ") : "Not found";
    }
    return String(value);
  }

  function createEmptyField(fieldName) {
    return {
      key: fieldName,
      label: FIELD_LABELS[fieldName],
      value: null,
      time: null,
      displayValue: "Not found",
      confidence: null,
      sourceDocumentId: null,
      sourceDocumentName: null,
      sourceDocumentType: null
    };
  }

  function normalizeExtractedField(fieldName, rawField, documentRecord) {
    const value = rawField?.value ?? null;
    const time = rawField?.time ?? null;
    const confidence = typeof rawField?.confidence === "number" ? rawField.confidence : null;

    return {
      key: fieldName,
      label: FIELD_LABELS[fieldName],
      value,
      time,
      displayValue: formatFieldValue(fieldName, value, time),
      confidence,
      sourceDocumentId: documentRecord.id,
      sourceDocumentName: documentRecord.name,
      sourceDocumentType: documentRecord.label
    };
  }

  function cleanupCandidate(value) {
    return cleanText(value)
      .replace(/\s{2,}/g, " ")
      .replace(/[|]+/g, " ")
      .replace(/^[,;:\-]+/, "")
      .replace(/[,;:\-]+$/, "")
      .replace(/\b(?:page|buyer|seller|purchase price|sales price|earnest money|binding date|closing date|inspection deadline|possession date)\b[\s\S]*$/i, "")
      .trim();
  }

  function looksLikePersonName(value) {
    const candidate = cleanupCandidate(value);
    if (!candidate || candidate.length < 5) return false;
    if (NAME_STOPWORDS.test(candidate)) return false;
    if (/\d/.test(candidate)) return false;

    const tokens = candidate
      .split(/\s+/)
      .map((token) => token.replace(/[^A-Za-z.'-]/g, ""))
      .filter(Boolean);

    if (tokens.length < 2 || tokens.length > 5) return false;
    return tokens.every((token) => NAMEISH_TOKEN.test(token));
  }

  function extractNamedPartiesFromText(text, role) {
    const patterns = role === "buyer"
      ? [
          /\bBuyer(?:\(s\))?\b\s*[:\-]?\s*([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,4}(?:\s*(?:,|and|&)\s*[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,4})*)/g
        ]
      : [
          /\bSeller(?:\(s\))?\b\s*[:\-]?\s*([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,4}(?:\s*(?:,|and|&)\s*[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,4})*)/g
        ];

    for (const pattern of patterns) {
      const match = pattern.exec(text);
      if (!match) continue;
      const names = splitPartyNames(match[1]).filter(looksLikePersonName);
      if (names.length) return names;
    }

    return [];
  }

  function getLineValueAfterLabel(lines, labelPatterns) {
    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index];
      for (const pattern of labelPatterns) {
        const match = line.match(pattern);
        if (!match) continue;

        const sameLineValue = cleanupCandidate(match[1] || "");
        if (sameLineValue && sameLineValue.length > 2) return sameLineValue;

        const nextLine = cleanupCandidate(lines[index + 1] || "");
        if (nextLine) return nextLine;
      }
    }
    return null;
  }

  function splitPartyNames(value) {
    const cleaned = cleanupCandidate(value)
      .replace(/\bas (?:joint tenants|tenants in common|husband and wife)[\s\S]*$/i, "")
      .replace(/\bmarital status[\s\S]*$/i, "")
      .replace(/\bwhose address is[\s\S]*$/i, "")
      .trim();

    if (!cleaned) return [];

    const rawParts = cleaned
      .split(/\s*;\s*|\s+\band\b\s+|\s*&\s*|\s*\/\s*/i)
      .map((part) => cleanupCandidate(part))
      .filter(Boolean);

    const names = rawParts.filter((part) => looksLikePersonName(part));
    return [...new Set(names)];
  }

  function extractAddress(lines, text) {
    const directValue = getLineValueAfterLabel(lines, [
      /(?:property address|property located at|subject property|property to be conveyed)\s*[:\-]?\s*(.*)$/i,
      /(?:street address)\s*[:\-]?\s*(.*)$/i
    ]);

    if (directValue && ADDRESS_REGEX.test(directValue)) {
      return buildField(directValue, 0.89);
    }

    const inlineMatch = text.match(ADDRESS_REGEX);
    if (inlineMatch) {
      return buildField(cleanupCandidate(inlineMatch[0]), 0.76);
    }

    return null;
  }

  function extractPartyField(lines, role) {
    const labelPatterns = role === "buyer"
      ? [
          /^(?:buyer(?:\(s\))?|buyer\(s\)|purchaser(?:\(s\))?)\s*[:\-]?\s*(.*)$/i,
          /^(?:name of buyer|buyer name(?:\(s\))?)\s*[:\-]?\s*(.*)$/i
        ]
      : [
          /^(?:seller(?:\(s\))?|seller\(s\)|owner(?:\(s\))?)\s*[:\-]?\s*(.*)$/i,
          /^(?:name of seller|seller name(?:\(s\))?)\s*[:\-]?\s*(.*)$/i
        ];

    const candidate = getLineValueAfterLabel(lines, labelPatterns);
    const names = splitPartyNames(candidate);
    if (names.length) return buildField(names, 0.86);
    return null;
  }

  function extractMoneyField(text, labels, confidence) {
    for (const label of labels) {
      const regex = new RegExp(`${escapeRegExp(label)}[^\\n$]{0,80}(\\$?\\s*[\\d,]+(?:\\.\\d{2})?)`, "i");
      const match = text.match(regex);
      if (!match) continue;
      const amount = parseCurrency(match[1]);
      if (amount != null) return buildField(amount, confidence);
    }
    return null;
  }

  function extractFinancingType(text, lines) {
    const labeledValue = getLineValueAfterLabel(lines, [
      /^(?:financing type|type of financing|loan type)\s*[:\-]?\s*(.*)$/i,
      /^(?:loan)\s*[:\-]?\s*(.*)$/i
    ]);

    const financingKeywords = ["Conventional", "Cash", "FHA", "VA", "USDA", "Owner Financing", "Seller Financing", "Assumption"];

    if (labeledValue) {
      const match = financingKeywords.find((keyword) => new RegExp(`\\b${escapeRegExp(keyword)}\\b`, "i").test(labeledValue));
      if (match) return buildField(match, 0.86);
    }

    const regex = /\bfinancing\b[\s\S]{0,40}\b(conventional|cash|fha|va|usda|owner financing|seller financing|assumption)\b/i;
    const match = text.match(regex);
    if (match) {
      return buildField(match[1].replace(/\b\w/g, (char) => char.toUpperCase()), 0.62);
    }

    return null;
  }

  function extractDateField(text, labels, confidence) {
    for (const label of labels) {
      const regex = new RegExp(`${escapeRegExp(label)}[\\s\\S]{0,80}`, "i");
      const snippetMatch = text.match(regex);
      if (!snippetMatch) continue;

      const snippet = snippetMatch[0];
      const dateMatch = snippet.match(DATE_REGEX);
      if (!dateMatch) continue;

      const normalizedDate = normalizeDate(dateMatch[0]);
      if (!normalizedDate) continue;

      const timeMatch = snippet.match(TIME_REGEX);
      return buildField(normalizedDate, confidence, {
        time: normalizeTime(timeMatch?.[0] || null)
      });
    }
    return null;
  }

  function extractFieldsFromText(text, lines) {
    const buyerNamesFromText = extractNamedPartiesFromText(text, "buyer");
    const sellerNamesFromText = extractNamedPartiesFromText(text, "seller");
    const buyerField = extractPartyField(lines, "buyer");
    const sellerField = extractPartyField(lines, "seller");

    return {
      property_address: extractAddress(lines, text),
      buyer_names: buyerField || (buyerNamesFromText.length ? buildField(buyerNamesFromText, 0.72) : null),
      seller_names: sellerField || (sellerNamesFromText.length ? buildField(sellerNamesFromText, 0.72) : null),
      purchase_price: extractMoneyField(text, ["purchase price", "sales price", "contract price"], 0.91),
      earnest_money_amount: extractMoneyField(text, ["earnest money", "earnest money amount", "deposit"], 0.86),
      financing_type: extractFinancingType(text, lines),
      binding_date: extractDateField(text, ["binding date", "binding agreement date", "effective date", "acceptance date"], 0.82),
      closing_date: extractDateField(text, ["closing date", "date of closing", "closing shall occur", "close of escrow"], 0.88),
      inspection_deadline: extractDateField(text, ["inspection deadline", "inspection period", "option period", "due diligence"], 0.78),
      possession_date: extractDateField(text, ["possession date", "date of possession", "possession", "occupancy date"], 0.8)
    };
  }

  function resolveFieldMap(documents) {
    const resolved = {};
    Object.keys(FIELD_LABELS).forEach((fieldName) => {
      resolved[fieldName] = createEmptyField(fieldName);
    });

    const sortedDocuments = [...documents].sort((left, right) => {
      if (right.priority !== left.priority) return right.priority - left.priority;
      return right.uploadOrder - left.uploadOrder;
    });

    sortedDocuments.forEach((documentRecord) => {
      Object.entries(documentRecord.extractedFields || {}).forEach(([fieldName, rawField]) => {
        if (!FIELD_LABELS[fieldName]) return;
        if (rawField?.value == null || rawField?.value === "") return;
        if (resolved[fieldName].value != null && resolved[fieldName].value !== "") return;
        resolved[fieldName] = normalizeExtractedField(fieldName, rawField, documentRecord);
      });
    });

    return resolved;
  }

  function buildSourceDetails(fieldMap) {
    return Object.values(fieldMap).map((field) => ({
      field: field.key,
      label: field.label,
      value: field.displayValue,
      confidence: field.confidence,
      sourceDocumentName: field.sourceDocumentName || "Not found",
      sourceDocumentType: field.sourceDocumentType || "Not found"
    }));
  }

  function buildDocumentsSummary(documents) {
    return documents.map((documentRecord) => ({
      id: documentRecord.id,
      name: documentRecord.name,
      likelyDocumentType: documentRecord.label,
      classificationConfidence: documentRecord.detectionConfidence,
      status: documentRecord.status,
      note: documentRecord.note,
      extractionMethod: documentRecord.extractionMethod,
      size: documentRecord.size,
      lastModified: documentRecord.lastModified,
      textLength: documentRecord.textLength,
      extractedFieldCount: countMappedFields(documentRecord.extractedFields),
      textPreview: documentRecord.textPreview
    }));
  }

  function buildNormalizedPayload(documents) {
    const fieldMap = resolveFieldMap(documents);
    return {
      schema_version: "ctc.contract.extraction.v1",
      provider: "pdfjs-text-plus-ocr-fallback",
      extracted_at: new Date().toISOString(),
      documents: buildDocumentsSummary(documents),
      fields: fieldMap,
      source_details: buildSourceDetails(fieldMap)
    };
  }

  async function buildDocumentRecord(file, index, onProgress) {
    reportProgress(onProgress, {
      stage: "load-file",
      fileName: file.name,
      message: `Reading ${file.name}...`
    });

    const pdf = await loadPdfDocument(file);
    const textLayer = await readPdfText(pdf).catch(() => "");
    const textTypeInfo = detectDocumentType(file.name || "", textLayer);
    const textLines = splitLines(textLayer);
    const textFields = textLayer ? extractFieldsFromText(textLayer, textLines) : {};

    let finalText = textLayer;
    let finalFields = textFields;
    let extractionMethod = "pdf_text";

    if (shouldRunOcrFallback(textLayer, textFields, textTypeInfo)) {
      reportProgress(onProgress, {
        stage: "ocr-start",
        fileName: file.name,
        message: `Embedded text was too sparse in ${file.name}. Trying OCR...`
      });

      const ocrText = await ocrPdfText(pdf, file.name, onProgress).catch(() => "");
      const ocrLines = splitLines(ocrText);
      const ocrFields = ocrText ? extractFieldsFromText(ocrText, ocrLines) : {};

      const textScore = scoreFieldMap(textFields);
      const ocrScore = scoreFieldMap(ocrFields);
      const mergedFields = mergeExtractedFields(textFields, ocrFields);
      const mergedScore = scoreFieldMap(mergedFields);

      if (mergedScore > Math.max(textScore, ocrScore)) {
        finalText = [textLayer, ocrText].filter(Boolean).join("\n\n");
        finalFields = mergedFields;
        extractionMethod = "hybrid";
      } else if (ocrScore > textScore) {
        finalText = ocrText;
        finalFields = ocrFields;
        extractionMethod = "ocr";
      }
    }

    const typeInfo = detectDocumentType(file.name || "", finalText);
    const populatedFieldCount = countMappedFields(finalFields);
    const status = !finalText
      ? "no text found"
      : populatedFieldCount
        ? "processed"
        : "text found, no confident matches";
    const note = !finalText
      ? "No usable text could be extracted from this PDF, even after OCR."
      : extractionMethod === "hybrid" && populatedFieldCount
        ? "Embedded PDF text was too weak on its own, so OCR was blended in for the final result."
        : extractionMethod === "ocr" && populatedFieldCount
        ? "Values were extracted using OCR from a scanned or image-based PDF."
        : extractionMethod === "ocr"
          ? "OCR recovered text, but the current field heuristics did not match enough labels confidently."
          : populatedFieldCount
            ? "Values were extracted from the embedded PDF text layer."
            : "Text was read, but the field labels in this form did not match the current heuristics well enough.";

    return {
      id: slugify(`${stripExtension(file.name || "")}-${file.lastModified || Date.now()}`),
      name: file.name,
      type: typeInfo.type,
      label: typeInfo.label,
      size: file.size || 0,
      lastModified: file.lastModified || Date.now(),
      detectionConfidence: typeInfo.confidence,
      priority: DOCUMENT_PRIORITIES[typeInfo.type] || DOCUMENT_PRIORITIES.unknown,
      uploadOrder: index,
      extractedFields: finalFields,
      status,
      note,
      extractionMethod,
      textLength: finalText.length,
      textPreview: finalText.slice(0, 220)
    };
  }

  async function extractFromFiles(files, options = {}) {
    const pdfFiles = Array.from(files || []).filter((file) => /\.pdf$/i.test(file.name || ""));
    const documentRecords = [];

    for (let index = 0; index < pdfFiles.length; index += 1) {
      documentRecords.push(await buildDocumentRecord(pdfFiles[index], index, options.onProgress));
    }

    return buildNormalizedPayload(documentRecords);
  }

  window.CTCExtraction = {
    extractFromFiles
  };
})();
