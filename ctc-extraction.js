(function () {
  // Replace this layer later with real PDF parsing, OCR, or API extraction.
  const DOCUMENT_PRIORITIES = {
    purchase_contract: 100,
    counteroffer: 220,
    amendment: 240,
    addendum: 180,
    disclosure: 80,
    unknown: 10
  };

  const BASE_SAMPLE_DATA = {
    property_address: {
      value: "742 Evergreen Terrace, Springfield, IL 62704",
      confidence: 0.94
    },
    buyer_names: {
      value: ["Avery Carter", "Jordan Carter"],
      confidence: 0.96
    },
    seller_names: {
      value: ["Morgan Hill", "Taylor Hill"],
      confidence: 0.93
    },
    purchase_price: {
      value: 412500,
      confidence: 0.91
    },
    earnest_money_amount: {
      value: 6000,
      confidence: 0.88
    },
    financing_type: {
      value: "Conventional",
      confidence: 0.89
    },
    binding_date: {
      value: "2026-03-15",
      time: "6:00 PM",
      confidence: 0.93
    },
    closing_date: {
      value: "2026-04-28",
      time: null,
      confidence: 0.95
    },
    inspection_deadline: {
      value: "2026-03-22",
      time: "5:00 PM",
      confidence: 0.87
    },
    possession_date: {
      value: "2026-04-29",
      time: "9:00 AM",
      confidence: 0.84
    }
  };

  const DOC_TYPE_PATTERNS = [
    { type: "counteroffer", label: "Counteroffer", pattern: /(counter|counteroffer|counter-offer)/i },
    { type: "amendment", label: "Amendment", pattern: /(amend|amendment)/i },
    { type: "addendum", label: "Addendum", pattern: /(addendum|addenda)/i },
    { type: "disclosure", label: "Disclosure", pattern: /(disclosure|lead|paint|hoa|seller[- ]?disclosure)/i },
    { type: "purchase_contract", label: "Purchase Contract", pattern: /(purchase|sale|contract|psa|offer)/i }
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

  function slugify(input) {
    return String(input || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");
  }

  function stripExtension(fileName) {
    return String(fileName || "").replace(/\.[^.]+$/, "");
  }

  function titleCase(input) {
    return String(input || "")
      .split(/[\s_-]+/)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
      .join(" ");
  }

  function detectDocumentType(fileName) {
    const match = DOC_TYPE_PATTERNS.find((entry) => entry.pattern.test(fileName));
    if (!match) {
      return { type: "unknown", label: "Unknown Document", confidence: 0.42 };
    }
    return {
      type: match.type,
      label: match.label,
      confidence: match.type === "purchase_contract" ? 0.88 : 0.84
    };
  }

  function normalizeNameFromToken(token) {
    return token
      .split(/[_-]+/)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
      .join(" ");
  }

  function inferPartyNames(fileName, fallback) {
    const cleanName = stripExtension(fileName);
    const buyerMatch = cleanName.match(/buyer[s]?[-_]?([a-z]+(?:[_-][a-z]+)?)/i);
    if (buyerMatch) {
      return [normalizeNameFromToken(buyerMatch[1])];
    }
    return fallback;
  }

  function deriveDocumentOverrides(file, documentType) {
    const lowerName = String(file.name || "").toLowerCase();
    const baseName = stripExtension(file.name || "");
    const documentLabel = documentType.label;
    const docId = slugify(`${baseName}-${file.lastModified || Date.now()}`);
    const overrides = {};

    if (documentType.type === "purchase_contract") {
      overrides.property_address = {
        value: BASE_SAMPLE_DATA.property_address.value,
        confidence: 0.94
      };
      overrides.buyer_names = {
        value: inferPartyNames(file.name, BASE_SAMPLE_DATA.buyer_names.value),
        confidence: 0.92
      };
      overrides.seller_names = {
        value: BASE_SAMPLE_DATA.seller_names.value,
        confidence: 0.9
      };
      overrides.purchase_price = {
        value: BASE_SAMPLE_DATA.purchase_price.value,
        confidence: 0.91
      };
      overrides.earnest_money_amount = {
        value: BASE_SAMPLE_DATA.earnest_money_amount.value,
        confidence: 0.88
      };
      overrides.financing_type = {
        value: /cash/i.test(lowerName) ? "Cash" : BASE_SAMPLE_DATA.financing_type.value,
        confidence: /cash/i.test(lowerName) ? 0.9 : 0.89
      };
      overrides.binding_date = {
        value: BASE_SAMPLE_DATA.binding_date.value,
        time: BASE_SAMPLE_DATA.binding_date.time,
        confidence: 0.93
      };
      overrides.closing_date = {
        value: BASE_SAMPLE_DATA.closing_date.value,
        time: BASE_SAMPLE_DATA.closing_date.time,
        confidence: 0.95
      };
      overrides.inspection_deadline = {
        value: BASE_SAMPLE_DATA.inspection_deadline.value,
        time: BASE_SAMPLE_DATA.inspection_deadline.time,
        confidence: 0.87
      };
      overrides.possession_date = {
        value: BASE_SAMPLE_DATA.possession_date.value,
        time: BASE_SAMPLE_DATA.possession_date.time,
        confidence: 0.84
      };
    }

    if (documentType.type === "counteroffer") {
      overrides.purchase_price = {
        value: 418000,
        confidence: 0.86
      };
      overrides.earnest_money_amount = {
        value: 7500,
        confidence: 0.8
      };
      overrides.binding_date = {
        value: "2026-03-16",
        time: "8:30 PM",
        confidence: 0.84
      };
      overrides.closing_date = {
        value: "2026-05-02",
        time: "2:00 PM",
        confidence: 0.88
      };
    }

    if (documentType.type === "amendment") {
      overrides.inspection_deadline = {
        value: "2026-03-25",
        time: "11:59 PM",
        confidence: 0.78
      };
      overrides.possession_date = {
        value: "2026-05-03",
        time: null,
        confidence: 0.8
      };
    }

    if (documentType.type === "addendum" && /financ/i.test(lowerName)) {
      overrides.financing_type = {
        value: "FHA",
        confidence: 0.73
      };
    }

    if (documentType.type === "unknown") {
      overrides.notes = {
        value: `No field mapping exists yet for ${titleCase(baseName)}.`,
        confidence: 0.4
      };
    }

    return {
      id: docId,
      name: file.name,
      type: documentType.type,
      label: documentLabel,
      size: file.size || 0,
      lastModified: file.lastModified || Date.now(),
      detectionConfidence: documentType.confidence,
      priority: DOCUMENT_PRIORITIES[documentType.type] || DOCUMENT_PRIORITIES.unknown,
      extractedFields: overrides
    };
  }

  function buildDocumentRecord(file, index) {
    const typeInfo = detectDocumentType(file.name || "");
    const baseRecord = deriveDocumentOverrides(file, typeInfo);
    return {
      ...baseRecord,
      uploadOrder: index,
      status: "processed"
    };
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

  function resolveFieldMap(documents) {
    // The merge order is intentionally isolated so later precedence rules can evolve here.
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
      size: documentRecord.size,
      lastModified: documentRecord.lastModified,
      extractedFieldCount: Object.keys(documentRecord.extractedFields || {}).length
    }));
  }

  function buildNormalizedPayload(documents) {
    const fieldMap = resolveFieldMap(documents);
    return {
      schema_version: "ctc.contract.extraction.v1",
      extracted_at: new Date().toISOString(),
      documents: buildDocumentsSummary(documents),
      fields: fieldMap,
      source_details: buildSourceDetails(fieldMap)
    };
  }

  function extractFromFiles(files) {
    const documentRecords = Array.from(files || [])
      .filter((file) => /\.pdf$/i.test(file.name || ""))
      .map((file, index) => buildDocumentRecord(file, index));

    return buildNormalizedPayload(documentRecords);
  }

  window.CTCExtraction = {
    extractFromFiles
  };
})();
