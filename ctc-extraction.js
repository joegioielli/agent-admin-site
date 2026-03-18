(function () {
  const PDFJS_SCRIPT_SOURCES = [
    "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js",
    "https://unpkg.com/pdfjs-dist@3.11.174/build/pdf.min.js"
  ];
  const PDFJS_WORKER_SOURCES = [
    "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js",
    "https://unpkg.com/pdfjs-dist@3.11.174/build/pdf.worker.min.js"
  ];
  const PDF_LIB_SCRIPT_SOURCES = [
    "https://cdn.jsdelivr.net/npm/pdf-lib/dist/pdf-lib.min.js",
    "https://unpkg.com/pdf-lib/dist/pdf-lib.min.js"
  ];
  const MAX_TEXT_CHARS_PER_DOCUMENT = 32000;

  const scriptLoadPromises = new Map();

  function normalizeServerError(rawText, status) {
    const text = String(rawText || "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    if (text) return text;
    if (status === 504) {
      return "The extraction request timed out before the server responded.";
    }
    if (status === 502) {
      return "The extraction function failed before it could return a normal response.";
    }
    return "";
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

  function reportProgress(onProgress, detail) {
    if (typeof onProgress === "function") {
      onProgress(detail);
    }
  }

  function loadScript(src) {
    if (scriptLoadPromises.has(src)) return scriptLoadPromises.get(src);

    const promise = new Promise((resolve, reject) => {
      const existing = document.querySelector(`script[src="${src}"]`);
      if (existing) {
        if (existing.dataset.loaded === "true") {
          resolve();
          return;
        }
        existing.addEventListener("load", () => resolve(), { once: true });
        existing.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), { once: true });
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

  async function ensurePdfLibReady() {
    await ensureGlobalLibrary("PDFLib", PDF_LIB_SCRIPT_SOURCES);
  }

  async function arrayBufferToBase64(buffer) {
    const bytes = new Uint8Array(buffer);
    const chunkSize = 0x8000;
    let binary = "";

    for (let index = 0; index < bytes.length; index += chunkSize) {
      const chunk = bytes.subarray(index, index + chunkSize);
      binary += String.fromCharCode(...chunk);
    }

    return btoa(binary);
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
        return;
      }

      rows.push({
        y,
        parts: [value]
      });
    });

    return rows
      .map((row) => cleanText(row.parts.join(" ")))
      .filter(Boolean)
      .join("\n");
  }

  async function extractPdfText(arrayBuffer) {
    await ensurePdfJsReady();
    const pdf = await window.pdfjsLib.getDocument({ data: arrayBuffer }).promise;
    const pageText = [];

    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
      const page = await pdf.getPage(pageNumber);
      const text = await extractTextRowsFromPage(page);
      if (text) pageText.push(`[[Page ${pageNumber}]]\n${text}`);
    }

    return {
      text: normalizePdfText(pageText.join("\n")),
      pageCount: pdf.numPages || null
    };
  }

  function normalizeFormValue(value) {
    if (value == null) return "";
    if (Array.isArray(value)) return value.map((item) => cleanText(item)).filter(Boolean).join(", ");
    return cleanText(String(value));
  }

  function normalizeFieldName(input) {
    return cleanText(input)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
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

  async function extractPdfFormFields(arrayBuffer) {
    await ensurePdfLibReady();

    try {
      const pdfDoc = await window.PDFLib.PDFDocument.load(arrayBuffer, {
        ignoreEncryption: true,
        updateMetadata: false
      });
      const form = pdfDoc.getForm();
      const fields = form.getFields();

      return fields
        .map((field) => {
          const name = cleanText(field.getName?.() || "");
          return {
            name,
            normalizedName: normalizeFieldName(name),
            value: normalizeFormValue(safeGetFieldValue(field))
          };
        })
        .filter((field) => field.name && field.value)
        .slice(0, 80);
    } catch {
      return [];
    }
  }

  function truncateText(text) {
    const normalized = normalizePdfText(text);
    if (!normalized) return { text: "", truncated: false };
    if (normalized.length <= MAX_TEXT_CHARS_PER_DOCUMENT) {
      return { text: normalized, truncated: false };
    }
    return {
      text: `${normalized.slice(0, MAX_TEXT_CHARS_PER_DOCUMENT).trim()}\n\n[truncated]`,
      truncated: true
    };
  }

  function buildDocumentStatus(text, formFields) {
    return text || formFields.length ? "processed" : "needs ocr";
  }

  function buildDocumentNote(text, formFields) {
    if (!text && !formFields.length) {
      return "No embedded text or fillable form fields were found. OCR would be needed for reliable extraction.";
    }
    if (!text && formFields.length) {
      return "No embedded text layer was found, but fillable form fields were detected and sent to the extractor.";
    }
    if (text && !formFields.length) {
      return "Embedded PDF text was extracted in the browser and sent to the server-side AI extractor.";
    }
    return "Embedded PDF text and form fields were extracted in the browser before server-side AI extraction.";
  }

  async function prepareDocument(file, index, onProgress) {
    reportProgress(onProgress, {
      stage: "read-file",
      fileName: file.name,
      message: `Reading ${file.name} in the browser...`
    });

    const arrayBuffer = await file.arrayBuffer();

    reportProgress(onProgress, {
      stage: "parse-text",
      fileName: file.name,
      message: `Extracting embedded text from ${file.name}...`
    });

    const [{ text, pageCount }, formFields] = await Promise.all([
      extractPdfText(arrayBuffer).catch(() => ({ text: "", pageCount: null })),
      extractPdfFormFields(arrayBuffer).catch(() => [])
    ]);

    const truncatedText = truncateText(text);

    return {
      name: file.name,
      size: file.size || 0,
      sourceIndex: index,
      pageCount,
      text,
      textTruncated: truncatedText.truncated,
      formFields,
      status: buildDocumentStatus(text, formFields),
      note: buildDocumentNote(text, formFields),
      extractionMethod: "client pdf parse"
    };
  }

  async function extractFromFiles(files, options = {}) {
    const pdfFiles = Array.from(files || []).filter((file) => /\.pdf$/i.test(file.name || ""));

    await Promise.all([
      ensurePdfJsReady(),
      ensurePdfLibReady()
    ]);

    const documents = [];

    for (let index = 0; index < pdfFiles.length; index += 1) {
      documents.push(await prepareDocument(pdfFiles[index], index, options.onProgress));
    }

    reportProgress(options.onProgress, {
      stage: "server-extract",
      message: "Sending extracted document text to the server extractor..."
    });

    const response = await fetch("/.netlify/functions/ctc-extract", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify({ documents })
    });

    const rawText = await response.text();
    let payload = {};

    try {
      payload = rawText ? JSON.parse(rawText) : {};
    } catch {
      const message = normalizeServerError(rawText, response.status);
      payload = message ? { error: message } : {};
    }

    if (!response.ok) {
      throw new Error(payload?.error || `Extraction failed (${response.status}).`);
    }

    return payload;
  }

  window.CTCExtraction = {
    extractFromFiles
  };
})();
