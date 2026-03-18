(function () {
  // UI controller stays separate from extraction so future provider swaps do not affect rendering.
  const fileInput = document.getElementById("pdfFiles");
  const uploadForm = document.getElementById("ctcUploadForm");
  const processButton = document.getElementById("processBtn");
  const resetButton = document.getElementById("resetBtn");
  const uploadSummary = document.getElementById("uploadSummary");
  const uploadList = document.getElementById("uploadedDocsList");
  const emptyDocumentsState = document.getElementById("emptyDocumentsState");
  const debugOutput = document.getElementById("rawJsonOutput");
  const resultsShell = document.getElementById("resultsShell");
  const lastRunLabel = document.getElementById("lastRunLabel");
  const sourceDetailsBody = document.getElementById("sourceDetailsBody");

  const dateFieldIds = {
    binding_date: "bindingDateValue",
    closing_date: "closingDateValue",
    inspection_deadline: "inspectionDeadlineValue",
    possession_date: "possessionDateValue"
  };

  const detailFieldIds = {
    property_address: "propertyAddressValue",
    buyer_names: "buyerNamesValue",
    seller_names: "sellerNamesValue",
    purchase_price: "purchasePriceValue",
    earnest_money_amount: "earnestMoneyValue",
    financing_type: "financingTypeValue"
  };

  function formatBytes(bytes) {
    if (!bytes) return "0 KB";
    if (bytes < 1024) return `${bytes} B`;
    const units = ["KB", "MB", "GB"];
    let value = bytes;
    let unitIndex = -1;
    do {
      value /= 1024;
      unitIndex += 1;
    } while (value >= 1024 && unitIndex < units.length - 1);
    return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unitIndex]}`;
  }

  function formatPercent(value) {
    if (typeof value !== "number") return "N/A";
    return `${Math.round(value * 100)}%`;
  }

  function formatDateTime(value) {
    if (!value) return "Not run yet";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return "Not run yet";
    return new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "numeric",
      minute: "2-digit"
    }).format(parsed);
  }

  function setFieldValue(elementId, field) {
    const node = document.getElementById(elementId);
    if (!node) return;
    node.textContent = field?.displayValue || "Not found";
  }

  function renderUploadedDocuments(documents) {
    uploadList.innerHTML = "";

    if (!documents.length) {
      emptyDocumentsState.hidden = false;
      return;
    }

    emptyDocumentsState.hidden = true;
    documents.forEach((documentRecord) => {
      const metaItems = [
        formatBytes(documentRecord.size),
        `${documentRecord.extractedFieldCount} mapped fields`
      ];

      if (documentRecord.extractionMethod) metaItems.push(documentRecord.extractionMethod);
      if (documentRecord.status) metaItems.push(documentRecord.status);

      const item = document.createElement("article");
      item.className = "document-item";
      item.innerHTML = `
        <div class="document-item__header">
          <div>
            <h4>${documentRecord.name}</h4>
            <p>${documentRecord.likelyDocumentType}</p>
          </div>
          <span class="pill">${formatPercent(documentRecord.classificationConfidence)} match</span>
        </div>
        <div class="document-item__meta">
          ${metaItems.map((itemValue) => `<span>${itemValue}</span>`).join("")}
        </div>
        <p class="document-item__note">${documentRecord.note || ""}</p>
      `;
      uploadList.appendChild(item);
    });
  }

  function renderImportantDates(fields) {
    Object.entries(dateFieldIds).forEach(([fieldKey, elementId]) => {
      setFieldValue(elementId, fields[fieldKey]);
    });
  }

  function renderDetailFields(fields) {
    Object.entries(detailFieldIds).forEach(([fieldKey, elementId]) => {
      setFieldValue(elementId, fields[fieldKey]);
    });
  }

  function renderSourceDetails(sourceDetails) {
    sourceDetailsBody.innerHTML = "";

    sourceDetails.forEach((item) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${item.label}</td>
        <td>${item.value}</td>
        <td>${item.sourceDocumentName}</td>
        <td>${item.sourceDocumentType}</td>
        <td>${formatPercent(item.confidence)}</td>
      `;
      sourceDetailsBody.appendChild(row);
    });
  }

  function renderPayload(payload) {
    resultsShell.dataset.ready = "true";
    const summary = `${payload.documents.length} PDF${payload.documents.length === 1 ? "" : "s"} processed`;
    uploadSummary.textContent = payload.warnings?.length
      ? `${summary} - fallback or extraction notes available in Raw JSON Debug`
      : summary;
    lastRunLabel.textContent = formatDateTime(payload.extracted_at);
    renderUploadedDocuments(payload.documents);
    renderImportantDates(payload.fields);
    renderDetailFields(payload.fields);
    renderSourceDetails(payload.source_details);
    debugOutput.textContent = JSON.stringify(payload, null, 2);
  }

  function resetResults() {
    resultsShell.dataset.ready = "false";
    uploadSummary.textContent = "No PDFs loaded yet";
    lastRunLabel.textContent = "Not run yet";
    uploadList.innerHTML = "";
    emptyDocumentsState.hidden = false;
    Object.values(dateFieldIds).forEach((elementId) => {
      document.getElementById(elementId).textContent = "Not found";
    });
    Object.values(detailFieldIds).forEach((elementId) => {
      document.getElementById(elementId).textContent = "Not found";
    });
    sourceDetailsBody.innerHTML = `
      <tr>
        <td colspan="5" class="table-empty">Run extraction to populate source and confidence details.</td>
      </tr>
    `;
    debugOutput.textContent = "{\n  \"status\": \"waiting_for_upload\"\n}";
  }

  function validateSelectedFiles() {
    const files = Array.from(fileInput.files || []);
    if (!files.length) return { valid: false, message: "Select one or more purchase contract PDFs to continue." };
    const nonPdf = files.find((file) => !/\.pdf$/i.test(file.name || ""));
    if (nonPdf) return { valid: false, message: `${nonPdf.name} is not a PDF.` };
    return { valid: true, files };
  }

  function setProcessingState(isProcessing) {
    processButton.disabled = isProcessing;
    processButton.textContent = isProcessing ? "Processing..." : "Run Extraction";
  }

  uploadForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const validation = validateSelectedFiles();
    if (!validation.valid) {
      uploadSummary.textContent = validation.message;
      return;
    }

    setProcessingState(true);
    try {
      uploadSummary.textContent = "Starting extraction...";
      const payload = await window.CTCExtraction.extractFromFiles(validation.files, {
        onProgress(detail) {
          uploadSummary.textContent = detail?.message || "Processing...";
        }
      });
      renderPayload(payload);
    } catch (error) {
      uploadSummary.textContent = error?.message || "Extraction failed.";
    } finally {
      setProcessingState(false);
    }
  });

  resetButton.addEventListener("click", () => {
    uploadForm.reset();
    resetResults();
  });

  fileInput.addEventListener("change", () => {
    const files = Array.from(fileInput.files || []);
    if (!files.length) {
      uploadSummary.textContent = "No PDFs loaded yet";
      return;
    }
    uploadSummary.textContent = `${files.length} file${files.length === 1 ? "" : "s"} selected`;
  });

  resetResults();
})();
