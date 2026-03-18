(function () {
  function fileToBase64(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        const result = String(reader.result || "");
        const base64 = result.includes(",") ? result.split(",")[1] : result;
        resolve(base64);
      };
      reader.onerror = () => reject(new Error(`Could not read ${file.name}.`));
      reader.readAsDataURL(file);
    });
  }

  async function encodeDocuments(files, onProgress) {
    const documents = [];

    for (let index = 0; index < files.length; index += 1) {
      const file = files[index];
      if (typeof onProgress === "function") {
        onProgress({
          stage: "encode-file",
          fileName: file.name,
          message: `Preparing ${file.name} for server extraction...`
        });
      }

      documents.push({
        name: file.name,
        mimeType: file.type || "application/pdf",
        size: file.size || 0,
        data: await fileToBase64(file)
      });
    }

    return documents;
  }

  async function extractFromFiles(files, options = {}) {
    const pdfFiles = Array.from(files || []).filter((file) => /\.pdf$/i.test(file.name || ""));
    const documents = await encodeDocuments(pdfFiles, options.onProgress);

    if (typeof options.onProgress === "function") {
      options.onProgress({
        stage: "server-extract",
        message: "Sending PDFs to the server extractor..."
      });
    }

    const response = await fetch("/.netlify/functions/ctc-extract", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify({ documents })
    });

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload?.error || `Extraction failed (${response.status}).`);
    }

    return payload;
  }

  window.CTCExtraction = {
    extractFromFiles
  };
})();
