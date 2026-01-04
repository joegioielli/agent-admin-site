// netlify/functions/makestore.js
import fs from "fs";
import path from "path";

export function makestore() {
  const dataFile = path.join("/tmp", "redirects.json");

  return {
    async getJSON() {
      try {
        const txt = fs.readFileSync(dataFile, "utf8");
        return JSON.parse(txt);
      } catch {
        return [];
      }
    },
    async setJSON(data) {
      fs.writeFileSync(dataFile, JSON.stringify(data, null, 2), "utf8");
    },
  };
}
