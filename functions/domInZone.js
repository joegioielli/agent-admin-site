// netlify/functions/domInZone.js — ESM

function ymdInZone(date = new Date(), timeZone) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts = Object.fromEntries(fmt.formatToParts(date).map((p) => [p.type, p.value]));
  return { y: +parts.year, m: +parts.month, d: +parts.day };
}

function parseToYMD(s) {
  if (!s) return null;
  const str = String(s).trim();

  // YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const [y, m, d] = str.split("-").map(Number);
    return { y, m, d };
  }

  // MM/DD/YY(YY) or MM-DD-YY(YY)
  const t = str.split(/[\/-]/).map((x) => x.trim());
  if (t.length === 3) {
    const [mm, dd, yy] = t;
    let y = Number(yy);

    // ✅ pivot like the dashboard: 70–99 => 1970–1999, 00–69 => 2000–2069
    if (yy.length === 2) y = y >= 70 ? 1900 + y : 2000 + y;

    return { y, m: Number(mm), d: Number(dd) };
  }

  return null;
}

export function domInZone(activeDateStr, timeZone = "America/Chicago", now = new Date()) {
  const a = parseToYMD(activeDateStr);
  if (!a) return 0;

  // "today" in the listing's timezone
  const n = ymdInZone(now, timeZone);

  // compare by calendar day (not clock time) => DST safe
  const aUTC = Date.UTC(a.y, a.m - 1, a.d);
  const nUTC = Date.UTC(n.y, n.m - 1, n.d);

  const days = Math.floor((nUTC - aUTC) / 86400000);
  return days < 0 ? 0 : days; // Day 0 inclusive
}