// Prescriptions per 1,000 patients by deprivation decile.
// Data: practice_month.parquet queried in the browser with DuckDB-WASM.
//   ?data=https://<your-r2-domain>   points at the published files (default ./data).
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

const DATA_BASE = (new URLSearchParams(location.search).get("data") || "./data").replace(/\/$/, "");
const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const SVG = "http://www.w3.org/2000/svg";

const $ = (id) => document.getElementById(id);
const fmt = (n) => Math.round(n).toLocaleString("en-GB");
const monthLabel = (m) => `${MON[+m.slice(5, 7) - 1]} ${m.slice(2, 4)}`;

// ---- data ---------------------------------------------------------------

async function openDb() {
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  // Workers can't be created cross-origin directly; wrap the CDN script in a blob.
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING), new Worker(workerUrl));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  return db;
}

/** Sum items and list size per decile-month, then divide. Never average per-practice rates:
 *  that would weight a 1,000-patient practice the same as a 20,000-patient one.
 *  BIGINT sums are cast to DOUBLE because DuckDB-WASM returns BIGINT as JS BigInt. */
const QUERY = `
  SELECT strftime(year_month, '%Y-%m')      AS month,
         CAST(imd_decile AS INTEGER)        AS decile,
         CAST(sum(items) AS DOUBLE)         AS items,
         CAST(sum(list_size) AS DOUBLE)     AS patients,
         1000 * sum(items) / sum(list_size) AS rate
  FROM read_parquet('practice_month.parquet')
  WHERE imd_decile IS NOT NULL AND list_size > 0
  GROUP BY ALL
  ORDER BY month, decile`;

async function loadAll() {
  const manifest = await fetch(`${DATA_BASE}/manifest.json`, { cache: "no-cache" }).then((r) => {
    if (!r.ok) throw new Error(`manifest.json: HTTP ${r.status} from ${DATA_BASE}`);
    return r.json();
  });
  const db = await openDb();
  // Plain GET into memory rather than HTTP range reads: the file is a few MB, and it keeps the
  // R2 CORS rule to "allow GET from this origin" (no Range header, no preflight).
  const res = await fetch(`${DATA_BASE}/${manifest.files.practice_month}`);
  if (!res.ok) throw new Error(`${manifest.files.practice_month}: HTTP ${res.status} from ${DATA_BASE}`);
  await db.registerFileBuffer("practice_month.parquet", new Uint8Array(await res.arrayBuffer()));
  const conn = await db.connect();
  const rows = (await conn.query(QUERY)).toArray().map((r) => r.toJSON());
  await conn.close();
  return { manifest, rows };
}

/** rows -> { months: ['2025-01', ...], byDecile: Map(decile -> [rate|null per month]) } */
export function shape(rows) {
  const months = [...new Set(rows.map((r) => r.month))].sort();
  const idx = new Map(months.map((m, i) => [m, i]));
  const byDecile = new Map();
  for (let d = 1; d <= 10; d++) byDecile.set(d, new Array(months.length).fill(null));
  for (const r of rows) byDecile.get(r.decile)?.splice(idx.get(r.month), 1, r.rate);
  return { months, byDecile };
}

// ---- drawing ------------------------------------------------------------

function el(name, attrs = {}, parent) {
  const node = document.createElementNS(SVG, name);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  if (parent) parent.appendChild(node);
  return node;
}

function niceMax(v) {
  const step = 10 ** Math.floor(Math.log10(v)) / 2 || 1;   // 1,2,... x 10^n halves: 500, 100, ...
  return Math.ceil(v / step) * step;
}

function drawChart(container, data, onHover) {
  const { months, byDecile } = data;
  container.replaceChildren();
  const W = Math.max(container.clientWidth, 320);
  const narrow = W < 560;
  const M = { top: 12, right: narrow ? 12 : 150, bottom: 32, left: 52 };
  const H = 380;                                   // plot + x-axis band, so nothing is clipped
  const pw = W - M.left - M.right, ph = H - M.top - M.bottom;

  const all = [...byDecile.values()].flat().filter((v) => v != null);
  const yMax = niceMax(Math.max(...all) * 1.05);
  const x = (i) => M.left + (months.length === 1 ? pw / 2 : (i / (months.length - 1)) * pw);
  const y = (v) => M.top + ph - (v / yMax) * ph;

  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", tabindex: 0,
    "aria-label": `Line chart of monthly prescription items per 1,000 patients for deprivation deciles 1 to 10, ` +
                  `${monthLabel(months[0])} to ${monthLabel(months.at(-1))}. A table view is available.` });
  container.appendChild(svg);

  // Hairline grid and y ticks (full numbers: values stay under 10,000).
  const grid = el("g", { class: "grid" }, svg);
  for (let i = 0; i <= 4; i++) {
    const v = (yMax / 4) * i;
    el("line", { x1: M.left, x2: W - M.right, y1: y(v), y2: y(v) }, grid);
    el("text", { x: M.left - 8, y: y(v) + 4, "text-anchor": "end" }, svg).textContent = fmt(v);
  }
  el("line", { class: "axis-line", x1: M.left, x2: W - M.right, y1: y(0), y2: y(0) }, svg);

  // x ticks: about 8 labels, always including the first month.
  const every = Math.max(1, Math.ceil(months.length / 8));
  months.forEach((m, i) => {
    if (i % every) return;
    el("text", { x: x(i), y: H - 10, "text-anchor": "middle" }, svg).textContent = monthLabel(m);
  });

  // Context deciles 2-9 behind, then D1 and D10 on top.
  // A gap (null) lifts the pen, so a missing month breaks the line instead of bridging it.
  const path = (vals) => {
    let d = "", penDown = false;
    vals.forEach((v, i) => {
      if (v == null) { penDown = false; return; }
      d += `${penDown ? "L" : "M"}${x(i)},${y(v)}`;
      penDown = true;
    });
    return d;
  };
  for (let d = 2; d <= 9; d++) {
    el("path", { class: "context", d: path(byDecile.get(d)), fill: "none", "stroke-width": 1,
                 "stroke-linejoin": "round", "stroke-linecap": "round" }, svg);
  }
  const lastValue = (vals) => { for (let i = vals.length - 1; i >= 0; i--) if (vals[i] != null) return [i, vals[i]]; };
  const emphasised = [[1, "var(--s1)", "Decile 1"], [10, "var(--s2)", "Decile 10"]];
  const ends = [];
  for (const [d, colour, name] of emphasised) {
    const vals = byDecile.get(d);
    el("path", { d: path(vals), fill: "none", stroke: colour, "stroke-width": 2,
                 "stroke-linejoin": "round", "stroke-linecap": "round" }, svg);
    const last = lastValue(vals);
    if (last) {
      // 8px end-dot with a 2px surface ring so it stays legible over the context lines.
      el("circle", { cx: x(last[0]), cy: y(last[1]), r: 5, fill: colour, stroke: "var(--surface)", "stroke-width": 2 }, svg);
      ends.push({ name, y: y(last[1]), value: last[1] });
    }
  }
  // Direct end labels, only when they fit apart; otherwise the legend and tooltip carry identity.
  if (!narrow && ends.length === 2 && Math.abs(ends[0].y - ends[1].y) >= 18) {
    for (const e of ends) {
      const t = el("text", { class: "end-label", x: W - M.right + 12, y: e.y + 4 }, svg);
      t.textContent = `${e.name}: ${fmt(e.value)}`;
    }
  }

  // Crosshair + hover layer: snaps to the nearest month, keyboard arrows do the same.
  const cross = el("line", { class: "crosshair", y1: M.top, y2: M.top + ph, visibility: "hidden" }, svg);
  const dots = [1, 10].map(() => el("circle", { r: 5, "stroke-width": 2, stroke: "var(--surface)", visibility: "hidden" }, svg));
  dots[0].setAttribute("fill", "var(--s1)"); dots[1].setAttribute("fill", "var(--s2)");
  let current = null;
  const show = (i) => {
    current = i;
    cross.setAttribute("x1", x(i)); cross.setAttribute("x2", x(i)); cross.setAttribute("visibility", "visible");
    [1, 10].forEach((d, k) => {
      const v = byDecile.get(d)[i];
      if (v == null) return dots[k].setAttribute("visibility", "hidden");
      dots[k].setAttribute("cx", x(i)); dots[k].setAttribute("cy", y(v)); dots[k].setAttribute("visibility", "visible");
    });
    onHover(i, x(i), svg);
  };
  const hide = () => {
    current = null; cross.setAttribute("visibility", "hidden");
    dots.forEach((d) => d.setAttribute("visibility", "hidden")); onHover(null);
  };
  const nearest = (clientX) => {
    const box = svg.getBoundingClientRect();
    const px = ((clientX - box.left) / box.width) * W;
    return Math.min(months.length - 1, Math.max(0, Math.round(((px - M.left) / pw) * (months.length - 1))));
  };
  svg.addEventListener("pointermove", (e) => show(nearest(e.clientX)));
  svg.addEventListener("pointerleave", hide);
  svg.addEventListener("focus", () => show(current ?? months.length - 1));
  svg.addEventListener("blur", hide);
  svg.addEventListener("keydown", (e) => {
    if (e.key === "ArrowLeft") { e.preventDefault(); show(Math.max(0, (current ?? 0) - 1)); }
    if (e.key === "ArrowRight") { e.preventDefault(); show(Math.min(months.length - 1, (current ?? -1) + 1)); }
    if (e.key === "Escape") hide();
  });
}

function drawLegend(node) {
  node.replaceChildren();
  const item = (colour, text, opacity = 1) => {
    const s = document.createElement("span");
    const k = document.createElement("i");
    k.className = "key"; k.style.borderTopColor = colour; k.style.opacity = opacity;
    s.append(k, document.createTextNode(text)); node.appendChild(s);
  };
  item("var(--s1)", "Decile 1 (most deprived)");
  item("var(--s2)", "Decile 10 (least deprived)");
  item("var(--muted)", "Deciles 2 to 9", 0.6);
  node.hidden = false;
}

function drawTooltip(tip, data, i, px, svg) {
  if (i == null) { tip.hidden = true; return; }
  tip.replaceChildren();
  const head = document.createElement("div");
  head.className = "month"; head.textContent = monthLabel(data.months[i]); tip.appendChild(head);
  for (let d = 1; d <= 10; d++) {
    const v = data.byDecile.get(d)[i];
    const row = document.createElement("div");
    row.className = "row" + (d === 1 || d === 10 ? "" : " dim");
    const key = document.createElement("i");
    key.className = "key";
    key.style.borderTopColor = d === 1 ? "var(--s1)" : d === 10 ? "var(--s2)" : "var(--muted)";
    const name = document.createElement("span"); name.className = "n"; name.textContent = `Decile ${d}`;
    const val = document.createElement("span"); val.className = "v"; val.textContent = v == null ? "no data" : fmt(v);
    row.append(key, name, val); tip.appendChild(row);
  }
  tip.hidden = false;
  // Position inside the card, flipping to the left of the pointer near the right edge.
  const card = tip.parentElement.getBoundingClientRect();
  const sv = svg.getBoundingClientRect();
  const left = sv.left - card.left + (px / svg.viewBox.baseVal.width) * sv.width;
  const w = tip.offsetWidth;
  tip.style.left = `${left + 16 + w > card.width ? left - 16 - w : left + 16}px`;
  tip.style.top = `${sv.top - card.top + 8}px`;
}

function drawTable(node, data) {
  const table = document.createElement("table");
  const cap = document.createElement("caption");
  cap.className = "sr-only"; cap.style.cssText = "position:absolute;left:-9999px";
  cap.textContent = "Prescription items per 1,000 registered patients by month and deprivation decile";
  const head = document.createElement("tr");
  ["Month", ...Array.from({ length: 10 }, (_, k) => `D${k + 1}`)].forEach((t) => {
    const th = document.createElement("th"); th.scope = "col"; th.textContent = t; head.appendChild(th);
  });
  const thead = document.createElement("thead"); thead.appendChild(head);
  const tbody = document.createElement("tbody");
  data.months.forEach((m, i) => {
    const tr = document.createElement("tr");
    const th = document.createElement("th"); th.scope = "row"; th.textContent = monthLabel(m); tr.appendChild(th);
    for (let d = 1; d <= 10; d++) {
      const td = document.createElement("td"); const v = data.byDecile.get(d)[i];
      td.textContent = v == null ? "" : fmt(v); tr.appendChild(td);
    }
    tbody.appendChild(tr);
  });
  table.append(cap, thead, tbody);
  node.replaceChildren(table);
}

// ---- wiring -------------------------------------------------------------

async function main() {
  const card = $("card"), status = $("status");
  let full, range = 0, resizeTimer;

  const render = () => {
    const keep = range ? Math.min(range, full.months.length) : full.months.length;
    const from = full.months.length - keep;
    const view = { months: full.months.slice(from),
                   byDecile: new Map([...full.byDecile].map(([d, v]) => [d, v.slice(from)])) };
    drawLegend($("legend"));
    drawChart($("chart"), view, (i, px, svg) => drawTooltip($("tooltip"), view, i, px, svg));
    drawTable($("tablewrap"), view);
    document.querySelectorAll("[data-range]").forEach((b) =>
      b.setAttribute("aria-pressed", String(+b.dataset.range === range)));
  };

  try {
    const { manifest, rows } = await loadAll();
    if (!rows.length) throw new Error("No rows with a list size and a deprivation decile.");
    full = shape(rows);
    status.hidden = true;
    render();
    $("meta").textContent =
      `Data: ${full.months.length} months (${monthLabel(full.months[0])} to ${monthLabel(full.months.at(-1))}), ` +
      `updated ${manifest.generated_at.slice(0, 10)}.`;
  } catch (err) {
    status.className = "error";
    status.textContent = `Couldn't load the data: ${err.message}. If the files are on another domain, ` +
      `check that its CORS policy allows GET from this page.`;
    return;
  }

  document.querySelectorAll("[data-range]").forEach((b) => b.addEventListener("click", () => {
    range = +b.dataset.range;
    card.classList.add("loading");            // hold the frame, no layout jump
    requestAnimationFrame(() => { render(); card.classList.remove("loading"); });
  }));
  $("toggle-table").addEventListener("click", (e) => {
    const on = e.currentTarget.getAttribute("aria-pressed") !== "true";
    e.currentTarget.setAttribute("aria-pressed", String(on));
    $("tablewrap").hidden = !on;
  });
  let lastWidth = $("chart").clientWidth;
  new ResizeObserver(() => {
    if ($("chart").clientWidth === lastWidth) return;   // ignore height-only changes from our own render
    lastWidth = $("chart").clientWidth;
    clearTimeout(resizeTimer); resizeTimer = setTimeout(render, 120);
  }).observe($("chart"));
}

if (typeof document !== "undefined" && document.getElementById("card")) main();
