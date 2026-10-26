// Renders every designs/**/*.diagram.json to <name>-light.png and <name>-dark.png next to it,
// using headless Chrome. No npm dependencies.
//
//   node tools/render-diagrams.mjs                 render all diagrams
//   node tools/render-diagrams.mjs path/a.diagram.json ...
//   node tools/render-diagrams.mjs --check         validate specs only, no Chrome needed
//
// Set CHROME to the browser binary if it is not in a standard place.
// The spec format is documented in tools/DIAGRAMS.md.

import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, relative, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const THEMES = {
  light: {
    bg: "#ffffff", ink: "#1f2328", muted: "#59636e", line: "#d1d9e0", fill: "#f6f8fa",
    edge: "#6e7781", pill: "#ffffff", note: "#fff8c5", noteLine: "#d4a72c", step: "#0969da",
    kinds: {
      client: "#59636e", service: "#0969da", worker: "#6639ba", gateway: "#0550ae",
      postgres: "#336791", nosql: "#1b7c83", redis: "#cf222e", queue: "#1f2328",
      search: "#9a6700", storage: "#bc4c00", external: "#6e7781", cdn: "#1a7f37",
    },
  },
  dark: {
    bg: "#0d1117", ink: "#e6edf3", muted: "#9198a1", line: "#3d444d", fill: "#151b23",
    edge: "#8b949e", pill: "#0d1117", note: "#2e2a17", noteLine: "#9e7c1d", step: "#4493f8",
    kinds: {
      client: "#9198a1", service: "#4493f8", worker: "#ab7df8", gateway: "#79c0ff",
      postgres: "#79a8d8", nosql: "#39c5cf", redis: "#ff7b72", queue: "#e6edf3",
      search: "#d29922", storage: "#f0883e", external: "#8b949e", cdn: "#3fb950",
    },
  },
};

const TAGS = {
  client: "CLIENT", service: "SERVICE", worker: "WORKER", gateway: "EDGE", postgres: "POSTGRESQL",
  nosql: "DATABASE", redis: "REDIS", queue: "KAFKA", search: "ELASTICSEARCH", storage: "OBJECT STORAGE",
  external: "EXTERNAL", cdn: "CDN",
};

const SANS = "'Segoe UI', system-ui, -apple-system, Helvetica, Arial, sans-serif";
const MONO = "'JetBrains Mono', 'Cascadia Mono', Consolas, ui-monospace, monospace";

const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

// Rough text width; good enough to size pills and canvases without a layout engine.
const textWidth = (s, size, mono = false) =>
  [...String(s).replace(/`/g, "")].length * size * (mono ? 0.6 : 0.55);

// Text with `code` spans rendered in the monospace font.
function rich(text, x, y, size, color, extra = "") {
  const parts = String(text).split("`");
  const spans = parts
    .map((p, i) => (p === "" ? "" : `<tspan font-family="${i % 2 ? MONO : SANS}">${esc(p)}</tspan>`))
    .join("");
  return `<text x="${x}" y="${y}" font-size="${size}" fill="${color}" ${extra}>${spans}</text>`;
}

function pill(cx, cy, label, t, step) {
  const size = 11.5;
  const w = textWidth(label, size) + (label ? 14 : 0) + (step ? 22 : 0);
  const h = 21;
  let out = "";
  if (label) {
    out += `<rect x="${cx - w / 2}" y="${cy - h / 2}" width="${w}" height="${h}" rx="10.5" fill="${t.pill}" stroke="${t.line}"/>`;
  }
  let tx = cx - w / 2 + 7;
  if (step) {
    const bx = label ? cx - w / 2 + 11 : cx;
    out += `<circle cx="${bx}" cy="${cy}" r="9" fill="${t.step}"/>`;
    out += `<text x="${bx}" y="${cy + 3.8}" text-anchor="middle" font-family="${SANS}" font-size="10.5" font-weight="700" fill="#ffffff">${esc(step)}</text>`;
    tx += 20;
  }
  if (label) out += rich(label, tx, cy + 4, size, t.ink);
  return out;
}

function header(spec, t) {
  let out = rich(spec.title ?? "", 32, 42, 19, t.ink, 'font-weight="700"');
  if (spec.subtitle) out += rich(spec.subtitle, 32, 64, 13, t.muted);
  return out;
}

function legend(x, y, t, items) {
  let out = "";
  let cx = x;
  for (const [dashed, label] of items) {
    out += `<line x1="${cx}" y1="${y}" x2="${cx + 28}" y2="${y}" stroke="${t.edge}" stroke-width="1.6" ${dashed ? 'stroke-dasharray="6 4"' : ""} marker-end="url(#head)"/>`;
    out += rich(label, cx + 36, y + 4, 12, t.muted);
    cx += 36 + textWidth(label, 12) + 28;
  }
  return out;
}

const defs = (t) =>
  `<defs><marker id="head" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="${t.edge}"/></marker></defs>`;

// ---------- architecture ----------

function layoutArchitecture(spec) {
  const g = { nodeW: 200, cellW: 290, rowH: 150, top: spec.subtitle ? 96 : 80, left: 32, ...(spec.grid ?? {}) };
  const nodes = new Map();
  for (const n of spec.nodes) {
    const lines = n.lines ?? [];
    const w = n.w ?? g.nodeW;
    const h = n.h ?? 52 + lines.length * 18 + (lines.length ? 6 : 0);
    const cx = g.left + n.col * g.cellW + g.nodeW / 2;
    const cy = g.top + n.row * g.rowH + g.rowH / 2 - 20;
    nodes.set(n.id, { ...n, lines, w, h, x: cx - w / 2, y: cy - h / 2, cx, cy });
  }
  return { g, nodes };
}

function route(e, a, b) {
  const off = e.offset ?? 0;
  const ox = (lo1, hi1, lo2, hi2) => [Math.max(lo1, lo2), Math.min(hi1, hi2)];
  let kind = e.route ?? "auto";
  if (kind === "auto") {
    const [xl, xh] = ox(a.x, a.x + a.w, b.x, b.x + b.w);
    const [yl, yh] = ox(a.y, a.y + a.h, b.y, b.y + b.h);
    if (yh - yl > 12) kind = "h";
    else if (xh - xl > 12) kind = "v";
    else kind = "hv";
  }
  if (kind === "h") {
    const [yl, yh] = ox(a.y, a.y + a.h, b.y, b.y + b.h);
    const y = (yl + yh) / 2 + off;
    const right = b.cx > a.cx;
    return [[right ? a.x + a.w : a.x, y], [right ? b.x : b.x + b.w, y]];
  }
  if (kind === "v") {
    const [xl, xh] = ox(a.x, a.x + a.w, b.x, b.x + b.w);
    const x = (xl + xh) / 2 + off;
    const down = b.cy > a.cy;
    return [[x, down ? a.y + a.h : a.y], [x, down ? b.y : b.y + b.h]];
  }
  if (kind === "hv") {
    const right = b.cx > a.cx;
    const down = b.cy > a.cy;
    const y = a.cy + off;
    const x = b.cx + (e.offset2 ?? 0);
    return [[right ? a.x + a.w : a.x, y], [x, y], [x, down ? b.y : b.y + b.h]];
  }
  if (kind === "vh") {
    const right = b.cx > a.cx;
    const down = b.cy > a.cy;
    const x = a.cx + off;
    const y = b.cy + (e.offset2 ?? 0);
    return [[x, down ? a.y + a.h : a.y], [x, y], [right ? b.x : b.x + b.w, y]];
  }
  if (kind === "hvh") {
    // Out of the side, across at a mid x, into the other side. `via` sets the mid x.
    const right = b.cx > a.cx;
    const sx = right ? a.x + a.w : a.x;
    const tx = right ? b.x : b.x + b.w;
    const mx = e.via ?? (sx + tx) / 2;
    return [[sx, a.cy + off], [mx, a.cy + off], [mx, b.cy + (e.offset2 ?? 0)], [tx, b.cy + (e.offset2 ?? 0)]];
  }
  if (kind === "vhv") {
    const down = b.cy > a.cy;
    const sy = down ? a.y + a.h : a.y;
    const ty = down ? b.y : b.y + b.h;
    const my = e.via ?? (sy + ty) / 2;
    return [[a.cx + off, sy], [a.cx + off, my], [b.cx + (e.offset2 ?? 0), my], [b.cx + (e.offset2 ?? 0), ty]];
  }
  throw new Error(`unknown route "${kind}"`);
}

function pointAt(pts, f) {
  const segs = pts.slice(1).map((p, i) => Math.hypot(p[0] - pts[i][0], p[1] - pts[i][1]));
  let d = segs.reduce((s, x) => s + x, 0) * f;
  for (let i = 0; i < segs.length; i++) {
    if (d <= segs[i] || i === segs.length - 1) {
      const r = segs[i] ? Math.min(d / segs[i], 1) : 0;
      return [pts[i][0] + (pts[i + 1][0] - pts[i][0]) * r, pts[i][1] + (pts[i + 1][1] - pts[i][1]) * r];
    }
    d -= segs[i];
  }
  return pts[0];
}

function renderArchitecture(spec, t) {
  const { g, nodes } = layoutArchitecture(spec);
  let maxX = 0;
  let maxY = 0;
  for (const n of nodes.values()) {
    maxX = Math.max(maxX, n.x + n.w);
    maxY = Math.max(maxY, n.y + n.h);
  }
  const body = [];

  for (const grp of spec.groups ?? []) {
    const ms = grp.nodes.map((id) => nodes.get(id));
    const pad = grp.pad ?? 18;
    const x = Math.min(...ms.map((n) => n.x)) - pad;
    const y = Math.min(...ms.map((n) => n.y)) - pad - 8;
    const x2 = Math.max(...ms.map((n) => n.x + n.w)) + pad;
    const y2 = Math.max(...ms.map((n) => n.y + n.h)) + pad;
    maxX = Math.max(maxX, x2);
    maxY = Math.max(maxY, y2);
    body.push(`<rect x="${x}" y="${y}" width="${x2 - x}" height="${y2 - y}" rx="14" fill="none" stroke="${t.line}" stroke-width="1.4" stroke-dasharray="5 5"/>`);
    const lw = textWidth(grp.label, 11.5) + 16;
    body.push(`<rect x="${x + 14}" y="${y - 10}" width="${lw}" height="20" fill="${t.bg}"/>`);
    body.push(rich(grp.label, x + 22, y + 4, 11.5, t.muted, 'font-weight="600" letter-spacing="0.4"'));
  }

  const labels = [];
  for (const e of spec.edges ?? []) {
    const a = nodes.get(e.from);
    const b = nodes.get(e.to);
    const pts = route(e, a, b);
    const d = pts.map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(1)} ${p[1].toFixed(1)}`).join(" ");
    const markers = `marker-end="url(#head)"${e.both ? ' marker-start="url(#head)"' : ""}`;
    body.push(`<path d="${d}" fill="none" stroke="${t.edge}" stroke-width="1.6" ${e.dashed ? 'stroke-dasharray="6 4"' : ""} ${markers}/>`);
    if (e.label || e.step) {
      const [lx, ly] =
        e.labelSeg === undefined
          ? pointAt(pts, e.labelPos ?? 0.5)
          : pointAt([pts[e.labelSeg], pts[e.labelSeg + 1]], e.labelPos ?? 0.5);
      labels.push(pill(lx + (e.labelDx ?? 0), ly + (e.labelDy ?? 0), e.label ?? "", t, e.step));
    }
  }

  for (const n of nodes.values()) {
    const accent = t.kinds[n.kind] ?? t.kinds.service;
    const dashed = n.kind === "external" ? 'stroke-dasharray="6 4"' : "";
    body.push(`<rect x="${n.x}" y="${n.y}" width="${n.w}" height="${n.h}" rx="10" fill="${t.fill}" stroke="${t.line}" stroke-width="1.3" ${dashed}/>`);
    body.push(`<path d="M${n.x + 10} ${n.y} H${n.x + n.w - 10}" stroke="${accent}" stroke-width="3" stroke-linecap="round"/>`);
    const tag = n.tag ?? TAGS[n.kind] ?? "";
    body.push(rich(tag, n.x + 14, n.y + 22, 10, accent, 'font-weight="700" letter-spacing="0.9"'));
    body.push(rich(n.label, n.x + 14, n.y + 42, 14.5, t.ink, 'font-weight="600"'));
    n.lines.forEach((l, i) => body.push(rich(l, n.x + 14, n.y + 64 + i * 18, 12, t.muted)));
  }

  body.push(...labels);

  const dashedUsed = (spec.edges ?? []).some((e) => e.dashed);
  let W = Math.max(maxX + 32, textWidth(spec.title ?? "", 19) + 64);
  let H = maxY + 32;
  if (dashedUsed) {
    H += 30;
    body.push(legend(32, H - 30, t, [[false, "request / call"], [true, "async event or message"]]));
  }
  W = Math.ceil(spec.width ?? W);
  H = Math.ceil(spec.height ?? H);
  return { W, H, svg: wrap(W, H, t, header(spec, t) + body.join("")) };
}

// ---------- sequence ----------

function renderSequence(spec, t) {
  const gap = spec.spacing ?? 200;
  const top = spec.subtitle ? 96 : 80;
  const left = 32;
  const boxW = Math.min(gap - 24, 176);
  const boxH = 52;
  const ps = new Map();
  spec.participants.forEach((p, i) => ps.set(p.id, { ...p, x: left + boxW / 2 + i * gap }));
  const lastX = left + boxW / 2 + (spec.participants.length - 1) * gap;

  const rows = [];
  let y = top + boxH + 34;
  let n = 0;
  for (const s of spec.steps) {
    if (s.section) {
      rows.push({ s, y });
      y += 40;
    } else if (s.note) {
      const lines = String(s.note).split("\n");
      const h = 14 + lines.length * 17;
      rows.push({ s, y: y - 14, h, lines });
      y += h + 22;
    } else {
      const self = s.from === s.to;
      if (spec.numbered !== false && !s.reply) n += 1;
      rows.push({ s, y, num: spec.numbered !== false && !s.reply ? n : null });
      y += self ? 58 : 46;
    }
  }
  const bottom = y + 6;

  const body = [];
  let noteRight = 0;
  for (const p of ps.values()) {
    const accent = t.kinds[p.kind] ?? t.kinds.service;
    body.push(`<line x1="${p.x}" y1="${top + boxH}" x2="${p.x}" y2="${bottom}" stroke="${t.line}" stroke-width="1.4" stroke-dasharray="4 5"/>`);
    const bx = p.x - boxW / 2;
    const dashed = p.kind === "external" ? 'stroke-dasharray="6 4"' : "";
    body.push(`<rect x="${bx}" y="${top}" width="${boxW}" height="${boxH}" rx="10" fill="${t.fill}" stroke="${t.line}" stroke-width="1.3" ${dashed}/>`);
    body.push(`<path d="M${bx + 10} ${top} H${bx + boxW - 10}" stroke="${accent}" stroke-width="3" stroke-linecap="round"/>`);
    body.push(rich(p.tag ?? TAGS[p.kind] ?? "", p.x, top + 20, 9.5, accent, 'text-anchor="middle" font-weight="700" letter-spacing="0.9"'));
    body.push(rich(p.label, p.x, top + 39, 13.5, t.ink, 'text-anchor="middle" font-weight="600"'));
  }

  for (const r of rows) {
    const s = r.s;
    if (s.section) {
      body.push(`<line x1="${left - 8}" y1="${r.y}" x2="${lastX + boxW / 2 + 8}" y2="${r.y}" stroke="${t.line}" stroke-width="1.2" stroke-dasharray="2 4"/>`);
      const w = textWidth(s.section, 11.5) + 18;
      body.push(`<rect x="${left - 8}" y="${r.y - 11}" width="${w}" height="22" rx="4" fill="${t.bg}" stroke="${t.line}"/>`);
      body.push(rich(s.section, left + 1, r.y + 4, 11.5, t.muted, 'font-weight="700"'));
    } else if (s.note) {
      const xs = (s.over ?? [s.at]).map((id) => ps.get(id).x);
      const mid = (Math.min(...xs) + Math.max(...xs)) / 2;
      const half = Math.max((Math.max(...xs) - Math.min(...xs)) / 2 + 70, ...r.lines.map((l) => textWidth(l, 12) / 2 + 18));
      const x1 = Math.max(mid - half, 16);
      const x2 = x1 + 2 * half;
      noteRight = Math.max(noteRight, x2);
      body.push(`<rect x="${x1}" y="${r.y}" width="${x2 - x1}" height="${r.h}" rx="6" fill="${t.note}" stroke="${t.noteLine}" stroke-width="1"/>`);
      r.lines.forEach((l, i) => body.push(rich(l, (x1 + x2) / 2, r.y + 21 + i * 17, 12, t.ink, 'text-anchor="middle"')));
    } else {
      const a = ps.get(s.from).x;
      const b = ps.get(s.to).x;
      const dash = s.dashed || s.reply ? 'stroke-dasharray="6 4"' : "";
      if (a === b) {
        body.push(`<path d="M${a} ${r.y} H${a + 40} V${r.y + 22} H${a + 4}" fill="none" stroke="${t.edge}" stroke-width="1.6" ${dash} marker-end="url(#head)"/>`);
        body.push(labelLeft(a + 48, r.y + 15, s.label, r.num, t));
      } else {
        const dir = b > a ? 1 : -1;
        body.push(`<line x1="${a}" y1="${r.y + 12}" x2="${b - dir * 2}" y2="${r.y + 12}" stroke="${t.edge}" stroke-width="1.6" ${dash} marker-end="url(#head)"/>`);
        body.push(labelLeft(Math.min(a, b) + 10, r.y, s.label, r.num, t));
      }
    }
  }

  const W = Math.ceil(spec.width ?? Math.max(lastX + boxW / 2 + 40, noteRight + 24, textWidth(spec.title ?? "", 19) + 64));
  const H = Math.ceil(spec.height ?? bottom + 24);
  return { W, H, svg: wrap(W, H, t, header(spec, t) + body.join("")) };
}

function labelLeft(x, y, label, num, t) {
  let out = "";
  let tx = x;
  if (num) {
    out += `<circle cx="${x + 8}" cy="${y - 4}" r="8.5" fill="${t.step}"/>`;
    out += `<text x="${x + 8}" y="${y - 0.3}" text-anchor="middle" font-family="${SANS}" font-size="10" font-weight="700" fill="#ffffff">${num}</text>`;
    tx += 22;
  }
  return out + rich(label ?? "", tx, y, 12.5, t.ink);
}

function wrap(W, H, t, inner) {
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" font-family="${SANS}"><rect width="${W}" height="${H}" fill="${t.bg}"/>${defs(t)}${inner}</svg>`;
}

// ---------- driver ----------

function validate(spec, file) {
  const errs = [];
  if (spec.type === "architecture") {
    const ids = new Set(spec.nodes.map((n) => n.id));
    for (const n of spec.nodes) {
      if (n.col === undefined || n.row === undefined) errs.push(`node ${n.id} needs col and row`);
      if (n.kind && !TAGS[n.kind]) errs.push(`node ${n.id} has unknown kind ${n.kind}`);
    }
    for (const e of spec.edges ?? []) {
      if (!ids.has(e.from) || !ids.has(e.to)) errs.push(`edge ${e.from} -> ${e.to} names a missing node`);
    }
    for (const grp of spec.groups ?? []) for (const id of grp.nodes) if (!ids.has(id)) errs.push(`group ${grp.label} names missing ${id}`);
  } else if (spec.type === "sequence") {
    const ids = new Set(spec.participants.map((p) => p.id));
    for (const s of spec.steps) {
      for (const id of [s.from, s.to, s.at, ...(s.over ?? [])].filter(Boolean)) {
        if (!ids.has(id)) errs.push(`step names missing participant ${id}`);
      }
    }
  } else {
    errs.push(`unknown type ${spec.type}`);
  }
  if (errs.length) throw new Error(`${file}:\n  ${errs.join("\n  ")}`);
}

function findSpecs(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) out.push(...findSpecs(p));
    else if (name.endsWith(".diagram.json")) out.push(p);
  }
  return out;
}

function chromePath() {
  const candidates = [
    process.env.CHROME,
    "C:/Program Files/Google/Chrome/Application/chrome.exe",
    "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
  ].filter(Boolean);
  const hit = candidates.find((c) => existsSync(c));
  if (!hit) throw new Error("No Chrome found. Set CHROME to the browser binary.");
  return hit;
}

const args = process.argv.slice(2);
const checkOnly = args.includes("--check");
const files = args.filter((a) => !a.startsWith("--")).map((f) => resolve(f));
const specs = files.length ? files : findSpecs(join(ROOT, "designs"));
const tmp = checkOnly ? null : mkdtempSync(join(tmpdir(), "sdp-"));
const chrome = checkOnly ? null : chromePath();

for (const file of specs) {
  const spec = JSON.parse(readFileSync(file, "utf8"));
  validate(spec, file);
  if (checkOnly) continue;
  const stem = basename(file, ".diagram.json");
  for (const [name, theme] of Object.entries(THEMES)) {
    const { W, H, svg } = spec.type === "architecture" ? renderArchitecture(spec, theme) : renderSequence(spec, theme);
    const page = join(tmp, `${stem}-${name}.html`);
    writeFileSync(page, `<!doctype html><html><head><meta charset="utf-8"><style>html,body{margin:0}svg{display:block}</style></head><body>${svg}</body></html>`);
    const out = join(dirname(file), `${stem}-${name}.png`);
    execFileSync(chrome, [
      "--headless=new", "--disable-gpu", "--hide-scrollbars", "--force-device-scale-factor=2",
      `--window-size=${W},${H}`, `--screenshot=${out}`, pathToFileURL(page).href,
    ], { stdio: "ignore" });
  }
  console.log(`rendered ${relative(ROOT, file)}`);
}
console.log(`${specs.length} diagram spec(s) ${checkOnly ? "valid" : "rendered"}`);
