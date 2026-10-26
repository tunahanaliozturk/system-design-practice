// Checks that every relative link and image in the Markdown files points at a file that exists,
// and that every diagram spec has both rendered PNGs next to it.
//
//   node tools/check-docs.mjs

import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function walk(dir) {
  return readdirSync(dir).flatMap((name) => {
    if (name.startsWith(".") || name === "node_modules") return [];
    const p = join(dir, name);
    return statSync(p).isDirectory() ? walk(p) : [p];
  });
}

const files = walk(ROOT);
const problems = [];

// The template links to diagrams that only exist once it is copied into a design.
for (const file of files.filter((f) => f.endsWith(".md") && !relative(ROOT, f).startsWith("template"))) {
  const text = readFileSync(file, "utf8").replace(/```[\s\S]*?```/g, "");
  const targets = [
    ...[...text.matchAll(/\]\(([^)\s]+)\)/g)].map((m) => m[1]),
    ...[...text.matchAll(/(?:src|srcset)="([^"]+)"/g)].map((m) => m[1]),
  ];
  for (const t of targets) {
    if (/^(https?:|mailto:|#)/.test(t)) continue;
    const path = decodeURIComponent(t.split("#")[0]);
    if (!existsSync(resolve(dirname(file), path))) problems.push(`${relative(ROOT, file)}: broken link ${t}`);
  }
}

for (const spec of files.filter((f) => f.endsWith(".diagram.json"))) {
  for (const theme of ["light", "dark"]) {
    const png = spec.replace(/\.diagram\.json$/, `-${theme}.png`);
    if (!existsSync(png)) problems.push(`${relative(ROOT, spec)}: missing ${theme} PNG, run tools/render-diagrams.mjs`);
  }
}

if (problems.length) {
  console.error(problems.join("\n"));
  process.exit(1);
}
console.log("docs ok");
