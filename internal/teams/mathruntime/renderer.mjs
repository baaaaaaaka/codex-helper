import MathJax from "mathjax/node-main.mjs";
import { Resvg } from "@resvg/resvg-js";

const MAX_SOURCE_BYTES = 8 * 1024;
const MAX_DIMENSION = 4096;
const MAX_PIXELS = 8 * 1024 * 1024;
const MAX_PNG_BYTES = 4 * 1024 * 1024;

function fail(index, code, message) {
  return { index, error: { code, message: String(message || code).slice(0, 500) } };
}

function hasSemanticError(svg) {
  const lower = svg.toLowerCase();
  return lower.includes("data-mjx-error") || lower.includes("<merror") ||
    /data-mml-node="mtext"[^>]*(fill="red"|stroke="red")/.test(lower);
}

const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);

let request;
try {
  request = JSON.parse(Buffer.concat(chunks).toString("utf8"));
} catch (error) {
  process.stdout.write(JSON.stringify({ error: { code: "invalid_request", message: String(error) } }));
  process.exit(2);
}

await MathJax.init({
  loader: { load: ["input/tex", "output/svg"] },
  output: { font: "mathjax-newcm" },
  svg: { fontCache: "none" },
  tex: { require: { allow: {}, defaultAllow: false } },
});

const adaptor = MathJax.startup.adaptor;
const results = [];
for (const item of Array.isArray(request.items) ? request.items : []) {
  const index = Number(item.index) || 0;
  const source = typeof item.source === "string" ? item.source : "";
  if (!source.trim() || Buffer.byteLength(source) > MAX_SOURCE_BYTES) {
    results.push(fail(index, "invalid_source", "empty or oversized TeX source"));
    continue;
  }
  try {
    const node = await MathJax.tex2svgPromise(source, {
      display: true,
      em: 16,
      ex: 8,
      containerWidth: 1200,
    });
    const svgNode = adaptor.tags(node, "svg")[0];
    const svg = adaptor.serializeXML(svgNode);
    if (hasSemanticError(svg)) {
      results.push(fail(index, "mathjax_error", "MathJax reported a semantic error"));
      continue;
    }

    const probe = new Resvg(svg, {
      background: "white",
      font: { loadSystemFonts: false },
    });
    const zoom = probe.width > 700 ? 1400 / probe.width : 2;
    const renderer = new Resvg(svg, {
      background: "white",
      fitTo: { mode: "zoom", value: Math.max(0.1, Math.min(2, zoom)) },
      font: { loadSystemFonts: false },
    });
    const rendered = renderer.render();
    const png = rendered.asPng();
    if (rendered.width <= 0 || rendered.height <= 0 || rendered.width > MAX_DIMENSION || rendered.height > MAX_DIMENSION || rendered.width * rendered.height > MAX_PIXELS) {
      results.push(fail(index, "invalid_dimensions", `${rendered.width}x${rendered.height}`));
      continue;
    }
    if (png.length > MAX_PNG_BYTES) {
      results.push(fail(index, "png_too_large", `${png.length} bytes`));
      continue;
    }
    results.push({
      index,
      png: Buffer.from(png).toString("base64"),
      width: rendered.width,
      height: rendered.height,
    });
  } catch (error) {
    results.push(fail(index, "render_failed", error?.message || error));
  }
}

MathJax.done();
process.stdout.write(JSON.stringify({ results }));
