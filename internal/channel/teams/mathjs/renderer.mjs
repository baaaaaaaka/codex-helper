import MathJax from "mathjax/node-main.mjs";
import { Resvg } from "@resvg/resvg-js";

const MAX_SOURCE_BYTES = 8 * 1024;
const MAX_DIMENSION = 4096;
const MAX_PIXELS = 8 * 1024 * 1024;
const MAX_PNG_BYTES = 4 * 1024 * 1024;
const TARGET_ZOOM = 6;
const MAX_RENDER_WIDTH = 3000;
const MAX_RENDER_HEIGHT = 2600;
const VERTICAL_PADDING_UNITS = 400;

function fail(index, code, message) {
  return { index, error: { code, message: String(message || code).slice(0, 500) } };
}

function hasSemanticError(svg) {
  const lower = svg.toLowerCase();
  return lower.includes("data-mjx-error") || lower.includes("<merror") ||
    /data-mml-node="mtext"[^>]*(fill="red"|stroke="red")/.test(lower);
}

function collectExternalText(adaptor, svgNode) {
  return adaptor.tags(svgNode, "text")
    .map((node) => adaptor.textContent(node))
    .join("");
}

function addVerticalPadding(adaptor, svgNode) {
  const viewBox = String(adaptor.getAttribute(svgNode, "viewBox") || "")
    .trim()
    .split(/[\s,]+/)
    .map(Number);
  if (viewBox.length !== 4 || !viewBox.every(Number.isFinite) || viewBox[2] <= 0 || viewBox[3] <= 0) {
    throw new Error("MathJax SVG has an invalid viewBox");
  }
  const [x, y, width, height] = viewBox;
  const paddedHeight = height + 2 * VERTICAL_PADDING_UNITS;
  adaptor.setAttribute(svgNode, "viewBox", `${x} ${y - VERTICAL_PADDING_UNITS} ${width} ${paddedHeight}`);

  const heightAttribute = String(adaptor.getAttribute(svgNode, "height") || "");
  const match = heightAttribute.match(/^([0-9]+(?:\.[0-9]+)?)([a-zA-Z%]+)$/);
  if (!match || Number(match[1]) <= 0) {
    throw new Error("MathJax SVG has an invalid height");
  }
  const scaledHeight = Number(match[1]) * paddedHeight / height;
  adaptor.setAttribute(svgNode, "height", `${scaledHeight.toFixed(4)}${match[2]}`);
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
const fonts = Array.isArray(request.fonts)
  ? request.fonts.filter((font) => font && typeof font.path === "string" && font.path)
  : [];
const fontOptions = {
  loadSystemFonts: false,
  ...(fonts.length ? {
    fontFiles: fonts.map((font) => font.path),
    defaultFontFamily: typeof fonts[0].family === "string" ? fonts[0].family : "",
  } : {}),
};
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
    const text = collectExternalText(adaptor, svgNode);
    addVerticalPadding(adaptor, svgNode);
    const svg = adaptor.serializeXML(svgNode);
    if (hasSemanticError(svg)) {
      results.push(fail(index, "mathjax_error", "MathJax reported a semantic error"));
      continue;
    }

    const probe = new Resvg(svg, {
      background: "white",
      font: fontOptions,
    });
    const zoom = Math.max(0.1, Math.min(
      TARGET_ZOOM,
      MAX_RENDER_WIDTH / probe.width,
      MAX_RENDER_HEIGHT / probe.height,
    ));
    const renderer = new Resvg(svg, {
      background: "white",
      fitTo: { mode: "zoom", value: zoom },
      font: fontOptions,
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
      text,
    });
  } catch (error) {
    results.push(fail(index, "render_failed", error?.message || error));
  }
}

MathJax.done();
process.stdout.write(JSON.stringify({ results }));
