# Teams math runtime

This directory contains the pinned, on-demand runtime used to render trusted
`<m>TeX</m>` nodes in Codex-authored Microsoft Teams messages.

Markers that occupy their own line are display math. Structurally complex TeX
in ordinary prose is promoted to display math; simple inline markers and all
markers inside Markdown containers render as code without starting this runtime
or creating hosted content.

- MathJax 4.1.2 converts TeX to self-contained SVG. MathJax is Apache-2.0.
- `@resvg/resvg-js` 2.6.2 converts SVG to PNG. resvg-js is MPL-2.0.
- Noto Serif SC 2.003 supplies external Han glyphs. It is downloaded only when
  the rendered SVG needs them, is pinned by size and SHA-256, and is licensed
  under SIL Open Font License 1.1; the embedded license is copied beside the
  cached font.

The packages are not installed during the normal codex-helper build or
startup. The helper copies these embedded files into its private user cache and
runs `npm ci` only after a valid trusted math node is encountered. Package
integrities are pinned by `package-lock.json`. If Node.js, npm, network access,
installation, or rendering is unavailable, the Teams message retains the exact
TeX source as a code block and omits only the preview image. Unsupported scripts
also fail closed to source code instead of sending a tofu-glyph image. A verified
cached font remains usable offline after a helper restart.

Rendered PNGs use a bounded FIFO cache. It grows to at most 128 MiB, then is
trimmed to 96 MiB so directory scans and eviction are amortized rather than run
for every formula. Cache hits are read-only, identical TeX sources are rendered
once per batch, and runtime cleanup keeps only the current and immediately
previous managed runtime versions.
