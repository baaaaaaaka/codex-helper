#!/usr/bin/env python3
"""Golden diff: new core vs v0.1.20 (and rc.59) over the owner's real data.

Inputs (all JSONL, produced by the harnesses described in README-fidelity):
  --old-transcript  v0.1.20 transcript-sync replay (one row per posted
                    message part, or a skipped record)
  --new-transcript  new-core Tracker + renderMirror replay
                    (internal/core/engine/fidelity_golden_test.go)
  --old-outbox      v0.1.20 render of every message the old helper sent
  --rc59-outbox     rc.59 render of the same rows
  --new-outbox      new-core render of the same bodies
  --sent            the sent rows themselves (kind, body)
  --now             the conversion time (120-day window; 2020-01-01T00:00:00Z
                    for none, as the core has no window any more)

Writes a JSON summary (--out) and examples (--examples).

Comparison layers for a matched message:
  text   the visible text with labels, part markers, the completion footer
         and mentions removed, whitespace collapsed;
  shape  the sequence of structural tags (p, pre, code, ul/ol/li, table,
         strong, em, a, h1-6, blockquote, br, img) after the label header;
  label  the label of the first part;
  parts  the number of parts.
"""
import argparse
import collections
import html as htmlmod
import json
import re
import sys
from datetime import datetime, timedelta, timezone

TAG = re.compile(r"<(/?)([a-zA-Z0-9]+)[^>]*>")
SHAPE_TAGS = {"p", "pre", "code", "ul", "ol", "li", "table", "tr", "td", "th", "strong", "b", "em", "i", "a",
              "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "br", "img", "hr", "del", "s"}
OLD_LABELS = ["🧑‍💻 User", "🤖 ✅ Codex answer", "🤖 ⏳ Codex status", "🔧 Helper", "💻 Code", "🤖 🛠️ Codex command"]
FOOTER = re.compile(r"<p><strong>🔧 Helper:</strong> ✅ Codex finished responding\.(?: <at[^>]*>[^<]*</at>)?</p>$")
HEADER = re.compile(r"^<p><strong>([^<]*?)(?: \[part \d+/\d+\])?:</strong>")
MARKER = re.compile(r"<!-- codex-helper-outbox:[^>]*-->")


def new_html(part):
    h = part["html"]
    try:
        h = json.loads(h)["body"]["content"]
    except Exception:
        pass
    return h


def split_label(h):
    """-> (label, html without header label, footer present)."""
    h = MARKER.sub("", h).strip()
    footer = bool(FOOTER.search(h))
    h = FOOTER.sub("", h)
    m = HEADER.match(h)
    label = ""
    if m:
        label = m.group(1)
        h = h[m.end():]
        h = re.sub(r"^\s*(<at[^>]*>[^<]*</at>)?\s*(<br\s*/?>)?\s*(</p>)?", "", h)
    return label, h, footer


def visible(h):
    h = re.sub(r"<at[^>]*>[^<]*</at>", "", h)
    t = TAG.sub(" ", h)
    t = htmlmod.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def shape(h):
    out = []
    for close, name in TAG.findall(h):
        n = name.lower()
        if n in SHAPE_TAGS:
            out.append(("/" if close else "") + n)
    # an empty leading paragraph left by the label header is not structure
    s = " ".join(out)
    return re.sub(r"^(/p ?)", "", s).strip()


def norm_text(s):
    return re.sub(r"\s+", " ", s or "").strip()


def load_jsonl(p):
    with open(p, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def compare_parts(old_parts, new_parts):
    """old_parts/new_parts: lists of HTML strings -> dict of layer results."""
    ol = [split_label(h) for h in old_parts]
    nl = [split_label(h) for h in new_parts]
    ot = norm_text(" ".join(visible(x[1]) for x in ol))
    nt = norm_text(" ".join(visible(x[1]) for x in nl))
    osh = " | ".join(shape(x[1]) for x in ol)
    nsh = " | ".join(shape(x[1]) for x in nl)
    return {
        "text": ot == nt,
        "text_nows": re.sub(r"\s", "", ot) == re.sub(r"\s", "", nt),
        "shape": re.sub(r"\s*\|\s*", " ", osh) == re.sub(r"\s*\|\s*", " ", nsh),
        "parts": len(old_parts) == len(new_parts),
        "label_old": ol[0][0] if ol else "",
        "label_new": nl[0][0] if nl else "",
        "footer_old": any(x[2] for x in ol),
        "exact": [MARKER.sub("", h).strip() for h in old_parts] == [h.strip() for h in new_parts],
        "_ot": ot, "_nt": nt, "_osh": osh, "_nsh": nsh,
    }


def kind_family(kind):
    k = kind or ""
    k = re.sub(r"-[0-9a-f]{16}(-\d{3})?$", "", k)
    k = re.sub(r"-\d{3,4}$", "", k)
    k = re.sub(r"-\d{3,4}$", "", k)
    return k


def transcript_diff(a, horizon, ex):
    # ---- v0.1.20: messages per record ----
    old = collections.OrderedDict()  # (conv, src_end_offset) -> record
    old_skips = collections.Counter()
    for d in load_jsonl(a.old_transcript):
        if d.get("skipped"):
            old_skips[d["skipped"]] += 1
            continue
        ts = d.get("ts") or ""
        key = (d["conv"], d["src_end_offset"])
        r = old.get(key)
        if r is None:
            r = old[key] = {"conv": d["conv"], "kind": d["record_kind"], "phase": d.get("phase") or "",
                            "src": d.get("source_type"), "turn": d.get("turn") or "", "ts": ts, "body": "",
                            "parts": [], "line": d["src_line"], "mention": False}
        r["parts"].append((d["part"], d["html"]))
        r["mention"] = r["mention"] or bool(d.get("mention"))
        if d["part"] == 1:
            r["body"] = d.get("body") or ""
    # ---- new core ----
    new = []
    for d in load_jsonl(a.new_transcript):
        if d.get("eof") or d.get("error"):
            continue
        new.append(d)

    def in_window(ts):
        if not ts:
            return True
        try:
            t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return True
        return t >= horizon

    res = {"old_posted_records_by_kind": collections.Counter(), "new_posted_by_kind": collections.Counter(),
           "selection": collections.Counter(), "render": collections.defaultdict(collections.Counter),
           "old_skips_all_ages": dict(old_skips),
           "old_only_by_conv": collections.Counter()}
    # index old by (conv, U/F, normalized text) -> list
    idx = collections.defaultdict(list)
    for key, r in old.items():
        if not in_window(r["ts"]):
            continue
        r["parts"].sort()
        k = r["kind"]
        # U prompt, F final, S status (commentary), C compaction notice
        fam = {"user": "U", "assistant": "F", "status": "S", "compact": "C"}.get(k, k)
        if k == "assistant" and r["phase"] != "final_answer":
            fam = "assistant-" + (r["phase"] or "nophase")
        res["old_posted_records_by_kind"][fam] += 1
        if fam in ("U", "F", "S", "C"):
            idx[(r["conv"], fam, norm_text(r["body"]))].append(r)
    used = set()
    for d in new:
        if d.get("skipped"):
            res["selection"]["new:not-posted:" + d["skipped"]] += 1
            continue
        fam = {"prompt": "U", "final": "F", "status": "S", "compact": "C"}.get(d.get("kind") or "", "U" if d["user"] else "F")
        res["new_posted_by_kind"][fam] += 1
        text = norm_text(d["text"])
        cands = [r for r in idx.get((d["conv"], fam, text), []) if id(r) not in used]
        if not cands:
            # text differs (formatting strips) -> try by turn + family
            cands = [r for rs in idx.values() for r in rs if False]
            res["selection"]["%s:new-only" % fam] += 1
            if len(ex["new-only-" + fam]) < 8:
                ex["new-only-" + fam].append({"conv": d["conv"], "turn": d["turn"], "text": d["text"][:200]})
            d["_matched"] = None
            continue
        r = cands[0]
        used.add(id(r))
        res["selection"]["%s:both" % fam] += 1
        c = compare_parts([h for _, h in r["parts"]], [new_html(p) for p in d["parts"]])
        rr = res["render"][fam]
        rr["n"] += 1
        for layer in ("text", "text_nows", "shape", "parts", "exact"):
            rr[layer] += c[layer]
        rr["label:%s -> %s" % (c["label_old"], c["label_new"])] += 1
        rr["old_footer"] += c["footer_old"]
        rr["old_mention"] += r["mention"]
        for layer in ("text_nows", "shape", "parts"):
            if not c[layer] and len(ex["%s-%s-diff" % (fam, layer)]) < 6:
                ex["%s-%s-diff" % (fam, layer)].append({"conv": d["conv"], "line": r["line"],
                                                         "old_text": c["_ot"][:300], "new_text": c["_nt"][:300],
                                                         "old_shape": c["_osh"][:300], "new_shape": c["_nsh"][:300],
                                                         "old_parts": len(r["parts"]), "new_parts": len(d["parts"])})
    for key, rs in idx.items():
        for r in rs:
            if id(r) not in used:
                res["selection"]["%s:old-only" % key[1]] += 1
                res["old_only_by_conv"]["%s:%s" % (r["conv"], key[1])] += 1
                k = "old-only-" + key[1]
                if len(ex[k]) < 8:
                    ex[k].append({"conv": r["conv"], "line": r["line"], "turn": r["turn"], "src": r["src"], "text": r["body"][:200]})
    return res


def outbox_diff(a, ex):
    sent = {d["id"]: d for d in load_jsonl(a.sent)}
    v = {d["id"]: d for d in load_jsonl(a.old_outbox)}
    rc = {d["id"]: d for d in load_jsonl(a.rc59_outbox)}
    res = {"rc59_vs_v0120_identical": 0, "rc59_vs_v0120_differs": 0, "families": {}}
    for i, d in v.items():
        if rc.get(i, {}).get("html") == d.get("html"):
            res["rc59_vs_v0120_identical"] += 1
        else:
            res["rc59_vs_v0120_differs"] += 1
    fam = collections.defaultdict(collections.Counter)
    for d in load_jsonl(a.new_outbox):
        ids = d["ids"]
        f = kind_family(sent[ids[0]]["kind"])
        c = fam[f]
        c["messages"] += 1
        if d["new_kind"] == "none":
            c["no-new-equivalent"] += 1
            continue
        if d.get("error"):
            c["new-render-error"] += 1
            continue
        old_parts = [v[i]["html"] for i in ids if i in v]
        cmp = compare_parts(old_parts, [new_html(p) for p in d["parts"]])
        for layer in ("text", "text_nows", "shape", "parts", "exact"):
            c[layer] += cmp[layer]
        c["label:%s -> %s" % (cmp["label_old"], cmp["label_new"])] += 1
        c["old_footer"] += cmp["footer_old"]
        for layer in ("text_nows", "shape", "parts"):
            k = "outbox-%s-%s-diff" % (f, layer)
            if not cmp[layer] and len(ex[k]) < 4:
                ex[k].append({"id": ids[0], "old_text": cmp["_ot"][:300], "new_text": cmp["_nt"][:300],
                              "old_shape": cmp["_osh"][:300], "new_shape": cmp["_nsh"][:300],
                              "old_parts": len(old_parts), "new_parts": len(d["parts"])})
    res["families"] = {k: dict(c) for k, c in sorted(fam.items(), key=lambda x: -x[1]["messages"])}
    return res


def main():
    ap = argparse.ArgumentParser()
    for k in ("old-transcript", "new-transcript", "old-outbox", "rc59-outbox", "new-outbox", "sent", "now", "out", "examples"):
        ap.add_argument("--" + k, required=True)
    a = ap.parse_args()
    now = datetime.fromisoformat(a.now.replace("Z", "+00:00"))
    horizon = now - timedelta(days=120)
    ex = collections.defaultdict(list)
    out = {"now": a.now, "transcript": transcript_diff(a, horizon, ex), "outbox": outbox_diff(a, ex)}
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1, default=dict)
    with open(a.examples, "w", encoding="utf-8") as f:
        json.dump(ex, f, ensure_ascii=False, indent=1)
    json.dump(out["transcript"], sys.stdout, ensure_ascii=False, indent=1, default=dict)
    print()


if __name__ == "__main__":
    main()
