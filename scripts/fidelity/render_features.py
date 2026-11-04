#!/usr/bin/env python3
"""Attribute golden-diff rendering mismatches to Markdown features.

For every prompt/final both versions post (matched by conversation and
text), compare the label-free HTML (exact after removing the label header
and the old completion footer) and report, per Markdown feature present in
the source, how many records with that feature render differently.
Usage: render_features.py v0120-transcript.jsonl new-transcript.jsonl out.json
"""
import collections, json, re, sys
sys.path.insert(0, __import__("os").path.dirname(__file__))
from golden_diff import split_label, new_html, load_jsonl, norm_text

FEATURES = {
    "local-file-link": re.compile(r"\]\((/|[A-Za-z]:\\|file:)"),
    "web-link": re.compile(r"\]\(https?://"),
    "bare-url": re.compile(r"(?<![(<\[])https?://"),
    "list-after-text-line": re.compile(r"(?m)^(?!\s*([-*+]|\d+[.)])\s)[^\n]*\S[^\n]*\n\s*([-*+]|\d+[.)]) "),
    "text-line-after-list-item": re.compile(r"(?m)^\s*([-*+]|\d+[.)]) [^\n]*\n(?!\s*([-*+]|\d+[.)])\s)(?!\s*\n)\S"),
    "plus-bullet": re.compile(r"(?m)^\s*\+ "),
    "ordered-not-1": re.compile(r"(?m)^(?:(?!\n\n).)*?^\s*([2-9]|\d\d+)[.)] "),
    "nested-list": re.compile(r"(?m)^(  |\t)+([-*+]|\d+[.)]) "),
    "table": re.compile(r"(?m)^\s*\|.*\|\s*$"),
    "heading": re.compile(r"(?m)^#{1,6} "),
    "fence": re.compile(r"(?m)^\s*(```|~~~)"),
    "inline-code": re.compile(r"`[^`\n]+`"),
    "bold": re.compile(r"\*\*[^*\n]+\*\*"),
    "italic": re.compile(r"(?<![*\w])[*_][^*_\n]+[*_](?![*\w])"),
    "math": re.compile(r"\$[^$\n]+\$|\\\(|\\\[|\$\$"),
    "blockquote": re.compile(r"(?m)^\s*> "),
    "hard-break-lines": re.compile(r"[^\n]\n[^\n]"),
    "html-like": re.compile(r"<[a-zA-Z/][^>]*>"),
    "rule": re.compile(r"(?m)^\s*(---|\*\*\*|___)\s*$"),
}


def body_html(parts):
    out = []
    for h in parts:
        label, rest, _ = split_label(h)
        out.append(rest.strip())
    return "".join(out)


def main():
    oldp, newp, outp = sys.argv[1:4]
    old = {}
    for d in load_jsonl(oldp):
        if d.get("skipped") or d["record_kind"] not in ("user", "assistant") or (d["record_kind"] == "assistant" and d.get("phase") != "final_answer"):
            continue
        k = (d["conv"], d["src_end_offset"])
        r = old.setdefault(k, {"conv": d["conv"], "fam": "U" if d["record_kind"] == "user" else "F", "body": d.get("body") or "", "parts": []})
        if d["part"] == 1:
            r["body"] = d.get("body") or ""
        r["parts"].append((d["part"], d["html"]))
    idx = collections.defaultdict(list)
    for r in old.values():
        r["parts"].sort()
        idx[(r["conv"], r["fam"], norm_text(r["body"]))].append(r)
    stat = {f: collections.Counter() for f in ("U", "F")}
    feat = {f: collections.defaultdict(collections.Counter) for f in ("U", "F")}
    plain_ex = {"U": [], "F": []}
    for d in load_jsonl(newp):
        if not d.get("parts"):
            continue
        fam = "U" if d["user"] else "F"
        c = idx.get((d["conv"], fam, norm_text(d["text"])))
        if not c:
            continue
        r = c.pop(0)
        o = body_html([h for _, h in r["parts"]])
        n = body_html([new_html(p) for p in d["parts"]])
        if fam == "U":
            # approved: the prompt follows the label inline ("**User:** text")
            o = re.sub(r"^<p>", "", o, count=1)
            n = re.sub(r"^\s*", "", n)
        same = o == n
        stat[fam]["n"] += 1
        stat[fam]["same"] += same
        fs = [k for k, rx in FEATURES.items() if rx.search(r["body"])]
        if not fs:
            fs = ["(plain)"]
        for f in fs:
            feat[fam][f]["n"] += 1
            feat[fam][f]["differs"] += not same
        if not same and len(fs) <= 2 and len(plain_ex[fam]) < 12:
            i = next((j for j in range(min(len(o), len(n))) if o[j] != n[j]), min(len(o), len(n)))
            plain_ex[fam].append({"features": fs, "conv": r["conv"], "old": o[max(0, i - 120):i + 160], "new": n[max(0, i - 120):i + 160]})
    out = {"stat": stat, "features": {f: {k: dict(v) for k, v in sorted(feat[f].items(), key=lambda x: -x[1]["differs"])} for f in feat}, "examples": plain_ex}
    json.dump(out, open(outp, "w"), ensure_ascii=False, indent=1)
    print(json.dumps({"stat": stat, "features": out["features"]}, ensure_ascii=False, indent=0)[:4000])


if __name__ == "__main__":
    main()
