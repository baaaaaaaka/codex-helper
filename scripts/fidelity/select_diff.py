#!/usr/bin/env python3
"""Selection diff: which rollout records the new core mirrors vs v0.1.20.

Inputs:
  --old  v0.1.20 transcript-sync replay (v0120-transcript.jsonl: one row per
         posted message part, or a skipped record with its reason)
  --new  new-core Tracker replay (internal/core/transcript
         TestFidelitySelect, FIDELITY_MODE=select: one row per output)

Per kind (U prompt, F final answer, status, compact) the posted records are
matched:
  offset  same conversation and record end offset;
  twin    otherwise the same conversation and kind, the same text (hash of
          the text without whitespace) and at most 5 lines apart: v0.1.20
          posted the first record of an event_msg/response_item pair (the
          agent_message or user_message event), the new core reads the
          response_item;
  near    otherwise the same conversation and kind at most 2 lines apart
          (text differs only by v0.1.20's body formatting).
and reported as both / old-only / new-only, with examples. A new-only record
is annotated with the reason v0.1.20 skipped the record at that offset, if
it did.

Residue causes ("explained", per kind):
  old-only  new-helper-turn: the new core took the record's turn for a
            helper turn (the end-of-source row of the new replay lists them:
            client id, turn id, or the legacy Teams wrapper of the prompt,
            which the golden replay, run with an empty store, could not
            know);
  new-only  old-turn-misattributed: v0.1.20 skipped it as teams_origin_turn
            but posted other records of the turn the new core gives it (the
            golden replay took the turn of a record without a turn id from
            the first "turn_id" in its line, which in a compacted record is
            inside the replacement history: an old turn of the thread).

Writes a JSON summary (--out); prints it too.
"""
import argparse
import bisect
import collections
import hashlib
import json
import sys


def load_jsonl(p):
    with open(p, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def text_hash(s):
    s = "".join(ch for ch in (s or "") if not ch.isspace())
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def old_family(kind, phase):
    if kind == "user":
        return "U"
    if kind == "assistant":
        return "F" if phase == "final_answer" else "assistant-" + (phase or "nophase")
    return kind


NEW_FAMILY = {"prompt": "U", "final": "F", "status": "status", "compact": "compact"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--out")
    ap.add_argument("--examples", type=int, default=12)
    a = ap.parse_args()

    old = collections.OrderedDict()  # (conv, end) -> record
    skipped = {}  # (conv, end) -> reason
    for d in load_jsonl(a.old):
        key = (d["conv"], d["src_end_offset"])
        if d.get("skipped"):
            skipped.setdefault(key, d["skipped"])
            continue
        r = old.get(key)
        if r is None:
            r = old[key] = {"conv": d["conv"], "end": d["src_end_offset"], "line": d["src_line"],
                            "fam": old_family(d["record_kind"], d.get("phase")), "src": d.get("source_type"),
                            "phase": d.get("phase") or "", "turn": d.get("turn") or "", "body": ""}
        if d.get("part", 1) == 1:
            r["body"] = d.get("body") or ""
    for r in old.values():
        r["hash"] = text_hash(r["body"])

    old_turns = collections.defaultdict(set)  # conv -> turns with posted records
    for r in old.values():
        old_turns[r["conv"]].add(r["turn"])
    new = []
    errors = 0
    helper_turns = collections.defaultdict(set)
    for d in load_jsonl(a.new):
        if d.get("eof"):
            helper_turns[d["conv"]].update(d.get("helper_turns") or [])
            continue
        if d.get("error"):
            errors += 1
            continue
        new.append({"conv": d["conv"], "end": d["end"], "line": d["line"], "fam": NEW_FAMILY.get(d["kind"], d["kind"]),
                    "id": d["record_id"], "turn": d.get("turn") or "", "hash": d["hash"]})

    fams = sorted({r["fam"] for r in old.values()} | {n["fam"] for n in new})
    res = {"selection": {}, "matched_by": {}, "old_only_by_source": {}, "new_only_vs_old_skip": {},
           "old_only_by_conv": {}, "new_only_by_conv": {},
           "examples": {}, "new_errors": errors}
    for fam in fams:
        olds = [r for r in old.values() if r["fam"] == fam]
        news = [n for n in new if n["fam"] == fam]
        used_old, used_new = set(), set()
        by = collections.Counter()
        okey = {(r["conv"], r["end"]): i for i, r in enumerate(olds)}
        for j, n in enumerate(news):
            i = okey.get((n["conv"], n["end"]))
            if i is not None and i not in used_old:
                used_old.add(i)
                used_new.add(j)
                by["offset"] += 1
        # twin / near passes, per conversation
        oc = collections.defaultdict(list)
        for i, r in enumerate(olds):
            if i not in used_old:
                oc[r["conv"]].append((r["line"], i))
        for v in oc.values():
            v.sort()
        for rule, dist in (("twin", 5), ("near", 2)):
            for j, n in enumerate(news):
                if j in used_new:
                    continue
                best = None
                cand = oc.get(n["conv"], [])
                lo = bisect.bisect_left(cand, (n["line"] - dist, -1))
                hi = bisect.bisect_right(cand, (n["line"] + dist, len(olds)))
                for _, i in cand[lo:hi]:
                    if i in used_old:
                        continue
                    r = olds[i]
                    d = abs(r["line"] - n["line"])
                    if d > dist or (rule == "twin" and r["hash"] != n["hash"]):
                        continue
                    if best is None or d < best[0]:
                        best = (d, i)
                if best:
                    used_old.add(best[1])
                    used_new.add(j)
                    by[rule] += 1
        both = len(used_new)
        old_only = [olds[i] for i in range(len(olds)) if i not in used_old]
        new_only = [news[j] for j in range(len(news)) if j not in used_new]
        res["selection"][fam] = {"old": len(olds), "new": len(news), "both": both,
                                 "old-only": len(old_only), "new-only": len(new_only)}
        res["matched_by"][fam] = dict(by)
        res["old_only_by_source"][fam] = dict(collections.Counter("%s/%s" % (r["src"], r["phase"]) for r in old_only))
        res["new_only_vs_old_skip"][fam] = dict(collections.Counter(
            skipped.get((n["conv"], n["end"]), "no-old-row") for n in new_only))
        oc_ = collections.Counter("new-helper-turn" if r["turn"] in helper_turns[r["conv"]] else "unexplained"
                                  for r in old_only)
        nc_ = collections.Counter("old-turn-misattributed"
                                  if skipped.get((n["conv"], n["end"])) == "teams_origin_turn" and n["turn"] in old_turns[n["conv"]]
                                  else "unexplained" for n in new_only)
        res["selection"][fam]["old-only-causes"] = dict(oc_)
        res["selection"][fam]["new-only-causes"] = dict(nc_)
        res["old_only_by_conv"][fam] = dict(collections.Counter(r["conv"] for r in old_only).most_common())
        res["new_only_by_conv"][fam] = dict(collections.Counter(n["conv"] for n in new_only).most_common())
        res["examples"][fam] = {
            "old-only": [{k: r[k] for k in ("conv", "line", "end", "src", "turn")} | {"text": r["body"][:160]}
                         for r in old_only[:a.examples]],
            "new-only": [{k: n[k] for k in ("conv", "line", "end", "id", "turn")}
                         | {"old_skip": skipped.get((n["conv"], n["end"]), "")}
                         for n in new_only[:a.examples]],
        }
    out = json.dumps(res, indent=1, ensure_ascii=False)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(out + "\n")
    print(json.dumps({"selection": res["selection"], "matched_by": res["matched_by"],
                      "old_only_by_source": res["old_only_by_source"],
                      "new_only_vs_old_skip": res["new_only_vs_old_skip"],
                      "old_only_by_conv": res["old_only_by_conv"], "new_only_by_conv": res["new_only_by_conv"]}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
