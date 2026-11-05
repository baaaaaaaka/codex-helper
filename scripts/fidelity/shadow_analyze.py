#!/usr/bin/env python3
"""Checks of a real-data shadow run (TestShadowRealData) against the oracle.

Inputs
  --run       the run's dump directory (messages-final.jsonl, writes.jsonl,
              phases.json, core.sqlite[-wal]): what the helper posted to the
              fake Graph and the store it ran on
  --old       the old store (read-only), for the Teams-run turn ids
  --base      a converter dry-run of the same old store made at about the
              same time (core.sqlite): which conversations existed before
              the run and where each rollout resumes (R)
  --details   checkpoint_oracle.py details for --base: every mirrorable
              record after the old delivery end, classified
  --sessions  the Codex sessions directory (for discovery of new sessions)
  --compare   another run's dump directory (a second fresh conversion):
              the posted content must be identical
  --out       JSON summary; --per-chat JSONL per conversation

Expected posts of a conversation that existed before the run: the oracle's
records classed post:*, read:short-unverifiable and DUP:* (the last are
posted again: listed as such). There is no catch-up window (owner
decision 2026-11-05; window_delta stays 0). A conversation the helper
created during the run (a local session that never had a chat) is expected
to post every mirrorable record of its rollout, except Teams-run turns.
Mirrored status/compaction notices (live records only) are counted apart
(posted_status), not matched.

Matching is by text: the visible text of a posted message (parts joined,
labels removed) and of a record are reduced to letters (punctuation,
digits, markup and whitespace removed), so rendering differences do not
matter; an inexact match above 0.85 similarity is counted as "fuzzy".
"""
import argparse
import collections
import difflib
import html as htmlmod
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from checkpoint_oracle import scan_rollout, OLD_WRAPPER  # noqa: E402

NONLETTER = re.compile(r"[\W\d_]+", re.UNICODE)
# A part header ("Codex [part 1/2]:", or an unlabelled " [part 1/2]:" as
# split prompts had before 2026-11-05), then possibly an inline "User:".
# v0.1.20 labels since the parity round: "🧑‍💻 User", "🤖 ✅ Codex answer",
# "🤖 ⏳ Codex status", "🔧 Helper", each possibly with " [part i/n]".
LABEL = re.compile(r"^\s*(?:(?:(?:🧑\u200d💻 )?User|Codex|🔧 Helper|🤖[^:]*)(?: \[part \d+/\d+\])?|\[part \d+/\d+\]):\s*")
USER = re.compile(r"^\s*(?:\[part \d+/\d+\]:\s*)?(?:🧑\u200d💻 )?User(?: \[part \d+/\d+\])?:")
STATUS = re.compile(r"^\s*🤖 ⏳ Codex status(?: \[part \d+/\d+\])?:")
# the footer v0.1.20 put on the last part of an answer
FOOTER = re.compile(r"\s*🔧 Helper:\s*✅ Codex finished responding\.\s*$")
TAG = re.compile(r"<[^>]+>")
EXPECT_CLASSES = ("post:", "read:short-unverifiable", "DUP:")


# v0.1.20 renders a Markdown link to a local file as the path alone
# ("[name](/abs/path:1)" -> "/abs/path:1"): key a record the same way.
LOCAL_LINK = re.compile(r"\[([^\]\n]*)\]\((/[^)\s]+)\)")


def letters(s):
    return NONLETTER.sub("", LOCAL_LINK.sub(r"\2", s or "")).lower()


def jl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def open_copy(run):
    """Open the run's store (with its WAL) from a private copy."""
    tmp = tempfile.mkdtemp(prefix="shadow-core-")
    for n in ("core.sqlite", "core.sqlite-wal"):
        p = os.path.join(run, n)
        if os.path.exists(p):
            shutil.copy(p, os.path.join(tmp, n))
    return sqlite3.connect(os.path.join(tmp, "core.sqlite")), tmp


def ro(path):
    return sqlite3.connect("file:%s?immutable=1" % path, uri=True)


def visible(m):
    t = m.get("text")
    if t is None:
        t = htmlmod.unescape(TAG.sub(" ", m.get("html") or ""))
    return t


def load_run(run, messages="messages-final.jsonl"):
    db, tmp = open_copy(run)
    convs = {}
    for cid, label, chat, state, kind in db.execute("select id, label, ext_chat_id, state, kind from conversations"):
        convs[cid] = dict(id=cid, label=label, chat=chat, state=state, kind=kind)
    by_chat = {c["chat"]: c for c in convs.values() if c["chat"]}
    out = {}
    meta0 = dict(db.execute("select key, value from meta").fetchall())
    migrated = int(meta0.get("migrated_at") or 0)
    for cid, kind, key, part, parts, state, ext, sent, created in db.execute(
            "select conv_id, kind, delivery_key, part, parts, state, ext_msg_id, sent_at, created_at from outbox"):
        if ext:
            out[(convs[cid]["chat"], ext)] = dict(conv=cid, kind=kind, key=key, part=part, parts=parts, state=state, sent=sent,
                                                  run=created > migrated)
    # rows created by the run (the converter's imported rows are older)
    outbox_states = collections.Counter(r[0] for r in db.execute("select state from outbox where created_at > ?", (migrated,)))
    imported_states = collections.Counter(r[0] for r in db.execute("select state from outbox where created_at <= ?", (migrated,)))
    sources = {}
    for path, conv, state, off, size in db.execute("select path, conv_id, state, offset, size from transcript_sources"):
        sources[path] = dict(conv=conv, state=state, offset=off, size=size)
    meta = dict(db.execute("select key, value from meta").fetchall())
    db.close()
    shutil.rmtree(tmp, ignore_errors=True)
    msgs = list(jl(os.path.join(run, messages)))
    return dict(convs=convs, by_chat=by_chat, outbox=out, outbox_states=outbox_states, imported_states=imported_states,
                sources=sources, meta=meta, msgs=msgs)


def logical(run):
    """Group the posted messages into logical messages per chat."""
    per = collections.defaultdict(list)
    orphans = []
    groups = collections.OrderedDict()
    for m in sorted(run["msgs"], key=lambda m: (m["chat"], m["created"], m["id"])):
        ob = run["outbox"].get((m["chat"], m["id"]))
        if ob is None:
            orphans.append(m)
            kind, key, part = "?", "?:" + m["id"], 1
        else:
            kind, key, part = ob["kind"], ob["key"], ob["part"]
            if ob["parts"] > 1:
                key = key.rsplit(":", 1)[0] if key.count(":") else key
        g = groups.get((m["chat"], key))
        if g is None:
            g = groups[(m["chat"], key)] = dict(chat=m["chat"], kind=kind, key=key, created=m["created"], parts=[])
        g["parts"].append((part, m))
    for (chat, _), g in groups.items():
        g["parts"].sort(key=lambda x: x[0])
        texts = [FOOTER.sub("", LABEL.sub("", LABEL.sub("", visible(m), count=1), count=1)) for _, m in g["parts"]]
        g["text"] = "\n".join(texts)
        g["user"] = bool(USER.match(visible(g["parts"][0][1])))
        g["status"] = bool(STATUS.match(visible(g["parts"][0][1])))
        g["n_parts"] = len(g["parts"])
        g["patched"] = sum(m.get("patches", 0) for _, m in g["parts"])
        per[chat].append(g)
    for chat in per:
        per[chat].sort(key=lambda g: g["created"])
    return per, orphans


def interactive_meta(path):
    try:
        with open(path, "rb") as f:
            first = f.readline()
        d = json.loads(first)
    except Exception:
        return None
    if d.get("type") != "session_meta":
        return None
    p = d.get("payload") or {}
    src = p.get("source")
    if isinstance(src, dict):
        src = next(iter(src), "")
    src = (src or "").lower()
    orig = (p.get("originator") or "").lower()
    sub = src in ("exec", "subagent", "mcp") or src.startswith("sub_agent")
    return dict(id=p.get("id") or "", subagent=sub, interactive=not sub and orig not in ("codex-helper", "codex_helper", "codex_exec"))


def match(expected, posted):
    """Greedy in-order matching by letters, then fuzzy. Returns details."""
    pool = collections.defaultdict(list)
    for i, e in enumerate(expected):
        pool[(e["user"], e["key"])].append(i)
    used = [None] * len(expected)
    res = dict(matched=0, fuzzy=0, unexpected=[], dup=[])
    seen_keys = collections.Counter()
    for j, p in enumerate(posted):
        k = (p["user"], letters(p["text"]))
        seen_keys[k] += 1
        idx = pool.get(k)
        if idx:
            i = idx.pop(0)
            used[i] = j
            res["matched"] += 1
            continue
        # fuzzy against unmatched expected of the same kind
        best, bi = 0.0, -1
        pk = k[1][:600]
        for i, e in enumerate(expected):
            if used[i] is not None or e["user"] != p["user"]:
                continue
            sm = difflib.SequenceMatcher(None, pk, e["key"][:600], autojunk=False)
            if sm.real_quick_ratio() < 0.85 or sm.quick_ratio() < 0.85:
                continue
            r = sm.ratio()
            if r > best:
                best, bi = r, i
        if best >= 0.85:
            used[bi] = j
            res["matched"] += 1
            res["fuzzy"] += 1
            pool[(expected[bi]["user"], expected[bi]["key"])].remove(bi)
            continue
        if any(e["user"] == p["user"] and e["key"] == k[1] for e in expected):
            res["dup"].append(j)
        else:
            res["unexpected"].append(j)
    res["missing"] = [i for i, u in enumerate(used) if u is None]
    res["used"] = used
    return res


def window_rate(times_ms, width_ms):
    times_ms = sorted(times_ms)
    best, lo = 0, 0
    for hi, t in enumerate(times_ms):
        while t - times_ms[lo] >= width_ms:
            lo += 1
        best = max(best, hi - lo + 1)
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--messages", default="messages-final.jsonl")
    ap.add_argument("--old", required=True)
    ap.add_argument("--base", required=True)
    ap.add_argument("--details", required=True)
    ap.add_argument("--sessions", required=True)
    ap.add_argument("--compare")
    ap.add_argument("--out", required=True)
    ap.add_argument("--per-chat")
    a = ap.parse_args()

    run = load_run(a.run, a.messages)
    now_ms = int(run["meta"].get("migrated_at") or 0)
    # No catch-up window (owner decision 2026-11-05): nothing is "too old".
    cut_ms = 0
    per, orphans = logical(run)
    old = ro(a.old)
    teams_turns = set()
    for (j,) in old.execute("select json from turns"):
        try:
            t = json.loads(j).get("codex_turn_id") or ""
        except ValueError:
            t = ""
        if t:
            teams_turns.add(t)
    base = ro(a.base)
    base_conv = {label: (cid, chat, state) for cid, label, chat, state in base.execute("select id, label, ext_chat_id, state from conversations")}
    base_paths = {p: (st, conv, off) for p, st, conv, off in base.execute("select path, state, conv_id, offset from transcript_sources")}
    base_label_by_id = {v[0]: k for k, v in base_conv.items()}

    # ---- expectations: conversations that existed before the run ----
    exp = collections.defaultdict(list)
    window = collections.defaultdict(list)
    for d in jl(a.details):
        if d["cls"].startswith(EXPECT_CLASSES):
            rec = dict(user=d["kind"] == "prompt", key=letters(d["text"]), text=d["text"], line=d["line"], path=d["path"],
                       ts=d.get("ts") or 0, cls=d["cls"], full=False)
            if rec["ts"] and rec["ts"] < cut_ms:
                window[d["conv"]].append(rec)
            else:
                exp[d["conv"]].append(rec)
        elif d["cls"] in ("skip:older-than-120d", "read:older-than-120d"):
            window[d["conv"]].append(dict(cls=d["cls"], line=d["line"], path=d["path"]))
    # the details carry 200 characters of text: complete the keys from the rollouts
    full_text = {}
    need = collections.defaultdict(set)
    for conv, recs in exp.items():
        for r in recs:
            need[r["path"]].add(r["line"])
    path_of = {os.path.basename(p): p for p in base_paths}
    latest = {}
    for base_name, lines in need.items():
        p = path_of.get(base_name)
        if not p:
            continue
        recs, _, _ = scan_rollout(p)
        for r in recs:
            if r["line"] in lines:
                full_text[(base_name, r["line"])] = r["text"]
    for conv, recs in exp.items():
        for r in recs:
            t = full_text.get((r["path"], r["line"]))
            if t is not None:
                r["text"], r["key"] = t, letters(t)

    # latest mirrorable record of every tracked rollout (within the window,
    # not a Teams-run turn): every conversation must reach it
    for p, (st, conv, off) in base_paths.items():
        if st != "tracking" or conv is None:
            continue
        recs, _, _ = scan_rollout(p)
        last = None
        # a turn whose prompt has the old helper's wrapper was run from Teams
        wrapped_turns = {r["turn"] for r in recs if r.get("wrapped") and r["turn"]}
        for r in recs:
            if r["turn"] in teams_turns or r["turn"] in wrapped_turns or r.get("wrapped") or (r["ts"] and r["ts"] < cut_ms):
                continue
            last = r
        if last is not None:
            latest[base_label_by_id.get(conv)] = dict(line=last["line"], start=last["start"], key=letters(last["text"]),
                                                      user=last["kind"] == "prompt", before_R=last["start"] < off, path=os.path.basename(p))

    # ---- discovery: local sessions that never had a chat ----
    expected_new = {}
    for root, _, files in os.walk(a.sessions):
        for n in files:
            if not (n.startswith("rollout-") and n.endswith(".jsonl")):
                continue
            p = os.path.join(root, n)
            if p in base_paths:
                continue
            if os.path.getmtime(p) * 1000 < cut_ms:
                continue
            m = interactive_meta(p)
            if m and m["interactive"]:
                expected_new[p] = m
    run_new = {}
    for p, s in run["sources"].items():
        conv = run["convs"].get(s["conv"])
        if conv and conv["label"] not in base_conv:
            run_new[p] = conv
    # a session the live mode wrote inside the container (not on the host):
    # measured by the scenario itself, left out of the comparison
    live_labels = {run_new[p]["label"] for p in list(run_new) if not os.path.exists(p)}
    run_new = {p: c for p, c in run_new.items() if os.path.exists(p)}
    disc = dict(live_injected=sorted(live_labels), expected=len(expected_new), created=len(run_new),
                missing=sorted(os.path.basename(p) for p in expected_new if p not in run_new),
                unexpected=sorted(os.path.basename(p) for p in run_new if p not in expected_new))
    for p, conv in run_new.items():
        recs, _, _ = scan_rollout(p)
        for r in recs:
            if r["turn"] in teams_turns or r.get("wrapped"):
                continue
            rec = dict(user=r["kind"] == "prompt", key=letters(r["text"]), text=r["text"], line=r["line"], path=os.path.basename(p),
                       ts=r["ts"], cls="new-chat", full=True)
            if r["ts"] and r["ts"] < cut_ms:
                window[conv["label"]].append(rec)
            else:
                exp[conv["label"]].append(rec)
        if recs:
            latest[conv["label"]] = dict(line=recs[-1]["line"], start=recs[-1]["start"], key=letters(recs[-1]["text"]),
                                         user=recs[-1]["kind"] == "prompt", before_R=False, path=os.path.basename(p))

    # ---- per conversation ----
    totals = collections.Counter()
    notices = collections.Counter()
    rows = []
    control_msgs = []
    labels = set(exp) | {c["label"] for c in run["convs"].values()}
    conv_by_label = {c["label"]: c for c in run["convs"].values()}
    for label in sorted(labels):
        c = conv_by_label.get(label)
        chat = c["chat"] if c else None
        posted = per.get(chat, []) if chat else []
        # mirrored status/compaction notices (live records only, v0.1.20)
        status = [g for g in posted if g["kind"] == "mirror" and g["status"]]
        mirror = [g for g in posted if g["kind"] == "mirror" and not g["status"]]
        other = [g for g in posted if g["kind"] != "mirror"]
        for g in other:
            tmpl = re.sub(r"\d+", "#", " ".join(g["text"].split()))[:70]
            notices[(g["kind"], tmpl)] += 1
            if c and c["kind"] == "control":
                control_msgs.append(dict(kind=g["kind"], text=g["text"][:400]))
        if label in live_labels:
            totals["live-injected convs (left out)"] += 1
            totals["live-injected mirror posts"] += len(mirror)
            totals["live-injected status posts"] += len(status)
            continue
        e = exp.get(label, [])
        res = match(e, mirror)
        row = dict(label=label, chat=chat, state=c["state"] if c else None, new=label not in base_conv,
                   expected=len(e), posted_mirror=len(mirror), posted_status=len(status), posted_other=len(other), matched=res["matched"], fuzzy=res["fuzzy"],
                   missing=len(res["missing"]), unexpected=len(res["unexpected"]), dup=len(res["dup"]),
                   window_delta=len(window.get(label, [])), multipart=sum(1 for g in mirror if g["n_parts"] > 1),
                   patched=sum(g["patched"] for g in posted))
        if e:
            first_i = next((i for i, u in enumerate(res["used"]) if u is not None), None)
            last_i = max((i for i, u in enumerate(res["used"]) if u is not None), default=None)
            row.update(first_expected=e[0]["line"], last_expected=e[-1]["line"],
                       first_posted=e[first_i]["line"] if first_i is not None else None,
                       last_posted=e[last_i]["line"] if last_i is not None else None)
        lt = latest.get(label)
        if lt:
            reached = lt["before_R"] or any(r["line"] == lt["line"] and res["used"][i] is not None for i, r in enumerate(e))
            row.update(latest_line=lt["line"], latest_before_resume=lt["before_R"], reached_latest=reached)
            totals["convs with a latest record"] += 1
            totals["convs reached latest"] += reached
        if res["missing"]:
            row["missing_examples"] = [dict(line=e[i]["line"], cls=e[i]["cls"], user=e[i]["user"], text=e[i]["text"][:160]) for i in res["missing"][:5]]
        if res["unexpected"]:
            row["unexpected_examples"] = [dict(user=mirror[j]["user"], text=mirror[j]["text"][:160]) for j in res["unexpected"][:5]]
        if res["dup"]:
            row["dup_examples"] = [dict(user=mirror[j]["user"], text=mirror[j]["text"][:160]) for j in res["dup"][:5]]
        # the same content twice in a chat beyond the history's own repeats
        # A posted message matched (exactly or fuzzily) to a record counts
        # under that record's key, so a repeated record posted once per
        # occurrence is not a repeat.
        ek = collections.Counter((r["user"], r["key"]) for r in e)
        gkey = {j: (e[i]["user"], e[i]["key"]) for i, j in enumerate(res["used"]) if j is not None}
        pk = collections.Counter(gkey.get(j, (g["user"], letters(g["text"]))) for j, g in enumerate(mirror))
        row["repeat_beyond_history"] = sum(max(0, n - max(1, ek.get(k, 0))) for k, n in pk.items())
        ok = collections.Counter(letters(g["text"]) for g in other)
        row["notice_repeats"] = {k[:60]: n for k, n in ok.items() if n > 1}
        for k in ("expected", "posted_mirror", "posted_status", "posted_other", "matched", "fuzzy", "missing", "unexpected", "dup", "window_delta", "repeat_beyond_history", "multipart", "patched"):
            totals[k] += row[k]
        totals["convs"] += 1
        totals["convs with missing"] += row["missing"] > 0
        totals["convs with unexpected"] += row["unexpected"] > 0
        rows.append(row)

    # ---- integrity: Graph vs the store's outbox ----
    graph_ids = {(m["chat"], m["id"]) for m in run["msgs"]}
    sent_rows = {k for k, v in run["outbox"].items() if v["state"] == "sent" and v["run"]}
    integrity = dict(graph_messages=len(graph_ids), outbox_sent_with_id=len(sent_rows),
                     graph_not_in_outbox=len(orphans), sent_not_in_graph=len(sent_rows - graph_ids),
                     orphan_examples=[dict(chat=m["chat"], topic=m.get("topic"), text=visible(m)[:120]) for m in orphans[:5]],
                     outbox_states=dict(run["outbox_states"]), imported_outbox_states=dict(run["imported_states"]))

    # ---- pacing ----
    posts = [w["at"] for w in jl(os.path.join(a.run, "writes.jsonl")) if w.get("route") == "POST /chats/{chat}/messages"]
    writes = [w for w in jl(os.path.join(a.run, "writes.jsonl")) if "route" in w]
    pacing = dict(posts=len(posts), max_in_1s=window_rate(posts, 1000), max_in_10s=window_rate(posts, 10000),
                  max_in_60s=window_rate(posts, 60000), writes_by_route=dict(collections.Counter(w["route"] for w in writes)),
                  status=dict(collections.Counter(str(w["status"]) for w in writes)))
    if posts:
        span = (max(posts) - min(posts)) / 1000
        pacing.update(first_to_last_s=span, mean_per_s=len(posts) / span if span else None)

    summary = dict(run=a.run, conversion_now_ms=now_ms, totals=dict(totals), discovery=disc, integrity=integrity, pacing=pacing,
                   phases=json.load(open(os.path.join(a.run, "phases.json"))) if os.path.exists(os.path.join(a.run, "phases.json")) else None,
                   notices=[dict(kind=k, template=t, n=n) for (k, t), n in notices.most_common()],
                   control_chat=control_msgs)

    if a.compare:
        other = load_run(a.compare, a.messages)
        oper, _ = logical(other)

        def seqs(r, p):
            # key by conversation label for converted conversations, by rollout for new ones
            label_of_path = {}
            for path, s in r["sources"].items():
                c = r["convs"].get(s["conv"])
                if c and c["label"] not in base_conv:
                    label_of_path[c["label"]] = os.path.basename(path)
            out = {}
            for c in r["convs"].values():
                k = c["label"] if c["label"] in base_conv else "new:" + label_of_path.get(c["label"], c["label"])
                out[k] = [(g["kind"], letters(g["text"])) for g in p.get(c["chat"], [])]
            return out
        s1, s2 = seqs(run, per), seqs(other, oper)
        diff = [k for k in set(s1) | set(s2) if s1.get(k) != s2.get(k)]
        summary["compare"] = dict(other=a.compare, convs=len(set(s1) | set(s2)), differing=len(diff),
                                  examples=[dict(conv=k, a=len(s1.get(k, [])), b=len(s2.get(k, []))) for k in sorted(diff)[:20]])

    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1)
    if a.per_chat:
        with open(a.per_chat, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    json.dump(dict(totals=summary["totals"], discovery={k: (v if not isinstance(v, list) else len(v)) for k, v in disc.items()},
                   integrity=integrity, pacing=pacing, compare=summary.get("compare")), sys.stdout, ensure_ascii=False, indent=1)
    print()


if __name__ == "__main__":
    main()
