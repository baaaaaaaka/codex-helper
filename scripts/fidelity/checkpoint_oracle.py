#!/usr/bin/env python3
"""Independent checkpoint/resume oracle for the legacy -> core store conversion.

It derives, from the OLD store tables and the real rollouts only, where the
old core's transcript sync stopped for every linked rollout, and checks the
resume position the converter wrote into core.sqlite against it.

It does not import or reuse any new-core code. Everything is read-only:
SQLite files are opened with immutable=1, rollouts are opened for reading.

Usage:
  checkpoint_oracle.py --old <store.sqlite> --core <core.sqlite> \
      --now 2026-11-04T15:25:43Z --out <summary.json> [--details <details.jsonl>]

Definitions (see FIDELITY-REPORT.md):
  D  old delivery end: the furthest source_offset of a transcript_deliveries
     row (sent or skipped) of the session for that rollout, validated as a
     record end in the current file.
  C  old checkpoint: import_checkpoints.last_offset of the session.
  R  the converter's resume offset (core.sqlite transcript_sources.offset).
  mirrorable record: a user prompt (response_item message role=user that is
     not Codex-injected context) or a final answer (response_item message
     role=assistant phase=final_answer; else a task_complete
     last_agent_message of a turn without one), CORE-DESIGN section 5.
  posted-by-old: the record's text appears in what the old core put into
     that chat: a sent outbox row (any kind) or, for a prompt, a Teams
     inbound message of the chat (a Teams-run turn's own prompt).
"""
import argparse
import bisect
import collections
import datetime as dt
import html
import json
import os
import re
import sqlite3
import sys

WORD = re.compile(r"[^\w]+", re.UNICODE)
TAG = re.compile(r"<[^>]+>")


def norm(text):
    return WORD.sub("", text).lower()


def strip_html(s):
    return html.unescape(TAG.sub(" ", s))


def ro(path):
    return sqlite3.connect("file:%s?immutable=1" % path, uri=True)


def jload(b):
    try:
        return json.loads(b)
    except Exception:
        return {}


def parse_ts(s):
    if not s:
        return 0
    try:
        s = s.replace("Z", "+00:00")
        # trim nanoseconds to microseconds
        m = re.match(r"(.*\.\d{6})\d*(.*)", s)
        if m:
            s = m.group(1) + m.group(2)
        return int(dt.datetime.fromisoformat(s).timestamp() * 1000)
    except Exception:
        return 0


INJECTED_PREFIXES = ("<", "# AGENTS.md instructions")
# Wrapper text the old helper put around a Teams prompt (rc.59 artifact_handoff.go).
OLD_WRAPPER = ("Teams helper safety:", "If you need to return generated files or images to the Teams user, write them under this local directory:",
               "<codex-helper-control-context", "[codex-helper-control]",
               "You are handling an unrecognized message from the user's Microsoft Teams control chat")


def scan_rollout(path):
    """Return (records, ends): mirrorable records and the set of record ends.

    Each record: dict(start, end, line, kind, id, turn, ts, text).
    """
    recs = []
    ends = {0: 0}
    turn = ""
    turn_final = False
    turn_started_here = False
    asst_ends = collections.defaultdict(list)  # (turn, normalized text) -> ends of assistant records
    off = 0
    line = 0
    with open(path, "rb") as f:
        for raw in f:
            start = off
            off += len(raw)
            if not raw.endswith(b"\n"):
                break  # partial trailing line is not a record
            line += 1
            ends[off] = line
            head = raw[:600]
            is_ctx = b'"type":"turn_context"' in head
            is_start = b'"type":"task_started"' in head
            is_done = b'"type":"task_complete"' in head
            is_msg = b'"type":"response_item"' in head and b'"type":"message"' in head
            is_agent = b'"type":"event_msg"' in head and b'"type":"agent_message"' in head
            if is_agent:
                d = jload(raw)
                m = ((d.get("payload") or {}).get("message") or "").strip()
                if m:
                    asst_ends[(turn, norm(m))].append(off)
                continue
            if not (is_ctx or is_start or is_done or is_msg):
                continue
            d = jload(raw)
            p = d.get("payload") or {}
            ts = parse_ts(d.get("timestamp"))
            if d.get("type") == "turn_context" or (d.get("type") == "event_msg" and p.get("type") == "task_started"):
                tid = p.get("turn_id") or ""
                if tid and tid != turn:
                    turn, turn_final, turn_started_here = tid, False, True
                continue
            if d.get("type") == "event_msg" and p.get("type") == "task_complete":
                tid = p.get("turn_id") or ""
                lam = p.get("last_agent_message")
                if tid and tid != turn:
                    turn, turn_final, turn_started_here = tid, False, False
                if turn_started_here and not turn_final and isinstance(lam, str) and lam.strip():
                    recs.append(dict(start=start, end=off, line=line, kind="final", id="t:%s:final" % turn,
                                     turn=turn, ts=ts, text=lam.strip(),
                                     alt_ends=list(asst_ends.get((turn, norm(lam)), []))))
                turn_final = True
                continue
            if d.get("type") != "response_item" or p.get("type") != "message":
                continue
            text = "".join((c.get("text") or "") for c in (p.get("content") or []) if isinstance(c, dict)).strip()
            role = p.get("role")
            if role == "user":
                if not text or text.startswith(INJECTED_PREFIXES):
                    continue
                wrapped = any(m in text for m in OLD_WRAPPER)
                recs.append(dict(start=start, end=off, line=line, kind="prompt", id=p.get("id") or "",
                                 turn=turn, ts=ts, text=text, wrapped=wrapped))
            elif role == "assistant" and text:
                prior = list(asst_ends.get((turn, norm(text)), []))
                asst_ends[(turn, norm(text))].append(off)
                if p.get("phase") == "final_answer":
                    turn_final = True
                    recs.append(dict(start=start, end=off, line=line, kind="final", id=p.get("id") or "",
                                     turn=turn, ts=ts, text=text, alt_ends=prior))
    return recs, ends, off


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--core", required=True)
    ap.add_argument("--now", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--details")
    a = ap.parse_args()
    now_ms = parse_ts(a.now)
    cut_ms = now_ms - 120 * 86400 * 1000

    old = ro(a.old)
    core = ro(a.core)

    # ---- old tables ----
    sessions = {}
    for sid, chat, status, j in old.execute("select id, teams_chat_id, status, json from sessions"):
        d = jload(j)
        sessions[sid] = dict(chat=chat or d.get("teams_chat_id") or "", status=status or d.get("status"),
                             thread=d.get("codex_thread_id") or "")
    teams_turns = collections.defaultdict(set)  # session -> codex turn ids run from Teams
    all_teams_turns = set()
    for sid, j in old.execute("select session_id, json from turns"):
        t = jload(j).get("codex_turn_id") or ""
        if t:
            teams_turns[sid].add(t)
            all_teams_turns.add(t)
    # deliveries per (session, path)
    deliv = collections.defaultdict(list)
    deliv_ids = collections.defaultdict(set)  # (session,path) -> record ids sent/skipped
    for sid, st, j in old.execute("select session_id, status, json from transcript_deliveries"):
        d = jload(j)
        status = d.get("status") or st
        if status not in ("sent", "skipped"):
            continue
        key = (sid, d.get("source_path") or "")
        deliv[key].append((int(d.get("source_offset") or 0), int(d.get("source_line") or 0), status,
                           d.get("source_record_id") or "", d.get("kind") or ""))
        if d.get("source_record_id"):
            deliv_ids[key].add(d["source_record_id"])
    ckpt = {}
    for sid, j in old.execute("select session_id, json from import_checkpoints"):
        d = jload(j)
        p = d.get("source_path") or ""
        if p:
            ckpt[(sid, p)] = max(ckpt.get((sid, p), 0), int(d.get("last_offset") or 0))
    # what the old core put into each chat (normalized text)
    chat_text = collections.defaultdict(list)
    for chat, st, j in old.execute("select teams_chat_id, status, json from outbox_messages where status='sent'"):
        b = jload(j).get("body") or ""
        if "<p>" in b or "<strong>" in b:
            b = strip_html(b)
        chat_text[chat].append(norm(b))
    chat_in = collections.defaultdict(list)
    for chat, j in old.execute("select teams_chat_id, json from inbound_events"):
        t = jload(j).get("text") or ""
        n = norm(t)
        if n:
            chat_in[chat].append(n)
    chat_blob = {c: "\x00".join(v) for c, v in chat_text.items()}

    # ---- core.sqlite ----
    conv_label = {}
    conv_chat = {}
    conv_state = {}
    for cid, label, chat, state in core.execute("select id, label, ext_chat_id, state from conversations"):
        conv_label[cid], conv_chat[cid], conv_state[cid] = label, chat, state
    mirror_keys = set()
    for (k,) in core.execute("select delivery_key from outbox where delivery_key like 'mirror:%'"):
        # mirror:<thread>:<record>:<part>
        k2 = k[len("mirror:"):]
        th, _, rest = k2.partition(":")
        rec = rest.rsplit(":", 1)[0]
        mirror_keys.add((th, rec))
    sources = list(core.execute(
        "select id, path, thread_id, conv_id, state, offset, line, size from transcript_sources where conv_id is not null"))

    out_rows = []
    totals = collections.Counter()
    detail_f = open(a.details, "w") if a.details else None
    for (src_id, path, thread, conv, state, R, Rline, size) in sources:
        label = conv_label.get(conv, "?")
        chat = conv_chat.get(conv) or ""
        row = dict(conv=label, path=path, state=state, conv_state=conv_state.get(conv), R=R)
        if not os.path.exists(path):
            row["verdict"] = "missing-file"
            out_rows.append(row)
            totals["source missing-file"] += 1
            continue
        recs, ends, eof = scan_rollout(path)
        # old delivery end D over every session that delivered from this path
        dl = []
        ids = set()
        sess_for_path = [s for (s, p) in deliv if p == path]
        for s in sess_for_path:
            dl.extend(deliv[(s, path)])
            ids |= deliv_ids[(s, path)]
        D = 0
        D_invalid = 0
        for (o, ln, st, rid, kind) in dl:
            if o in ends:
                D = max(D, o)
            else:
                D_invalid += 1
        # record ids also prove records (rewritten files)
        for r in recs:
            if r["id"] and r["id"] in ids:
                D = max(D, r["end"])
        C = 0
        for (s, p), v in ckpt.items():
            if p == path:
                C = max(C, v)
        row.update(D=D, D_invalid=D_invalid, C=C, size=os.path.getsize(path), sessions=sess_for_path)
        teams_ids = set(all_teams_turns)
        blob = chat_blob.get(chat, "")
        inb = chat_in.get(chat, [])

        def posted(r):
            # the same answer text was delivered earlier in the turn (an agent_message
            # or unphased assistant record at or before the old delivery end)
            if any(e <= D for e in r.get("alt_ends", [])):
                return "delivered-as-earlier-record"
            n = norm(r["text"])
            if len(n) < 12:
                return "short"
            pre = n[:80]
            # repeated texts: the k-th record with a text is posted only when the
            # chat holds more than k-1 copies of it
            k = seen_pre[pre]
            seen_pre[pre] += 1
            if blob.count(pre) > k:
                return "outbox"
            if r["kind"] == "prompt":
                hits = sum(1 for t in inb if len(t) >= 12 and t[:80] in n)
                if hits > k:
                    return "inbound"
            return ""

        wrapped_turns = {r["turn"] for r in recs if r.get("wrapped") and r["turn"]}
        seen_pre = collections.Counter()
        cls = collections.Counter()
        examples = collections.defaultdict(list)
        for r in recs:
            teams = r["turn"] in teams_ids or r["turn"] in wrapped_turns or r.get("wrapped")
            keyed = (thread, r["id"]) in mirror_keys if r["id"] else False
            old = r["ts"] and r["ts"] < cut_ms
            p = posted(r)
            # position classes
            if r["start"] < min(D, R):
                pos = "before-both"
            elif r["start"] < R and r["start"] >= D:
                pos = "skipped-by-R"  # after old delivery end, before resume: new core never reads it
            elif r["start"] < D and r["start"] >= R:
                pos = "reread"  # before old delivery end, after resume: new core reads it again
            else:
                pos = "after-both"
            if pos == "before-both":
                continue
            if pos == "skipped-by-R":
                if old:
                    c = "skip:older-than-120d"
                elif p in ("outbox", "inbound", "delivered-as-earlier-record"):
                    c = "skip:posted-by-old"
                elif teams:
                    c = "skip:teams-turn"
                elif p == "short":
                    c = "skip:short-unverifiable"
                else:
                    c = "LOSS:never-posted"
            else:
                # the new core reads it: it posts it unless deduped, a Teams turn, or too old
                if keyed:
                    c = "read:deduped-by-mirror-key"
                elif old:
                    c = "read:older-than-120d"
                elif teams:
                    c = "read:teams-turn(skipped)"
                elif p in ("outbox", "inbound", "delivered-as-earlier-record"):
                    c = "DUP:posted-by-old-and-will-repost"
                elif p == "short":
                    c = "read:short-unverifiable"
                elif r["start"] < C:
                    c = "post:gap-never-posted-by-old"
                else:
                    c = "post:new-after-old-checkpoint"
            cls[c] += 1
            totals[c] += 1
            if len(examples[c]) < 3:
                examples[c].append(dict(line=r["line"], start=r["start"], kind=r["kind"], id=r["id"],
                                        turn=r["turn"], text=r["text"][:120]))
            if detail_f:
                detail_f.write(json.dumps(dict(conv=label, path=os.path.basename(path), cls=c, line=r["line"],
                                               start=r["start"], end=r["end"], kind=r["kind"], id=r["id"], turn=r["turn"],
                                               ts=r["ts"], text=r["text"][:200]), ensure_ascii=False) + "\n")
        row["classes"] = dict(cls)
        row["examples"] = examples
        if R == D:
            row["pos"] = "R==D"
        elif R > D:
            row["pos"] = "R>D"
        else:
            row["pos"] = "R<D"
        totals["sources " + state + " " + row["pos"]] += 1
        out_rows.append(row)
    summary = dict(now=a.now, sources=len(out_rows), totals=dict(totals), rows=out_rows)
    with open(a.out, "w") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1, default=list)
    for k, v in sorted(totals.items()):
        print("%7d  %s" % (v, k))


if __name__ == "__main__":
    main()
