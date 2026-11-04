#!/usr/bin/env python3
"""Seed for the real-data shadow run (internal/e2e/scenarios/entry/shadow_test.go).

Every Teams message the old helper saw or sent, per chat, with its real id
and time, so the fake Graph serves the chats' history as Teams would:

  * inbound_events (all statuses): what the old helper read from the chats
    (the owner's messages and the echoes of its own), as plain-text HTML;
  * sent outbox rows: the old helper's own messages, rendered as rc.59 sent
    them (--rc59-outbox, produced by the golden-diff harness). A sent row
    replaces the plain-text echo of the same message id.

Output: gzip JSONL {"chat","id","at" (ms),"html"} sorted by chat, time, id.
The old store is opened read-only (immutable=1).

Usage: shadow_prep.py --old store.sqlite --rc59-outbox rc59-outbox.jsonl --out seed.jsonl.gz
"""
import argparse
import collections
import gzip
import html
import json
import sqlite3


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--rc59-outbox", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    db = sqlite3.connect("file:%s?immutable=1" % a.old, uri=True)
    rc = {}
    with open(a.rc59_outbox, encoding="utf-8") as f:
        for line in f:
            d = json.loads(line)
            rc[d["id"]] = d.get("html") or ""
    msgs = {}  # (chat, id) -> (at, html, origin)
    stats = collections.Counter()
    for chat, mid, j in db.execute("select teams_chat_id, teams_message_id, json from inbound_events"):
        if not chat or not mid or not mid.isdigit():
            stats["inbound-skipped-no-id"] += 1
            continue
        try:
            text = json.loads(j).get("text") or ""
        except ValueError:
            text = ""
        body = "".join("<p>%s</p>" % html.escape(p) for p in text.split("\n\n")) or "<p></p>"
        msgs[(chat, mid)] = (int(mid), body, "inbound")
        stats["inbound"] += 1
    for oid, chat, mid in db.execute("select id, teams_chat_id, teams_message_id from outbox_messages where status='sent'"):
        if not chat or not mid or not mid.isdigit():
            stats["sent-skipped-no-id"] += 1
            continue
        h = rc.get(oid)
        if h is None:
            stats["sent-no-render"] += 1
            continue
        if (chat, mid) in msgs:
            stats["sent-replaces-echo"] += 1
        msgs[(chat, mid)] = (int(mid), h, "sent")
        stats["sent"] += 1
    rows = sorted(((c, at, mid, h) for (c, mid), (at, h, _) in msgs.items()), key=lambda r: (r[0], r[1], r[2]))
    with gzip.open(a.out, "wt", encoding="utf-8") as f:
        for c, at, mid, h in rows:
            f.write(json.dumps({"chat": c, "id": mid, "at": at, "html": h}, ensure_ascii=False) + "\n")
    stats["messages"] = len(rows)
    stats["chats"] = len({r[0] for r in rows})
    print(json.dumps(stats, indent=1))


if __name__ == "__main__":
    main()
