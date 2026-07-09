#!/usr/bin/env python3
"""Validate the phase-marked strace emitted by the preview-cache I/O probe."""

from __future__ import annotations

import re
import sys
from pathlib import Path


CACHE_NAME = "preview.sqlite3"
PHASES = ("exact-hit", "close", "reopen", "batched-small", "threshold-append")
MARKER_RE = re.compile(r"CXP_PREVIEW_IO_PHASE ([a-z-]+)")
CALL_RE = re.compile(
    r"\b(pwrite64|write|writev|fdatasync|fsync|ftruncate|unlink|unlinkat|"
    r"rename|renameat|chmod|fchmodat|msync)\("
)
RESULT_RE = re.compile(r"= (\d+)$")


def fail(message: str, lines: list[str] | None = None) -> None:
    print(f"preview-cache I/O validation failed: {message}", file=sys.stderr)
    for line in lines or []:
        print(f"  {line}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: verify_session_preview_sqlite_io.py TRACE")
    trace = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace").splitlines()
    observed: set[str] = set()
    writes: dict[str, list[tuple[str, str]]] = {phase: [] for phase in PHASES}
    current: str | None = None

    for line in trace:
        marker = MARKER_RE.search(line)
        if marker:
            name = marker.group(1)
            if name.endswith("-begin"):
                current = name[: -len("-begin")]
                if current not in writes:
                    fail(f"unknown begin marker {name!r}")
                observed.add(current)
            elif name.endswith("-end"):
                phase = name[: -len("-end")]
                if current != phase:
                    fail(f"unbalanced end marker {name!r}; active phase is {current!r}")
                current = None
            continue
        if current is None or CACHE_NAME not in line:
            continue
        call = CALL_RE.search(line)
        if call:
            writes[current].append((call.group(1), line))

    missing = set(PHASES) - observed
    if missing:
        fail(f"missing probe phases: {sorted(missing)}")
    for phase in ("exact-hit", "close", "reopen", "batched-small"):
        if writes[phase]:
            fail(f"{phase} unexpectedly wrote cache files", [line for _, line in writes[phase]])

    threshold = writes["threshold-append"]
    if not threshold:
        fail("threshold append did not write its required rollback transaction")
    unexpected_threshold = [
        line
        for _, line in threshold
        if CACHE_NAME not in line
        or f"{CACHE_NAME}-wal" in line
        or f"{CACHE_NAME}-shm" in line
    ]
    if unexpected_threshold:
        fail("threshold append wrote outside the main database and rollback journal", unexpected_threshold)
    if not any(f"{CACHE_NAME}-journal" in line for _, line in threshold):
        fail("threshold append did not use a rollback journal", [line for _, line in threshold])
    if not any(f"{CACHE_NAME}-journal" not in line and call in {"pwrite64", "write", "writev"} for call, line in threshold):
        fail("threshold append did not update the main database", [line for _, line in threshold])
    if not any(call in {"fsync", "fdatasync"} for call, _ in threshold):
        fail("threshold append did not durably complete its rollback transaction")

    written_bytes = 0
    for call, line in threshold:
        if call not in {"pwrite64", "write", "writev"}:
            continue
        result = RESULT_RE.search(line)
        if result:
            written_bytes += int(result.group(1))
    if not 64 * 1024 <= written_bytes <= 512 * 1024:
        fail(f"64 KiB append wrote an unexpected {written_bytes} cache bytes", [line for _, line in threshold])

    print(
        "preview-cache I/O validation passed: "
        f"zero cache writes for exact-hit/close/reopen/batched-small; "
        f"threshold rollback bytes={written_bytes}"
    )


if __name__ == "__main__":
    main()
