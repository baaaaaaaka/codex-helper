#!/usr/bin/env python3
"""Validate catalog SQLite writes from the phase-marked strace probe."""

from __future__ import annotations

import re
import sys
from pathlib import Path


CACHE_NAME = "catalog.sqlite3"
PHASES = ("exact-hit", "session-append", "history-append", "steady-state")
MARKER_RE = re.compile(r"CXP_CATALOG_IO_PHASE ([a-z-]+)")
CALL_RE = re.compile(
    r"\b(pwrite64|write|writev|fdatasync|fsync|ftruncate|unlink|unlinkat|"
    r"rename|renameat|chmod|fchmodat|msync)\("
)
RESULT_RE = re.compile(r"= (\d+)$")


def fail(message: str, lines: list[str] | None = None) -> None:
    print(f"catalog-cache I/O validation failed: {message}", file=sys.stderr)
    for line in lines or []:
        print(f"  {line}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: verify_codexhistory_catalog_sqlite_io.py TRACE")
    trace = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace").splitlines()
    writes: dict[str, list[tuple[str, str]]] = {phase: [] for phase in PHASES}
    observed: set[str] = set()
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
    for phase in ("exact-hit", "steady-state"):
        if writes[phase]:
            fail(f"{phase} unexpectedly wrote catalog files", [line for _, line in writes[phase]])

    summaries: list[str] = []
    for phase in ("session-append", "history-append"):
        phase_writes = writes[phase]
        if not phase_writes:
            fail(f"{phase} did not write its required transaction")
        if any(f"{CACHE_NAME}-wal" in line or f"{CACHE_NAME}-shm" in line for _, line in phase_writes):
            fail(f"{phase} created WAL/SHM activity", [line for _, line in phase_writes])
        if not any(f"{CACHE_NAME}-journal" in line for _, line in phase_writes):
            fail(f"{phase} did not use a rollback journal", [line for _, line in phase_writes])
        if not any(f"{CACHE_NAME}-journal" not in line and call in {"pwrite64", "write", "writev"} for call, line in phase_writes):
            fail(f"{phase} did not update the main database", [line for _, line in phase_writes])
        written_bytes = 0
        for call, line in phase_writes:
            if call not in {"pwrite64", "write", "writev"}:
                continue
            result = RESULT_RE.search(line)
            if result:
                written_bytes += int(result.group(1))
        if not 1 <= written_bytes <= 256 * 1024:
            fail(f"{phase} wrote an unexpected {written_bytes} bytes", [line for _, line in phase_writes])
        summaries.append(f"{phase}={written_bytes}")

    print(
        "catalog-cache I/O validation passed: exact/steady-state zero writes; "
        + ", ".join(summaries)
    )


if __name__ == "__main__":
    main()
