#!/usr/bin/env python3
"""Verify that the steady canonical resolver performs bounded read-only I/O."""

from __future__ import annotations

import pathlib
import re
import sys


MUTATING = re.compile(
    r"\b(?:write|pwrite64|writev|fdatasync|fsync|ftruncate|unlinkat?|"
    r"renameat2?|mkdirat?|chmod|fchmodat|msync)\("
)
OPEN = re.compile(r"\bopenat2?\(")
DIRECTORY_SCAN = re.compile(r"\bgetdents64\(")
METADATA = re.compile(r"\b(?:newfstatat|statx)\(")


def fail(message: str, lines: list[str] | None = None) -> None:
    print(message, file=sys.stderr)
    if lines:
        for line in lines[:40]:
            print(f"  {line}", file=sys.stderr)
    raise SystemExit(1)


def trace_phases(lines: list[str], begin_marker: str, end_marker: str) -> list[list[str]]:
    phases: list[list[str]] = []
    cursor = 0
    while cursor < len(lines):
        begin = next(
            (i for i, line in enumerate(lines[cursor:], cursor) if begin_marker in line),
            -1,
        )
        if begin < 0:
            break
        end = next(
            (i for i, line in enumerate(lines[begin + 1 :], begin + 1) if end_marker in line),
            -1,
        )
        if end <= begin:
            fail(f"resolver trace is missing phase end marker {end_marker}")
        phases.append(lines[begin + 1 : end])
        cursor = end + 1
    if len(phases) != 2:
        fail(
            f"resolver trace contains {len(phases)} {begin_marker}/{end_marker} phases; "
            "want two independent process startups"
        )
    return phases


def verify_phase(
    phase: list[str],
    root: str,
    label: str,
    *,
    allow_legacy_metadata: bool,
    metadata_minimum: int,
    metadata_limit: int,
) -> int:
    rooted = [line for line in phase if root in line]
    legacy = [line for line in rooted if f"{root}/config/" in line]
    sqlite = [line for line in rooted if ".sqlite" in line or "-wal" in line or "-shm" in line]
    opened = [line for line in rooted if OPEN.search(line)]
    scanned = [line for line in rooted if DIRECTORY_SCAN.search(line)]
    mutated = [line for line in rooted if MUTATING.search(line)]
    metadata = [line for line in rooted if METADATA.search(line)]

    if legacy and not allow_legacy_metadata:
        fail(f"{label} accessed the legacy config tree", legacy)
    if sqlite:
        fail("steady canonical resolver opened or inspected SQLite sidecars", sqlite)
    if opened:
        fail("steady canonical resolver opened files instead of using path metadata only", opened)
    if scanned:
        fail("steady canonical resolver enumerated a directory", scanned)
    if mutated:
        fail("steady canonical resolver issued mutating filesystem syscalls", mutated)
    if len(metadata) > metadata_limit:
        fail(
            f"{label} used {len(metadata)} metadata syscalls; want at most {metadata_limit}",
            metadata,
        )
    if len(metadata) < metadata_minimum:
        fail(
            f"{label} used only {len(metadata)} rooted metadata syscalls; "
            f"want at least {metadata_minimum} so the probe covers the expected paths",
            metadata,
        )
    return len(metadata)


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: verify_teams_runtime_resolver_io.py TRACE ROOT_FILE")
    trace_path = pathlib.Path(sys.argv[1])
    root_file = pathlib.Path(sys.argv[2])
    root = root_file.read_text(encoding="utf-8").strip()
    if not root:
        fail("resolver probe root file is empty")

    lines = trace_path.read_text(encoding="utf-8", errors="replace").splitlines()
    resolver_metadata = sum(
        verify_phase(
            phase,
            root,
            f"canonical resolution process {index}",
            allow_legacy_metadata=True,
            metadata_minimum=2,
            metadata_limit=2,
        )
        for index, phase in enumerate(
            trace_phases(lines, "CXP_TEAMS_RESOLVER_IO_BEGIN", "CXP_TEAMS_RESOLVER_IO_END"),
            1,
        )
    )
    listener_metadata = sum(
        verify_phase(
            phase,
            root,
            f"listener preparation process {index}",
            allow_legacy_metadata=True,
            metadata_minimum=2,
            metadata_limit=3,
        )
        for index, phase in enumerate(
            trace_phases(
                lines,
                "CXP_TEAMS_LISTENER_PREP_IO_BEGIN",
                "CXP_TEAMS_LISTENER_PREP_IO_END",
            ),
            1,
        )
    )
    bridge_prestore_metadata = sum(
        verify_phase(
            phase,
            root,
            f"Bridge pre-store inspection process {index}",
            allow_legacy_metadata=True,
            metadata_minimum=2,
            metadata_limit=2,
        )
        for index, phase in enumerate(
            trace_phases(
                lines,
                "CXP_TEAMS_BRIDGE_PRESTORE_IO_BEGIN",
                "CXP_TEAMS_BRIDGE_PRESTORE_IO_END",
            ),
            1,
        )
    )

    print(
        "Teams resolver syscall budget passed: "
        f"resolver_metadata={resolver_metadata} listener_metadata={listener_metadata} "
        f"bridge_prestore_metadata={bridge_prestore_metadata} "
        "open=0 scan=0 sqlite=0 mutate=0"
    )


if __name__ == "__main__":
    main()
