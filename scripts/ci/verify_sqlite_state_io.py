#!/usr/bin/env python3
"""Count actual native config I/O between the opt-in probe's phase markers."""
from collections import Counter, defaultdict
from pathlib import Path
import re
import sys


def verify(path):
    phases = defaultdict(Counter)
    phase = None
    pending = {}
    for line in Path(path).read_text().splitlines():
        begin = re.search(r"CXP_PERF_BEGIN ([a-z-]+) rows=(\d+)", line)
        if begin:
            phase = begin[1]
            phases[phase]
        if "CXP_PERF_END " in line:
            phase = None
        # strace can split an operation across another thread's syscall.
        pid = line.split(maxsplit=1)[0]
        if "<unfinished ...>" in line:
            pending[pid] = line.split("<unfinished ...>")[0]
            continue
        if "resumed>" in line:
            line = pending.pop(pid, "") + line.split("resumed>", 1)[1]
        if phase is None or not re.search(r"config\.sqlite(?:[>\"-])", line):
            continue
        call = re.search(r"\b(\w+)\(.*= (\d+)", line)
        if not call:
            continue
        operation, returned = call[1], int(call[2])
        phases[phase][operation + "_calls"] += 1
        if operation in ("pwrite64", "write", "writev", "pread64", "read"):
            phases[phase][operation + "_bytes"] += returned
    required = {"migration", "read-only", "unchanged", "heartbeat", "settings", "cold-open"}
    if phases.keys() != required:
        raise AssertionError(f"missing measured phases: {phases.keys()}")
    writes = ("pwrite64", "write", "writev", "fdatasync", "fsync", "ftruncate", "rename", "renameat", "unlink", "unlinkat", "chmod", "fchmodat")
    for name in ("read-only", "unchanged", "cold-open"):
        assert not any(phases[name][op + "_calls"] for op in writes), (name, phases[name])
    # 100 single-object changes; this remains independent of 100k other rows.
    # Include rollback journal headers/pages and leave room for page splits.
    # A heartbeat must not also dirty the two unchanged listing indexes.
    for name in ("heartbeat", "settings"):
        changes = phases[name]
        assert changes["pwrite64_bytes"] > 0, f"{name} did not reach storage"
        assert changes["pwrite64_bytes"] < 2 * 1024 * 1024, changes
        assert changes["fsync_calls"] + changes["fdatasync_calls"] <= 400, changes
    for name, counts in phases.items():
        print(name, " ".join(f"{key}={value}" for key, value in sorted(counts.items())))


if __name__ == "__main__":
    verify(sys.argv[1])
