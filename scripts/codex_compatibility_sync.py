#!/usr/bin/env python3
"""Update docs/codex_compatibility.md from monitor test artifacts."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

PLATFORMS = ["linux", "mac", "windows", "rockylinux8", "ubuntu20.04"]
HEADER = [
    "| Codex version | linux | mac | windows | rockylinux8 | ubuntu20.04 | last_tested_utc |",
    "| --- | --- | --- | --- | --- | --- | --- |",
]


def normalize_version(v: str) -> str:
    return v.strip().lstrip("v")


def parse_table(table_path: Path) -> Dict[str, Dict[str, str]]:
    rows: Dict[str, Dict[str, str]] = {}
    if not table_path.exists():
        return rows

    lines = table_path.read_text(encoding="utf-8").splitlines()
    columns = []
    for line in lines:
        if not line.startswith("|"):
            continue
        parts = [part.strip() for part in line.strip().strip("|").split("|")]
        if len(parts) < 2:
            continue
        if not columns:
            columns = parts
            continue
        if all(re.fullmatch(r"-+", part or "") for part in parts):
            continue
        row = {columns[i]: parts[i] for i in range(min(len(columns), len(parts)))}
        version = normalize_version(row.get("Codex version", ""))
        if not re.fullmatch(r"\d+(?:\.\d+)+", version):
            continue
        parsed = {platform: row.get(platform, "").strip().lower() or "not-run" for platform in PLATFORMS}
        parsed["last_tested_utc"] = row.get("last_tested_utc", "").strip()
        rows[version] = parsed
    return rows


def load_results(results_dir: Path) -> Dict[str, Dict[str, str]]:
    updates: Dict[str, Dict[str, str]] = {}
    if not results_dir.exists():
        return updates

    for path in sorted(results_dir.rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue

        platform = str(payload.get("platform", "")).strip()
        if platform not in PLATFORMS:
            continue
        results = payload.get("results", {})
        if not isinstance(results, dict):
            continue
        for version, status in results.items():
            normalized = normalize_version(str(version))
            if not re.fullmatch(r"\d+(?:\.\d+)+", normalized):
                continue
            normalized_status = str(status).strip().lower()
            if normalized_status not in {"pass", "fail", "not-run"}:
                normalized_status = "fail"
            updates.setdefault(normalized, {})[platform] = normalized_status
    return updates


def version_sort_key(version: str):
    return tuple(int(part) for part in version.split("."))


def write_table(table_path: Path, rows: Dict[str, Dict[str, str]]) -> None:
    table_path.parent.mkdir(parents=True, exist_ok=True)
    out = [
        "# Codex Compatibility",
        "",
        "Auto-updated by `.github/workflows/codex-release-monitor.yml`.",
        "",
        *HEADER,
    ]

    for version in sorted(rows.keys(), key=version_sort_key, reverse=True):
        row = rows[version]
        line = (
            f"| {version} | {row.get('linux', 'not-run')} | {row.get('mac', 'not-run')} | "
            f"{row.get('windows', 'not-run')} | {row.get('rockylinux8', 'not-run')} | "
            f"{row.get('ubuntu20.04', 'not-run')} | {row.get('last_tested_utc', '')} |"
        )
        out.append(line)

    table_path.write_text("\n".join(out) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True, help="Codex version to update")
    parser.add_argument("--results-dir", required=True, help="Directory containing *.json results")
    parser.add_argument(
        "--table-path",
        default="docs/codex_compatibility.md",
        help="Compatibility table path",
    )
    args = parser.parse_args()

    version = normalize_version(args.version)
    if not re.fullmatch(r"\d+(?:\.\d+)+", version):
        raise SystemExit(f"Invalid version: {args.version}")

    table_path = Path(args.table_path)
    rows = parse_table(table_path)
    updates = load_results(Path(args.results_dir))

    base = rows.get(version, {})
    merged = {platform: base.get(platform, "not-run") for platform in PLATFORMS}
    merged.update(updates.get(version, {}))
    merged["last_tested_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows[version] = merged

    write_table(table_path, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
