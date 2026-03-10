#!/usr/bin/env python3
"""Select @openai/codex versions for release monitor smoke tests."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from codex_version_sweep import normalize_version, select_versions as sweep_select_versions, version_sort_key

PLATFORMS = ["linux", "mac", "windows", "rockylinux8", "ubuntu20.04"]
VERSION_RE = re.compile(r"\d+(?:\.\d+)+(?:-[0-9A-Za-z.-]+)?")


def parse_timestamp(value: str):
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_table(table_path: Path) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    if not table_path.exists():
        return rows

    columns: list[str] = []
    for line in table_path.read_text(encoding="utf-8").splitlines():
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
        if not VERSION_RE.fullmatch(version):
            continue
        rows[version] = row
    return rows


def select_targeted_versions(
    *,
    table_rows: dict[str, dict[str, str]],
    latest_version: str,
    alpha_version: str = "",
    platforms: list[str] | None = None,
    recent_stable_window: int = 3,
    revalidate_after: timedelta = timedelta(hours=72),
    now: datetime | None = None,
) -> list[str]:
    latest = normalize_version(latest_version)
    alpha = normalize_version(alpha_version) if alpha_version else ""
    now = now or datetime.now(timezone.utc)
    platforms = platforms or PLATFORMS

    candidate_versions: list[str] = []

    def add_candidate(version: str) -> None:
        version = normalize_version(version)
        if not version or not VERSION_RE.fullmatch(version):
            return
        if version not in candidate_versions:
            candidate_versions.append(version)

    add_candidate(latest)
    if alpha and alpha != latest:
        add_candidate(alpha)

    stable_versions = sorted(
        {version for version in table_rows if "-" not in version} | {latest},
        key=version_sort_key,
        reverse=True,
    )

    force_revalidate: set[str] = set()
    for version in stable_versions[:recent_stable_window]:
        add_candidate(version)
        row = table_rows.get(version)
        if row is None:
            force_revalidate.add(version)
            continue
        last_tested = parse_timestamp(row.get("last_tested_utc", ""))
        if last_tested is None or now - last_tested >= revalidate_after:
            force_revalidate.add(version)

    if alpha:
        row = table_rows.get(alpha)
        last_tested = parse_timestamp(row.get("last_tested_utc", "")) if row else None
        if row is None or last_tested is None or now - last_tested >= revalidate_after:
            force_revalidate.add(alpha)

    selected: list[str] = []
    for version in candidate_versions:
        row = table_rows.get(version)
        if row is None:
            selected.append(version)
            continue
        status_ok = all(row.get(platform, "").strip().lower() == "pass" for platform in platforms)
        if not status_ok or version in force_revalidate:
            selected.append(version)
    return selected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["targeted", "range", "versions"], default="targeted")
    parser.add_argument("--table-path", default="docs/codex_compatibility.md")
    parser.add_argument("--latest-version", default="", help="Latest stable Codex version")
    parser.add_argument("--alpha-version", default="", help="Latest prerelease Codex version")
    parser.add_argument("--versions", default="", help="Explicit versions (JSON array or comma/newline separated)")
    parser.add_argument("--min-version", default="", help="Inclusive min version for range mode")
    parser.add_argument("--max-version", default="", help="Inclusive max version for range mode")
    parser.add_argument(
        "--include-prerelease",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include prereleases in range or explicit selection",
    )
    parser.add_argument("--recent-stable-window", type=int, default=3)
    parser.add_argument("--revalidate-after-hours", type=int, default=72)
    parser.add_argument("--now", default="", help="Override current time (ISO-8601 UTC)")
    args = parser.parse_args()

    if args.mode == "targeted":
        if not args.latest_version:
            raise SystemExit("--latest-version is required in targeted mode")
        now = None
        if args.now:
            now = datetime.strptime(args.now, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        selected = select_targeted_versions(
            table_rows=parse_table(Path(args.table_path)),
            latest_version=args.latest_version,
            alpha_version=args.alpha_version,
            recent_stable_window=args.recent_stable_window,
            revalidate_after=timedelta(hours=args.revalidate_after_hours),
            now=now,
        )
    elif args.mode == "range":
        if not args.min_version:
            raise SystemExit("--min-version is required in range mode")
        selected = sweep_select_versions(
            versions_arg="",
            min_version=normalize_version(args.min_version),
            max_version=normalize_version(args.max_version) if args.max_version else None,
            include_prerelease=args.include_prerelease,
        )
    else:
        if not args.versions.strip():
            raise SystemExit("--versions is required in versions mode")
        selected = sweep_select_versions(
            versions_arg=args.versions,
            min_version=None,
            max_version=None,
            include_prerelease=args.include_prerelease,
        )

    print(json.dumps(selected))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
