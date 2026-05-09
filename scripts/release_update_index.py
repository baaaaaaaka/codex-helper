#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PRIORITY_BODY_RE = re.compile(r"<!--\s*codex-helper-release:\s*(\{.*?\})\s*-->", re.DOTALL)
PRIORITY_ASSET_RE = re.compile(r"^codex-helper-auto-update-(p[012])(?:\.(?:txt|json))?$")
VALID_PRIORITIES = {"p0", "p1", "p2"}


def normalize_priority(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    if value in VALID_PRIORITIES:
        return value
    return None


def priority_from_body(body: Any) -> str | None:
    if not isinstance(body, str):
        return None
    matches = PRIORITY_BODY_RE.findall(body)
    if len(matches) != 1:
        return None
    try:
        payload = json.loads(matches[0])
    except json.JSONDecodeError:
        return None
    return normalize_priority(payload.get("auto_update_priority") or payload.get("priority"))


def priority_from_assets(assets: list[dict[str, Any]]) -> str | None:
    values: list[str] = []
    for asset in assets:
        name = str(asset.get("name") or "").strip()
        match = PRIORITY_ASSET_RE.match(name)
        if match:
            values.append(match.group(1))
    if len(values) != 1:
        return None
    return values[0]


def release_priority(release: dict[str, Any], assets: list[dict[str, Any]]) -> str:
    values = [
        value
        for value in (
            normalize_priority(release.get("priority")),
            priority_from_assets(assets),
            priority_from_body(release.get("body")),
        )
        if value is not None
    ]
    if not values:
        return "p2"
    if any(value != values[0] for value in values[1:]):
        return "p2"
    return values[0]


def release_assets(release: dict[str, Any]) -> list[dict[str, str]]:
    raw_assets = release.get("assets") or []
    assets: list[dict[str, str]] = []
    for asset in raw_assets:
        if isinstance(asset, str):
            name = asset.strip()
        elif isinstance(asset, dict):
            name = str(asset.get("name") or "").strip()
        else:
            name = ""
        if name:
            assets.append({"name": name})
    assets.sort(key=lambda item: item["name"])
    return assets


def release_tag(release: dict[str, Any]) -> str:
    return str(release.get("tagName") or release.get("tag_name") or "").strip()


def release_name(release: dict[str, Any]) -> str:
    return str(release.get("name") or "").strip()


def release_published_at(release: dict[str, Any]) -> str:
    value = str(release.get("publishedAt") or release.get("published_at") or "").strip()
    if value:
        return value
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def release_prerelease(release: dict[str, Any]) -> bool:
    return bool(release.get("isPrerelease") if "isPrerelease" in release else release.get("prerelease"))


def release_draft(release: dict[str, Any]) -> bool:
    return bool(release.get("isDraft") if "isDraft" in release else release.get("draft"))


def index_entry(repo: str, release: dict[str, Any]) -> dict[str, Any]:
    tag = release_tag(release)
    if not tag:
        raise ValueError("release JSON is missing tagName/tag_name")
    assets = release_assets(release)
    entry: dict[str, Any] = {
        "tag_name": tag,
        "version": tag.removeprefix("v"),
        "priority": release_priority(release, assets),
        "prerelease": release_prerelease(release),
        "draft": release_draft(release),
        "published_at": release_published_at(release),
        "assets": assets,
    }
    name = release_name(release)
    if name:
        entry["name"] = name
    return entry


def comparable_version(tag: str) -> tuple[tuple[int, ...], int, tuple[Any, ...]]:
    raw = tag.strip().removeprefix("v")
    base, sep, suffix = raw.partition("-")
    nums: list[int] = []
    for part in base.split("."):
        if not part.isdigit():
            return ((-1,), (), 0)
        nums.append(int(part))
    prerelease: list[Any] = []
    if sep:
        for part in re.split(r"[.-]", suffix.split("+", 1)[0]):
            if part.isdigit():
                prerelease.append((0, int(part)))
            else:
                prerelease.append((1, part))
    stable_rank = 1 if not sep else 0
    return (tuple(nums), stable_rank, tuple(prerelease))


def sort_releases(releases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        releases,
        key=lambda rel: (
            comparable_version(str(rel.get("tag_name") or "")),
            str(rel.get("published_at") or ""),
        ),
        reverse=True,
    )


def load_existing(path: Path | None, repo: str) -> dict[str, Any]:
    if path is None or not path.exists() or path.stat().st_size == 0:
        return {"version": 1, "repo": repo, "releases": []}
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if data.get("version") != 1:
        raise ValueError(f"unsupported existing update index version {data.get('version')!r}")
    if data.get("repo") and str(data["repo"]).lower() != repo.lower():
        raise ValueError(f"existing update index repo mismatch: {data['repo']}")
    data["repo"] = repo
    data.setdefault("releases", [])
    return data


def update_index(repo: str, release: dict[str, Any], existing: dict[str, Any], generated_at: str, max_releases: int) -> dict[str, Any]:
    current = index_entry(repo, release)
    releases = [current]
    for rel in existing.get("releases") or []:
        if not isinstance(rel, dict):
            continue
        if str(rel.get("tag_name") or "").strip() == current["tag_name"]:
            continue
        releases.append(rel)
    releases = sort_releases(releases)[:max_releases]
    return {
        "version": 1,
        "repo": repo,
        "generated_at": generated_at,
        "releases": releases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Update the static codex-helper release index.")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--release-json", required=True, type=Path)
    parser.add_argument("--existing-index", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--generated-at", default="")
    parser.add_argument("--max-releases", type=int, default=100)
    args = parser.parse_args()

    generated_at = args.generated_at.strip() or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    with args.release_json.open("r", encoding="utf-8") as fh:
        release = json.load(fh)
    existing = load_existing(args.existing_index, args.repo)
    out = update_index(args.repo, release, existing, generated_at, args.max_releases)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, sort_keys=True)
        fh.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
