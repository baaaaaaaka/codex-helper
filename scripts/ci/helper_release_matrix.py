#!/usr/bin/env python3
"""Build a reproducible matrix from formal releases and fixed rescue tags."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


STABLE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
SUPPORTED_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(?:-rc\.(\d+))?$")


def version_key(tag: str) -> tuple[int, int, int]:
    match = STABLE_TAG.fullmatch(tag.strip())
    if not match:
        raise ValueError(f"not a formal semantic version tag: {tag}")
    return tuple(int(value) for value in match.groups())


def semantic_tag_key(tag: str) -> tuple[int, int, int, int, int]:
    match = SUPPORTED_TAG.fullmatch(tag.strip())
    if not match:
        raise ValueError(f"not a supported semantic version tag: {tag}")
    major, minor, patch, rc = match.groups()
    return int(major), int(minor), int(patch), 1 if rc is None else 0, int(rc or 0)


def select_tags(
    releases: list[dict[str, object]], minimum: str, exclude: str, includes: list[str] | None = None
) -> list[str]:
    minimum_key = version_key(minimum)
    selected: set[str] = set()
    for release in releases:
        tag = str(release.get("tagName") or release.get("tag_name") or "").strip()
        if not STABLE_TAG.fullmatch(tag):
            continue
        if bool(release.get("isDraft") or release.get("draft")):
            continue
        if bool(release.get("isPrerelease") or release.get("prerelease")):
            continue
        if tag == exclude or version_key(tag) < minimum_key:
            continue
        selected.add(tag)
    inventory = {
        str(release.get("tagName") or release.get("tag_name") or "").strip(): release
        for release in releases
    }
    for tag in includes or []:
        tag = tag.strip()
        if not tag or tag == exclude:
            continue
        if not SUPPORTED_TAG.fullmatch(tag):
            raise ValueError(f"explicit migration tag is not supported: {tag}")
        release = inventory.get(tag)
        if release is None:
            raise ValueError(f"explicit migration tag is not published: {tag}")
        if bool(release.get("isDraft") or release.get("draft")):
            raise ValueError(f"explicit migration tag is still a draft: {tag}")
        selected.add(tag)
    return sorted(selected, key=semantic_tag_key)


def validate_matrix_size(tags: list[str], multiplier: int, maximum: int) -> None:
    jobs = len(tags) * multiplier
    if jobs > maximum:
        raise ValueError(
            f"helper migration matrix would create {jobs} jobs "
            f"({len(tags)} releases x {multiplier} targets), above GitHub's {maximum}-job limit; "
            "split the workflow by platform before adding another formal release"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--releases", required=True)
    parser.add_argument("--minimum", default="v0.0.38")
    parser.add_argument("--exclude", default="")
    parser.add_argument("--include", action="append", default=[])
    parser.add_argument("--matrix-multiplier", type=int, default=1)
    parser.add_argument("--max-jobs", type=int, default=256)
    args = parser.parse_args()

    releases = json.loads(Path(args.releases).read_text(encoding="utf-8"))
    if not isinstance(releases, list):
        raise SystemExit("release inventory must be a JSON array")
    try:
        tags = select_tags(releases, args.minimum, args.exclude, args.include)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not tags:
        raise SystemExit("formal release migration matrix is empty")
    try:
        validate_matrix_size(tags, args.matrix_multiplier, args.max_jobs)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(tags, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
