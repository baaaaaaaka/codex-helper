#!/usr/bin/env python3
"""Plan GitHub issues for failing Codex release monitor runs."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

PLATFORMS = ["linux", "mac", "windows", "rockylinux8", "ubuntu20.04"]
VERSION_RE = re.compile(r"\d+(?:\.\d+)+(?:-[0-9A-Za-z.-]+)?")


def normalize_version(value: str) -> str:
    return value.strip().lstrip("v")


def load_results(results_dir: Path) -> dict[str, dict[str, str]]:
    results: dict[str, dict[str, str]] = {}
    if not results_dir.exists():
        return results

    for path in sorted(results_dir.rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        platform = str(payload.get("platform", "")).strip()
        if platform not in PLATFORMS:
            continue
        platform_results = payload.get("results", {})
        if not isinstance(platform_results, dict):
            continue
        for version, status in platform_results.items():
            normalized_version = normalize_version(str(version))
            if not VERSION_RE.fullmatch(normalized_version):
                continue
            normalized_status = str(status).strip().lower()
            if normalized_status not in {"pass", "fail", "not-run"}:
                normalized_status = "fail"
            results.setdefault(normalized_version, {})[platform] = normalized_status
    return results


def load_open_issues(path: Path | None) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    issues: list[dict[str, str]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        issues.append(
            {
                "title": str(item.get("title", "")).strip(),
                "body": str(item.get("body", "")).strip(),
            }
        )
    return issues


def issue_marker(version: str) -> str:
    return f"<!-- codex-monitor version:{version} -->"


def issue_title(version: str) -> str:
    return f"Codex monitor detected compatibility failures for v{version}"


def build_issue_body(
    *,
    version: str,
    statuses: dict[str, str],
    run_url: str,
    repository: str,
    event_name: str,
    sha: str,
) -> str:
    lines = [
        issue_marker(version),
        f"Automated Codex release monitor detected non-pass results for `@openai/codex@{version}`.",
        "",
        "Platform results:",
    ]
    for platform in PLATFORMS:
        lines.append(f"- `{platform}`: `{statuses.get(platform, 'not-run')}`")
    if run_url:
        lines.extend(["", f"Run: {run_url}"])
    if repository:
        lines.append(f"Repository: `{repository}`")
    if event_name:
        lines.append(f"Trigger: `{event_name}`")
    if sha:
        lines.append(f"Commit: `{sha}`")
    lines.extend(
        [
            "",
            "This issue was opened automatically by `.github/workflows/codex-release-monitor.yml`.",
        ]
    )
    return "\n".join(lines)


def has_open_issue(version: str, open_issues: list[dict[str, str]]) -> bool:
    title = issue_title(version)
    marker = issue_marker(version)
    for issue in open_issues:
        if issue.get("title") == title:
            return True
        if marker and marker in issue.get("body", ""):
            return True
    return False


def plan_issues(
    *,
    versions: list[str],
    results: dict[str, dict[str, str]],
    open_issues: list[dict[str, str]],
    run_url: str,
    repository: str,
    event_name: str,
    sha: str,
) -> list[dict[str, str]]:
    planned: list[dict[str, str]] = []
    for raw_version in versions:
        version = normalize_version(raw_version)
        if not VERSION_RE.fullmatch(version):
            continue
        statuses = {platform: "not-run" for platform in PLATFORMS}
        statuses.update(results.get(version, {}))
        if all(status == "pass" for status in statuses.values()):
            continue
        if has_open_issue(version, open_issues):
            continue
        planned.append(
            {
                "version": version,
                "title": issue_title(version),
                "body": build_issue_body(
                    version=version,
                    statuses=statuses,
                    run_url=run_url,
                    repository=repository,
                    event_name=event_name,
                    sha=sha,
                ),
            }
        )
    return planned


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--versions-json", required=True)
    parser.add_argument("--open-issues-path", default="")
    parser.add_argument("--run-url", default="")
    parser.add_argument("--repository", default="")
    parser.add_argument("--event-name", default="")
    parser.add_argument("--sha", default="")
    args = parser.parse_args()

    versions = json.loads(args.versions_json)
    if not isinstance(versions, list):
        raise SystemExit("--versions-json must decode to a JSON array")

    issues = plan_issues(
        versions=[str(version) for version in versions],
        results=load_results(Path(args.results_dir)),
        open_issues=load_open_issues(Path(args.open_issues_path)) if args.open_issues_path else [],
        run_url=args.run_url,
        repository=args.repository,
        event_name=args.event_name,
        sha=args.sha,
    )
    print(json.dumps(issues))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
