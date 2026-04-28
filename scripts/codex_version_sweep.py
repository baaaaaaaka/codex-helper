#!/usr/bin/env python3
"""Enumerate and smoke-test @openai/codex versions over a semver range."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable

VERSION_RE = re.compile(r"^0\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
PLATFORM_SUFFIX_RE = re.compile(r"-(?:darwin|linux|win32)-(?:arm64|x64)$")

DEFAULT_CLOUDGATE_CMD = (
    "go test ./internal/cloudgate "
    "-run 'TestCodexPatchIntegration|TestCodexPatchedYoloLaunchIntegration' "
    "-count=1 -v"
)
DEFAULT_CLI_CMD = (
    "go test ./internal/cli "
    "-run 'TestProbeCodexIntegration|TestRunCodexNewSessionRealCodexYoloIntegration' "
    "-count=1 -v"
)


def version_sort_key(version: str):
    release, _, prerelease = version.partition("-")
    release_key = tuple(int(part) for part in release.split("."))
    if not prerelease:
        return release_key, 1, ()

    prerelease_key = []
    for part in prerelease.split("."):
        if part.isdigit():
            prerelease_key.append((0, int(part)))
        else:
            prerelease_key.append((1, part))
    return release_key, 0, tuple(prerelease_key)


def normalize_version(version: str) -> str:
    return version.strip().lstrip("v")


def parse_versions_arg(raw: str) -> list[str]:
    raw = raw.strip()
    if not raw:
        return []
    if raw.startswith("["):
        values = json.loads(raw)
        if not isinstance(values, list):
            raise ValueError("--versions must be a JSON list or comma/newline separated string")
        return [normalize_version(str(value)) for value in values if str(value).strip()]
    parts = re.split(r"[\s,]+", raw)
    return [normalize_version(part) for part in parts if part.strip()]


def fetch_all_versions() -> list[str]:
    payload = subprocess.check_output(["npm", "view", "@openai/codex", "versions", "--json"], text=True)
    raw_versions = json.loads(payload)
    versions = []
    for version in raw_versions:
        version = normalize_version(str(version))
        if PLATFORM_SUFFIX_RE.search(version):
            continue
        if VERSION_RE.fullmatch(version):
            versions.append(version)
    return sorted(set(versions), key=version_sort_key)


def within_range(version: str, min_version: str | None, max_version: str | None) -> bool:
    key = version_sort_key(version)
    if min_version and key < version_sort_key(min_version):
        return False
    if max_version and key > version_sort_key(max_version):
        return False
    return True


def select_versions(
    *,
    versions_arg: str,
    min_version: str | None,
    max_version: str | None,
    include_prerelease: bool,
) -> list[str]:
    if versions_arg:
        versions = parse_versions_arg(versions_arg)
    else:
        versions = fetch_all_versions()

    selected = []
    for version in versions:
        if not VERSION_RE.fullmatch(version):
            continue
        if not include_prerelease and "-" in version:
            continue
        if not within_range(version, min_version, max_version):
            continue
        selected.append(version)
    return sorted(set(selected), key=version_sort_key)


def run_command(cmd: str, *, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        shell=True,
        text=True,
        capture_output=True,
    )


def run_smoke_for_version(version: str, *, repo_root: Path) -> dict[str, object]:
    temp_root = Path(tempfile.mkdtemp(prefix="codex-sweep-"))
    prefix = temp_root / "prefix"
    started_at = time.time()
    summary: dict[str, object] = {
        "version": version,
        "status": "fail",
        "cloudgate_rc": None,
        "cli_rc": None,
        "duration_seconds": None,
    }
    try:
        install = subprocess.run(
            ["npm", "install", "-g", "--include=optional", f"--prefix={prefix}", f"@openai/codex@{version}"],
            cwd=str(repo_root),
            text=True,
            capture_output=True,
        )
        if install.returncode != 0:
            summary["install_rc"] = install.returncode
            summary["stderr_tail"] = (install.stderr or "")[-4000:]
            return summary

        env = os.environ.copy()
        env["PATH"] = f"{prefix / 'bin'}{os.pathsep}{env['PATH']}"
        env["CODEX_PATCH_TEST"] = "1"
        env["CODEX_YOLO_PATCH_TEST"] = "1"

        cloudgate = run_command(DEFAULT_CLOUDGATE_CMD, cwd=repo_root, env=env)
        summary["cloudgate_rc"] = cloudgate.returncode
        if cloudgate.returncode != 0:
            summary["stderr_tail"] = ((cloudgate.stdout or "") + (cloudgate.stderr or ""))[-4000:]
            return summary

        cli = run_command(DEFAULT_CLI_CMD, cwd=repo_root, env=env)
        summary["cli_rc"] = cli.returncode
        if cli.returncode != 0:
            summary["stderr_tail"] = ((cli.stdout or "") + (cli.stderr or ""))[-4000:]
            return summary

        summary["status"] = "pass"
        return summary
    finally:
        summary["duration_seconds"] = round(time.time() - started_at, 2)
        shutil.rmtree(temp_root, ignore_errors=True)


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def build_payload(versions: list[str], results: list[dict[str, object]]) -> dict[str, object]:
    return {
        "versions": versions,
        "results": results,
        "passed": sum(1 for result in results if result["status"] == "pass"),
        "failed": sum(1 for result in results if result["status"] != "pass"),
        "completed": len(results),
        "remaining": len(versions) - len(results),
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument("--versions", default="", help="Explicit versions (JSON array or comma/newline separated)")
    parser.add_argument("--min-version", default="", help="Inclusive min version")
    parser.add_argument("--max-version", default="", help="Inclusive max version")
    parser.add_argument(
        "--include-prerelease",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include prerelease versions in range selection",
    )
    parser.add_argument("--list-only", action="store_true", help="Only print selected versions as JSON")
    parser.add_argument("--output-json", default="", help="Write summary JSON to this path")
    args = parser.parse_args(list(argv) if argv is not None else None)

    repo_root = Path(args.repo_root).resolve()
    min_version = normalize_version(args.min_version) if args.min_version else None
    max_version = normalize_version(args.max_version) if args.max_version else None

    versions = select_versions(
        versions_arg=args.versions,
        min_version=min_version,
        max_version=max_version,
        include_prerelease=args.include_prerelease,
    )
    if args.list_only:
        print(json.dumps(versions))
        return 0

    results = []
    overall_ok = True
    for version in versions:
        result = run_smoke_for_version(version, repo_root=repo_root)
        results.append(result)
        print(f"{version}: {result['status']} (cloudgate={result['cloudgate_rc']} cli={result['cli_rc']})", flush=True)
        if args.output_json:
            write_json(Path(args.output_json), build_payload(versions, results))
        if result["status"] != "pass":
            overall_ok = False

    payload = build_payload(versions, results)
    if args.output_json:
        write_json(Path(args.output_json), payload)

    if overall_ok:
        return 0
    if args.output_json:
        print(f"Summary written to {args.output_json}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
