#!/usr/bin/env python3
"""Compile a pinned historical caller, changing only its test HTTP transport.

No production contract code is copied from HEAD or patched in the old source.
The published-artifact release gate additionally runs the unmodified old binary.
"""
import argparse
import io
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import urllib.request

COMMIT = "27a1c281395d7fdf86217fa5423ee4ece1a73a60"  # v0.1.22-rc.96
NATIVE_COMMIT = "b1cd1dd5b82fc333f555dc0544b7a5bb75e2f56e"  # v0.1.22-rc.104

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scratch", required=True)
    parser.add_argument("--baseline", choices=("legacy", "native"), default="legacy")
    parser.add_argument("--archive", default="")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    scratch = Path(args.scratch).resolve()
    commit, label = (COMMIT, "rc96") if args.baseline == "legacy" else (NATIVE_COMMIT, "rc104")
    archive_path = args.archive or os.environ.get("CXP_LEGACY_ARCHIVE" if args.baseline == "legacy" else "CXP_NATIVE_ARCHIVE", "")
    source = scratch / (label + "-source")
    source.mkdir(parents=True)
    # Prefer the exact local object (offline development), otherwise GitHub's
    # immutable commit archive. Release tags can never silently change it.
    archived = subprocess.run(["git", "archive", commit], cwd=repo, capture_output=True) if not archive_path else None
    if archive_path:
        data, strip = Path(archive_path).read_bytes(), False
        identified = subprocess.run(["git", "get-tar-commit-id"], input=data, capture_output=True, check=True)
        if identified.stdout.decode().strip() != commit:
            raise RuntimeError("legacy archive does not identify the pinned commit")
    elif archived.returncode == 0:
        data, strip = archived.stdout, False
    else:
        with urllib.request.urlopen(f"https://api.github.com/repos/baaaaaaaka/codex-helper/tarball/{commit}", timeout=120) as response:
            data, strip = response.read(), True
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        for item in archive:
            if strip:
                item.name = "/".join(item.name.split("/")[1:])
            if item.name:
                # The pinned tree contains regular files and directories only.
                # Keep this usable in the Docker harness's Python 3.11 too.
                destination = (source / item.name).resolve()
                if not destination.is_relative_to(source) or not (item.isfile() or item.isdir()):
                    raise RuntimeError("unsafe historical archive member")
                archive.extract(item, source)
    shutil.copyfile(repo / "internal/candidateupdate/testdata/legacy_release_transport.go", source / "cmd/codex-proxy/upgrade_fixture.go")
    binary = scratch / (label + (".exe" if os.name == "nt" else ""))
    version = "v0.1.22-rc." + label[2:]
    subprocess.run(["go", "build", "-trimpath", "-ldflags", "-X github.com/baaaaaaaka/codex-helper/internal/cli.version=" + version, "-o", str(binary), "./cmd/codex-proxy"], cwd=source, check=True)
    print(binary, flush=True)

if __name__ == "__main__":
    main()
