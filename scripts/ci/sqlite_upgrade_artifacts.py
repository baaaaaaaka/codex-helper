#!/usr/bin/env python3
"""Upgrade released binaries in an isolated CI profile, never the runner's install.

Protocol-one updaters must refuse the incompatible candidate before replacement.
The explicit installer then admits protocol two; legacy rollback is not rearmed.
All fixtures and download bytes live in one disposable directory.
"""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import subprocess
import tempfile
import urllib.request


def main():
    repo = os.environ["REPO"]
    old = os.environ["OLD_TAG"]
    target = os.environ["TARGET_TAG"]
    if old == target:
        raise RuntimeError("upgrade tags must differ")
    system = {"Linux": "linux", "Darwin": "darwin", "Windows": "windows"}[platform.system()]
    arch = {"x86_64": "amd64", "AMD64": "amd64", "arm64": "arm64", "aarch64": "arm64"}[platform.machine()]
    suffix = ".exe" if system == "windows" else ""
    with tempfile.TemporaryDirectory(prefix="cxp-upgrade-", dir=os.environ.get("RUNNER_TEMP")) as temporary:
        root = Path(temporary)
        home = root / "home"
        home.mkdir()
        env = {k: v for k, v in os.environ.items() if not k.startswith(("CXP_", "CODEX_PROXY_", "CODEX_HELPER_"))}
        env.update(HOME=str(home), USERPROFILE=str(home), APPDATA=str(home / "AppData" / "Roaming"),
                   LOCALAPPDATA=str(home / "AppData" / "Local"), XDG_CONFIG_HOME=str(home / "config"),
                   XDG_CACHE_HOME=str(home / "cache"), XDG_STATE_HOME=str(home / "state"),
                   CODEX_HOME=str(home / ".codex"), CODEX_PROXY_SKIP_BUILTIN_SKILLS="1", CXP_AUTO_UPDATE="0")

        def run(binary, *args, check=True, extra=None):
            result = subprocess.run([str(binary), *map(str, args)], env=env | (extra or {}), cwd=root,
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=240)
            print(result.stdout, end="", flush=True)
            if check and result.returncode:
                raise RuntimeError(f"{binary.name} {args} failed ({result.returncode})")
            return result

        def download(tag):
            name = f"codex-proxy_{tag.lstrip('v')}_{system}_{arch}{suffix}"
            base = f"https://github.com/{repo}/releases/download/{tag}/"
            binary = root / name
            with urllib.request.urlopen(base + name, timeout=180) as response, binary.open("wb") as output:
                shutil.copyfileobj(response, output)
            with urllib.request.urlopen(base + "checksums.txt", timeout=180) as response:
                checksums = response.read().decode().splitlines()
            expected = next(line.split()[0] for line in checksums if line.split()[-1].lstrip("*") == name)
            assert hashlib.sha256(binary.read_bytes()).hexdigest() == expected, "release checksum mismatch"
            binary.chmod(0o755)
            return binary

        previous, current = download(old), download(target)
        install = root / "install"
        install.mkdir()
        entry = install / ("cxp" + suffix)
        shutil.copy2(previous, entry)
        assert old.lstrip("v") in run(entry, "--version").stdout
        protocol = run(entry, "--cxp-boot-protocol", check=False).stdout.strip()
        if protocol == "2":
            run(entry, "--cxp-install-bootstrap", "--target", entry)
        before = hashlib.sha256(entry.read_bytes()).digest()
        automatic = run(entry, "upgrade", "--repo", repo, "--version", target, "--install-path", entry, check=False)
        if protocol != "2":
            assert automatic.returncode != 0, "legacy updater admitted incompatible SQLite release"
            assert "SQLite bootstrap" in automatic.stdout or "bootstrap" in automatic.stdout.lower(), "failure did not reach bootstrap admission"
            assert hashlib.sha256(entry.read_bytes()).digest() == before, "old updater changed entry before admission"
        else:
            assert automatic.returncode == 0, "compatible updater failed"

        # Stopped old writer: this is the explicit installer boundary, not an
        # automatic candidate activation or an implicit downgrade mechanism.
        shutil.copy2(current, entry)
        run(entry, "--cxp-install-bootstrap", "--target", entry)
        runtime = install / ".cxp-runtime"
        with sqlite3.connect(f"{(runtime / 'boot.sqlite').as_uri()}?mode=ro", uri=True) as db:
            record = db.execute("SELECT protocol,good,previous FROM boot").fetchone()
            assert record[0] == 2 and record[1].lstrip("v") == target.lstrip("v"), record
            if protocol != "2":
                assert not record[2], "protocol-one runtime became a rollback target"
        assert target.lstrip("v") in run(entry, "--version").stdout

        # Frozen state, independent of the new serializer. The real released
        # CLI imports it and a fresh invocation must read native state only.
        config = root / "legacy-config.json"
        raw = b'{"version":6,"minReader":1,"runtimeGeneration":1,"profiles":[{"id":"old-profile","name":"preserved-profile","sshArgs":["-o","BatchMode=yes"]}],"instances":[]}'
        config.write_bytes(raw)
        config.chmod(0o600)
        run(entry, "--config", config, "proxy", "prune")  # frozen fixture has no instances
        run(entry, "--config", config, "proxy", "list")
        native = config.with_suffix(".sqlite")
        assert native.is_file(), "real CLI did not create native config"
        assert Path(str(config) + ".migrated").read_bytes() == raw, "lost legacy evidence"
        with sqlite3.connect(f"{native.as_uri()}?mode=ro", uri=True) as db:
            assert db.execute("SELECT name FROM profiles WHERE id='old-profile'").fetchone() == ("preserved-profile",)
        config.write_bytes(b"poisoned legacy source")
        run(entry, "--config", config, "proxy", "list")
        print(f"SQLite artifact upgrade passed: {old} -> {target} ({system}/{arch})", flush=True)


if __name__ == "__main__":
    main()
