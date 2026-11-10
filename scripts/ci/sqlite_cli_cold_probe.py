#!/usr/bin/env python3
"""Measure fresh native-bootstrap CLI processes against a tiny synthetic profile.

Run inside the existing isolated source harness locally. Build time and the
initial migration are outside the cold-process measurements. Disk blocks are
kernel accounting, not a promise that warm page-cache reads reach a disk.
"""
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import tempfile
import time


def main():
    with tempfile.TemporaryDirectory(prefix="cxp-cold-", dir=os.environ.get("TMPDIR")) as temporary:
        root = Path(temporary)
        home = root / "home"
        home.mkdir()
        entry = root / "cxp"
        shutil.copy2(sys.argv[1], entry)
        entry.chmod(0o700)
        env = {k: v for k, v in os.environ.items() if not k.startswith(("CXP_", "CODEX_HELPER_", "CODEX_PROXY_"))}
        env.update(HOME=str(home), XDG_CONFIG_HOME=str(home / "config"),
                   XDG_CACHE_HOME=str(home / "cache"), XDG_STATE_HOME=str(home / "state"),
                   CODEX_HOME=str(home / ".codex"), CODEX_DIR="", CXP_AUTO_UPDATE="0",
                   CODEX_PROXY_SKIP_BUILTIN_SKILLS="1")
        config = root / "config.sqlite"

        def run(*args):
            result = subprocess.run([str(entry), *map(str, args)], env=env, cwd=root,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
            if result.returncode:
                raise RuntimeError(result.stdout.decode(errors="replace"))

        run("--cxp-install-bootstrap", "--target", entry)
        run("--config", config, "proxy", "prune")  # empty synthetic registry only
        assert config.read_bytes()[:16] == b"SQLite format 3\0"

        def native_files():
            return {str(p.relative_to(root)): (p.stat().st_size, p.stat().st_mtime_ns)
                    for p in root.rglob("*") if p.is_file() and
                    (p.name.endswith(".sqlite") or p.name.endswith(".published"))}

        for name, args in (("native-version", ["--version"]),
                           ("native-config-list", ["--config", config, "proxy", "list"])):
            files = native_files()
            before = resource.getrusage(resource.RUSAGE_CHILDREN)
            start = time.monotonic()
            for _ in range(20):
                run(*args)
            wall = time.monotonic() - start
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            assert native_files() == files, f"{name} rewrote native state"
            print(f"phase={name} processes=20 wall_s={wall:.6f} "
                  f"child_user_s={after.ru_utime-before.ru_utime:.6f} "
                  f"child_system_s={after.ru_stime-before.ru_stime:.6f} "
                  f"child_maxrss_kib={after.ru_maxrss} "
                  f"child_disk_input_bytes={(after.ru_inblock-before.ru_inblock)*512} "
                  f"child_disk_output_bytes={(after.ru_oublock-before.ru_oublock)*512}", flush=True)


if __name__ == "__main__":
    main()
