from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "ci" / "install_node_local.sh"


class InstallNodeLocalTests(unittest.TestCase):
    def setUp(self) -> None:
        if os.name == "nt":
            self.skipTest("install_node_local.sh tests require POSIX paths")
        self.bash = shutil.which("bash")
        if not self.bash:
            self.skipTest("bash not available")

    def write_executable(self, directory: Path, name: str, body: str) -> Path:
        path = directory / name
        path.write_text(textwrap.dedent(body), encoding="utf-8")
        path.chmod(0o755)
        return path

    def write_uname(self, directory: Path) -> None:
        self.write_executable(
            directory,
            "uname",
            """\
            #!/usr/bin/env bash
            case "${1:-}" in
              -s) printf '%s\n' "${FAKE_UNAME_S:-Linux}" ;;
              -m) printf '%s\n' "${FAKE_UNAME_M:-x86_64}" ;;
              *) printf '%s\n' "${FAKE_UNAME_S:-Linux}" ;;
            esac
            """,
        )

    def write_download_stubs(self, directory: Path) -> None:
        self.write_uname(directory)
        self.write_executable(
            directory,
            "getconf",
            """\
            #!/usr/bin/env bash
            if [ "${1:-}" = "GNU_LIBC_VERSION" ]; then
              printf 'glibc %s\n' "${FAKE_GLIBC_VERSION:-2.31}"
              exit 0
            fi
            exit 1
            """,
        )
        self.write_executable(
            directory,
            "curl",
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            out=""
            url=""
            while [ "$#" -gt 0 ]; do
              case "$1" in
                -o)
                  shift
                  out="${1:-}"
                  ;;
                http*)
                  url="$1"
                  ;;
              esac
              shift || true
            done
            if [ -z "$out" ]; then
              echo "missing -o" >&2
              exit 2
            fi
            printf '%s\n' "$url" >> "${FAKE_CURL_LOG:?}"
            case "$url" in
              *unofficial-builds.nodejs.org*/index.tab)
                printf 'v22.9.0\t2026-01-01\tlinux-x64-glibc-217\n' > "$out"
                ;;
              *unofficial-builds.nodejs.org*/v22.9.0/SHASUMS256.txt)
                printf 'abc  node-v22.9.0-linux-x64-glibc-217.tar.xz\n' > "$out"
                ;;
              *unofficial-builds.nodejs.org*/node-v22.9.0-linux-x64-glibc-217.tar.xz)
                printf 'archive\n' > "$out"
                ;;
              *nodejs.org/dist/latest-v22.x/SHASUMS256.txt)
                printf 'abc  node-v22.10.0-linux-x64.tar.gz\n' > "$out"
                ;;
              *nodejs.org/dist/v22.10.0/SHASUMS256.txt)
                printf 'abc  node-v22.10.0-linux-x64.tar.gz\n' > "$out"
                ;;
              *nodejs.org/dist/v22.10.0/node-v22.10.0-linux-x64.tar.gz)
                printf 'archive\n' > "$out"
                ;;
              *)
                echo "unexpected url: $url" >&2
                exit 3
                ;;
            esac
            """,
        )
        self.write_executable(
            directory,
            "sha256sum",
            """\
            #!/usr/bin/env bash
            printf '%s  %s\n' "${FAKE_SHA256:-abc}" "$1"
            """,
        )
        self.write_executable(
            directory,
            "tar",
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            dest=""
            while [ "$#" -gt 0 ]; do
              case "$1" in
                -C)
                  shift
                  dest="${1:-}"
                  ;;
              esac
              shift || true
            done
            if [ -z "$dest" ]; then
              echo "missing destination" >&2
              exit 2
            fi
            mkdir -p "$dest/bin"
            cat > "$dest/bin/node" <<'EOF'
            #!/usr/bin/env bash
            if [ "${1:-}" = "-v" ]; then
              echo "v22.9.0"
              exit 0
            fi
            exit 0
            EOF
            chmod +x "$dest/bin/node"
            """,
        )

    def run_script(self, fake_bin: Path, extra_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(extra_env)
        env["PATH"] = f"{fake_bin}{os.pathsep}{env.get('PATH', '')}"
        return subprocess.run(
            [self.bash, str(SCRIPT)],
            env=env,
            text=True,
            capture_output=True,
        )

    def test_uses_system_node_when_major_is_high_enough(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            self.write_uname(fake_bin)
            self.write_executable(
                fake_bin,
                "node",
                """\
                #!/usr/bin/env bash
                if [ "${1:-}" = "-v" ]; then
                  echo "v20.1.0"
                  exit 0
                fi
                exit 0
                """,
            )

            proc = self.run_script(
                fake_bin,
                {
                    "HOME": str(root / "home"),
                    "NODE_MIN_MAJOR": "16",
                },
            )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), str(fake_bin))

    def test_rejects_unsupported_os(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            self.write_uname(fake_bin)

            proc = self.run_script(
                fake_bin,
                {
                    "FAKE_UNAME_S": "FreeBSD",
                    "NODE_ALLOW_SYSTEM_NODE": "0",
                    "HOME": str(root / "home"),
                },
            )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("unsupported OS: FreeBSD", proc.stderr)

    def test_selects_unofficial_glibc217_build_for_old_glibc(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake_bin = root / "bin"
            curl_log = root / "curl.log"
            fake_bin.mkdir()
            self.write_download_stubs(fake_bin)

            proc = self.run_script(
                fake_bin,
                {
                    "NODE_ALLOW_SYSTEM_NODE": "0",
                    "NODE_INSTALL_ROOT": str(root / "node"),
                    "HOME": str(root / "home"),
                    "FAKE_GLIBC_VERSION": "2.17",
                    "FAKE_CURL_LOG": str(curl_log),
                },
            )
            log = curl_log.read_text(encoding="utf-8")

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), str(root / "node" / "v22-linux-x64" / "bin"))
        self.assertIn("unofficial-builds.nodejs.org/download/release/index.tab", log)
        self.assertIn("node-v22.9.0-linux-x64-glibc-217.tar.xz", log)

    def test_selects_official_tarball_for_modern_glibc(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake_bin = root / "bin"
            curl_log = root / "curl.log"
            fake_bin.mkdir()
            self.write_download_stubs(fake_bin)

            proc = self.run_script(
                fake_bin,
                {
                    "NODE_ALLOW_SYSTEM_NODE": "0",
                    "NODE_INSTALL_ROOT": str(root / "node"),
                    "HOME": str(root / "home"),
                    "FAKE_GLIBC_VERSION": "2.31",
                    "FAKE_CURL_LOG": str(curl_log),
                },
            )
            log = curl_log.read_text(encoding="utf-8")

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), str(root / "node" / "v22-linux-x64" / "bin"))
        self.assertIn("nodejs.org/dist/latest-v22.x/SHASUMS256.txt", log)
        self.assertIn("node-v22.10.0-linux-x64.tar.gz", log)
        self.assertNotIn("unofficial-builds.nodejs.org", log)

    def test_rejects_checksum_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake_bin = root / "bin"
            curl_log = root / "curl.log"
            fake_bin.mkdir()
            self.write_download_stubs(fake_bin)

            proc = self.run_script(
                fake_bin,
                {
                    "NODE_ALLOW_SYSTEM_NODE": "0",
                    "NODE_INSTALL_ROOT": str(root / "node"),
                    "HOME": str(root / "home"),
                    "FAKE_GLIBC_VERSION": "2.31",
                    "FAKE_CURL_LOG": str(curl_log),
                    "FAKE_SHA256": "bad",
                },
            )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("Node.js checksum mismatch", proc.stderr)


if __name__ == "__main__":
    unittest.main()
