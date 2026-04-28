from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "ci" / "configure_container_repos.sh"


class ConfigureContainerReposTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bash = shutil.which("bash")
        if not self.bash:
            self.skipTest("bash not available")

    def run_script(self, strategy: str, root: Path) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["CI_CONTAINER_ROOT"] = str(root)
        return subprocess.run(
            [self.bash, str(SCRIPT), strategy],
            env=env,
            text=True,
            capture_output=True,
        )

    def test_centos_vault_rewrites_repo_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            repo_dir = root / "etc" / "yum.repos.d"
            repo_dir.mkdir(parents=True)
            repo = repo_dir / "CentOS-Base.repo"
            repo.write_text(
                "\n".join(
                    [
                        "mirrorlist=http://mirrorlist.centos.org/?release=7&arch=$basearch",
                        "#baseurl=http://mirror.centos.org/centos/$releasever/os/$basearch/",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            proc = self.run_script("centos-vault", root)

            self.assertEqual(proc.returncode, 0, proc.stderr)
            text = repo.read_text(encoding="utf-8")
            self.assertIn("#mirrorlist=http://mirrorlist.centos.org/", text)
            self.assertIn("baseurl=http://vault.centos.org/centos/$releasever/os/$basearch/", text)

    def test_rocky_official_rewrites_repo_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            repo_dir = root / "etc" / "yum.repos.d"
            repo_dir.mkdir(parents=True)
            repo = repo_dir / "Rocky-BaseOS.repo"
            repo.write_text(
                "\n".join(
                    [
                        "mirrorlist=https://mirrors.rockylinux.org/mirrorlist?arch=$basearch",
                        "#baseurl=http://dl.rockylinux.org/$contentdir/$releasever/BaseOS/$basearch/os/",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            proc = self.run_script("rocky-official", root)

            self.assertEqual(proc.returncode, 0, proc.stderr)
            text = repo.read_text(encoding="utf-8")
            self.assertIn("#mirrorlist=https://mirrors.rockylinux.org/", text)
            self.assertIn("baseurl=https://dl.rockylinux.org/$contentdir/$releasever/BaseOS/$basearch/os/", text)

    def test_ubuntu_azure_archive_rewrites_list_and_sources_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            apt_dir = root / "etc" / "apt"
            sources_dir = apt_dir / "sources.list.d"
            lists_dir = root / "var" / "lib" / "apt" / "lists"
            sources_dir.mkdir(parents=True)
            lists_dir.mkdir(parents=True)
            (lists_dir / "stale").write_text("stale", encoding="utf-8")
            sources = apt_dir / "sources.list"
            sources.write_text("deb http://archive.ubuntu.com/ubuntu focal main\n", encoding="utf-8")
            deb822 = sources_dir / "ubuntu.sources"
            deb822.write_text("URIs: https://archive.ubuntu.com/ubuntu/\n", encoding="utf-8")

            proc = self.run_script("ubuntu-azure-archive", root)

            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("http://azure.archive.ubuntu.com/ubuntu/", sources.read_text(encoding="utf-8"))
            self.assertIn("http://azure.archive.ubuntu.com/ubuntu/", deb822.read_text(encoding="utf-8"))
            self.assertFalse((lists_dir / "stale").exists())

    def test_unknown_strategy_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proc = self.run_script("not-a-strategy", Path(temp_dir))

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("unsupported repo strategy", proc.stderr)


if __name__ == "__main__":
    unittest.main()
