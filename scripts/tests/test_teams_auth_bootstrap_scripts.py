from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BASH_SCRIPT = REPO_ROOT / "scripts" / "teams-auth-bootstrap.sh"
POWERSHELL_SCRIPT = REPO_ROOT / "scripts" / "teams-auth-bootstrap.ps1"
GORELEASER_CONFIG = REPO_ROOT / ".goreleaser.yaml"
README = REPO_ROOT / "README.md"


class TeamsAuthBootstrapScriptTests(unittest.TestCase):
    def test_release_config_attaches_setup_scripts(self) -> None:
        config = GORELEASER_CONFIG.read_text(encoding="utf-8")
        self.assertIn("scripts/teams-auth-bootstrap.sh", config)
        self.assertIn("scripts/teams-auth-bootstrap.ps1", config)

    def test_powershell_script_avoids_ambiguous_variable_colon(self) -> None:
        script = POWERSHELL_SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("$LASTEXITCODE:", script)

    def test_readme_powershell_bootstrap_downloads_file_before_running(self) -> None:
        readme = README.read_text(encoding="utf-8")
        self.assertIn(
            '$u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/scripts/teams-auth-bootstrap.ps1"',
            readme,
        )
        self.assertIn("Unblock-File -LiteralPath $p", readme)
        self.assertIn(
            "& powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p",
            readme,
        )
        self.assertNotIn("-ExecutionPolicy Bypass", readme)
        self.assertNotIn("Invoke-Expression", readme)

    def test_bash_script_interactive_flow_configures_one_client(self) -> None:
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("bash not available")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log = root / "commands.log"
            fake = root / "codex-proxy"
            fake.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    for arg in "$@"; do
                      printf '[%s]' "$arg"
                    done >> "$CODEX_HELPER_TEST_LOG"
                    printf '\\n' >> "$CODEX_HELPER_TEST_LOG"
                    case "$*" in
                      "teams auth login")
                        echo "Authenticated Teams full access as CI <ci@example.test>"
                        ;;
                      "teams auth status")
                        echo "Teams full auth cache: present"
                        ;;
                      "teams service bootstrap --no-open-control")
                        echo "Teams service bootstrap ready: ci"
                        ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            fake.chmod(0o755)

            env = os.environ.copy()
            env["CODEX_HELPER_TEST_LOG"] = str(log)
            proc = subprocess.run(
                [
                    bash,
                    str(BASH_SCRIPT),
                    "--codex-proxy",
                    str(fake),
                    "--no-open-control",
                ],
                input="tenant-ci\nclient-ci\n",
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

            output = proc.stdout + proc.stderr
            for want in [
                "STEP 1/4: Configure Teams Graph auth",
                "STEP 2/4: Sign in with Microsoft device login",
                "STEP 3/4: Verify local Teams auth cache",
                "STEP 4/4: Bootstrap the Teams helper service",
                "Teams auth and service bootstrap completed.",
            ]:
                self.assertIn(want, output)

            lines = log.read_text(encoding="utf-8").splitlines()
            self.assertEqual(
                lines,
                [
                    "[teams][auth][config][--tenant-id][tenant-ci][--client-id][client-ci]",
                    "[teams][auth][login]",
                    "[teams][auth][status]",
                    "[teams][service][bootstrap][--no-open-control]",
                ],
            )

    def test_bash_script_rejects_missing_interactive_values(self) -> None:
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("bash not available")

        proc = subprocess.run(
            [
                bash,
                str(BASH_SCRIPT),
                "--codex-proxy",
                "codex-proxy",
                "--no-open-control",
            ],
            input="",
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("tenant id is required", proc.stderr)

    def test_bash_script_rejects_split_read_client(self) -> None:
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("bash not available")

        proc = subprocess.run(
            [
                bash,
                str(BASH_SCRIPT),
                "--codex-proxy",
                "codex-proxy",
                "--tenant-id",
                "tenant-ci",
                "--read-client-id",
                "read-client-ci",
            ],
            input="",
            text=True,
            capture_output=True,
        )
        self.assertEqual(proc.returncode, 2)
        self.assertIn("--read-client-id is no longer supported", proc.stderr)

    def test_bash_script_write_client_id_is_the_client_id(self) -> None:
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("bash not available")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log = root / "commands.log"
            fake = root / "codex-proxy"
            fake.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    for arg in "$@"; do
                      printf '[%s]' "$arg"
                    done >> "$CODEX_HELPER_TEST_LOG"
                    printf '\\n' >> "$CODEX_HELPER_TEST_LOG"
                    """
                ),
                encoding="utf-8",
            )
            fake.chmod(0o755)

            env = os.environ.copy()
            env["CODEX_HELPER_TEST_LOG"] = str(log)
            subprocess.run(
                [
                    bash,
                    str(BASH_SCRIPT),
                    "--codex-proxy",
                    str(fake),
                    "--tenant-id",
                    "tenant-ci",
                    "--write-client-id",
                    "write-client-ci",
                ],
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )
            lines = log.read_text(encoding="utf-8").splitlines()
            self.assertEqual(
                lines,
                [
                    "[teams][auth][config][--tenant-id][tenant-ci][--client-id][write-client-ci]",
                    "[teams][auth][login]",
                    "[teams][auth][status]",
                    "[teams][service][bootstrap]",
                ],
            )

    def test_powershell_script_interactive_flow_configures_one_client(self) -> None:
        powershell = shutil.which("pwsh") or shutil.which("powershell")
        if not powershell:
            self.skipTest("PowerShell not available")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log = root / "commands.log"
            fake = root / "fake-codex-proxy.ps1"
            fake.write_text(
                textwrap.dedent(
                    """\
                    param([Parameter(ValueFromRemainingArguments=$true)][string[]]$Rest)
                    $line = ($Rest | ForEach-Object { "[$_]" }) -join ""
                    Add-Content -Path $env:CODEX_HELPER_TEST_LOG -Value $line
                    $joined = $Rest -join " "
                    if ($joined -eq "teams auth login") {
                      Write-Output "Authenticated Teams full access as CI <ci@example.test>"
                    } elseif ($joined -eq "teams auth status") {
                      Write-Output "Teams full auth cache: present"
                    } elseif ($joined -eq "teams service bootstrap --no-open-control") {
                      Write-Output "Teams service bootstrap ready: ci"
                    }
                    exit 0
                    """
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["CODEX_HELPER_TEST_LOG"] = str(log)
            proc = subprocess.run(
                [
                    powershell,
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(POWERSHELL_SCRIPT),
                    "-CodexProxy",
                    str(fake),
                    "-NoOpenControl",
                ],
                input="tenant-ci\nclient-ci\n",
                text=True,
                capture_output=True,
                env=env,
            )
            self.assertEqual(
                proc.returncode,
                0,
                "stdout:\n" + proc.stdout + "\nstderr:\n" + proc.stderr,
            )

            output = proc.stdout + proc.stderr
            for want in [
                "STEP 1/4: Configure Teams Graph auth",
                "STEP 2/4: Sign in with Microsoft device login",
                "STEP 3/4: Verify local Teams auth cache",
                "STEP 4/4: Bootstrap the Teams helper service",
                "Teams auth and service bootstrap completed.",
            ]:
                self.assertIn(want, output)

            lines = log.read_text(encoding="utf-8").splitlines()
            self.assertEqual(
                lines,
                [
                    "[teams][auth][config][--tenant-id][tenant-ci][--client-id][client-ci]",
                    "[teams][auth][login]",
                    "[teams][auth][status]",
                    "[teams][service][bootstrap][--no-open-control]",
                ],
            )

    def test_powershell_script_rejects_split_read_client(self) -> None:
        powershell = shutil.which("pwsh") or shutil.which("powershell")
        if not powershell:
            self.skipTest("PowerShell not available")

        proc = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(POWERSHELL_SCRIPT),
                "-CodexProxy",
                "codex-proxy",
                "-TenantId",
                "tenant-ci",
                "-ReadClientId",
                "read-client-ci",
            ],
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("-ReadClientId is no longer supported", proc.stdout + proc.stderr)
