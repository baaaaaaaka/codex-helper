from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CI_DIR = REPO_ROOT / "scripts" / "ci"
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"


class CIHelperScriptTests(unittest.TestCase):
    def test_all_bash_ci_scripts_parse(self) -> None:
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("bash not available")

        for script in sorted(CI_DIR.glob("*.sh")):
            with self.subTest(script=script.name):
                subprocess.run([bash, "-n", str(script)], check=True)

    def test_retry_sh_retries_until_success(self) -> None:
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("bash not available")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state = root / "attempts"
            helper = root / "flaky.sh"
            helper.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    state="$1"
                    count=0
                    if [[ -f "$state" ]]; then
                      count="$(cat "$state")"
                    fi
                    count=$((count + 1))
                    printf '%s' "$count" > "$state"
                    [[ "$count" -ge 2 ]]
                    """
                ),
                encoding="utf-8",
            )
            helper.chmod(0o755)

            subprocess.run(
                [bash, str(CI_DIR / "retry.sh"), "3", "0", str(helper), str(state)],
                check=True,
                text=True,
                capture_output=True,
            )
            self.assertEqual(state.read_text(encoding="utf-8"), "2")

    def test_retry_sh_rejects_zero_attempts(self) -> None:
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("bash not available")

        proc = subprocess.run(
            [bash, str(CI_DIR / "retry.sh"), "0", "0", "true"],
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("attempts must be >= 1", proc.stderr)

    def test_apt_update_isolates_optional_chrome_source_and_restores_it(self) -> None:
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("bash not available")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            bin_dir = root / "bin"
            source_dir = root / "sources.list.d"
            bin_dir.mkdir()
            source_dir.mkdir()
            chrome_source = source_dir / "google-chrome.list"
            chrome_source.write_text("deb https://dl.google.com/linux/chrome/deb stable main\n", encoding="utf-8")
            (source_dir / "ubuntu.sources").write_text("official\n", encoding="utf-8")
            state = root / "apt-update-attempts"
            fake_apt = bin_dir / "apt-get"
            fake_apt.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    case "${1:-}" in
                      clean)
                        exit 0
                        ;;
                      update)
                        count=0
                        if [[ -f "$APT_FAKE_STATE" ]]; then
                          count="$(cat "$APT_FAKE_STATE")"
                        fi
                        count=$((count + 1))
                        printf '%s' "$count" > "$APT_FAKE_STATE"
                        if [[ -e "$APT_UPDATE_SOURCE_DIR/google-chrome.list" ]]; then
                          echo "optional Chrome source was not isolated" >&2
                          exit 9
                        fi
                        [[ "$count" -ge 2 ]]
                        ;;
                      *)
                        echo "unexpected apt-get command: $*" >&2
                        exit 10
                        ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            fake_apt.chmod(0o755)

            subprocess.run(
                [bash, str(CI_DIR / "apt_update.sh")],
                check=True,
                text=True,
                capture_output=True,
                env={
                    **os.environ,
                    "PATH": f"{bin_dir}:{os.environ['PATH']}",
                    "APT_UPDATE_SOURCE_DIR": str(source_dir),
                    "APT_UPDATE_LISTS_DIR": str(root / "lists"),
                    "APT_UPDATE_ATTEMPTS": "2",
                    "APT_UPDATE_SLEEP_SECONDS": "0",
                    "APT_FAKE_STATE": str(state),
                },
            )
            self.assertEqual(state.read_text(encoding="utf-8"), "2")
            self.assertEqual(chrome_source.read_text(encoding="utf-8"), "deb https://dl.google.com/linux/chrome/deb stable main\n")
            self.assertEqual((source_dir / "ubuntu.sources").read_text(encoding="utf-8"), "official\n")

    def test_retry_ps1_retries_until_success(self) -> None:
        powershell = shutil.which("pwsh") or shutil.which("powershell")
        if not powershell:
            self.skipTest("PowerShell not available")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state = root / "attempts.txt"
            helper = root / "flaky.ps1"
            helper.write_text(
                textwrap.dedent(
                    """\
                    param([string]$StatePath)
                    $count = 0
                    if (Test-Path $StatePath) {
                      $count = [int](Get-Content -Raw -Path $StatePath)
                    }
                    $count += 1
                    Set-Content -NoNewline -Path $StatePath -Value $count
                    if ($count -lt 2) {
                      throw "not yet"
                    }
                    """
                ),
                encoding="utf-8",
            )

            subprocess.run(
                [
                    powershell,
                    "-NoProfile",
                    "-File",
                    str(CI_DIR / "retry.ps1"),
                    "3",
                    "0",
                    powershell,
                    "-NoProfile",
                    "-File",
                    str(helper),
                    str(state),
                ],
                check=True,
            )
            self.assertEqual(state.read_text(encoding="utf-8"), "2")

    def test_retry_ps1_rejects_zero_attempts(self) -> None:
        powershell = shutil.which("pwsh") or shutil.which("powershell")
        if not powershell:
            self.skipTest("PowerShell not available")

        proc = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-File",
                str(CI_DIR / "retry.ps1"),
                "0",
                "0",
                "noop",
            ],
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("Attempts must be >= 1", proc.stderr + proc.stdout)

    def test_upgrade_artifact_contract_is_shared_across_platforms(self) -> None:
        # Both native runners must exercise actual published artifacts through
        # the same migration/admission contract. Source version overrides are
        # covered separately by the native candidate test.
        for name in ("helper_upgrade_compat_smoke.sh", "helper_upgrade_compat_smoke.ps1"):
            script = (CI_DIR / name).read_text(encoding="utf-8")
            self.assertIn("sqlite_upgrade_artifacts.py", script)
        script = (CI_DIR / "sqlite_upgrade_artifacts.py").read_text(encoding="utf-8")
        compile(script, "sqlite_upgrade_artifacts.py", "exec")
        self.assertIn("checksums.txt", script)
        self.assertIn('download(old), download(target)', script)
        self.assertIn('"--cxp-install-bootstrap"', script)
        self.assertIn('"proxy", "list"', script)

    def test_windows_locked_cxp_regression_and_release_gates_are_both_wired(self) -> None:
        script = (CI_DIR / "windows_locked_cxp_self_upgrade.ps1").read_text(
            encoding="utf-8"
        )
        # The Split reproduction downloads released binaries, so it runs in
        # the nightly tier (nightly.yml), which also gates every release.
        ci = (WORKFLOW_DIR / "nightly.yml").read_text(encoding="utf-8")
        release = (WORKFLOW_DIR / "release.yml").read_text(encoding="utf-8")

        self.assertIn('[ValidateSet("Split", "Converged")]', script)
        self.assertIn("Start-Process -FilePath $cxpExe", script)
        self.assertIn('if ($ExpectedState -eq "Split")', script)
        self.assertIn("failed to unify helper entrypoint", script)
        self.assertIn("post-parent repair did not converge", script)
        self.assertIn("Assert-Version $cxpExe $TargetTag $true", script)
        self.assertIn("Assert-Version $cxpCmd $TargetTag", script)

        self.assertIn("OLD_TAG: v0.1.12", ci)
        self.assertIn("TARGET_TAG: v0.1.13-rc.53", ci)
        self.assertIn(
            "windows_locked_cxp_self_upgrade.ps1 -ExpectedState Split", ci
        )
        # Current releases have a new protocol. The old Split characterization
        # stays frozen in nightly; current compatibility uses the native
        # locked-entry test and the released-artifact migration contract.
        self.assertIn("windows_stale_cxp_entry_smoke.ps1", release)
        self.assertIn("helper_upgrade_compat_smoke.ps1", release)


if __name__ == "__main__":
    unittest.main()
