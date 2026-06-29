from __future__ import annotations

import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CI_DIR = REPO_ROOT / "scripts" / "ci"


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

    def test_helper_upgrade_fixture_models_stable_cxp_entries(self) -> None:
        script = (CI_DIR / "helper_upgrade_compat_smoke.sh").read_text(encoding="utf-8")

        self.assertIn("symlink|stale-symlink|missing)", script)
        self.assertNotIn("symlink|stale-symlink|missing|current-missing-cxp)", script)
        self.assertIn(
            'managed cxp should be a stable executable for seed mode $seed_mode',
            script,
        )
        self.assertIn(
            "current managed helper did not publish a stable cxp executable",
            script,
        )

    def test_windows_upgrade_fixture_seeds_valid_and_repairs_broken_canonical_shims(self) -> None:
        script = (CI_DIR / "helper_upgrade_compat_smoke.ps1").read_text(encoding="utf-8")

        valid_start = script.index('"existing-cmd" {')
        broken_start = script.index('"existing-cmd-missing-exe" {')
        broken_end = script.index('"stale-helper-cmd" {', broken_start)
        valid_block = script[valid_start:broken_start]
        broken_block = script[broken_start:broken_end]
        self.assertIn("Download-Binary $OldTag $cxpExe", valid_block)
        self.assertLess(
            valid_block.index("Download-Binary $OldTag $cxpExe"),
            valid_block.index("Assert-Version $cxp $OldTag"),
        )
        self.assertIn("Remove-Item -Force -LiteralPath $cxpExe", broken_block)
        self.assertNotIn("Assert-Version $cxp $OldTag", broken_block)
        self.assertIn(
            'Run-UpgradeScenario "existing-cxp-cmd-missing-exe" "existing-cmd-missing-exe"',
            script,
        )


if __name__ == "__main__":
    unittest.main()
