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
                powershell,
                "-NoProfile",
                "-Command",
                "exit 0",
            ],
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("Attempts must be >= 1", proc.stderr + proc.stdout)


if __name__ == "__main__":
    unittest.main()
