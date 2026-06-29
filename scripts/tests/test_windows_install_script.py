from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALL_PS1 = REPO_ROOT / "install.ps1"


class WindowsInstallScriptTests(unittest.TestCase):
    def test_latest_resolution_uses_redirect_before_api(self) -> None:
        script = INSTALL_PS1.read_text(encoding="utf-8")

        function_start = script.index("function Get-LatestTag")
        function_end = script.index("Assert-DiskSpace -label", function_start)
        latest_function = script[function_start:function_end]

        self.assertLess(
            latest_function.index("Invoke-WebRequest -Uri $latestUri"),
            latest_function.index("Invoke-RestMethod -Uri $apiUri"),
        )
        self.assertIn("API fallback", latest_function)

    def test_installer_waits_for_existing_helper_before_replacing_binary(self) -> None:
        script = INSTALL_PS1.read_text(encoding="utf-8")

        for expected in [
            "function Get-CodexProxyProcessesForPath",
            "$processName -ine \"codex-proxy.exe\" -and $processName -ine \"cxp.exe\"",
            "Get-CimInstance Win32_Process -Filter \"Name = '$processName'\"",
            "Stop-CodexProxyTeamsTasksForInstall",
            "Codex Helper Teams Watchdog",
            "Codex Helper Teams Bridge",
            "Wait-CodexProxyInstallPathReleased $dst",
            "Wait-CodexProxyInstallPathReleased $cxpExe",
            "Move-Item -Force -LiteralPath $tmp -Destination $dst -ErrorAction Stop",
            "Copy-Item -Force -LiteralPath $dst -Destination $cxpExe -ErrorAction Stop",
            "Pending codex-proxy binary still exists after Move-Item",
        ]:
            self.assertIn(expected, script)

        self.assertLess(
            script.index("Wait-CodexProxyInstallPathReleased $dst"),
            script.index("Move-Item -Force -LiteralPath $tmp -Destination $dst -ErrorAction Stop"),
        )
        self.assertLess(
            script.index("Wait-CodexProxyInstallPathReleased $cxpExe"),
            script.index("Copy-Item -Force -LiteralPath $dst -Destination $cxpExe -ErrorAction Stop"),
        )


if __name__ == "__main__":
    unittest.main()
