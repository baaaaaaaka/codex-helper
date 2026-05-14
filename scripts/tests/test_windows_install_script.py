from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALL_PS1 = REPO_ROOT / "install.ps1"


class WindowsInstallScriptTests(unittest.TestCase):
    def test_installer_waits_for_existing_helper_before_replacing_binary(self) -> None:
        script = INSTALL_PS1.read_text(encoding="utf-8")

        for expected in [
            "function Get-CodexProxyProcessesForPath",
            "Get-CimInstance Win32_Process -Filter \"Name = 'codex-proxy.exe'\"",
            "Stop-CodexProxyTeamsTasksForInstall",
            "Codex Helper Teams Watchdog",
            "Codex Helper Teams Bridge",
            "Wait-CodexProxyInstallPathReleased $dst",
            "Move-Item -Force -Path $tmp -Destination $dst",
        ]:
            self.assertIn(expected, script)

        self.assertLess(
            script.index("Wait-CodexProxyInstallPathReleased $dst"),
            script.index("Move-Item -Force -Path $tmp -Destination $dst"),
        )


if __name__ == "__main__":
    unittest.main()
