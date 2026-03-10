from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import codex_release_monitor_select as monitor_select


def compatibility_row(version: str, *, linux: str = "pass", mac: str = "pass", windows: str = "pass", rockylinux8: str = "pass", ubuntu20: str = "pass", tested_at: str = "2026-03-10T00:00:00Z") -> str:
    return (
        f"| {version} | {linux} | {mac} | {windows} | {rockylinux8} | "
        f"{ubuntu20} | {tested_at} |"
    )


class CodexReleaseMonitorSelectTests(unittest.TestCase):
    def test_select_targeted_versions_retests_stale_or_failed_recent_versions(self) -> None:
        rows = {
            "0.112.0": {
                "linux": "pass",
                "mac": "pass",
                "windows": "pass",
                "rockylinux8": "pass",
                "ubuntu20.04": "pass",
                "last_tested_utc": "2026-03-10T09:00:00Z",
            },
            "0.111.0": {
                "linux": "pass",
                "mac": "pass",
                "windows": "pass",
                "rockylinux8": "pass",
                "ubuntu20.04": "pass",
                "last_tested_utc": "2026-03-05T00:00:00Z",
            },
            "0.110.0": {
                "linux": "pass",
                "mac": "pass",
                "windows": "fail",
                "rockylinux8": "pass",
                "ubuntu20.04": "pass",
                "last_tested_utc": "2026-03-10T09:00:00Z",
            },
            "0.107.0": {
                "linux": "fail",
                "mac": "fail",
                "windows": "fail",
                "rockylinux8": "fail",
                "ubuntu20.04": "fail",
                "last_tested_utc": "2026-03-01T00:00:00Z",
            },
        }

        selected = monitor_select.select_targeted_versions(
            table_rows=rows,
            latest_version="0.112.0",
            alpha_version="0.113.0-alpha.2",
            now=datetime(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(selected, ["0.113.0-alpha.2", "0.111.0", "0.110.0"])

    def test_parse_table_accepts_prerelease_versions(self) -> None:
        table = "\n".join(
            [
                "# Codex Compatibility",
                "",
                "| Codex version | linux | mac | windows | rockylinux8 | ubuntu20.04 | last_tested_utc |",
                "| --- | --- | --- | --- | --- | --- | --- |",
                compatibility_row("0.113.0-alpha.2"),
                compatibility_row("0.112.0"),
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            table_path = Path(temp_dir) / "compatibility.md"
            table_path.write_text(table + "\n", encoding="utf-8")

            rows = monitor_select.parse_table(table_path)

        self.assertIn("0.113.0-alpha.2", rows)
        self.assertIn("0.112.0", rows)


if __name__ == "__main__":
    unittest.main()
