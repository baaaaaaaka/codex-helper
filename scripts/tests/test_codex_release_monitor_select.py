from __future__ import annotations

import sys
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import codex_release_monitor_select as monitor_select


def compatibility_row(
    version: str,
    *,
    linux: str = "pass",
    mac: str = "pass",
    windows: str = "pass",
    centos7: str = "pass",
    rockylinux8: str = "pass",
    ubuntu20: str = "pass",
    tested_at: str = "2026-03-10T00:00:00Z",
) -> str:
    return (
        f"| {version} | {linux} | {mac} | {windows} | {centos7} | {rockylinux8} | "
        f"{ubuntu20} | {tested_at} |"
    )


class CodexReleaseMonitorSelectTests(unittest.TestCase):
    def test_select_targeted_versions_retests_stale_or_failed_recent_versions(self) -> None:
        rows = {
            "0.112.0": {
                "linux": "pass",
                "mac": "pass",
                "windows": "pass",
                "centos7": "pass",
                "rockylinux8": "pass",
                "ubuntu20.04": "pass",
                "last_tested_utc": "2026-03-10T09:00:00Z",
            },
            "0.111.0": {
                "linux": "pass",
                "mac": "pass",
                "windows": "pass",
                "centos7": "pass",
                "rockylinux8": "pass",
                "ubuntu20.04": "pass",
                "last_tested_utc": "2026-03-05T00:00:00Z",
            },
            "0.110.0": {
                "linux": "pass",
                "mac": "pass",
                "windows": "fail",
                "centos7": "pass",
                "rockylinux8": "pass",
                "ubuntu20.04": "pass",
                "last_tested_utc": "2026-03-10T09:00:00Z",
            },
            "0.107.0": {
                "linux": "fail",
                "mac": "fail",
                "windows": "fail",
                "centos7": "fail",
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
                "| Codex version | linux | mac | windows | centos7 | rockylinux8 | ubuntu20.04 | last_tested_utc |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
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

    def test_invalid_timestamp_forces_revalidation(self) -> None:
        selected = monitor_select.select_targeted_versions(
            table_rows={
                "0.112.0": {
                    "linux": "pass",
                    "mac": "pass",
                    "windows": "pass",
                    "centos7": "pass",
                    "rockylinux8": "pass",
                    "ubuntu20.04": "pass",
                    "last_tested_utc": "not-a-timestamp",
                }
            },
            latest_version="0.112.0",
            now=datetime(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(selected, ["0.112.0"])

    def test_cli_range_mode_excludes_prerelease_when_disabled(self) -> None:
        out = StringIO()
        argv = [
            "codex_release_monitor_select.py",
            "--mode",
            "range",
            "--min-version",
            "v0.110.0",
            "--max-version",
            "v0.112.0",
            "--no-include-prerelease",
        ]

        with mock.patch.object(sys, "argv", argv), mock.patch.object(
            monitor_select,
            "sweep_select_versions",
            return_value=["0.111.0", "0.112.0"],
        ) as select_versions, redirect_stdout(out):
            code = monitor_select.main()

        self.assertEqual(code, 0)
        self.assertEqual(out.getvalue().strip(), '["0.111.0", "0.112.0"]')
        select_versions.assert_called_once_with(
            versions_arg="",
            min_version="0.110.0",
            max_version="0.112.0",
            include_prerelease=False,
        )

    def test_cli_versions_mode_accepts_comma_newline_and_json(self) -> None:
        cases = [
            "v0.112.0,\n0.111.0",
            '["v0.112.0", "0.111.0"]',
        ]

        for versions_arg in cases:
            with self.subTest(versions_arg=versions_arg):
                out = StringIO()
                argv = [
                    "codex_release_monitor_select.py",
                    "--mode",
                    "versions",
                    "--versions",
                    versions_arg,
                ]

                with mock.patch.object(sys, "argv", argv), redirect_stdout(out):
                    code = monitor_select.main()

                self.assertEqual(code, 0)
                self.assertEqual(json.loads(out.getvalue()), ["0.111.0", "0.112.0"])


if __name__ == "__main__":
    unittest.main()
