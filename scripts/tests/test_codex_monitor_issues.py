from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import codex_monitor_issues as monitor_issues


class CodexMonitorIssuesTests(unittest.TestCase):
    def test_plan_issues_creates_issue_for_failed_version(self) -> None:
        versions = ["0.112.0", "0.111.0"]
        results = {
            "0.112.0": {
                "linux": "pass",
                "mac": "pass",
                "windows": "fail",
                "centos7": "pass",
                "rockylinux8": "pass",
                "ubuntu20.04": "pass",
            },
            "0.111.0": {
                "linux": "pass",
                "mac": "pass",
                "windows": "pass",
                "centos7": "pass",
                "rockylinux8": "pass",
                "ubuntu20.04": "pass",
            },
        }

        planned = monitor_issues.plan_issues(
            versions=versions,
            results=results,
            open_issues=[],
            run_url="https://example.invalid/run",
            repository="baaaaaaaka/codex-helper",
            event_name="schedule",
            sha="abc123",
        )

        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0]["version"], "0.112.0")
        self.assertIn("windows", planned[0]["body"])
        self.assertIn("`fail`", planned[0]["body"])
        self.assertIn("https://example.invalid/run", planned[0]["body"])

    def test_plan_issues_treats_missing_platforms_as_not_run(self) -> None:
        planned = monitor_issues.plan_issues(
            versions=["0.113.0"],
            results={"0.113.0": {"linux": "pass"}},
            open_issues=[],
            run_url="",
            repository="",
            event_name="schedule",
            sha="",
        )

        self.assertEqual(len(planned), 1)
        self.assertIn("`mac`: `not-run`", planned[0]["body"])
        self.assertIn("`windows`: `not-run`", planned[0]["body"])
        self.assertIn("`centos7`: `not-run`", planned[0]["body"])

    def test_plan_issues_skips_duplicate_open_issue(self) -> None:
        version = "0.112.0"
        planned = monitor_issues.plan_issues(
            versions=[version],
            results={
                version: {
                    "linux": "fail",
                    "mac": "pass",
                    "windows": "pass",
                    "centos7": "pass",
                    "rockylinux8": "pass",
                    "ubuntu20.04": "pass",
                }
            },
            open_issues=[
                {
                    "title": monitor_issues.issue_title(version),
                    "body": monitor_issues.issue_marker(version),
                }
            ],
            run_url="",
            repository="",
            event_name="schedule",
            sha="",
        )

        self.assertEqual(planned, [])

    def test_load_results_ignores_invalid_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "linux-0.112.0.json").write_text(
                '{"platform":"linux","results":{"0.112.0":"pass"}}\n',
                encoding="utf-8",
            )
            (root / "junk.json").write_text("not json\n", encoding="utf-8")
            (root / "bad-platform.json").write_text(
                '{"platform":"plan9","results":{"0.112.0":"fail"}}\n',
                encoding="utf-8",
            )

            results = monitor_issues.load_results(root)

        self.assertEqual(results, {"0.112.0": {"linux": "pass"}})

    def test_script_cli_emits_issue_plan_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            results_dir = root / "results"
            results_dir.mkdir()
            (results_dir / "windows-0.112.0.json").write_text(
                '{"platform":"windows","results":{"0.112.0":"fail"}}\n',
                encoding="utf-8",
            )
            open_issues_path = root / "open-issues.json"
            open_issues_path.write_text("[]\n", encoding="utf-8")

            output = subprocess.check_output(
                [
                    sys.executable,
                    str(Path(__file__).resolve().parents[1] / "codex_monitor_issues.py"),
                    "--results-dir",
                    str(results_dir),
                    "--versions-json",
                    '["0.112.0"]',
                    "--open-issues-path",
                    str(open_issues_path),
                    "--run-url",
                    "https://example.invalid/run",
                    "--repository",
                    "baaaaaaaka/codex-helper",
                    "--event-name",
                    "schedule",
                    "--sha",
                    "abc123",
                ],
                text=True,
            )

        payload = json.loads(output)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["title"], monitor_issues.issue_title("0.112.0"))
        self.assertIn("`windows`: `fail`", payload[0]["body"])


if __name__ == "__main__":
    unittest.main()
