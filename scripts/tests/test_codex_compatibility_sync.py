from __future__ import annotations

import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class CodexCompatibilitySyncTests(unittest.TestCase):
    def test_refreshes_timestamp_when_status_is_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            table_path = root / "compatibility.md"
            results_dir = root / "results"
            results_dir.mkdir()

            table_path.write_text(
                "\n".join(
                    [
                        "# Codex Compatibility",
                        "",
                        "| Codex version | linux | mac | windows | rockylinux8 | ubuntu20.04 | last_tested_utc |",
                        "| --- | --- | --- | --- | --- | --- | --- |",
                        "| 0.112.0 | pass | pass | pass | pass | pass | 2026-03-01T00:00:00Z |",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            (results_dir / "linux-0.112.0.json").write_text(
                '{"platform":"linux","results":{"0.112.0":"pass"}}\n',
                encoding="utf-8",
            )

            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve().parents[1] / "codex_compatibility_sync.py"),
                    "--version",
                    "0.112.0",
                    "--results-dir",
                    str(results_dir),
                    "--table-path",
                    str(table_path),
                ],
                check=True,
            )

            updated = table_path.read_text(encoding="utf-8")

        self.assertIn("| 0.112.0 | pass | pass | pass | pass | pass |", updated)
        self.assertNotIn("2026-03-01T00:00:00Z", updated)
        self.assertRegex(updated, re.compile(r"2026-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"))


if __name__ == "__main__":
    unittest.main()
