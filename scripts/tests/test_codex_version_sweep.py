from __future__ import annotations

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import codex_version_sweep as sweep


class CodexVersionSweepTests(unittest.TestCase):
    def test_parse_versions_arg_accepts_json_and_separators(self) -> None:
        self.assertEqual(sweep.parse_versions_arg('["v0.112.0", "0.113.0-alpha.1"]'), ["0.112.0", "0.113.0-alpha.1"])
        self.assertEqual(sweep.parse_versions_arg("v0.112.0,\n0.111.0  0.110.0"), ["0.112.0", "0.111.0", "0.110.0"])

    def test_select_versions_filters_prerelease_and_range(self) -> None:
        selected = sweep.select_versions(
            versions_arg="0.110.0 0.111.0-alpha.1 0.111.0 0.112.0",
            min_version="0.111.0-alpha.1",
            max_version="0.112.0",
            include_prerelease=False,
        )

        self.assertEqual(selected, ["0.111.0", "0.112.0"])

    def test_list_only_prints_selected_versions_json(self) -> None:
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = sweep.main(["--versions", "0.112.0,0.111.0", "--list-only"])

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out.getvalue()), ["0.111.0", "0.112.0"])

    def test_run_smoke_for_version_reports_install_failure(self) -> None:
        failed = subprocess.CompletedProcess(args=["npm"], returncode=42, stdout="", stderr="install failed\n")
        with mock.patch.object(sweep.subprocess, "run", return_value=failed) as run:
            result = sweep.run_smoke_for_version("0.112.0", repo_root=Path.cwd())

        install_cmd = run.call_args_list[0].args[0]
        self.assertIn("--include=optional", install_cmd)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["install_rc"], 42)
        self.assertIn("install failed", result["stderr_tail"])
        self.assertIsNone(result["native_resolver_rc"])
        self.assertIsNone(result["contract_rc"])
        self.assertIsNone(result["cli_rc"])
        self.assertIsNone(result["runtime_rc"])
        self.assertIsInstance(result["duration_seconds"], float)

    def test_contract_failure_stops_before_cli_and_runtime(self) -> None:
        completed = [
            subprocess.CompletedProcess(args=["npm"], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=["native"], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=["contract"], returncode=9, stdout="schema changed", stderr=""),
        ]
        with mock.patch.object(sweep.subprocess, "run", side_effect=completed) as run:
            result = sweep.run_smoke_for_version("0.115.0", repo_root=Path.cwd())

        self.assertEqual(run.call_count, 3)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["native_resolver_rc"], 0)
        self.assertEqual(result["contract_rc"], 9)
        self.assertIsNone(result["cli_rc"])
        self.assertIsNone(result["runtime_rc"])
        self.assertIn("schema changed", result["stderr_tail"])

    def test_success_runs_native_contract_cli_and_runtime_in_order(self) -> None:
        completed = [
            subprocess.CompletedProcess(args=["npm"], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=["native"], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=["contract"], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=["cli"], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=["runtime"], returncode=0, stdout="", stderr=""),
        ]
        with mock.patch.object(sweep.subprocess, "run", side_effect=completed) as run:
            result = sweep.run_smoke_for_version("0.116.0", repo_root=Path.cwd())

        commands = [call.args[0] for call in run.call_args_list[1:]]
        self.assertIn("internal/codexbinary", commands[0])
        self.assertIn("internal/codexcontract", commands[1])
        self.assertIn("internal/cli", commands[2])
        self.assertIn("internal/codexrunner", commands[3])
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["runtime_rc"], 0)

    def test_output_json_updates_incrementally(self) -> None:
        results = [
            {"version": "0.111.0", "status": "pass", "native_resolver_rc": 0, "contract_rc": 0, "cli_rc": 0, "runtime_rc": 0, "duration_seconds": 0.1},
            {"version": "0.112.0", "status": "fail", "native_resolver_rc": 0, "contract_rc": 1, "cli_rc": None, "runtime_rc": None, "duration_seconds": 0.2},
        ]

        def fake_smoke(version: str, *, repo_root: Path) -> dict[str, object]:
            self.assertEqual(repo_root, Path.cwd().resolve())
            return results.pop(0)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_json = Path(temp_dir) / "summary.json"
            with mock.patch.object(sweep, "run_smoke_for_version", side_effect=fake_smoke):
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    code = sweep.main(["--versions", "0.111.0 0.112.0", "--output-json", str(output_json)])

            payload = json.loads(output_json.read_text(encoding="utf-8"))

        self.assertEqual(code, 1)
        self.assertEqual(payload["completed"], 2)
        self.assertEqual(payload["remaining"], 0)
        self.assertEqual(payload["passed"], 1)
        self.assertEqual(payload["failed"], 1)


if __name__ == "__main__":
    unittest.main()
