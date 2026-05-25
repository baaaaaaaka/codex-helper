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

import cxp_perf_model as model


class CXPPerfModelScriptTests(unittest.TestCase):
    def test_list_contains_expected_user_profiles_and_environments(self) -> None:
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = model.main(["list", "--json"])

        payload = json.loads(out.getvalue())
        profile_names = {profile["name"] for profile in payload["profiles"]}
        environment_names = {environment["name"] for environment in payload["environments"]}
        scenario_names = {scenario["name"] for scenario in payload["external_scenarios"]}
        self.assertEqual(code, 0)
        self.assertIn("light-user", profile_names)
        self.assertIn("many-short-chats", profile_names)
        self.assertIn("few-very-long-chats", profile_names)
        self.assertIn("many-long-chats", profile_names)
        self.assertIn("recovery-replay-user", profile_names)
        self.assertIn("multi-workspace-power-user", profile_names)
        self.assertIn("current-single-machine", environment_names)
        self.assertIn("multi-machine-handoff", environment_names)
        self.assertIn("legacy-schema-v1", environment_names)
        self.assertIn("all-ok-streaming", scenario_names)
        self.assertIn("graph-read-429", scenario_names)
        self.assertIn("graph-send-403", scenario_names)
        self.assertIn("service-helper-restart", scenario_names)

    def test_generate_writes_scaled_state_registry_history_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = model.main(
                    [
                        "generate",
                        "--out",
                        temp_dir,
                        "--profile",
                        "many-short-chats",
                        "--environment",
                        "multi-machine-handoff",
                        "--scale",
                        "0.05",
                    ]
                )

            summary = json.loads(out.getvalue())
            fixture_root = Path(temp_dir) / "many-short-chats" / "multi-machine-handoff"
            state = json.loads((fixture_root / "state.json").read_text(encoding="utf-8"))
            registry = json.loads((fixture_root / "registry.json").read_text(encoding="utf-8"))
            manifest = json.loads((fixture_root / "manifest.json").read_text(encoding="utf-8"))
            root_manifest = json.loads((Path(temp_dir) / "manifest.json").read_text(encoding="utf-8"))
            history_files = list((fixture_root / "history").glob("*.jsonl"))

        self.assertEqual(code, 0)
        self.assertEqual(summary["generated"], 1)
        self.assertEqual(state["schema_version"], 5)
        self.assertEqual(len(state["machines"]), 2)
        self.assertEqual(len(state["sessions"]), len(registry["sessions"]))
        self.assertGreaterEqual(len(state["sessions"]), 1)
        self.assertGreaterEqual(len(state["message_provenance"]), len(state["sessions"]))
        self.assertGreaterEqual(len(history_files), 1)
        self.assertEqual(manifest["environment"]["name"], "multi-machine-handoff")
        self.assertEqual(root_manifest["profiles"], ["many-short-chats"])

    def test_generate_legacy_environment_uses_legacy_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with contextlib.redirect_stdout(io.StringIO()):
                code = model.main(
                    [
                        "generate",
                        "--out",
                        temp_dir,
                        "--profile",
                        "light-user",
                        "--environment",
                        "legacy-schema-v1",
                    ]
                )
            state_path = Path(temp_dir) / "light-user" / "legacy-schema-v1" / "state.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertEqual(code, 0)
        self.assertEqual(state["schema_version"], 1)
        self.assertTrue(state["chat_polls"])

    def test_bench_uses_profile_specific_go_benchmark_regex(self) -> None:
        completed = subprocess.CompletedProcess(args=["go"], returncode=0)
        with mock.patch.object(model.subprocess, "run", return_value=completed) as run:
            with contextlib.redirect_stderr(io.StringIO()):
                code = model.main(
                    [
                        "bench",
                        "--target",
                        "listen-once",
                        "--profile",
                        "many-long-chats",
                        "--benchtime",
                        "1x",
                        "--count",
                        "1",
                    ]
                )

        cmd = run.call_args.args[0]
        self.assertEqual(code, 0)
        self.assertIn("-bench", cmd)
        self.assertIn("BenchmarkCXPPerfModelListenOnceProfiles/many-long-chats$", cmd)
        self.assertIn("-benchtime", cmd)
        self.assertIn("1x", cmd)

    def test_bench_uses_external_scenario_benchmark_regex(self) -> None:
        completed = subprocess.CompletedProcess(args=["go"], returncode=0)
        with mock.patch.object(model.subprocess, "run", return_value=completed) as run:
            with contextlib.redirect_stderr(io.StringIO()):
                code = model.main(
                    [
                        "bench",
                        "--target",
                        "external",
                        "--scenario",
                        "graph-read-429",
                        "--benchtime",
                        "1x",
                        "--count",
                        "1",
                    ]
                )

        cmd = run.call_args.args[0]
        self.assertEqual(code, 0)
        self.assertIn("BenchmarkCXPPerfModelExternalScenarios/graph-read-429$", cmd)

    def test_bench_supports_sqlite_daemon_target(self) -> None:
        completed = subprocess.CompletedProcess(args=["go"], returncode=0)
        with mock.patch.object(model.subprocess, "run", return_value=completed) as run:
            with contextlib.redirect_stderr(io.StringIO()):
                code = model.main(
                    [
                        "bench",
                        "--target",
                        "sqlite-daemon-queued-drain",
                        "--profile",
                        "many-long-chats",
                        "--benchtime",
                        "1x",
                        "--count",
                        "1",
                    ]
                )

        cmd = run.call_args.args[0]
        self.assertEqual(code, 0)
        self.assertIn("BenchmarkCXPPerfModelSQLiteDaemonQueuedTurnDrainProfiles/many-long-chats$", cmd)

    def test_bench_supports_sqlite_daemon_outbox_target(self) -> None:
        completed = subprocess.CompletedProcess(args=["go"], returncode=0)
        with mock.patch.object(model.subprocess, "run", return_value=completed) as run:
            with contextlib.redirect_stderr(io.StringIO()):
                code = model.main(
                    [
                        "bench",
                        "--target",
                        "sqlite-daemon-outbox-flush",
                        "--profile",
                        "ci-burst-user",
                        "--benchtime",
                        "1x",
                        "--count",
                        "1",
                    ]
                )

        cmd = run.call_args.args[0]
        self.assertEqual(code, 0)
        self.assertIn("BenchmarkCXPPerfModelSQLiteDaemonOutboxFlushProfiles/ci-burst-user$", cmd)

    def test_bench_supports_sqlite_hotspot_targets(self) -> None:
        completed = subprocess.CompletedProcess(args=["go"], returncode=0)
        for target, benchmark in [
            ("sqlite-deferred-inbound", "BenchmarkCXPPerfModelSQLiteDeferredInboundNoDeferredProfiles/many-long-chats$"),
            ("sqlite-history-watch-checkpoint", "BenchmarkCXPPerfModelSQLiteHistoryWatchCheckpointUpdateProfiles/many-long-chats$"),
            ("sqlite-history-watch-active", "BenchmarkCXPPerfModelSQLiteHistoryWatchActiveAppendProfiles/many-long-chats$"),
            ("sqlite-active-parked-main-loop", "BenchmarkCXPPerfModelSQLiteActiveParkedMainLoopProfiles/many-long-chats$"),
            ("sqlite-legacy-linked-transcript-backfilled-idle", "BenchmarkCXPPerfModelSQLiteLegacyLinkedTranscriptBackfilledIdleProfiles/many-long-chats$"),
            ("sqlite-invalid-workflow-notification-idle", "BenchmarkCXPPerfModelSQLiteInvalidWorkflowNotificationIdleTickProfiles/many-long-chats$"),
            (
                "sqlite-pending-workflow-notifications",
                "BenchmarkCXPPerfModelSQLiteSelectedSnapshotLargeColdStateProfiles/many-long-chats/pending-workflow-notifications$",
            ),
        ]:
            with self.subTest(target=target):
                with mock.patch.object(model.subprocess, "run", return_value=completed) as run:
                    with contextlib.redirect_stderr(io.StringIO()):
                        code = model.main(
                            [
                                "bench",
                                "--target",
                                target,
                                "--profile",
                                "many-long-chats",
                                "--benchtime",
                                "1x",
                                "--count",
                                "1",
                            ]
                        )

                cmd = run.call_args.args[0]
                self.assertEqual(code, 0)
                self.assertIn(benchmark, cmd)


if __name__ == "__main__":
    unittest.main()
