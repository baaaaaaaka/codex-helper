import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
SQLITE_RUNTIME_SHARD = ROOT / "scripts" / "ci" / "teams_sqlite_runtime_shard.sh"


def targeted_job() -> str:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    start = workflow.index("  targeted-test:\n")
    end = workflow.index("  codex-runtime-contract:\n", start)
    return workflow[start:end]


def step_blocks(job: str) -> dict[str, str]:
    matches = list(re.finditer(r"^      - name: (.+)$", job, re.MULTILINE))
    blocks: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(job)
        blocks[match.group(1)] = job[match.start() : end]
    return blocks


class TargetedShardWorkflowTests(unittest.TestCase):
    def test_declares_parallel_shards_and_limits_platform_only_shards(self):
        job = targeted_job()
        self.assertIn(
            "shard: [core, platform-integration, state-migration, "
            "state-store-perf, state-runtime-perf, ubuntu-stress, "
            "windows-state-runtime-tests, windows-state-mixed-bench, "
            "windows-state-ledger-bench, "
            "windows-skills-desktop, windows-codex-e2e]",
            job,
        )
        for os_name in ("macos-latest", "windows-latest"):
            self.assertIn(
                f"- os: {os_name}\n            shard: ubuntu-stress",
                job,
            )
        self.assertIn(
            "- os: windows-latest\n            shard: state-runtime-perf",
            job,
        )
        for os_name in ("ubuntu-latest", "macos-latest"):
            for shard in (
                "windows-state-runtime-tests",
                "windows-state-mixed-bench",
                "windows-state-ledger-bench",
            ):
                self.assertIn(
                    f"- os: {os_name}\n            shard: {shard}",
                    job,
                )
            self.assertIn(
                f"- os: {os_name}\n            shard: windows-skills-desktop",
                job,
            )
            self.assertIn(
                f"- os: {os_name}\n            shard: windows-codex-e2e",
                job,
            )
        self.assertNotIn("needs:", job)

    def test_every_non_setup_step_selects_exactly_one_shard(self):
        for name, block in step_blocks(targeted_job()).items():
            if name in {"Checkout", "Setup Go"}:
                continue
            matches = re.findall(
                r"^        if: matrix\.shard == '("
                r"core|platform-integration|state-migration|"
                r"state-store-perf|state-runtime-perf|ubuntu-stress|"
                r"windows-state-runtime-tests|windows-state-mixed-bench|"
                r"windows-state-ledger-bench|"
                r"windows-skills-desktop|windows-codex-e2e"
                r")'(?: && .+)?$",
                block,
                re.MULTILINE,
            )
            self.assertEqual(len(matches), 1, name)

    def test_heavy_steps_are_assigned_to_expected_shards(self):
        blocks = step_blocks(targeted_job())
        expected = {
            "Teams recreate and full-history race regressions (Linux only)": "ubuntu-stress",
            "Cross-compile check (Linux only)": "ubuntu-stress",
            "Teams Graph 429 stress (Linux only)": "ubuntu-stress",
            "Teams SQLite row-level runtime regressions": "state-store-perf",
            "CXP preview SQLite correctness, concurrency, and write budgets": "state-store-perf",
            "CXP preview SQLite actual syscall write budget (Linux only)": "state-store-perf",
            "CXP cache v2 real NFS concurrency smoke (Linux only)": "state-store-perf",
            "Teams SQLite schema and path migration regressions": "state-migration",
            "Teams SQLite store runtime and perf regressions": "state-store-perf",
            "Teams SQLite bridge runtime and perf regressions (Linux/macOS)": "state-runtime-perf",
            "Teams SQLite bridge runtime regressions (Windows)": "windows-state-runtime-tests",
            "Teams SQLite mixed-write benchmarks (Windows)": "windows-state-mixed-bench",
            "Teams SQLite ledger benchmarks (Windows)": "windows-state-ledger-bench",
            "Teams perf benchmark smoke": "state-runtime-perf",
            "Skills local git smoke (Windows)": "windows-skills-desktop",
            "Codex desktop app network install smoke (Windows)": "windows-skills-desktop",
            "Install Codex for integration (Windows)": "windows-codex-e2e",
            "Teams app-server probe (Windows)": "windows-codex-e2e",
            "Codex upgrade integration (system npm, Windows)": "windows-codex-e2e",
            "Codex upgrade integration (local npm, Windows)": "windows-codex-e2e",
            "Teams target-account PATH Codex upgrade (Windows)": "windows-codex-e2e",
            "Codex approval, history, and cancellation runtime integration (Windows)": "windows-codex-e2e",
            "Native managed-node install integration (Windows)": "windows-codex-e2e",
        }
        for name, shard in expected.items():
            self.assertIn(
                f"if: matrix.shard == '{shard}'",
                blocks[name],
                name,
            )

    def test_state_migration_fixture_setup_is_isolated_to_migration_shard(self):
        blocks = step_blocks(targeted_job())
        migration = blocks["Teams SQLite schema and path migration regressions"]
        self.assertIn("git fetch --force --tags --prune origin", migration)
        self.assertIn("CODEX_HELPER_REQUIRE_RELEASE_TAG_FIXTURES=1", migration)
        self.assertIn("OfficialRelease(StoresUpgradeToCurrent|FixtureListCoversStableTags)", migration)
        self.assertIn("SubprocessMigrationStressCI", migration)

        for name in (
            "Teams SQLite store runtime and perf regressions",
            "Teams SQLite bridge runtime and perf regressions (Linux/macOS)",
            "Teams SQLite bridge runtime regressions (Windows)",
            "Teams SQLite mixed-write benchmarks (Windows)",
            "Teams SQLite ledger benchmarks (Windows)",
        ):
            self.assertNotIn("git fetch --force --tags --prune origin", blocks[name])
            self.assertNotIn("CODEX_HELPER_REQUIRE_RELEASE_TAG_FIXTURES", blocks[name])

    def test_state_runtime_commands_remain_in_their_owning_shards(self):
        blocks = step_blocks(targeted_job())
        store = blocks["Teams SQLite store runtime and perf regressions"]
        self.assertIn("BenchmarkSQLiteManualWALCheckpointHotWrite", store)
        self.assertIn("SQLiteRecordTranscript", store)

        runtime = blocks["Teams SQLite bridge runtime and perf regressions (Linux/macOS)"]
        self.assertIn("teams_sqlite_runtime_shard.sh all", runtime)
        self.assertIn(
            "teams_sqlite_runtime_shard.sh tests",
            blocks["Teams SQLite bridge runtime regressions (Windows)"],
        )
        self.assertIn(
            "teams_sqlite_runtime_shard.sh mixed-bench",
            blocks["Teams SQLite mixed-write benchmarks (Windows)"],
        )
        self.assertIn(
            "teams_sqlite_runtime_shard.sh ledger-bench",
            blocks["Teams SQLite ledger benchmarks (Windows)"],
        )

        script = SQLITE_RUNTIME_SHARD.read_text(encoding="utf-8")
        self.assertIn("run_runtime_tests", script)
        self.assertIn("run_mixed_write_benchmarks", script)
        self.assertIn("run_ledger_benchmarks", script)
        self.assertIn("BenchmarkCXPPerfModelSQLiteRealisticMixedUserWALSpikeBreakdown", script)
        self.assertIn("BenchmarkCXPPerfModelSQLiteRegistryProjectionRetentionChurn", script)
        self.assertIn(
            "Benchmark(GlobalOutboundLedgerRecord|GlobalInboundLedgerClaim|ControlChatHistoryAppend)",
            script,
        )

    def test_windows_codex_e2e_installs_before_runtime_consumers(self):
        job = targeted_job()
        ordered_steps = [
            "Install Codex for integration (Windows)",
            "Teams app-server probe (Windows)",
            "Codex upgrade integration (system npm, Windows)",
            "Codex upgrade integration (local npm, Windows)",
            "Teams target-account PATH Codex upgrade (Windows)",
            "Codex approval, history, and cancellation runtime integration (Windows)",
            "Native managed-node install integration (Windows)",
        ]
        positions = [job.index(f"- name: {name}") for name in ordered_steps]
        self.assertEqual(positions, sorted(positions))


if __name__ == "__main__":
    unittest.main()
