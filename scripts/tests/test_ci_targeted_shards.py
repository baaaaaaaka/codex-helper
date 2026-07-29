import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


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
            "shard: [core, platform-integration, state-perf, ubuntu-stress, "
            "windows-skills-desktop, windows-codex-e2e]",
            job,
        )
        for os_name in ("macos-latest", "windows-latest"):
            self.assertIn(
                f"- os: {os_name}\n            shard: ubuntu-stress",
                job,
            )
        for os_name in ("ubuntu-latest", "macos-latest"):
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
                r"^        if: (?:always\(\) && )?matrix\.shard == '("
                r"core|platform-integration|state-perf|ubuntu-stress|"
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
            "Teams SQLite row-level migration regressions": "state-perf",
            "CXP preview SQLite correctness, concurrency, and write budgets": "state-perf",
            "CXP preview SQLite actual syscall write budget (Linux only)": "state-perf",
            "CXP cache v2 real NFS concurrency smoke (Linux only)": "state-perf",
            "Teams SQLite store migration and perf regressions": "state-perf",
            "Teams perf benchmark smoke": "state-perf",
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

    def test_state_perf_keeps_fixture_and_runtime_commands_together(self):
        blocks = step_blocks(targeted_job())
        state = blocks["Teams SQLite store migration and perf regressions"]
        self.assertIn("git fetch --force --tags --prune origin", state)
        self.assertIn("CODEX_HELPER_REQUIRE_RELEASE_TAG_FIXTURES=1", state)
        self.assertIn("OfficialRelease(StoresUpgradeToCurrent|FixtureListCoversStableTags)", state)
        self.assertIn("SubprocessMigrationStressCI", state)
        self.assertIn("BenchmarkSQLiteManualWALCheckpointHotWrite", state)
        self.assertIn("TestCXPPerfModelSQLite", state)
        self.assertIn("BenchmarkCXPPerfModelSQLiteRealisticMixedUserWALSpikeBreakdown", state)
        self.assertIn("Benchmark(GlobalOutboundLedgerRecord|GlobalInboundLedgerClaim|ControlChatHistoryAppend)", state)

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
