import json
import pathlib
import re
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
TEAMS_RUNTIME_SHARD = ROOT / "scripts" / "tests" / "run_teams_runtime_safety_shard.sh"
OWNERSHIP_STRESS_TESTS = ROOT / "internal" / "teams" / "ownership_stress_ci_test.go"
FULL_GO_TEST_SHARDS = ROOT / "scripts" / "ci" / "run_full_go_test_shards.go"


def targeted_job() -> str:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    start = workflow.index("  targeted-test:\n")
    end = workflow.index("  teams-recovery-test:\n", start)
    return workflow[start:end]


def step_blocks(job: str) -> dict[str, str]:
    matches = list(re.finditer(r"^      - name: (.+)$", job, re.MULTILINE))
    blocks: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(job)
        blocks[match.group(1)] = job[match.start() : end]
    return blocks


class TargetedShardWorkflowTests(unittest.TestCase):
    def test_full_runner_partitions_cover_every_runnable_job_exactly_once(self):
        def plan(*, race: bool, partition_count: int, partition_index: int) -> list[str]:
            command = [
                "go",
                "run",
                "./scripts/ci/run_full_go_test_shards.go",
                "-timeout=30m",
                "-parallel=16",
                "-shards=16",
            ]
            if race:
                command.append("-race")
            command.extend(
                [
                    f"-partition-count={partition_count}",
                    f"-partition-index={partition_index}",
                    "-list-only",
                ]
            )
            completed = subprocess.run(
                command,
                cwd=ROOT,
                check=True,
                text=True,
                capture_output=True,
            )
            return [line for line in completed.stdout.splitlines() if ": go " in line]

        for race, partition_count in ((False, 2), (True, 4)):
            with self.subTest(race=race, partition_count=partition_count):
                complete_plan = plan(
                    race=race,
                    partition_count=1,
                    partition_index=0,
                )
                self.assertTrue(complete_plan)
                self.assertTrue(
                    any(
                        "TestTeamsMainLoopOutboxFairnessCursorWalksPastDistinctChatScanPrefix" in job
                        for job in complete_plan
                    )
                )
                if len(complete_plan) != len(set(complete_plan)):
                    self.fail("unpartitioned full-test plan contains duplicate jobs")
                for job in complete_plan:
                    if ' "-run" ' in job or ' "-skip" ' in job:
                        self.assertNotRegex(job, r' "-(?:run|skip)" ""')
                partition_plan_by_index = [
                    plan(
                        race=race,
                        partition_count=partition_count,
                        partition_index=partition_index,
                    )
                    for partition_index in range(partition_count)
                ]
                partition_plans = [job for jobs in partition_plan_by_index for job in jobs]
                self.assertEqual(len(partition_plans), len(complete_plan))
                self.assertEqual(len(partition_plans), len(set(partition_plans)))
                self.assertEqual(set(partition_plans), set(complete_plan))
                def metadata(job: str) -> tuple[int, bool]:
                    match = re.search(
                        r" # estimated-weight=(\d+) estimated-exclusive=(true|false)$", job
                    )
                    self.assertIsNotNone(match, job)
                    return int(match.group(1)), match.group(2) == "true"

                for job in partition_plans:
                    self.assertGreater(metadata(job)[0], 0, job)
                exclusive_loads = []
                parallel_spans = []
                total_loads = []
                critical_paths = []
                for partition_jobs in partition_plan_by_index:
                    self.assertTrue(partition_jobs)
                    exclusive = [metadata(job)[0] for job in partition_jobs if metadata(job)[1]]
                    parallel = [metadata(job)[0] for job in partition_jobs if not metadata(job)[1]]
                    exclusive_loads.append(sum(exclusive))
                    total_loads.append(sum(exclusive) + sum(parallel))
                    worker_loads = [0, 0, 0, 0]
                    for weight in sorted(parallel, reverse=True):
                        target = min(range(len(worker_loads)), key=worker_loads.__getitem__)
                        worker_loads[target] += weight
                    parallel_spans.append(max(worker_loads))
                    critical_paths.append(sum(exclusive) + max(worker_loads))
                self.assertLessEqual(
                    max(exclusive_loads) - min(exclusive_loads),
                    max(1, max(exclusive_loads) // 10),
                    f"exclusive phase estimate is skewed: {exclusive_loads}",
                )
                if race and partition_count == 4:
                    # There are 16 large Teams shards and four workers per
                    # runner. One runner must receive a fifth shard; assert
                    # that this is only one extra wave instead of requiring
                    # an impossible equal span across all partitions.
                    largest_parallel = max(metadata(job)[0] for job in partition_plans if not metadata(job)[1])
                    self.assertLessEqual(
                        max(parallel_spans),
                        2 * largest_parallel,
                        f"parallel phase exceeds two largest-job waves: {parallel_spans}",
                    )
                else:
                    self.assertLessEqual(
                        max(parallel_spans) - min(parallel_spans),
                        max(1, max(parallel_spans) // 10),
                        f"parallel phase estimate is skewed: {parallel_spans}",
                    )
                self.assertLessEqual(
                    max(total_loads) - min(total_loads),
                    max(1, max(total_loads) // 10),
                    f"estimated total work is skewed: {total_loads}",
                )
                if race and partition_count == 4:
                    # A fifth heavy job is unavoidable with 17 heavy jobs and
                    # four workers per runner. Keep the bound explicit: no
                    # partition may grow beyond two largest-job waves plus its
                    # already-balanced exclusive phase.
                    largest_parallel = max(metadata(job)[0] for job in partition_plans if not metadata(job)[1])
                    self.assertLessEqual(
                        max(critical_paths),
                        max(exclusive_loads) + 2 * largest_parallel,
                        f"estimated critical path exceeds two largest-job waves: {critical_paths}",
                    )
                else:
                    self.assertLessEqual(
                        max(critical_paths) - min(critical_paths),
                        max(1, max(critical_paths) // 10),
                        f"estimated critical path is skewed: {critical_paths}",
                    )

    def test_direct_isolated_lane_reports_run_and_pass(self):
        test_name = "TestTeamsMainLoopOutboxFairnessCursorWalksPastDistinctChatScanPrefix"
        for race in (False, True):
            command = ["go", "test", "./internal/teams"]
            if race:
                command.append("-race")
            command.extend(
                [
                    "-count=1",
                    "-timeout=5m",
                    "-run",
                    f"^{test_name}$",
                    "-json",
                ]
            )
            completed = subprocess.run(
                command,
                cwd=ROOT,
                check=True,
                text=True,
                capture_output=True,
            )
            events = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
            for suffix in ("json", "sqlite"):
                subtest_name = f"{test_name}/{suffix}"
                self.assertTrue(any(event.get("Action") == "run" and event.get("Test") == subtest_name for event in events))
                self.assertTrue(any(event.get("Action") == "pass" and event.get("Test") == subtest_name for event in events))

    def test_teams_runtime_safety_uses_one_shared_shard_definition(self):
        script = TEAMS_RUNTIME_SHARD.read_text(encoding="utf-8")
        for shard in (
            "isolation",
            "store",
            "store-io",
            "store-process",
            "service-update",
            "wsl-process",
            "diagnostics",
            "windows",
        ):
            self.assertIn(f"  {shard})", script)
            call = f"bash scripts/tests/run_teams_runtime_safety_shard.sh {shard}"
            self.assertIn(call, WORKFLOW.read_text(encoding="utf-8"))
            self.assertIn(call, RELEASE_WORKFLOW.read_text(encoding="utf-8"))

        for workflow in (WORKFLOW, RELEASE_WORKFLOW):
            text = workflow.read_text(encoding="utf-8")
            self.assertNotRegex(
                text,
                r"go test .*TestTeamsRuntimeSafety",
                f"{workflow.name} duplicated a Teams runtime-safety regex",
            )

    def test_declares_parallel_shards_and_limits_platform_only_shards(self):
        job = targeted_job()
        self.assertIn(
            "shard: [core, core-b, platform-integration, state-perf, ubuntu-stress, "
            "windows-skills-desktop, windows-skills-desktop-b, windows-codex-e2e]",
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
                f"- os: {os_name}\n            shard: windows-skills-desktop-b",
                job,
            )
            self.assertIn(
                f"- os: {os_name}\n            shard: windows-codex-e2e",
                job,
            )
        self.assertNotIn("needs:", job)

    def test_state_perf_partitions_only_the_expensive_sequence(self):
        job = targeted_job()
        self.assertIn("partition: [0]", job)
        for os_name in ("ubuntu-latest", "macos-latest", "windows-latest"):
            self.assertIn(
                f"- os: {os_name}\n            shard: state-perf\n            partition: 1",
                job,
            )
        blocks = step_blocks(job)
        expensive = blocks["Teams SQLite store migration and perf regressions"]
        self.assertIn("if: matrix.shard == 'state-perf'", expensive)
        self.assertNotIn("matrix.partition == 0", expensive)
        for name, block in blocks.items():
            if name == "Teams SQLite store migration and perf regressions":
                continue
            if "matrix.shard == 'state-perf'" in block:
                self.assertIn("matrix.partition == 0", block, name)

    def test_recovery_manifest_runs_in_parallel_platform_mode_jobs(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        start = workflow.index("  teams-recovery-test:\n")
        end = workflow.index("  codex-runtime-contract:\n", start)
        job = workflow[start:end]
        self.assertIn(
            "name: Teams transcript recovery (${{ matrix.os }} / ${{ matrix.mode }} / partition ${{ matrix.partition }})",
            job,
        )
        self.assertIn("os: [ubuntu-latest, macos-latest, windows-latest]", job)
        self.assertIn("mode: [normal, race]", job)
        self.assertIn("partition: [0, 1]", job)
        self.assertIn("- os: ubuntu-latest\n            partition: 1", job)
        self.assertIn("- os: macos-latest\n            partition: 1", job)
        self.assertIn(
            "go run ./scripts/ci/check_teams_recovery_manifest.go -job teams-recovery -list-only",
            job,
        )
        self.assertIn(
            "partition_flags=(\"-partition-count=2\" \"-partition-index=${{ matrix.partition }}\")",
            job,
        )
        self.assertIn(
            "go run ./scripts/ci/check_teams_recovery_manifest.go -job teams-recovery \"${partition_flags[@]}\"",
            job,
        )
        self.assertIn(
            "go run ./scripts/ci/check_teams_recovery_manifest.go -job teams-recovery -race \"${partition_flags[@]}\"",
            job,
        )
        self.assertNotIn("Teams transcript recovery state-machine regressions", targeted_job())
        aggregate_start = workflow.index("  test:\n")
        aggregate = workflow[aggregate_start:]
        self.assertIn("      - teams-recovery-test\n", aggregate)
        self.assertIn(
            'check teams-recovery-test "${{ needs.teams-recovery-test.result }}"',
            aggregate,
        )

    def test_recovery_jobs_keep_phase_diagnostics_and_resource_contract(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        start = workflow.index("  teams-recovery-test:\n")
        end = workflow.index("  codex-runtime-contract:\n", start)
        job = workflow[start:end]
        self.assertIn("CODEX_HELPER_CI_PHASE_DIR: ${{ runner.temp }}/teams-recovery-phases", job)
        self.assertIn("name: Upload Teams recovery phase diagnostics", job)
        self.assertIn("if: always()", job)
        self.assertIn("if-no-files-found: ignore", job)

        manifest = json.loads((ROOT / "scripts" / "ci" / "teams_recovery_tests.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["version"], 2)
        allowed = {"pure_cpu", "listener_async", "sqlite_fsync", "host_exclusive"}
        self.assertTrue(manifest["tests"])
        for item in manifest["tests"]:
            self.assertIn(item.get("resource_class"), allowed, item["name"])

    def test_frontier_reopen_fixture_has_durable_io_budget_and_exclusive_phase(self):
        manifest = json.loads((ROOT / "scripts" / "ci" / "teams_recovery_tests.json").read_text(encoding="utf-8"))
        item = next(
            entry
            for entry in manifest["tests"]
            if entry["name"] == "TestTeamsListenFalsePollFrontierSurvivesStoreReopenAndOwnerTakeover"
        )
        self.assertEqual(item["backends"], ["json", "sqlite"])
        self.assertEqual(item["resource_class"], "sqlite_fsync")
        self.assertTrue(item["exclusive"])
        self.assertGreaterEqual(item["max_seconds"], 180)

    def test_long_full_suite_jobs_use_independent_runner_partitions(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        full_start = workflow.index("  full-go-test:\n")
        full_end = workflow.index("  race-test:\n", full_start)
        full = workflow[full_start:full_end]
        self.assertIn(
            "name: Full go test (${{ matrix.os }} / partition ${{ matrix.partition }})",
            full,
        )
        self.assertIn("partition: [0, 1]", full)
        self.assertIn("- os: ubuntu-latest\n            partition: 1", full)
        self.assertIn(
            "-partition-count=2 -partition-index=\"${{ matrix.partition }}\"",
            full,
        )

        race_start = workflow.index("  race-test:\n")
        race_end = workflow.index("  runtime-env-contract:\n", race_start)
        race = workflow[race_start:race_end]
        self.assertIn(
            "name: Race test (ubuntu-latest / partition ${{ matrix.partition }})",
            race,
        )
        self.assertIn("partition: [0, 1, 2, 3]", race)
        self.assertIn(
            "-partition-count=4 -partition-index=\"${{ matrix.partition }}\"",
            race,
        )

    def test_full_runner_keeps_new_listener_liveness_families_isolated(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        self.assertIn("func autoIsolatedRunnableName", runner)
        self.assertIn(
            'strings.HasPrefix(name, "TestTeamsListenFalse")',
            runner,
        )
        self.assertIn(
            'strings.HasPrefix(name, "TestTeamsMainLoopOutbox")',
            runner,
        )
        self.assertIn(
            'strings.HasPrefix(name, "TestTeamsOwnershipStress")',
            runner,
        )
        self.assertIn(
            'strings.HasPrefix(name, "TestTeamsGraph429Stress")',
            runner,
        )
        for name in (
            "TestProxyStartBackgroundReapsExitedDetachedChild",
            "TestStartCodexAppProxyDaemonReapsExitedDetachedChild",
        ):
            self.assertRegex(runner, rf'"{name}"\s*:\s*true')
        self.assertIn("runnableIsolationMap(packageName, names)", runner)
        self.assertIn("runnableExclusivityMap(packageName, names)", runner)

    def test_outbox_fixtures_keep_process_isolation_without_host_exclusivity(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        isolated_start = runner.index("func autoIsolatedRunnableName")
        isolated_end = runner.index("func isCodexRunnerPackage", isolated_start)
        isolated = runner[isolated_start:isolated_end]
        self.assertIn('strings.HasPrefix(name, "TestTeamsMainLoopOutbox")', isolated)

        exclusive_start = runner.index("func autoExclusiveRunnableName")
        exclusive_end = runner.index("func isCodexRunnerPackage", exclusive_start)
        exclusive = runner[exclusive_start:exclusive_end]
        self.assertNotIn('strings.HasPrefix(name, "TestTeamsMainLoopOutbox")', exclusive)
        self.assertRegex(runner, r'"TestTeamsMainLoopOutboxFairnessCursorWalksPastDistinctChatScanPrefix"\s*:\s*true')
        self.assertRegex(runner, r'"TestTeamsMainLoopOutboxLedgerFailureDoesNotStarveHealthyTail"\s*:\s*true')
        self.assertIn("Process isolation protects package globals", runner)
        self.assertIn("if autoExclusiveRunnableName(packageName, name)", runner)

    def test_ubuntu_package_bootstrap_uses_source_isolated_update(self):
        targeted = targeted_job()
        self.assertIn("sudo bash scripts/ci/apt_update.sh", targeted)
        release = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("sudo bash scripts/ci/apt_update.sh", release)

    def test_full_runner_isolates_process_tree_lifecycle_family(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        self.assertIn("isCodexRunnerPackage(packageName)", runner)
        self.assertIn(
            'strings.HasPrefix(name, "TestAppServerProcessCloseTerminates")',
            runner,
        )
        self.assertIn(
            "Ordinary packages may still contain a small, reviewed family",
            runner,
        )
        self.assertIn(
            "exclusive := runnableExclusivityMap(packageName, names)",
            runner,
        )

    def test_full_runner_isolates_async_cache_and_audience_budget_fixtures(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        self.assertIn(
            'strings.HasPrefix(name, "TestTeamsThirdPartyCacheStress")',
            runner,
        )
        self.assertIn(
            'name == "TestTeamsWorkChatAudienceLookupUsesPollBudget"',
            runner,
        )
        self.assertIn("Cache-stress and audience-admission", runner)

    def test_full_runner_isolates_cross_backend_legacy_owner_fixture(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        fixture_name = "TestStoreOwnerBindsLegacyQueuedTurnAndRejectsPreviousOwnerCallbacks"
        self.assertEqual(
            runner.count(f'"{fixture_name}"'),
            2,
            f"{fixture_name} must be both process-isolated and host-exclusive",
        )

    def test_ci_serializes_superseded_runs_and_keeps_failure_evidence(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "concurrency:\n  group: ci-${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}\n  cancel-in-progress: true",
            workflow,
        )
        full_start = workflow.index("  full-go-test:\n")
        full_end = workflow.index("  race-test:\n", full_start)
        full = workflow[full_start:full_end]
        self.assertIn('tail -n +2 "$isolated_profile" >> coverage.out', full)
        self.assertIn("migration_process_pattern='^TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup$'", full)
        self.assertIn("-skip \"$isolated_skip_pattern\"", full)
        self.assertIn('go test ./internal/cli -timeout=2m -parallel=16 -count=1 -run "$migration_process_pattern"', full)
        self.assertIn("Upload full-suite diagnostics", full)
        self.assertNotIn("full-go-test-cli-retry", full)
        self.assertNotIn("isolated internal/cli retry", full)

        race_start = workflow.index("  race-test:\n")
        race_end = workflow.index("  runtime-env-contract:\n", race_start)
        race = workflow[race_start:race_end]
        self.assertIn('tee "$RUNNER_TEMP/race-test.log"', race)
        self.assertIn("Upload race diagnostics", race)

        aggregate = workflow[workflow.index("  test:\n") :]
        self.assertIn("    name: Test\n", aggregate)
        self.assertNotIn("name: Test (${{ matrix.os }})", aggregate)

        windows_start = workflow.index("  windows-proxy-lifecycle:\n")
        windows_end = workflow.index("  proxy-recovery-docker:\n", windows_start)
        windows = workflow[windows_start:windows_end]
        self.assertIn(
            "go test -p=1 ./internal/helperruntime -count=1 -run '^TestRuntimeProcessIdentityWindows$' -v",
            windows,
        )

    def test_partition_flags_have_exactly_once_runner_selection_support(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        self.assertIn('flag.Int("partition-count", 1', runner)
        self.assertIn('flag.Int("partition-index", 0', runner)
        self.assertIn("jobs = partitionTestJobs(jobs, *partitionCount, *partitionIndex)", runner)
        self.assertIn("func partitionTestJobs", runner)

        manifest = (ROOT / "scripts" / "ci" / "check_teams_recovery_manifest.go").read_text(encoding="utf-8")
        self.assertIn('flag.Int("partition-count", 1', manifest)
        self.assertIn('flag.Int("partition-index", 0', manifest)
        self.assertIn("selected = partitionManifestTests(selected, *partitionCount, *partitionIndex)", manifest)
        self.assertIn("func partitionManifestTests", manifest)

    def test_full_go_runner_isolates_listener_liveness_fixture_from_shard_pool(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        fixtures = (
            "TestTeamsListenFalseGraphWorkerSaturationPreservesHealthyPoll",
            "TestTeamsListenFalseGraphHeadFailureDoesNotStarveHealthyTail",
            "TestTeamsListenFalseLinkedTranscriptFullPoolDoesNotStarveHealthyTail",
            "TestTeamsListenFalseStartupHeartbeatProtectsSlowInitialization",
        )
        for fixture_name in fixtures:
            fixture = f'"{fixture_name}"'
            self.assertEqual(
                runner.count(fixture),
                2,
                f"{fixture_name} must be both process-isolated and host-exclusive",
            )

    def test_full_go_runner_isolates_cli_process_group_fixture(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        fixture_name = "TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup"
        self.assertEqual(
            runner.count(f'"{fixture_name}"'),
            2,
            f"{fixture_name} must be both process-isolated and host-exclusive",
        )

    def test_full_go_runner_isolates_app_gateway_daemon_fixtures(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        fixture_names = (
            "TestRunAppGatewayDaemonKeepsStableFrontendWhileBackendRuns",
            "TestRunAppGatewayDaemonDoesNotConsumeLegacyBlockedBudget",
            "TestRunAppGatewayDaemonModernStandbyDNSGapThenRecoveryKeepsClientPort",
            "TestRunAppGatewayDaemonBoundsBackendRecoveryBeforeCooldown",
            "TestRunAppGatewayDaemonBackendSwapKeepsFrontendPort",
            "TestRunAppGatewayDaemonRestartReusesStablePort",
        )
        for fixture_name in fixture_names:
            self.assertEqual(
                runner.count(f'"{fixture_name}"'),
                2,
                f"{fixture_name} must be both process-isolated and host-exclusive",
            )
        self.assertIn('strings.HasSuffix(packageName, "/internal/cli")', runner)
        self.assertIn('"-skip"', runner)

    def test_every_non_setup_step_selects_exactly_one_shard(self):
        for name, block in step_blocks(targeted_job()).items():
            if name in {"Checkout", "Setup Go"}:
                continue
            matches = re.findall(
                r"^        if: (?:always\(\) && )?matrix\.shard == '("
                r"core|core-b|platform-integration|state-perf|ubuntu-stress|"
                r"windows-skills-desktop|windows-skills-desktop-b|windows-codex-e2e"
                r")'(?: && .+)?$",
                block,
                re.MULTILINE,
            )
            self.assertEqual(len(matches), 1, name)

    def test_heavy_steps_are_assigned_to_expected_shards(self):
        blocks = step_blocks(targeted_job())
        expected = {
            "CXP TUI preview, navigation, refresh, and thread-name regressions": "core-b",
            "Codex streaming transcript visibility regressions": "core-b",
            "Teams Codex runner classification regressions": "core-b",
            "Teams metadata-only resume and final integrity regressions": "core-b",
            "Teams user-marker compact and fallback regressions": "core-b",
            "Teams thread recovery and self-echo regressions": "core-b",
            "Teams frontier recovery and fenced-page regressions": "core-b",
            "Teams bridge scheduling and history sync regressions": "core-b",
            "Teams recreate and full-history race regressions (Linux only)": "ubuntu-stress",
            "Cross-compile check (Linux only)": "ubuntu-stress",
            "Teams Graph 429 stress (Linux only)": "ubuntu-stress",
            "Teams SQLite row-level migration regressions": "state-perf",
            "CXP preview SQLite correctness, concurrency, and write budgets": "state-perf",
            "CXP preview SQLite actual syscall write budget (Linux only)": "state-perf",
            "Teams runtime safety resolver syscall budget": "state-perf",
            "Teams runtime safety real-process, SIGKILL, and disk-full boundaries": "state-perf",
            "CXP cache v2 real NFS concurrency smoke (Linux only)": "state-perf",
            "Teams SQLite store migration and perf regressions": "state-perf",
            "Teams perf benchmark smoke": "state-perf",
            "Skills local git smoke (Windows)": "windows-skills-desktop",
            "Codex desktop app network install smoke (Windows)": "windows-skills-desktop-b",
            "Codex desktop app managed runtime smoke (Windows)": "windows-skills-desktop-b",
            "Install Codex for integration (Windows)": "windows-codex-e2e",
            "Teams app-server probe (Windows)": "windows-codex-e2e",
            "Codex upgrade integration (system npm, Windows)": "windows-codex-e2e",
            "Codex upgrade integration (managed npm with unrelated PATH Codex, Windows)": "windows-codex-e2e",
            "Teams target-account PATH Codex upgrade (Windows)": "windows-codex-e2e",
            "Codex approval, history, and cancellation runtime integration (Windows)": "windows-codex-e2e",
            "Native managed-node install integration (Windows)": "windows-codex-e2e",
        }
        for name, shard in expected.items():
            self.assertRegex(
                blocks[name],
                rf"if: (?:always\(\) && )?matrix\.shard == '{re.escape(shard)}'",
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

    def test_teams_ownership_stress_gate_is_strict_and_nonempty(self):
        blocks = step_blocks(targeted_job())
        block = blocks["Teams ownership stress regressions"]
        smoke = (ROOT / "scripts" / "ci" / "teams_ownership_stress_docker_smoke.sh").read_text(encoding="utf-8")
        self.assertIn("bash scripts/ci/teams_ownership_stress_docker_smoke.sh", block)
        self.assertIn("CODEX_HELPER_TEAMS_OWNERSHIP_STRESS_STRICT=1", smoke)
        self.assertIn("-test.run '^TestTeamsOwnershipStress'", smoke)
        self.assertTrue((ROOT / "scripts" / "ci" / "Dockerfile.teams-ownership-stress").is_file())
        tests = re.findall(r"^func (TestTeamsOwnershipStress\w*)\(", OWNERSHIP_STRESS_TESTS.read_text(encoding="utf-8"), re.MULTILINE)
        self.assertGreater(len(tests), 0)

    def test_targeted_poll_selectors_match_current_test_names(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("SkipsExplicitParkedPollWithStaleContinuation", workflow)
        self.assertNotIn("RecoversStaleWorkContinuationErrorWithTerminalHead", workflow)
        self.assertNotIn("RecoversStaleContinuationErrorWithTerminalHead", workflow)
        self.assertIn("KeepsExplicitParkedPollBlockedUntilRetryDeadline", workflow)
        self.assertIn("PreservesStaleWorkContinuationWithTerminalHead", workflow)
        self.assertIn("PreservesStaleContinuationWithTerminalHead", workflow)
        self.assertIn("ProbesAlreadyParkedNoticeSentWithoutFreezeRewrite", workflow)

    def test_windows_codex_e2e_installs_before_runtime_consumers(self):
        job = targeted_job()
        ordered_steps = [
            "Install Codex for integration (Windows)",
            "Teams app-server probe (Windows)",
            "Codex upgrade integration (system npm, Windows)",
            "Codex upgrade integration (managed npm with unrelated PATH Codex, Windows)",
            "Teams target-account PATH Codex upgrade (Windows)",
            "Codex approval, history, and cancellation runtime integration (Windows)",
            "Native managed-node install integration (Windows)",
        ]
        positions = [job.index(f"- name: {name}") for name in ordered_steps]
        self.assertEqual(positions, sorted(positions))

    def test_desktop_app_update_contracts_are_selected_by_ci(self):
        job = targeted_job()
        blocks = step_blocks(job)
        self.assertIn(
            "RootCommandWiresExpectedSubcommandsAndFlags|RootUpgradeCodexApp",
            job,
        )
        unsupported = blocks["Codex desktop app unsupported smoke (Linux)"]
        self.assertIn("app --cwd", unsupported)
        self.assertIn("only available for macOS and Windows", unsupported)

        updater_unsupported = blocks["Codex desktop app updater unsupported smoke (Linux/macOS)"]
        self.assertIn("--upgrade-codex-app", updater_unsupported)
        self.assertIn("only supported on native Windows or WSL", updater_unsupported)

        managed = blocks["Codex desktop app managed runtime smoke (Windows)"]
        self.assertIn("^Test(RootUpgradeCodexApp|WindowsManagedApp)", managed)
        self.assertIn("codex_app_managed_install_smoke.ps1", managed)
        managed_smoke = (ROOT / "scripts" / "ci" / "codex_app_managed_install_smoke.ps1").read_text(encoding="utf-8")
        self.assertIn("--upgrade-codex-app", managed_smoke)

    def test_release_install_smoke_checks_root_desktop_update_help(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('grep -q -- "--upgrade-codex-app"', workflow)
        self.assertIn('& $cxp --help | Select-String "--upgrade-codex-app"', workflow)


if __name__ == "__main__":
    unittest.main()
