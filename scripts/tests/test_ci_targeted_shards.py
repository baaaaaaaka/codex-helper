import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
NIGHTLY_WORKFLOW = ROOT / ".github" / "workflows" / "nightly.yml"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
TEAMS_RUNTIME_SHARD = ROOT / "scripts" / "tests" / "run_teams_runtime_safety_shard.sh"
FULL_GO_TEST_SHARDS = ROOT / "scripts" / "ci" / "run_full_go_test_shards.go"
LINUX_FULL_GO_TEST_COLLECT = ROOT / "scripts" / "ci" / "run_linux_full_go_test_collect.sh"
E2E_SHARDS = ROOT / "scripts" / "ci" / "e2e_shards.py"
E2E_SCENARIOS_PREFIX = "github.com/baaaaaaaka/codex-helper/internal/e2e/scenarios/"


def job_block(workflow_path: pathlib.Path, job: str) -> str:
    workflow = workflow_path.read_text(encoding="utf-8")
    start = workflow.index(f"  {job}:\n")
    following = re.search(r"^  [a-z][a-z0-9-]*:\n", workflow[start + len(job) + 4 :], re.MULTILINE)
    end = start + len(job) + 4 + following.start() if following else len(workflow)
    return workflow[start:end]


def targeted_job() -> str:
    """The nightly per-OS integration job (the old targeted-test matrix)."""
    return job_block(NIGHTLY_WORKFLOW, "platform-integration")


def step_blocks(job: str) -> dict[str, str]:
    matches = list(re.finditer(r"^      - name: (.+)$", job, re.MULTILINE))
    blocks: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(job)
        blocks[match.group(1)] = job[match.start() : end]
    return blocks


class TargetedShardWorkflowTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "POSIX credential guard")
    def test_credential_guard_rejects_inputs_when_ripgrep_is_unavailable(self):
        block = step_blocks(job_block(WORKFLOW, "lint"))[
            "Forbid external account credentials in repository CI"
        ]
        script = textwrap.dedent(block.split("        run: |\n", 1)[1])
        shell, grep = shutil.which("bash"), shutil.which("grep")
        self.assertIsNotNone(shell)
        self.assertIsNotNone(grep)
        with tempfile.TemporaryDirectory(prefix="cxp-credential-guard-") as temp_dir:
            root = pathlib.Path(temp_dir)
            tools = root / "bin"
            tools.mkdir()
            (tools / "grep").symlink_to(grep)
            (tools / "rg").write_text("#!/bin/sh\nexit 127\n", encoding="utf-8")
            (tools / "rg").chmod(0o755)
            workflows = root / ".github" / "workflows"
            workflows.mkdir(parents=True)
            environment = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ.get("PATH", ""))
            environment.pop("BASH_ENV", None)
            for contents, allowed in (
                ("name: safe\n", True),
                ("token: ${{ secrets.TEST_TOKEN }}\n", False),
                ("env: TEAMS_TOKEN_CACHE_JSON\n", False),
            ):
                (workflows / "test.yml").write_text(contents, encoding="utf-8")
                result = subprocess.run(
                    [shell, "-c", script], cwd=root, env=environment,
                    text=True, capture_output=True,
                )
                self.assertEqual(result.returncode == 0, allowed, result.stderr)
                if not allowed:
                    self.assertIn("Repository CI must not", result.stderr)
            # An unavailable or failing scanner must fail closed as well.
            (tools / "grep").unlink()
            (tools / "grep").write_text("#!/bin/sh\nexit 2\n", encoding="utf-8")
            (tools / "grep").chmod(0o755)
            result = subprocess.run(
                [shell, "-c", script], cwd=root, env=environment,
                text=True, capture_output=True,
            )
            self.assertEqual(result.returncode, 2)

    def test_full_runner_partitions_cover_every_runnable_job_exactly_once(self):
        with tempfile.TemporaryDirectory(prefix="cxp-ci-shard-runner-") as temp_dir:
            runner_path = pathlib.Path(temp_dir) / (
                "run_full_go_test_shards.exe" if os.name == "nt" else "run_full_go_test_shards"
            )
            subprocess.run(
                ["go", "build", "-o", str(runner_path), "./scripts/ci/run_full_go_test_shards.go"],
                cwd=ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            def plans(*, race: bool, partition_count: int) -> dict[str, list[str]]:
                command = [
                    str(runner_path),
                    "-timeout=30m",
                    "-parallel=16",
                    "-shards=16",
                ]
                if race:
                    command.append("-race")
                command.extend(
                    [
                        f"-exclude-prefix={E2E_SCENARIOS_PREFIX}",
                        f"-partition-count={partition_count}",
                        "-partition-index=0",
                        "-list-only",
                        "-list-all-partitions",
                    ]
                )
                completed = subprocess.run(
                    command,
                    cwd=ROOT,
                    check=True,
                    text=True,
                    capture_output=True,
                )
                parsed: dict[str, list[str]] = {}
                current_plan = None
                for line in completed.stdout.splitlines():
                    if line.startswith("plan ") and line.endswith(":"):
                        current_plan = line[len("plan ") : -1]
                        parsed[current_plan] = []
                    elif ": go " in line:
                        self.assertIsNotNone(current_plan, line)
                        parsed[current_plan].append(line)
                return parsed

            self._assert_full_runner_partition_plans(plans)

    def _assert_full_runner_partition_plans(self, run_plans):
        for race, partition_count in ((False, 2), (True, 4)):
            with self.subTest(race=race, partition_count=partition_count):
                plans = run_plans(race=race, partition_count=partition_count)
                complete_plan = plans["complete"]
                self.assertTrue(complete_plan)
                self.assertTrue(
                    any(
                        # An exclusive test built on every OS.
                        "TestRunAppGatewayDaemonKeepsStableFrontendWhileBackendRuns" in job
                        for job in complete_plan
                    )
                )
                self.assertFalse(
                    any("/internal/e2e/scenarios/" in job for job in complete_plan),
                    "the end-to-end scenarios belong to the e2e job",
                )
                if len(complete_plan) != len(set(complete_plan)):
                    self.fail("unpartitioned full-test plan contains duplicate jobs")
                for job in complete_plan:
                    if ' "-run" ' in job or ' "-skip" ' in job:
                        self.assertNotRegex(job, r' "-(?:run|skip)" ""')
                partition_plan_by_index = [
                    plans[f"partition-{partition_index}"]
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
                # A single test process cannot be split across runners: the
                # balance bound is 10% of the load or the largest job,
                # whichever is larger.
                largest_job = max(metadata(job)[0] for job in partition_plans)

                def bound(loads: list[int]) -> int:
                    return max(1, max(loads) // 10, largest_job)
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
                # Few exclusive jobs cannot always balance within 10%: the
                # unavoidable skew is one job, so the largest one bounds it.
                largest_exclusive = max(
                    (metadata(job)[0] for job in partition_plans if metadata(job)[1]), default=1
                )
                self.assertLessEqual(
                    max(exclusive_loads) - min(exclusive_loads),
                    max(1, max(exclusive_loads) // 10, largest_exclusive),
                    f"exclusive phase estimate is skewed: {exclusive_loads}",
                )
                if race and partition_count == 4:
                    # Large packages are split into 16 prefix shards and each
                    # runner has four workers. One runner may receive a fifth
                    # shard; assert that this is only one extra wave instead
                    # of requiring an impossible equal span across partitions.
                    largest_parallel = max(metadata(job)[0] for job in partition_plans if not metadata(job)[1])
                    self.assertLessEqual(
                        max(parallel_spans),
                        2 * largest_parallel,
                        f"parallel phase exceeds two largest-job waves: {parallel_spans}",
                    )
                else:
                    self.assertLessEqual(
                        max(parallel_spans) - min(parallel_spans),
                        bound(parallel_spans),
                        f"parallel phase estimate is skewed: {parallel_spans}",
                    )
                self.assertLessEqual(
                    max(total_loads) - min(total_loads),
                    bound(total_loads),
                    f"estimated total work is skewed: {total_loads}",
                )
                if race and partition_count == 4:
                    # A fifth heavy job can be unavoidable with four workers
                    # per runner. Keep the bound explicit: no
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
                        bound(critical_paths),
                        f"estimated critical path is skewed: {critical_paths}",
                    )

    def test_teams_runtime_safety_uses_one_shared_shard_definition(self):
        script = TEAMS_RUNTIME_SHARD.read_text(encoding="utf-8")
        # The isolation case sets CXP_TEAMS_TEST_PRESERVE_USER_DIRS, which the
        # full suite does not, so it runs on every pull request; the other
        # cases are subsets of the full suite and stay release preflights.
        self.assertIn(
            "bash scripts/tests/run_teams_runtime_safety_shard.sh isolation",
            job_block(WORKFLOW, "unit-linux"),
        )
        for shard in (
            "isolation",
            "service-update",
            "wsl-process",
            "diagnostics",
            "windows",
        ):
            self.assertIn(f"  {shard})", script)
        for shard in ("service-update", "wsl-process", "diagnostics"):
            call = f"bash scripts/tests/run_teams_runtime_safety_shard.sh {shard}"
            self.assertIn(call, RELEASE_WORKFLOW.read_text(encoding="utf-8"))

        for workflow in (WORKFLOW, NIGHTLY_WORKFLOW, RELEASE_WORKFLOW):
            text = workflow.read_text(encoding="utf-8")
            self.assertNotRegex(
                text,
                r"go test .*TestTeamsRuntimeSafety",
                f"{workflow.name} duplicated a Teams runtime-safety regex",
            )

    def test_declares_parallel_shards_and_limits_platform_only_shards(self):
        job = targeted_job()
        combos = re.findall(
            r"^          - os: (\S+)\n            shard: (\S+)$", job, re.MULTILINE
        )
        self.assertEqual(
            sorted(combos),
            sorted(
                [
                    ("ubuntu-latest", "platform-integration"),
                    ("macos-latest", "platform-integration"),
                    ("windows-latest", "platform-integration"),
                    ("ubuntu-latest", "state-perf"),
                    ("ubuntu-latest", "scripts"),
                    ("macos-latest", "scripts"),
                    ("windows-latest", "scripts"),
                    ("windows-latest", "windows-skills-desktop"),
                    ("windows-latest", "windows-skills-desktop-b"),
                    ("windows-latest", "windows-codex-e2e"),
                ]
            ),
        )
        self.assertNotIn("needs:", job)

    def test_state_perf_runs_only_linux_measurements(self):
        blocks = step_blocks(targeted_job())
        state_perf = [name for name, block in blocks.items() if "matrix.shard == 'state-perf'" in block]
        self.assertEqual(
            sorted(state_perf),
            sorted(
                [
                    "CXP preview SQLite actual syscall write budget (Linux only)",
                    "CXP cache v2 real NFS concurrency smoke (Linux only)",
                    "Codex runner benchmark smoke",
                    "TUI and session history benchmark smoke",
                ]
            ),
        )
        self.assertNotIn("matrix.partition", targeted_job())

    def test_long_full_suite_jobs_use_independent_runner_partitions(self):
        full = job_block(WORKFLOW, "unit-windows-macos")
        self.assertIn(
            "name: Unit tests (${{ matrix.os }} / partition ${{ matrix.partition }})",
            full,
        )
        self.assertIn("os: [windows-latest, macos-latest]", full)
        self.assertIn("partition: [0, 1]", full)
        self.assertIn(
            "-partition-count=2 -partition-index=\"${{ matrix.partition }}\"",
            full,
        )
        self.assertIn(f"-exclude-prefix={E2E_SCENARIOS_PREFIX}", full)

        race = job_block(NIGHTLY_WORKFLOW, "race-test")
        self.assertIn(
            "name: Race test (ubuntu-latest / partition ${{ matrix.partition }})",
            race,
        )
        self.assertIn("partition: [0, 1, 2, 3]", race)
        self.assertIn(
            "-partition-count=4 -partition-index=\"${{ matrix.partition }}\"",
            race,
        )
        self.assertIn(f"-exclude-prefix={E2E_SCENARIOS_PREFIX}", race)
        self.assertIn(
            "go test -race -p=1 -count=1 -timeout=15m -parallel=8 ./internal/core/... ./internal/channel/... ./internal/asr",
            job_block(WORKFLOW, "race-core"),
        )

    def test_e2e_scenarios_run_once_on_linux_through_the_production_entry_point(self):
        job = job_block(WORKFLOW, "e2e")
        self.assertIn("runs-on: ubuntu-latest", job)
        self.assertIn("shard: [0, 1, 2]", job)
        self.assertIn("E2E_SUT: cxp", job)
        self.assertIn(
            'python3 scripts/ci/e2e_shards.py --count 3 --index "${{ matrix.shard }}"', job
        )
        self.assertIn('go test -count=1 -parallel=8 -timeout=600s "${packages[@]}"', job)
        aggregate = job_block(WORKFLOW, "test")
        self.assertIn("      - e2e\n", aggregate)
        self.assertIn('check e2e "${{ needs.e2e.result }}"', aggregate)
        collector = LINUX_FULL_GO_TEST_COLLECT.read_text(encoding="utf-8")
        self.assertIn("*/internal/e2e/scenarios/*) ;;", collector)

    def test_e2e_shards_partition_every_scenario_package_exactly_once(self):
        prefix = "github.com/baaaaaaaka/codex-helper/internal/e2e/scenarios/"
        listed = sorted(
            prefix + path.name
            for path in (ROOT / "internal" / "e2e" / "scenarios").iterdir()
            if path.is_dir()
        )
        # A package nobody measured yet must still be scheduled.
        packages = listed + [prefix + "zz-new-scenario"]
        weights = json.loads(
            (ROOT / "scripts" / "ci" / "e2e_shard_weights.json").read_text(encoding="utf-8")
        )["weights"]
        for name in weights:
            self.assertIn(prefix + name, listed, f"stale E2E weight for {name}")
        for count in (1, 2, 3, 5, len(packages) + 2):
            with self.subTest(count=count):
                shards = []
                for index in range(count):
                    out = subprocess.run(
                        ["python3", str(E2E_SHARDS), "--count", str(count), "--index", str(index),
                         "--packages", *packages],
                        check=True, text=True, capture_output=True,
                    ).stdout.split()
                    shards.append(out)
                flat = [pkg for shard in shards for pkg in shard]
                self.assertEqual(sorted(flat), sorted(packages))
                self.assertEqual(len(flat), len(set(flat)))
                if count <= len(packages):
                    self.assertTrue(all(shards), shards)
                # Heaviest first, so `go test -p` starts the long package
                # at once; an unmeasured package counts as DEFAULT_WEIGHT.
                for shard in shards:
                    loads = [weights.get(pkg.rsplit("/", 1)[-1], 60) for pkg in shard]
                    self.assertEqual(loads, sorted(loads, reverse=True), shard)

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

    def test_ci_serializes_superseded_runs_and_keeps_failure_evidence(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "concurrency:\n  group: ci-${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}\n  cancel-in-progress: true",
            workflow,
        )
        full = job_block(WORKFLOW, "unit-linux")
        collector = LINUX_FULL_GO_TEST_COLLECT.read_text(encoding="utf-8")
        self.assertIn("bash scripts/ci/run_linux_full_go_test_collect.sh", full)
        self.assertIn('tail -n +2 "$profile" >> "$coverage_file"', collector)
        self.assertIn("migration_process_pattern='^TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup$'", collector)
        self.assertIn("-skip \"$isolated_skip_pattern\"", collector)
        self.assertIn('test ./internal/cli -timeout=2m -parallel=16 -count=1 -run "$migration_process_pattern"', collector)
        self.assertIn('status=${PIPESTATUS[0]}', collector)
        self.assertIn('failures+=("$label")', collector)
        self.assertIn("Upload full-suite diagnostics", full)
        self.assertNotIn("full-go-test-cli-retry", full)
        self.assertNotIn("isolated internal/cli retry", full)

        race = job_block(NIGHTLY_WORKFLOW, "race-test")
        self.assertIn('tee "$RUNNER_TEMP/race-test.log"', race)
        self.assertIn("Upload race diagnostics", race)
        self.assertIn("Upload race diagnostics", job_block(WORKFLOW, "race-core"))

        aggregate = job_block(WORKFLOW, "test")
        self.assertIn("    name: Test\n", aggregate)
        self.assertNotIn("name: Test (${{ matrix.os }})", aggregate)

        windows = job_block(NIGHTLY_WORKFLOW, "windows-proxy-lifecycle")
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
        self.assertIn('flag.Var(&excludedPrefixes, "exclude-prefix"', runner)
        self.assertIn("packages = excludePackages(packages, excludedPrefixes)", runner)

    def test_full_go_runner_isolates_runtime_process_identity_fixture(self):
        runner = FULL_GO_TEST_SHARDS.read_text(encoding="utf-8")
        fixtures = ("TestRuntimeProcessIdentityWindows",)
        for fixture_name in fixtures:
            fixture = f'"{fixture_name}"'
            self.assertEqual(
                runner.count(fixture),
                2,
                f"{fixture_name} must be both process-isolated and host-exclusive",
            )

    @unittest.skipIf(os.name == "nt", "Linux Bash CI collector")
    def test_linux_coverage_runner_collects_all_suites_after_failure(self):
        with tempfile.TemporaryDirectory(prefix="cxp-ci-linux-coverage-collect-") as temp_dir:
            temp = pathlib.Path(temp_dir)
            fake_go = temp / "fake-go"
            calls = temp / "calls"
            fail_once = temp / "fail-once"
            fake_go.write_text(
                """#!/usr/bin/env bash
set -u
profile=""
for arg in "$@"; do
  case "$arg" in
    -coverprofile=*) profile="${arg#-coverprofile=}" ;;
  esac
done
printf '%s\n' "$*" >> "$CXP_FAKE_GO_CALLS"
if [[ "${1:-}" == list ]]; then
  printf '%s\n' example.com/m/internal/cli example.com/m/internal/e2e/scenarios/turns example.com/m/internal/e2e/harness
  exit 0
fi
if [[ -n "$profile" ]]; then
  mkdir -p "$(dirname "$profile")"
  printf 'mode: set\nfake.go:1.1,1.2 1 1\n' > "$profile"
fi
if [[ ! -e "$CXP_FAKE_GO_FAIL_ONCE" ]]; then
  : > "$CXP_FAKE_GO_FAIL_ONCE"
  echo 'fake go failure' >&2
  exit 17
fi
echo 'ok'
""",
                encoding="utf-8",
            )
            fake_go.chmod(0o755)
            environment = os.environ.copy()
            environment.update(
                {
                    "CXP_CI_GO_BIN": str(fake_go),
                    "CXP_FAKE_GO_CALLS": str(calls),
                    "CXP_FAKE_GO_FAIL_ONCE": str(fail_once),
                    "RUNNER_TEMP": str(temp / "runner-temp"),
                }
            )
            completed = subprocess.run(
                ["bash", str(LINUX_FULL_GO_TEST_COLLECT)],
                cwd=temp,
                env=environment,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            invocations = calls.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(invocations), 3, invocations)
            self.assertEqual("list ./...", invocations[0])
            # The end-to-end scenarios run in their own job.
            self.assertIn("example.com/m/internal/cli", invocations[1])
            self.assertIn("example.com/m/internal/e2e/harness", invocations[1])
            self.assertNotIn("/internal/e2e/scenarios/", invocations[1])
            self.assertIn("./internal/cli", invocations[-1])
            for log_name in (
                "full-go-test.log",
                "migration-process-isolated.log",
            ):
                self.assertTrue((temp / "runner-temp" / log_name).is_file(), log_name)
            coverage = (temp / "coverage.out").read_text(encoding="utf-8").splitlines()
            self.assertEqual(coverage.count("mode: set"), 1)
            self.assertEqual(len(coverage), 3)
            self.assertIn("Isolated migration process", completed.stdout)

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
                r"platform-integration|state-perf|scripts|"
                r"windows-skills-desktop|windows-skills-desktop-b|windows-codex-e2e"
                r")'(?: && .+)?$",
                block,
                re.MULTILINE,
            )
            self.assertEqual(len(matches), 1, name)

    def test_heavy_steps_are_assigned_to_expected_shards(self):
        blocks = step_blocks(targeted_job())
        expected = {
            "Script unit tests (macOS)": "scripts",
            "Script unit tests (Windows)": "scripts",
            "Patched-binary cleanup real HOME safety (Linux only)": "scripts",
            "CXP preview SQLite actual syscall write budget (Linux only)": "state-perf",
            "CXP cache v2 real NFS concurrency smoke (Linux only)": "state-perf",
            "Codex runner benchmark smoke": "state-perf",
            "TUI and session history benchmark smoke": "state-perf",
            "Skills local git smoke (Windows)": "windows-skills-desktop",
            "Codex desktop app Windows build and policy regressions": "windows-skills-desktop-b",
            "Codex desktop app managed runtime smoke (Windows)": "windows-skills-desktop-b",
            "Codex desktop signed AppX provisioning and launch smoke (Windows)": "windows-skills-desktop-b",
            "Install Codex for integration (Windows)": "windows-codex-e2e",
            "Teams app-server probe (Windows)": "windows-codex-e2e",
            "Codex upgrade integration (system npm, Windows)": "windows-codex-e2e",
            "Codex upgrade integration (managed npm with unrelated PATH Codex, Windows)": "windows-codex-e2e",
            "Codex approval, history, and cancellation runtime integration (Windows)": "windows-codex-e2e",
            "Native managed-node install integration (Windows)": "windows-codex-e2e",
        }
        for name, shard in expected.items():
            self.assertRegex(
                blocks[name],
                rf"if: (?:always\(\) && )?matrix\.shard == '{re.escape(shard)}'",
                name,
            )

    def test_windows_codex_e2e_installs_before_runtime_consumers(self):
        job = targeted_job()
        ordered_steps = [
            "Install Codex for integration (Windows)",
            "Teams app-server probe (Windows)",
            "Codex upgrade integration (system npm, Windows)",
            "Codex upgrade integration (managed npm with unrelated PATH Codex, Windows)",
            "Codex approval, history, and cancellation runtime integration (Windows)",
            "Native managed-node install integration (Windows)",
        ]
        positions = [job.index(f"- name: {name}") for name in ordered_steps]
        self.assertEqual(positions, sorted(positions))

    def test_desktop_app_update_contracts_are_selected_by_ci(self):
        job = targeted_job()
        blocks = step_blocks(job)
        # The unit suite runs every test on every OS on each pull request
        # (ci.yml), so these contracts only have to exist as tests.
        go_tests = "\n".join(
            path.read_text(encoding="utf-8")
            for path in ROOT.rglob("*_test.go")
            if "/.git/" not in path.as_posix()
        )
        for test_name in (
            "TestRootCommandWiresExpectedSubcommandsAndFlags",
            "TestRootUpgradeCodexAppDispatchesManagedUpdate",
            "TestInstallPs1UsesUniqueTemporaryPathsAndCleansThemOnFailure",
            "TestInstallPs1ChecksumMismatchFailsBeforeReplacingBinaries",
            "TestInstallPs1ConcurrentDownloadsUseIsolatedTemporaryPaths",
        ):
            self.assertIn(f"func {test_name}(", go_tests)
        unsupported = blocks["Codex desktop app unsupported smoke (Linux)"]
        self.assertIn("app --cwd", unsupported)
        self.assertIn("only available for macOS and Windows", unsupported)

        updater_unsupported = blocks["Codex desktop app updater unsupported smoke (Linux)"]
        self.assertIn("--upgrade-codex-app", updater_unsupported)
        self.assertIn("only supported on macOS, native Windows, or WSL", updater_unsupported)
        self.assertIn("runner.os == 'Linux'", updater_unsupported)

        mac_smoke = blocks["Codex desktop app network install smoke (macOS)"]
        self.assertIn("UpgradeCodexDesktopAppMac", mac_smoke)
        self.assertIn("codex_app_network_install_smoke.sh", mac_smoke)
        mac_script = (ROOT / "scripts" / "ci" / "codex_app_network_install_smoke.sh").read_text(encoding="utf-8")
        self.assertIn("--upgrade-codex-app", mac_script)
        self.assertIn("Codex desktop app install/update smoke passed", mac_script)

        desktop_build = blocks["Codex desktop app Windows build and policy regressions"]
        for selector in ("CurrentWindowsTokenElevationQuery", "WindowsInstall", "PreflightCodexWindowsAppElevation", "RootUpgradeCodexApp|WindowsManagedApp"):
            self.assertIn(selector, desktop_build)
        self.assertIn("codex_app_smoke_process_test.ps1", desktop_build)
        self.assertIn("TestRunMainHandlesAppInstallWorkerBeforeRuntimeDispatch", desktop_build)

        self.assertNotIn("Codex desktop app network install smoke (Windows)", blocks)
        managed = blocks["Codex desktop app managed runtime smoke (Windows)"]
        signed_appx = blocks["Codex desktop signed AppX provisioning and launch smoke (Windows)"]
        for block, mode in ((managed, "managed"), (signed_appx, "appx")):
            self.assertIn("!cancelled()", block)
            self.assertIn("steps.windows_desktop_build.outcome == 'success'", block)
            self.assertIn("-TokenProbe", block)
            self.assertIn("-Mode " + mode, block)
            self.assertNotIn("continue-on-error", block)
        self.assertNotIn("steps.windows_desktop_store.outcome", managed)
        self.assertNotIn("steps.windows_desktop_store.outcome", signed_appx)
        self.assertNotIn("steps.windows_desktop_managed.outcome", signed_appx)

        network_smoke = (ROOT / "scripts" / "ci" / "codex_app_network_install_smoke.ps1").read_text(encoding="utf-8")
        self.assertIn("proxyEnabled = $false", network_smoke)
        self.assertIn("the Store installation smoke requires a fresh account", network_smoke)
        self.assertIn("does not test Microsoft Store acquisition", network_smoke)
        standard_user_smoke = (ROOT / "scripts" / "ci" / "codex_app_smoke_as_standard_user.ps1").read_text(encoding="utf-8")
        self.assertIn('"store"', standard_user_smoke)
        managed_smoke = (ROOT / "scripts" / "ci" / "codex_app_managed_install_smoke.ps1").read_text(encoding="utf-8")
        self.assertIn("--upgrade-codex-app", managed_smoke)

        diagnostics = blocks["Upload Windows desktop smoke diagnostics"]
        self.assertIn("!cancelled()", diagnostics)
        self.assertIn("actions/upload-artifact@", diagnostics)
        self.assertIn("*.log", diagnostics)
        self.assertIn("*.out", diagnostics)

    def test_release_install_smoke_checks_root_desktop_update_help(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('grep -q -- "--upgrade-codex-app"', workflow)
        self.assertIn('& $cxp --help | Select-String "--upgrade-codex-app"', workflow)


if __name__ == "__main__":
    unittest.main()
