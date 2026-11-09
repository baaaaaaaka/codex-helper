import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest

from test_ci_targeted_shards import job_block, RELEASE_WORKFLOW, WORKFLOW


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/test_teams_startup_recovery.sh"
NAMES = [
    "TestLegacyTeamsWatchdogHandoff",
    "TestTeamsServiceWatchdogObservesUnpublishedMigrationClaim",
    "TestEntryLegacyWatchdogHandoff",
    "TestVersionPointerReplacementWhileReaderOpen",
    "TestVersionPointerReadsLongPath",
    "TestConversionOwnershipAtEveryCheckpoint",
    "TestConversionCanceledBeforeStartCreatesNothing",
    "TestTeamsCoreStoreDoesNotWriteAfterLeaseLoss",
    "TestTeamsCoreStoreLossAfterPreparationStarts",
    "TestTeamsCoreStoreResumesMigrationAfterAbortedStart",
    "TestTeamsCoreStorePreparationAfterMigrationBeforeActivation",
    "TestTeamsServiceWatchdogRefreshesRuntimeBeforeHealthCheck",
    "TestTeamsServiceWatchdogRuntimeRefreshErrorsDoNotStopLoop",
    "TestTeamsServiceWatchdogRuntimeRestartErrorsDoNotStopLoop",
    "TestTeamsServiceWatchdogRuntimeProcessUpgradeAndRollback",
    "TestEntryMigrationResumesAfterInterruptedStart",
    "TestEntryMigrationResumesAfterCrashDuringImport",
    "TestEntryMigrationTakeoverFencesWaitingOwner",
    "TestEntryMigrationTakeoverFencesImporter",
    "TestEntryWatchdogRuntimeUpgradeAndRollback",
    "TestEntryWatchdogRuntimeLegacyLauncher",
    "TestEntryMigrationRefusesMissingPublishedDatabase",
]


class StartupRecoveryGateTests(unittest.TestCase):
    def run_gate(self, mode, *, missing="", fail=False):
        shell = shutil.which("bash")
        if os.name == "nt":
            # PowerShell may find WSL's bash.exe before Git Bash. Use the
            # same native shell as Actions' shell: bash steps.
            git = Path(shutil.which("git") or "")
            shell = next((str(p / "bin/bash.exe") for p in git.parents
                          if (p / "bin/bash.exe").is_file()), None)
        self.assertIsNotNone(shell, "native Bash is required for the gate tests")
        with tempfile.TemporaryDirectory(prefix="cxp recovery gate ") as tmp:
            tmp = Path(tmp)
            fake = tmp / "go"
            fake_python = tmp / "fake_go.py"
            log = tmp / "calls.jsonl"
            fake_python.write_text("""import json,os,sys
with open(os.environ['CALL_LOG'],'a') as f:
    f.write(json.dumps(sys.argv[1:])+'\\n')
if '-list' in sys.argv:
    print('\\n'.join(n for n in json.loads(os.environ['TEST_NAMES']) if n != os.environ['MISSING']))
    sys.exit(0)
sys.exit(int(os.environ['FAIL_TEST']))
""")
            # Use this interpreter explicitly; Git Bash need not provide
            # python3, and Windows does not execute Python shebangs itself.
            fake.write_text("#!/usr/bin/env bash\nexec " +
                            shlex.quote(Path(sys.executable).as_posix()) + " " +
                            shlex.quote(fake_python.as_posix()) + ' "$@"\n',
                            newline="\n")
            fake.chmod(0o700)
            env = os.environ.copy()
            env.update(CXP_CI_GO_BIN=fake.as_posix(), CALL_LOG=str(log),
                       TEST_NAMES=json.dumps(NAMES), MISSING=missing,
                       FAIL_TEST="1" if fail else "0")
            p = subprocess.run([shell, SCRIPT.as_posix(), mode], cwd=ROOT, env=env,
                               capture_output=True, text=True)
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return p, calls

    def test_native_unit_packages_are_all_run(self):
        p, calls = self.run_gate("unit")
        self.assertEqual(p.returncode, 0, p.stderr)
        executed = [c for c in calls if "-run" in c]
        self.assertEqual([c[1] for c in executed], ["./internal/helperruntime", "./internal/core/migrate", "./internal/cli"])
        self.assertTrue(all("-count=1" in c for c in executed))

    def test_e2e_contracts_are_repeated(self):
        p, calls = self.run_gate("e2e")
        self.assertEqual(p.returncode, 0, p.stderr)
        executed = [c for c in calls if "-run" in c]
        self.assertEqual(len(executed), 1)
        self.assertIn("-count=3", executed[0])

    def test_each_required_test_must_exist(self):
        for name in NAMES:
            mode = "e2e" if name.startswith("TestEntry") else "unit"
            with self.subTest(name=name):
                p, _ = self.run_gate(mode, missing=name)
                self.assertNotEqual(p.returncode, 0)
                self.assertIn("required startup recovery test is missing", p.stderr)

    def test_failure_cannot_be_swallowed(self):
        for mode in ("unit", "e2e"):
            p, _ = self.run_gate(mode, fail=True)
            self.assertNotEqual(p.returncode, 0)

    def test_unknown_mode_is_an_error(self):
        p, calls = self.run_gate("typo")
        self.assertEqual(p.returncode, 2)
        self.assertEqual(calls, [])

    def test_ci_reaches_native_unit_and_repeated_e2e_gates(self):
        for job in ("unit-linux", "unit-windows-macos"):
            with self.subTest(job=job):
                self.assertIn("bash scripts/ci/test_teams_startup_recovery.sh unit", job_block(WORKFLOW, job))
        native = job_block(WORKFLOW, "unit-windows-macos")
        self.assertIn("windows-latest", native)
        self.assertIn("macos-latest", native)
        self.assertIn("bash scripts/ci/test_teams_startup_recovery.sh e2e", job_block(WORKFLOW, "e2e"))

    def test_ci_race_pattern_selects_startup_and_runtime_process(self):
        race = job_block(WORKFLOW, "race-core")
        patterns = re.findall(r"go test -race[^\n]+-run '([^']+)'", race)
        self.assertTrue(patterns, "startup CLI race step is missing")
        self.assertIn("./internal/helperruntime -run '^TestVersionPointer'", race)
        for name in NAMES:
            if name.startswith(("TestLegacy", "TestTeams", "TestFirstStart", "TestVersionPointer")):
                with self.subTest(name=name):
                    self.assertTrue(any(re.search(pattern, name) for pattern in patterns))

    def test_historical_watchdog_regression_is_required(self):
        self.assertIn("bash scripts/ci/test_legacy_watchdog_handoff.sh", job_block(WORKFLOW, "e2e"))
        script = (ROOT / "scripts/ci/test_legacy_watchdog_handoff.sh").read_text()
        self.assertIn("fe71a0c790b55ece0d5567cef05dcd6ce911f6f2a0c72b6a3c2ac2e4e81693b7", script)
        self.assertIn("sha256sum --check --strict", script)
        self.assertIn("export CXP_LEGACY_WATCHDOG_BINARY=", script)
        self.assertIn("-count=3", script)
        systemd = job_block(WORKFLOW, "watchdog-systemd")
        self.assertIn('CXP_TEST_REAL_SYSTEMD: "1"', systemd)
        self.assertIn("bash scripts/ci/test_legacy_watchdog_handoff.sh", systemd)
        self.assertIn("bash scripts/ci/test_watchdog_systemd.sh", script)

    def test_final_ci_aggregate_requires_the_recovery_jobs(self):
        aggregate = job_block(WORKFLOW, "test")
        for job in ("unit-linux", "unit-windows-macos", "e2e", "race-core", "watchdog-systemd"):
            with self.subTest(job=job):
                self.assertIn("      - " + job + "\n", aggregate)
                self.assertIn("${{ needs." + job + ".result }}", aggregate)

    def test_release_requires_the_same_ci_gate(self):
        self.assertIn("uses: ./.github/workflows/ci.yml", job_block(RELEASE_WORKFLOW, "ci"))
        release = job_block(RELEASE_WORKFLOW, "goreleaser")
        needs = re.search(r"^    needs: \[([^]]+)\]", release, re.MULTILINE)
        self.assertIsNotNone(needs)
        self.assertIn("ci", [name.strip() for name in needs.group(1).split(",")])


if __name__ == "__main__":
    unittest.main()
