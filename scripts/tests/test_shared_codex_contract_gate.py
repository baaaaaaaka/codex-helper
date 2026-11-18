import os
import pathlib
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


@unittest.skipUnless(os.name == "posix", "shell fixtures require POSIX")
class SharedCodexContractGateTest(unittest.TestCase):
    def test_required_contracts_must_pass_not_skip_or_match_zero(self):
        with tempfile.TemporaryDirectory(prefix="cxp-contract-gate-") as tmp:
            root = pathlib.Path(tmp)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            go = bin_dir / "go"
            go.write_text('''#!/bin/bash
set -eu
while [[ $# -gt 0 && "$1" != -o ]]; do shift; done
shift
out=$1
cat > "$out" <<'SCRIPT'
#!/bin/bash
case "$0" in
  */probe) printf '%s\\n' TestRealCodexNewChatTUI TestRealCodexGatewayRecoveryTUI TestRealCodexAAASharedTUI ;;
  */agent) printf '%s\\n' TestRealCodexSharedTUIThreadBinding TestRealCodexGoalDisabledStop TestRealCodexGoalReadDoesNotLoadExecution TestRealCodexGoalSwitchAndPause ;;
  */client) printf '%s\\n' TestRealCodexSharedModelSwitch TestRealCodexSharedSwitchInputBoundary TestRealCodexGoalReadWithoutResume ;;
esac
SCRIPT
chmod +x "$out"
''')
            go.chmod(0o755)
            docker = bin_dir / "docker"
            docker.write_text('''#!/bin/bash
case "$*" in
 *GatewayRecovery*) printf '%s\\n' "$FAKE_RESULT" ;;
 *'/contracts/client'*) printf '%s\\n' "$FAKE_SWITCH_RESULT" ;;
 *'/contracts/agent'*) printf '%s\\n' "$FAKE_AGENT_RESULT" ;;
 *) echo PASS ;;
esac
''')
            docker.chmod(0o755)
            codex = root / "codex"
            codex.write_text("#!/bin/sh\nexit 0\n")
            codex.chmod(0o755)
            env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ.get("PATH", ""), TMPDIR=tmp)
            passed_ui = "--- PASS: TestRealCodexNewChatTUI (0.1s)\n--- PASS: TestRealCodexGatewayRecoveryTUI (0.1s)\n--- PASS: TestRealCodexAAASharedTUI (0.1s)"
            passed_switch = "--- PASS: TestRealCodexSharedModelSwitch (0.1s)\n--- PASS: TestRealCodexSharedSwitchInputBoundary (0.1s)"
            passed_switch += "\n--- PASS: TestRealCodexGoalReadWithoutResume (0.1s)"
            passed_agent = "--- PASS: TestRealCodexGoalDisabledStop (0.1s)\n--- PASS: TestRealCodexGoalReadDoesNotLoadExecution (0.1s)"
            passed_agent += "\n--- PASS: TestRealCodexGoalSwitchAndPause (0.1s)"
            cases = (
                (passed_ui, passed_switch, True),
                (passed_ui.replace("--- PASS: TestRealCodexAAASharedTUI", "--- SKIP: TestRealCodexAAASharedTUI"), passed_switch, False),
                (passed_ui.replace("\n--- PASS: TestRealCodexAAASharedTUI (0.1s)", ""), passed_switch, False),
                ("--- PASS: TestRealCodexNewChatTUI (0.1s)\n--- SKIP: TestRealCodexGatewayRecoveryTUI (0.1s)", passed_switch, False),
                ("--- SKIP: TestRealCodexNewChatTUI (0.0s)", passed_switch, False),
                ("testing: warning: no tests to run", passed_switch, False),
                (passed_ui, "--- SKIP: TestRealCodexSharedModelSwitch (0.1s)\n--- PASS: TestRealCodexSharedSwitchInputBoundary (0.1s)", False),
                (passed_ui, "--- PASS: TestRealCodexSharedModelSwitch (0.1s)\n--- SKIP: TestRealCodexSharedSwitchInputBoundary (0.1s)", False),
                (passed_ui, "--- PASS: TestRealCodexSharedModelSwitch (0.1s)", False),
                (passed_ui, "testing: warning: no tests to run", False),
            )
            for output, switch_output, expected in cases:
                result = subprocess.run(["bash", "scripts/ci/run_shared_codex_contracts.sh", str(codex)],
                                        cwd=ROOT, env=dict(env, FAKE_RESULT=output, FAKE_SWITCH_RESULT=switch_output, FAKE_AGENT_RESULT=passed_agent),
                                        text=True, capture_output=True)
                self.assertEqual(result.returncode == 0, expected, result.stdout + result.stderr)
            for missing in ("TestRealCodexGoalDisabledStop", "TestRealCodexGoalReadDoesNotLoadExecution", "TestRealCodexGoalSwitchAndPause"):
                result = subprocess.run(["bash", "scripts/ci/run_shared_codex_contracts.sh", str(codex)],
                                        cwd=ROOT, env=dict(env, FAKE_RESULT=passed_ui, FAKE_SWITCH_RESULT=passed_switch,
                                                           FAKE_AGENT_RESULT=passed_agent.replace("--- PASS: " + missing, "--- SKIP: " + missing)),
                                        text=True, capture_output=True)
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
            result = subprocess.run(["bash", "scripts/ci/run_shared_codex_contracts.sh", str(codex)],
                                    cwd=ROOT, env=dict(env, FAKE_RESULT=passed_ui, FAKE_AGENT_RESULT=passed_agent,
                                                       FAKE_SWITCH_RESULT=passed_switch.replace("--- PASS: TestRealCodexGoalReadWithoutResume", "--- SKIP: TestRealCodexGoalReadWithoutResume")),
                                    text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
