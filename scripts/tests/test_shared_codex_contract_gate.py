import os
import pathlib
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


@unittest.skipUnless(os.name == "posix", "shell fixtures require POSIX")
class SharedCodexContractGateTest(unittest.TestCase):
    def test_new_chat_must_pass_not_skip_or_match_zero(self):
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
  */probe) printf '%s\\n' TestRealCodexNewChatTUI TestRealCodexGatewayRecoveryTUI ;;
  */agent) echo TestRealCodexSharedTUIThreadBinding ;;
esac
SCRIPT
chmod +x "$out"
''')
            go.chmod(0o755)
            docker = bin_dir / "docker"
            docker.write_text('''#!/bin/bash
case "$*" in
 *GatewayRecovery*) printf '%s\\n' "$FAKE_RESULT" ;;
 *) echo PASS ;;
esac
''')
            docker.chmod(0o755)
            codex = root / "codex"
            codex.write_text("#!/bin/sh\nexit 0\n")
            codex.chmod(0o755)
            env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ.get("PATH", ""), TMPDIR=tmp)
            for output, expected in (("--- PASS: TestRealCodexNewChatTUI (0.1s)\n--- PASS: TestRealCodexGatewayRecoveryTUI (0.1s)", True),
                                     ("--- PASS: TestRealCodexNewChatTUI (0.1s)\n--- SKIP: TestRealCodexGatewayRecoveryTUI (0.1s)", False),
                                     ("--- SKIP: TestRealCodexNewChatTUI (0.0s)", False),
                                     ("testing: warning: no tests to run", False)):
                result = subprocess.run(["bash", "scripts/ci/run_shared_codex_contracts.sh", str(codex)],
                                        cwd=ROOT, env=dict(env, FAKE_RESULT=output),
                                        text=True, capture_output=True)
                self.assertEqual(result.returncode == 0, expected, result.stdout + result.stderr)
