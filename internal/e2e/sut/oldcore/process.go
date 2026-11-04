package oldcore

import (
	"os"
	"sort"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

// Binaries are the executables the process adapter runs.
type Binaries struct {
	SUT   string // cmd/oldcore-sut
	Codex string // fakecodex/cmd/fakecodex
}

// ProcessFactory returns a factory for the subprocess adapter.
//
// Lifecycle semantics of the subprocess adapter ("oldcore", the default):
//
//   - Start launches cmd/oldcore-sut with only the scenario's environment
//     (HOME, XDG_*, CODEX_HOME, token caches, ...) plus PATH. It talks to the
//     fake Graph through the sandbox transport and runs Codex turns through
//     the real codexrunner.AppServerRunner against the fake app-server
//     binary, whose "brain" is the scenario's FakeCodex.
//   - Stop sends SIGTERM and waits for the process to exit (like systemd
//     stopping `cxp teams run`).
//   - Crash sends SIGKILL to the helper process only. No deferred code runs.
//     Its app-server children live in their own process group (like
//     production) and see stdin EOF; unless a behaviour says SurviveHelper
//     they then exit and their running turn is lost (interrupted).
//   - Restart is Stop + Start on the same HOME / store.
//   - `helper restart` (BridgeOptions.HelperRestarter) exits the helper with
//     harness.ServiceRestartExitCode and the harness process runner starts
//     a new instance on the same state (see harness.ProcessSUT).
func ProcessFactory(bins Binaries) harness.SUTFactory {
	return func(cfg harness.SUTConfig) (harness.SUT, error) {
		s := &ProcSUT{cfg: cfg, bins: bins}
		s.ProcessSUT = harness.NewProcessSUT(cfg, harness.ProcessSpec{Name: "oldcore", Binary: bins.SUT, Env: s.childEnv})
		return s, nil
	}
}

// ProcSUT runs the old core as a child process through the harness process
// runner; it only adds the old core's environment.
type ProcSUT struct {
	*harness.ProcessSUT
	cfg  harness.SUTConfig
	bins Binaries
}

func (s *ProcSUT) childEnv(tag string) []string {
	vars := map[string]string{}
	for k, v := range s.cfg.Env.Vars {
		vars[k] = v
	}
	vars["PATH"] = os.Getenv("PATH")
	vars["TMPDIR"] = os.Getenv("TMPDIR")
	vars["GOTRACEBACK"] = "all"
	vars["E2E_SUT_GRAPH_URL"] = s.cfg.Graph.URL()
	vars["E2E_SUT_INSTANCE"] = tag
	vars["E2E_SUT_INTERVAL"] = s.cfg.Interval.String()
	vars["E2E_SUT_CODEX_BIN"] = s.bins.Codex
	vars[fakecodex.EnvBrainURL] = s.cfg.Codex.BrainURL()
	vars["E2E_SUT_WORKFLOW_WEBHOOK_URL"] = s.cfg.WorkflowWebhookURL
	for k, v := range s.cfg.ExtraEnv {
		vars[k] = v
	}
	env := make([]string, 0, len(vars))
	for k, v := range vars {
		if v != "" || k == "CODEX_DIR" {
			env = append(env, k+"="+v)
		}
	}
	sort.Strings(env)
	return env
}

// BlockedPrefix starts the line the SUT main prints for a refused foreign
// request (see cmd/oldcore-sut).
const BlockedPrefix = harness.BlockedLinePrefix
