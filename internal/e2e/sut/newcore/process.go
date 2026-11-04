package newcore

import (
	"os"
	"sort"
	"strconv"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

// Binaries are the executables the process adapter runs.
type Binaries struct {
	SUT   string // internal/e2e/sut/newcore/cmd/newcore-sut
	Codex string // fakecodex/cmd/fakecodex
}

// ProcessFactory returns a factory for the new-core subprocess adapter.
//
// Lifecycle semantics (same as the oldcore process adapter, both run by
// harness.ProcessSUT):
//
//   - Start launches newcore-sut with only the scenario's environment
//     plus PATH. It talks to the fake Graph through the sandbox transport
//     with the real internal/channel/teams adapter, and runs Codex turns
//     through internal/core/codexagent against the fake app-server binary.
//   - Stop sends SIGTERM and waits for the process to exit.
//   - Crash sends SIGKILL to the helper process only. Its app-servers live
//     in their own process groups and see stdin EOF.
//   - Restart is Stop + Start on the same HOME / store.
//   - `helper restart` exits with harness.ServiceRestartExitCode and the
//     runner starts a new instance on the same state.
func ProcessFactory(bins Binaries) harness.SUTFactory {
	return func(cfg harness.SUTConfig) (harness.SUT, error) {
		s := &ProcSUT{cfg: cfg, bins: bins}
		s.ProcessSUT = harness.NewProcessSUT(cfg, harness.ProcessSpec{Name: peerName("newcore", cfg), Binary: bins.SUT, Env: s.childEnv})
		return s, nil
	}
}

// ProcSUT runs the new core as a child process through the harness process
// runner; it only adds the new core's environment.
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

// CXPFactory returns a factory that runs the production entry point,
// `cxp teams run`, built with the e2e tag (bins.SUT is that cxp
// binary). Same lifecycle as ProcessFactory; Codex is found through
// --codex-path and the fake app-server gets its settings from the
// inherited environment.
func CXPFactory(bins Binaries) harness.SUTFactory {
	return func(cfg harness.SUTConfig) (harness.SUT, error) {
		s := &ProcSUT{cfg: cfg, bins: bins}
		registry := cfg.ExtraEnv["E2E_SUT_MACHINE_REGISTRY"] == "1"
		args := []string{"teams", "run", "--codex-path", bins.Codex,
			"--machine-registry=" + strconv.FormatBool(registry), "--auto-update=false", "--auto-service=false"}
		if cfg.ExtraEnv["E2E_SUT_MANAGED_CHILD"] == "1" {
			// Run as the service runs the helper (pacing, standby).
			args = append(args, "--managed-service-child")
		}
		env := func(tag string) []string {
			return append(s.childEnv(tag),
				fakecodex.EnvKind+"=work",
				fakecodex.EnvInstance+"="+tag,
				"CXP_RUNTIME_DISABLE=1",
			)
		}
		s.ProcessSUT = harness.NewProcessSUT(cfg, harness.ProcessSpec{Name: peerName("cxp", cfg), Binary: bins.SUT, Args: args, Env: env})
		return s, nil
	}
}

// peerName is the instance tag prefix: name, or name-<peer> for a second
// helper on the same state (harness.SUTConfig.Peer).
func peerName(name string, cfg harness.SUTConfig) string {
	if cfg.Peer != "" {
		return name + "-" + cfg.Peer
	}
	return name
}
