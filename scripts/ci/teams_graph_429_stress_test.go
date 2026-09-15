package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestTeamsGraph429StressRejectsFalseGreenConfiguration(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller did not return the test source path")
	}
	script := filepath.Join(filepath.Dir(sourceFile), "teams_graph_429_stress.sh")
	for _, tc := range []struct {
		name  string
		env   string
		value string
		want  string
	}{
		{name: "zero count", env: "CODEX_HELPER_TEAMS_GRAPH_429_STRESS_COUNT", value: "0", want: "must be an integer >= 1"},
		{name: "one chat", env: "CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS", value: "1", want: "must be an integer >= 2"},
		{name: "zero race chats", env: "CODEX_HELPER_TEAMS_GRAPH_429_STRESS_RACE_CHATS", value: "0", want: "must be an integer >= 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("bash", script)
			cmd.Env = append(os.Environ(), "GOPROXY=off", "CXP_RUNTIME_DISABLE=1", tc.env+"="+tc.value)
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("stress wrapper accepted invalid configuration; output=%s", output)
			}
			if !strings.Contains(string(output), tc.want) {
				t.Fatalf("stress wrapper error = %s, want substring %q", output, tc.want)
			}
		})
	}
}
