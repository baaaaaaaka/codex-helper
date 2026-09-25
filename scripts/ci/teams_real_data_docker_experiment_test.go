package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestTeamsRealDataDockerWrapperIsolatedAndRuntimeDisabled(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller did not return the test source path")
	}
	scriptPath := filepath.Join(filepath.Dir(sourceFile), "teams_real_data_docker_experiment.sh")
	script, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read real-data Docker wrapper: %v", err)
	}
	source := string(script)
	for _, required := range []string{
		"--network none",
		"--read-only",
		"--cap-drop ALL",
		"CXP_RUNTIME_DISABLE=1",
		"CXP_TEAMS_DOCKER_INCLUDE_RECENT_HISTORY",
		"CXP_TEAMS_DOCKER_MISSING_HISTORY_RECOVERY",
		"CXP_TEAMS_DOCKER_REAL_DATA_CHAT_COVERAGE",
		"CXP_TEAMS_DOCKER_REAL_DATA_REQUIRE_ALL_LAGGING",
		"CXP_TEAMS_DOCKER_CHAT_COVERAGE_DURATION",
		"CXP_TEAMS_DOCKER_REAL_DATA_TESTS",
		"CXP_TEAMS_DOCKER_REAL_DATA_MODE=complete",
		"all-lagging",
		"CXP_TEAMS_DOCKER_SOURCE_PROOF_MANIFEST",
		"CXP_TEAMS_DOCKER_REUSE_FIXTURE_DIR",
		"CXP_TEAMS_DOCKER_KEEP_FIXTURE",
		"CXP_TEAMS_DOCKER_RUNTIME_DIR",
		"CXP_TEAMS_DOCKER_RUNTIME_REUSE",
		"CXP_TEAMS_DOCKER_REAL_DATA_RESUME",
		"disposable mutable runtime retained",
		"persistent Docker runtime directory must not overlap source/fixture data",
		"validate_reusable_fixture",
		"Derive every",
		"never resolve, stat, hash, or otherwise consult a live",
		"if [[ \"$reused_fixture\" != \"1\" ]]; then",
		"source integrity check: reused fixture was mounted read-only",
		"sqlite-source-XXXXXX",
		"-wal -shm -journal",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("real-data Docker wrapper is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"CXP_TEAMS_ACCESS_TOKEN",
		"CXP_TEAMS_GRAPH_TOKEN",
		"auth.json",
		"/.env",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("real-data Docker wrapper references forbidden credential input %q", forbidden)
		}
	}
}
