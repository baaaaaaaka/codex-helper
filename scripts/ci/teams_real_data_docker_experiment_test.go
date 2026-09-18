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
		"CXP_TEAMS_DOCKER_SOURCE_PROOF_MANIFEST",
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
