package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestTeamsBoundedAcceptanceDockerWrapperIsCredentialFreeAndIsolated(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller did not return the test source path")
	}
	scriptPath := filepath.Join(filepath.Dir(sourceFile), "teams_bounded_acceptance_docker.sh")
	script, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read bounded acceptance wrapper: %v", err)
	}
	source := string(script)
	for _, required := range []string{
		"--network=none",
		"--read-only",
		"--cap-drop=ALL",
		"CXP_RUNTIME_DISABLE=1",
		"TestDockerBoundedAcceptanceScenario",
		"Dockerfile.teams-bounded-acceptance",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("bounded acceptance wrapper is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"CXP_TEAMS_DOCKER_FIXTURE_DIR",
		"CXP_TEAMS_DOCKER_REAL_DATA_EXPERIMENT",
		"CXP_TEAMS_ACCESS_TOKEN",
		"CXP_TEAMS_GRAPH_TOKEN",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("bounded acceptance wrapper references forbidden credential/fixture input %q", forbidden)
		}
	}

	dockerfilePath := filepath.Join(filepath.Dir(sourceFile), "Dockerfile.teams-bounded-acceptance")
	dockerfile, err := os.ReadFile(dockerfilePath)
	if err != nil {
		t.Fatalf("read bounded acceptance Dockerfile: %v", err)
	}
	if got := string(dockerfile); !strings.Contains(got, "FROM scratch") || !strings.Contains(got, "teams-bounded-acceptance.test") {
		t.Fatalf("bounded acceptance Dockerfile is not a scratch-only test image: %s", got)
	}
}
