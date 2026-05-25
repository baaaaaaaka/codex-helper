package installtest

import (
	"os"
	"strings"
	"testing"
)

func TestCIWorkflowFullTestStepsDoNotSilentlySkipCoverageOrNonLinuxTests(t *testing.T) {
	data, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read ci workflow: %v", err)
	}
	workflow := string(data)
	if strings.Contains(workflow, "- name: go test (with coverage)\n") {
		t.Fatal("old all-platform coverage step is still present")
	}

	linuxCoverage := workflowStepBlock(t, workflow, "go test (with coverage, Linux only)")
	requireStepContains(t, linuxCoverage,
		"if: runner.os == 'Linux'",
		"shell: bash",
		"go test -timeout=20m -parallel=16 -coverprofile=coverage.out ./...",
	)

	nonLinuxTest := workflowStepBlock(t, workflow, "go test (without coverage, non-Linux)")
	requireStepContains(t, nonLinuxTest,
		"if: runner.os != 'Linux'",
		"shell: bash",
		"go test -timeout=20m -parallel=16 ./...",
	)
	if strings.Contains(nonLinuxTest, "coverprofile") {
		t.Fatal("non-Linux full test step should not generate coverage.out")
	}

	coverageGate := workflowStepBlock(t, workflow, "Coverage gate (Linux only)")
	requireStepContains(t, coverageGate,
		"if: runner.os == 'Linux'",
		"go tool cover -func=coverage.out",
	)
}

func TestTeamsGraph429StressScriptKeepsCISizedDefaultsVisible(t *testing.T) {
	data, err := os.ReadFile("scripts/ci/teams_graph_429_stress.sh")
	if err != nil {
		t.Fatalf("read teams graph stress script: %v", err)
	}
	script := string(data)
	requireStepContains(t, script,
		`CODEX_HELPER_TEAMS_GRAPH_429_STRESS_CHATS:=32`,
		`CODEX_HELPER_TEAMS_GRAPH_429_STRESS_MESSAGES:=6`,
		`CODEX_HELPER_TEAMS_GRAPH_429_STRESS_ROUNDS:=8`,
		`CODEX_HELPER_TEAMS_GRAPH_429_STRESS_COUNT:=1`,
		`Teams Graph 429 stress scale:`,
	)
}

func workflowStepBlock(t *testing.T, workflow string, name string) string {
	t.Helper()
	marker := "      - name: " + name
	start := strings.Index(workflow, marker)
	if start < 0 {
		t.Fatalf("workflow step %q not found", name)
	}
	rest := workflow[start:]
	next := strings.Index(rest[len(marker):], "\n      - name: ")
	if next < 0 {
		return rest
	}
	return rest[:len(marker)+next]
}

func requireStepContains(t *testing.T, text string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}
