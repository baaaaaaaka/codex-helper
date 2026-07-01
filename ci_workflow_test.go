package installtest

import (
	"os"
	"strings"
	"testing"
)

func TestCIWorkflowFullTestStepsRunInParallelWithoutWeakeningRequiredChecks(t *testing.T) {
	data, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read ci workflow: %v", err)
	}
	workflow := string(data)
	if strings.Contains(workflow, "- name: go test (with coverage)\n") {
		t.Fatal("old all-platform coverage step is still present")
	}

	targetedJob := workflowJobBlock(t, workflow, "targeted-test")
	requireStepContains(t, targetedJob,
		"name: Targeted test (${{ matrix.os }} / ${{ matrix.shard }})",
		"os: [ubuntu-latest, macos-latest, windows-latest]",
		"shard: [core, platform-integration, state-perf]",
	)
	requireStepNotContains(t, targetedJob,
		"go test (with coverage, Linux only)",
		"go test (without coverage, non-Linux)",
		"go test -race (Linux only)",
		"Coverage gate (Linux only)",
		"Upload coverage artifact (Linux only)",
	)
	pathActivation := workflowStepBlock(t, targetedJob, "Install PATH activation matrix (Linux only)")
	requireStepContains(t, pathActivation,
		"if: matrix.shard == 'platform-integration' && runner.os == 'Linux'",
		"apt-get install -y --no-install-recommends zsh fish csh tcsh",
		"CODEX_PROXY_REQUIRE_SHELL_MATRIX=1 bash scripts/ci/install_path_activation_matrix.sh",
	)
	macPathActivation := workflowStepBlock(t, targetedJob, "Install PATH activation matrix (macOS only)")
	requireStepContains(t, macPathActivation,
		"if: matrix.shard == 'platform-integration' && runner.os == 'macOS'",
		"HOMEBREW_NO_AUTO_UPDATE=1 brew install fish",
		"for attempt in 1 2 3",
		"CODEX_PROXY_REQUIRE_SHELL_MATRIX=1 bash scripts/ci/install_path_activation_matrix.sh",
	)

	fullJob := workflowJobBlock(t, workflow, "full-go-test")
	requireStepContains(t, fullJob,
		"name: Full go test (${{ matrix.os }})",
		"os: [ubuntu-latest, macos-latest, windows-latest]",
	)

	linuxCoverage := workflowStepBlock(t, fullJob, "go test (with coverage, Linux only)")
	requireStepContains(t, linuxCoverage,
		"if: runner.os == 'Linux'",
		"shell: bash",
		"go test -timeout=20m -parallel=16 -coverprofile=coverage.out ./...",
	)

	nonLinuxTest := workflowStepBlock(t, fullJob, "go test (without coverage, non-Linux)")
	requireStepContains(t, nonLinuxTest,
		"if: runner.os != 'Linux'",
		"shell: bash",
		"go test -timeout=20m -parallel=16 ./...",
	)
	if strings.Contains(nonLinuxTest, "coverprofile") {
		t.Fatal("non-Linux full test step should not generate coverage.out")
	}

	coverageGate := workflowStepBlock(t, fullJob, "Coverage gate (Linux only)")
	requireStepContains(t, coverageGate,
		"if: runner.os == 'Linux'",
		"go tool cover -func=coverage.out",
	)

	raceJob := workflowJobBlock(t, workflow, "race-test")
	requireStepContains(t, raceJob,
		"name: Race test (ubuntu-latest)",
		"go test -race ./...",
	)

	distroJob := workflowJobBlock(t, workflow, "linux-distro-smoke")
	requireStepContains(t, distroJob,
		"path_activation_package_args: zsh tcsh",
		"path_activation_package_args: zsh fish csh tcsh",
		"PATH_ACTIVATION_PACKAGE_ARGS: ${{ matrix.path_activation_package_args }}",
		"PATH_ACTIVATION_CASES: ${{ matrix.path_activation_cases }}",
	)
	distroPathActivation := workflowStepBlock(t, distroJob, "Installer PATH activation matrix in container")
	requireStepContains(t, distroPathActivation,
		"CODEX_PROXY_REQUIRE_SHELL_MATRIX=1",
		"CODEX_PROXY_SHELL_MATRIX_CASES=\"$PATH_ACTIVATION_CASES\"",
		"bash /repo/scripts/ci/install_path_activation_matrix.sh",
	)

	aggregateJob := workflowJobBlock(t, workflow, "test")
	requireStepContains(t, aggregateJob,
		"name: Test (${{ matrix.os }})",
		"- targeted-test",
		"- full-go-test",
		"- race-test",
		"- linux-distro-smoke-build",
		"- linux-distro-smoke",
		"if: ${{ always() }}",
		"check targeted-test \"${{ needs.targeted-test.result }}\"",
		"check full-go-test \"${{ needs.full-go-test.result }}\"",
		"check race-test \"${{ needs.race-test.result }}\"",
		"check linux-distro-smoke-build \"${{ needs.linux-distro-smoke-build.result }}\"",
		"check linux-distro-smoke \"${{ needs.linux-distro-smoke.result }}\"",
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

func workflowJobBlock(t *testing.T, workflow string, id string) string {
	t.Helper()
	marker := "  " + id + ":"
	var builder strings.Builder
	inJob := false
	for _, line := range strings.SplitAfter(workflow, "\n") {
		trimmed := strings.TrimRight(line, "\r\n")
		if !inJob {
			if trimmed == marker {
				inJob = true
				builder.WriteString(line)
			}
			continue
		}
		if strings.HasPrefix(line, "  ") && len(line) > 2 && line[2] != ' ' && strings.TrimSpace(line) != "" {
			break
		}
		builder.WriteString(line)
	}
	if !inJob {
		t.Fatalf("workflow job %q not found", id)
	}
	return builder.String()
}

func requireStepContains(t *testing.T, text string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}

func requireStepNotContains(t *testing.T, text string, forbidden ...string) {
	t.Helper()
	for _, value := range forbidden {
		if strings.Contains(text, value) {
			t.Fatalf("unexpected %q in:\n%s", value, text)
		}
	}
}
