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
		"name: Targeted test (${{ matrix.os }} / ${{ matrix.shard }} / partition ${{ matrix.partition }})",
		"os: [ubuntu-latest, macos-latest, windows-latest]",
		"shard: [core, core-b, platform-integration, state-perf, ubuntu-stress, windows-skills-desktop, windows-skills-desktop-b, windows-codex-e2e]",
		"- os: macos-latest\n            shard: ubuntu-stress",
		"- os: windows-latest\n            shard: ubuntu-stress",
		"- os: ubuntu-latest\n            shard: windows-skills-desktop",
		"- os: ubuntu-latest\n            shard: windows-codex-e2e",
		"- os: macos-latest\n            shard: windows-skills-desktop",
		"- os: ubuntu-latest\n            shard: windows-skills-desktop-b",
		"- os: macos-latest\n            shard: windows-skills-desktop-b",
		"- os: macos-latest\n            shard: windows-codex-e2e",
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
	windowsSkills := workflowStepBlock(t, targetedJob, "Skills local git smoke (Windows)")
	requireStepContains(t, windowsSkills,
		"if: matrix.shard == 'windows-skills-desktop' && runner.os == 'Windows'",
		`.\scripts\ci\skills_smoke.ps1`,
		`.\scripts\ci\skills_migration_smoke.ps1`,
	)
	requireStepNotContains(t, targetedJob, "- name: Codex desktop app network install smoke (Windows)")
	managedDesktop := workflowStepBlock(t, targetedJob, "Codex desktop app managed runtime smoke (Windows)")
	requireStepContains(t, managedDesktop,
		"matrix.shard == 'windows-skills-desktop-b' && runner.os == 'Windows' && !cancelled()",
		"steps.windows_desktop_build.outcome == 'success'",
		"-Mode managed",
	)
	requireStepNotContains(t, managedDesktop, "continue-on-error", "steps.windows_desktop_store")
	signedDesktop := workflowStepBlock(t, targetedJob, "Codex desktop signed AppX provisioning and launch smoke (Windows)")
	requireStepContains(t, signedDesktop, "!cancelled()", "steps.windows_desktop_build.outcome == 'success'", "-Mode appx", "-TokenProbe")
	requireStepNotContains(t, signedDesktop, "continue-on-error", "steps.windows_desktop_store", "steps.windows_desktop_managed")
	desktopBuild := workflowStepBlock(t, targetedJob, "Codex desktop app Windows build and policy regressions")
	requireStepContains(t, desktopBuild, "CurrentWindowsTokenElevationQuery", "RootUpgradeCodexApp|WindowsManagedApp", "codex_app_smoke_process_test.ps1")
	for _, name := range []string{
		"Install Codex for integration (Windows)",
		"Teams app-server probe (Windows)",
		"Codex upgrade integration (system npm, Windows)",
		"Codex upgrade integration (managed npm with unrelated PATH Codex, Windows)",
		"Codex approval, history, and cancellation runtime integration (Windows)",
		"Native managed-node install integration (Windows)",
	} {
		step := workflowStepBlock(t, targetedJob, name)
		requireStepContains(t, step,
			"if: matrix.shard == 'windows-codex-e2e' && runner.os == 'Windows'",
		)
	}
	crossCompile := workflowStepBlock(t, targetedJob, "Cross-compile check (Linux only)")
	requireStepContains(t, crossCompile,
		"if: matrix.shard == 'ubuntu-stress' && runner.os == 'Linux'",
	)
	statePerf := workflowStepBlock(t, targetedJob, "Helper state directory migration regressions")
	requireStepContains(t, statePerf,
		"if: matrix.shard == 'state-perf' && matrix.partition == 1",
		"SubprocessMigrationStressCI",
	)

	windowsLifecycle := workflowJobBlock(t, workflow, "windows-proxy-lifecycle")
	requireStepContains(t, windowsLifecycle,
		"go test -p=1 ./internal/helperruntime -count=1 -run '^TestRuntimeProcessIdentityWindows$' -v",
	)

	fullJob := workflowJobBlock(t, workflow, "full-go-test")
	requireStepContains(t, fullJob,
		"name: Full go test (${{ matrix.os }} / partition ${{ matrix.partition }})",
		"os: [ubuntu-latest, macos-latest, windows-latest]",
		"partition: [0, 1]",
		"timeout-minutes: 45",
	)

	linuxCoverage := workflowStepBlock(t, fullJob, "go test (with coverage, Linux only)")
	requireStepContains(t, linuxCoverage,
		"if: runner.os == 'Linux'",
		"shell: bash",
		"bash scripts/ci/run_linux_full_go_test_collect.sh",
	)
	collectorData, err := os.ReadFile("scripts/ci/run_linux_full_go_test_collect.sh")
	if err != nil {
		t.Fatalf("read Linux full-test collector: %v", err)
	}
	collector := string(collectorData)
	requireStepContains(t, collector,
		"migration_process_pattern='^TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup$'",
		"*/internal/e2e/scenarios/*) ;;",
		"test -timeout=20m -parallel=16 -skip \"$isolated_skip_pattern\" -coverprofile=\"$coverage_file\" \"${packages[@]}\"",
		"test ./internal/cli -timeout=2m -parallel=16 -count=1 -run \"$migration_process_pattern\"",
		"status=${PIPESTATUS[0]}",
		"failures+=(\"$label\")",
	)

	nonLinuxTest := workflowStepBlock(t, fullJob, "go test (without coverage, non-Linux)")
	requireStepContains(t, nonLinuxTest,
		"if: runner.os != 'Linux'",
		"shell: bash",
		"go run ./scripts/ci/run_full_go_test_shards.go -timeout=20m -parallel=16 -shards=16 -partition-count=2 -partition-index=\"${{ matrix.partition }}\" -exclude-prefix=github.com/baaaaaaaka/codex-helper/internal/e2e/scenarios/",
		"Test/Example/Fuzz",
	)
	if strings.Contains(nonLinuxTest, "go test -timeout=20m -parallel=16 ./...") {
		t.Fatal("non-Linux full test step should use validated package shards")
	}
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
		"name: Race test (ubuntu-latest / partition ${{ matrix.partition }})",
		"partition: [0, 1, 2, 3, 4, 5, 6, 7]",
		"timeout-minutes: 45",
		"go run ./scripts/ci/run_full_go_test_shards.go -race -timeout=30m -parallel=16 -shards=16 -partition-count=8 -partition-index=\"${{ matrix.partition }}\" -exclude-prefix=github.com/baaaaaaaka/codex-helper/internal/e2e/scenarios/",
		"go test -race -count=1 -timeout=20m ./internal/core/... ./internal/channel/...",
	)

	e2eJob := workflowJobBlock(t, workflow, "e2e")
	requireStepContains(t, e2eJob,
		"runs-on: ubuntu-latest",
		"E2E_SUT: cxp",
		"go test -count=1 -timeout=600s ./internal/e2e/scenarios/...",
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
		"name: Test",
		"- targeted-test",
		"- e2e",
		"- full-go-test",
		"- race-test",
		"- linux-distro-smoke-build",
		"- linux-distro-smoke",
		"if: ${{ always() }}",
		"check targeted-test \"${{ needs.targeted-test.result }}\"",
		"check e2e \"${{ needs.e2e.result }}\"",
		"check full-go-test \"${{ needs.full-go-test.result }}\"",
		"check race-test \"${{ needs.race-test.result }}\"",
		"check linux-distro-smoke-build \"${{ needs.linux-distro-smoke-build.result }}\"",
		"check linux-distro-smoke \"${{ needs.linux-distro-smoke.result }}\"",
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
