package installtest

import (
	"os"
	"strings"
	"testing"
)

func readWorkflow(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(".github/workflows/" + name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

// The pull-request tier (ci.yml) runs the whole unit suite on every OS, the
// core packages under -race, the whole E2E suite, and the static checks.
func TestCIWorkflowPullRequestTierRunsEverySuiteWithoutWeakeningRequiredChecks(t *testing.T) {
	workflow := readWorkflow(t, "ci.yml")
	requireStepContains(t, workflow,
		"  push:\n    branches: [\"main\"]\n  pull_request:\n  workflow_call:\n",
		"concurrency:\n  group: ci-${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}\n  cancel-in-progress: true",
	)
	// Slow-tier jobs live in nightly.yml, not here.
	requireStepNotContains(t, workflow,
		"  targeted-test:", "  race-test:", "  linux-distro-smoke:", "  codex-runtime-contract:",
		"  proxy-recovery-soak:", "  proxy-recovery-loopback:", "  windows-proxy-lifecycle:",
		"-count=5", "SSH_TEST_ENABLED", "CODEX_INSTALL_TEST",
	)

	lint := workflowJobBlock(t, workflow, "lint")
	for _, step := range []struct{ name, want string }{
		{"gofmt (verify)", "gofmt -l ."},
		{"go mod tidy (verify)", "git diff --quiet go.mod go.sum"},
		{"Workflow lint", "go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.11"},
		{"Forbid external account credentials in repository CI", "secrets\\."},
		{"Check CI helper scripts", "bash -n \"$script\""},
		{"Check CI PowerShell scripts", "[scriptblock]::Create"},
		{"Script unit tests", "python3 -m unittest discover -s scripts/tests -p 'test_*.py'"},
	} {
		requireStepContains(t, workflowStepBlock(t, lint, step.name), step.want)
	}

	cross := workflowJobBlock(t, workflow, "cross-build")
	requireStepContains(t, workflowStepBlock(t, cross, "go vet (Linux, Windows, macOS)"),
		"go vet ./...",
		"for goos in windows darwin; do",
		"GOOS=\"$goos\" go vet \"${packages[@]}\"",
	)
	requireStepContains(t, workflowStepBlock(t, cross, "Cross-compile check"),
		"for target in darwin/arm64 darwin/amd64 windows/amd64",
		`GOOS="$goos" GOARCH="$goarch" go list`,
		`GOOS="$goos" GOARCH="$goarch" go build "${packages[@]}"`,
		"GOOS=linux   GOARCH=arm64 go build ./...",
	)

	unitLinux := workflowJobBlock(t, workflow, "unit-linux")
	requireStepContains(t, workflowStepBlock(t, unitLinux, "go test (with coverage)"),
		"bash scripts/ci/run_linux_full_go_test_collect.sh",
	)
	requireStepContains(t, workflowStepBlock(t, unitLinux, "Coverage gate"),
		`threshold="40.0"`,
		"go tool cover -func=coverage.out",
	)
	requireStepContains(t, unitLinux,
		"bash scripts/tests/check_teams_test_user_dir_isolation.sh",
		"bash scripts/tests/run_teams_runtime_safety_shard.sh isolation",
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

	race := workflowJobBlock(t, workflow, "race-core")
	requireStepContains(t, race,
		"go test -race -p=1 -count=1 -timeout=15m -parallel=8 ./internal/core/... ./internal/channel/... ./internal/asr",
	)

	e2e := workflowJobBlock(t, workflow, "e2e")
	requireStepContains(t, e2e,
		"runs-on: ubuntu-latest",
		"shard: [0, 1, 2]",
		"E2E_SUT: cxp",
		"python3 scripts/ci/e2e_shards.py --count 3 --index \"${{ matrix.shard }}\"",
		"go test -count=1 -parallel=8 -timeout=600s \"${packages[@]}\"",
	)

	platforms := workflowJobBlock(t, workflow, "unit-windows-macos")
	requireStepContains(t, platforms,
		"os: [windows-latest, macos-latest]",
		"partition: [0, 1]",
	)
	platformTest := workflowStepBlock(t, platforms, "go test")
	requireStepContains(t, platformTest,
		"go run ./scripts/ci/run_full_go_test_shards.go -timeout=20m -parallel=16 -shards=16 -partition-count=2 -partition-index=\"${{ matrix.partition }}\" \"${exclude[@]}\"",
		"-exclude-prefix=github.com/baaaaaaaka/codex-helper/internal/e2e/scenarios/",
		"Test/Example/Fuzz",
	)
	requireStepNotContains(t, platformTest, "coverprofile", "go test -timeout=20m -parallel=16 ./...")
	requireStepContains(t, workflowStepBlock(t, platforms, "Teams test user-directory isolation guard (Windows)"),
		"if: runner.os == 'Windows' && matrix.partition == 0",
		`.\scripts\tests\check_teams_test_user_dir_isolation.ps1`,
	)

	aggregate := workflowJobBlock(t, workflow, "test")
	requireStepContains(t, aggregate, "name: Test", "if: ${{ always() }}")
	for _, job := range []string{"lint", "cross-build", "unit-linux", "race-core", "e2e", "unit-windows-macos"} {
		requireStepContains(t, aggregate,
			"      - "+job+"\n",
			"check "+job+" \"${{ needs."+job+".result }}\"",
		)
	}
}

// The slow tier (nightly.yml) keeps every check the per-OS unit suite cannot
// make, and release.yml publishes only after both tiers pass.
func TestNightlyWorkflowKeepsSlowTierChecksAndGatesReleases(t *testing.T) {
	workflow := readWorkflow(t, "nightly.yml")
	requireStepContains(t, workflow, "  schedule:\n", "  workflow_dispatch:\n", "  workflow_call:\n")

	platform := workflowJobBlock(t, workflow, "platform-integration")
	for _, combo := range []string{
		"- os: ubuntu-latest\n            shard: platform-integration",
		"- os: macos-latest\n            shard: platform-integration",
		"- os: windows-latest\n            shard: platform-integration",
		"- os: ubuntu-latest\n            shard: state-perf",
		"- os: windows-latest\n            shard: windows-skills-desktop",
		"- os: windows-latest\n            shard: windows-skills-desktop-b",
		"- os: windows-latest\n            shard: windows-codex-e2e",
	} {
		requireStepContains(t, platform, combo)
	}
	requireStepContains(t, workflowStepBlock(t, platform, "Install PATH activation matrix (Linux only)"),
		"if: matrix.shard == 'platform-integration' && runner.os == 'Linux'",
		"apt-get install -y --no-install-recommends zsh fish csh tcsh",
		"CODEX_PROXY_REQUIRE_SHELL_MATRIX=1 bash scripts/ci/install_path_activation_matrix.sh",
	)
	requireStepContains(t, workflowStepBlock(t, platform, "Install PATH activation matrix (macOS only)"),
		"if: matrix.shard == 'platform-integration' && runner.os == 'macOS'",
		"HOMEBREW_NO_AUTO_UPDATE=1 brew install fish",
		"CODEX_PROXY_REQUIRE_SHELL_MATRIX=1 bash scripts/ci/install_path_activation_matrix.sh",
	)
	requireStepContains(t, workflowStepBlock(t, platform, "Skills local git smoke (Windows)"),
		"if: matrix.shard == 'windows-skills-desktop' && runner.os == 'Windows'",
		`.\scripts\ci\skills_smoke.ps1`,
		`.\scripts\ci\skills_migration_smoke.ps1`,
	)
	requireStepNotContains(t, platform, "- name: Codex desktop app network install smoke (Windows)")
	managedDesktop := workflowStepBlock(t, platform, "Codex desktop app managed runtime smoke (Windows)")
	requireStepContains(t, managedDesktop,
		"matrix.shard == 'windows-skills-desktop-b' && runner.os == 'Windows' && !cancelled()",
		"steps.windows_desktop_build.outcome == 'success'",
		"-Mode managed",
	)
	requireStepNotContains(t, managedDesktop, "continue-on-error", "steps.windows_desktop_store")
	signedDesktop := workflowStepBlock(t, platform, "Codex desktop signed AppX provisioning and launch smoke (Windows)")
	requireStepContains(t, signedDesktop, "!cancelled()", "steps.windows_desktop_build.outcome == 'success'", "-Mode appx", "-TokenProbe")
	requireStepNotContains(t, signedDesktop, "continue-on-error", "steps.windows_desktop_store", "steps.windows_desktop_managed")
	requireStepContains(t, workflowStepBlock(t, platform, "Codex desktop app Windows build and policy regressions"),
		"CurrentWindowsTokenElevationQuery", "RootUpgradeCodexApp|WindowsManagedApp", "codex_app_smoke_process_test.ps1")
	for _, name := range []string{
		"Install Codex for integration (Windows)",
		"Teams app-server probe (Windows)",
		"Codex upgrade integration (system npm, Windows)",
		"Codex upgrade integration (managed npm with unrelated PATH Codex, Windows)",
		"Codex approval, history, and cancellation runtime integration (Windows)",
		"Native managed-node install integration (Windows)",
	} {
		requireStepContains(t, workflowStepBlock(t, platform, name),
			"if: matrix.shard == 'windows-codex-e2e' && runner.os == 'Windows'")
	}
	for _, name := range []string{
		"CXP preview SQLite actual syscall write budget (Linux only)",
		"CXP cache v2 real NFS concurrency smoke (Linux only)",
		"Codex runner benchmark smoke",
		"TUI and session history benchmark smoke",
	} {
		requireStepContains(t, workflowStepBlock(t, platform, name), "if: matrix.shard == 'state-perf'")
	}

	race := workflowJobBlock(t, workflow, "race-test")
	requireStepContains(t, race,
		"partition: [0, 1, 2, 3]",
		"go run ./scripts/ci/run_full_go_test_shards.go -race -timeout=30m -parallel=16 -shards=16 -partition-count=4 -partition-index=\"${{ matrix.partition }}\" -exclude-prefix=github.com/baaaaaaaka/codex-helper/internal/e2e/scenarios/",
	)
	requireStepContains(t, workflowJobBlock(t, workflow, "windows-proxy-lifecycle"),
		"go test -p=1 ./internal/helperruntime -count=1 -run '^TestRuntimeProcessIdentityWindows$' -v",
	)
	requireStepContains(t, workflowJobBlock(t, workflow, "proxy-recovery-soak"), "-count=5")
	requireStepContains(t, workflowJobBlock(t, workflow, "proxy-recovery-docker"), "bash scripts/ci/proxy_recovery_docker_smoke.sh")
	requireStepContains(t, workflowJobBlock(t, workflow, "codex-runtime-contract"), `codex-version: ["0.131.0", "latest"]`)

	distro := workflowJobBlock(t, workflow, "linux-distro-smoke")
	requireStepContains(t, distro,
		"path_activation_package_args: zsh tcsh",
		"path_activation_package_args: zsh fish csh tcsh",
		"PATH_ACTIVATION_PACKAGE_ARGS: ${{ matrix.path_activation_package_args }}",
		"PATH_ACTIVATION_CASES: ${{ matrix.path_activation_cases }}",
	)
	requireStepContains(t, workflowStepBlock(t, distro, "Installer PATH activation matrix in container"),
		"CODEX_PROXY_REQUIRE_SHELL_MATRIX=1",
		"CODEX_PROXY_SHELL_MATRIX_CASES=\"$PATH_ACTIVATION_CASES\"",
		"bash /repo/scripts/ci/install_path_activation_matrix.sh",
	)

	aggregate := workflowJobBlock(t, workflow, "nightly")
	requireStepContains(t, aggregate, "if: ${{ always() }}")
	for _, job := range []string{
		"platform-integration", "race-test", "proxy-recovery-loopback", "windows-proxy-lifecycle",
		"proxy-recovery-docker", "proxy-recovery-soak", "codex-runtime-contract",
		"linux-distro-smoke-build", "linux-distro-smoke",
	} {
		requireStepContains(t, aggregate,
			"      - "+job+"\n",
			"check "+job+" \"${{ needs."+job+".result }}\"",
		)
	}

	release := readWorkflow(t, "release.yml")
	requireStepContains(t, release,
		"uses: ./.github/workflows/ci.yml",
		"uses: ./.github/workflows/nightly.yml",
	)
	requireStepContains(t, workflowJobBlock(t, release, "goreleaser"), "needs: [ci, nightly, ")
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

func TestCITitleProtocolContractSelection(t *testing.T) {
	workflow := readWorkflow(t, "ci.yml")
	requireStepContains(t, workflowJobBlock(t, workflow, "shared-codex-contract"), "bash scripts/ci/run_shared_codex_contracts.sh")
	raw, err := os.ReadFile("scripts/ci/run_shared_codex_contracts.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "-test.run 'TestRealCodex(Shared|NativeQueue|Title)'") {
		t.Fatal("native title contract is not selected by CI")
	}
}
