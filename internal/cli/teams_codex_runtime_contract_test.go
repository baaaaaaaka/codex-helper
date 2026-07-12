package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

func TestTeamsCodexRuntimeContractFindsManagedCodexOutsideServicePATH(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX managed launcher fixture")
	}
	lockCLITestHooks(t)
	prefix := t.TempDir()
	managedBin := filepath.Join(prefix, "bin")
	if err := os.MkdirAll(managedBin, 0o755); err != nil {
		t.Fatal(err)
	}
	managed := writeProbeScript(t, managedBin, "codex", "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'codex-cli 1.0.0'; exit 0; fi\n")
	packageRoot := filepath.Join(prefix, "lib", "node_modules", "@openai", "codex")
	if err := os.MkdirAll(packageRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packageRoot, "package.json"), []byte(`{"version":"1.0.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	serviceBin := t.TempDir()
	t.Setenv("PATH", serviceBin)
	t.Setenv("CODEX_NPM_PREFIX", prefix)

	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	contract, err := resolveTeamsCodexRuntimeContract(context.Background(), &rootOptions{configPath: configPath}, "", t.TempDir(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if contract.Runtime.Command != managed {
		t.Fatalf("runtime command = %q, want managed %q", contract.Runtime.Command, managed)
	}
	if strings.Contains(envValue(contract.Runtime.Environment, "PATH"), managedBin) {
		t.Fatalf("test precondition failed: managed launcher leaked into service PATH: %q", envValue(contract.Runtime.Environment, "PATH"))
	}
	if strings.TrimSpace(contract.Fingerprint) == "" || strings.TrimSpace(envValue(contract.Runtime.Environment, "CODEX_HOME")) == "" {
		t.Fatalf("incomplete runtime contract: %#v", contract)
	}
}

func TestTeamsCodexRuntimeContractSilentlyInstallsMissingManagedCodex(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX managed launcher fixture")
	}
	lockCLITestHooks(t)
	serviceBin := t.TempDir()
	t.Setenv("PATH", serviceBin)
	t.Setenv("CODEX_NPM_PREFIX", t.TempDir())
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	installed := writeProbeScript(t, t.TempDir(), "codex", "#!/bin/sh\necho 'codex-cli 1.0.0'\n")
	previousEnsure := ensureManagedTeamsCodexForRuntime
	t.Cleanup(func() { ensureManagedTeamsCodexForRuntime = previousEnsure })
	calls := 0
	ensureManagedTeamsCodexForRuntime = func(_ context.Context, gotStore *config.Store, _ config.Config, environment []string, identity *execIdentity, _ io.Writer) (string, error) {
		calls++
		if gotStore.Path() != store.Path() || envValue(environment, "PATH") != serviceBin || identity != nil {
			t.Fatalf("install contract = store %q PATH %q identity %#v", gotStore.Path(), envValue(environment, "PATH"), identity)
		}
		return installed, nil
	}
	contract, err := resolveTeamsCodexRuntimeContract(context.Background(), &rootOptions{configPath: configPath}, "", t.TempDir(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || contract.Runtime.Command != installed {
		t.Fatalf("first-use install calls=%d command=%q, want 1 and %q", calls, contract.Runtime.Command, installed)
	}
}

func TestTeamsModelSurfacesShareResolvedRuntimeContract(t *testing.T) {
	lockCLITestHooks(t)
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	contract := teamsCodexRuntimeContract{
		Runtime:     resolvedCodexRuntime{Command: "/managed/private/codex", Environment: []string{"PATH=/service/without-codex", "CODEX_HOME=/target/.codex"}},
		Fingerprint: "runtime-fingerprint",
	}
	resolverCalls := 0
	resolver := func(context.Context) (teamsCodexRuntimeContract, error) {
		resolverCalls++
		return contract, nil
	}
	previousProbe := codexLoginStatusProbeFn
	previousCatalog := listTeamsOfficialModelsFn
	t.Cleanup(func() {
		codexLoginStatusProbeFn = previousProbe
		listTeamsOfficialModelsFn = previousCatalog
	})
	codexLoginStatusProbeFn = func(_ context.Context, path string, environment []string, _ io.Writer) bool {
		if path != contract.Runtime.Command || envValue(environment, "CODEX_HOME") != "/target/.codex" {
			t.Fatalf("login probe runtime = path %q env %#v", path, environment)
		}
		return true
	}
	listTeamsOfficialModelsFn = func(ctx context.Context, path string) ([]teamsOfficialModel, error) {
		invocation, ok := codexInvocationFromContext(ctx)
		if path != contract.Runtime.Command || !ok || invocation.Fingerprint != contract.Fingerprint || envValue(invocation.Environment, "CODEX_HOME") != "/target/.codex" {
			t.Fatalf("catalog runtime = path %q invocation %#v ok=%v", path, invocation, ok)
		}
		return []teamsOfficialModel{{Slug: "gpt-test", DisplayName: "GPT Test", IsDefault: true, DefaultReasoningLevel: "high"}}, nil
	}

	out, err := newTeamsModelProfileManagerWithRuntime(&rootOptions{configPath: configPath}, resolver).ListModelProfiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "GPT Test (`gpt-test`)") {
		t.Fatalf("model list did not use shared runtime:\n%s", out)
	}
	snapshot, err := newTeamsModelProfileResolverWithRuntime(&rootOptions{configPath: configPath}, resolver)(context.Background(), "official:gpt-test")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Provider != modelprofile.DefaultProvider || snapshot.Model != "gpt-test" {
		t.Fatalf("official snapshot = %#v", snapshot)
	}
	if resolverCalls != 2 {
		t.Fatalf("runtime resolver calls = %d, want one per user operation", resolverCalls)
	}
}

func TestTeamsUnifiedPreparationUsesResolvedRuntimeEnvironment(t *testing.T) {
	lockCLITestHooks(t)
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Version: config.CurrentVersion, ModelProfiles: map[string]config.ModelProfile{
		"mimo": {Provider: "mimo", Model: "mimo/mimo-v2.5", APIKeyRef: "env:TEST_TEAMS_RUNTIME_MIMO_KEY", Revision: 1},
	}}
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_TEAMS_RUNTIME_MIMO_KEY", "test-key")
	previousProbe := codexLoginStatusProbeFn
	previousCatalog := loadBundledCodexModelCatalogFn
	t.Cleanup(func() {
		codexLoginStatusProbeFn = previousProbe
		loadBundledCodexModelCatalogFn = previousCatalog
	})
	codexLoginStatusProbeFn = func(_ context.Context, path string, environment []string, _ io.Writer) bool {
		if path != "/managed/private/codex" || envValue(environment, "CODEX_HOME") != "/target/.codex" {
			t.Fatalf("unified login probe runtime = path %q env %#v", path, environment)
		}
		return true
	}
	loadBundledCodexModelCatalogFn = func(ctx context.Context, path string) ([]byte, error) {
		invocation, ok := codexInvocationFromContext(ctx)
		if path != "/managed/private/codex" || !ok || envValue(invocation.Environment, "CODEX_HOME") != "/target/.codex" {
			t.Fatalf("unified catalog runtime = path %q invocation %#v ok=%v", path, invocation, ok)
		}
		return []byte(`{"models":[{"slug":"gpt-test","priority":1}]}`), nil
	}
	ctx := withCodexInvocation(context.Background(), codexInvocation{
		Command:     "/managed/private/codex",
		Environment: []string{"PATH=/service/without-codex", "CODEX_HOME=/target/.codex"},
		Fingerprint: "runtime-fingerprint",
	})
	args, _, cleanup, err := prepareTeamsAppServerModelProfileWithContext(ctx, &rootOptions{configPath: configPath}, "mimo", modelprofile.Snapshot{}, io.Discard)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		t.Fatal(err)
	}
	if joined := strings.Join(args, "\n"); !strings.Contains(joined, `model="mimo/mimo-v2.5"`) {
		t.Fatalf("unified launch args missing selected model: %v", args)
	}
}

func TestTeamsModelListReportsRuntimeFailureInsteadOfPretendingLoggedOut(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion}); err != nil {
		t.Fatal(err)
	}
	manager := newTeamsModelProfileManagerWithRuntime(&rootOptions{configPath: configPath}, func(context.Context) (teamsCodexRuntimeContract, error) {
		return teamsCodexRuntimeContract{}, fmt.Errorf("managed launcher missing")
	})
	out, err := manager.ListModelProfiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "official catalog unavailable") || !strings.Contains(out, "managed launcher missing") {
		t.Fatalf("runtime failure was hidden as logout:\n%s", out)
	}
}

func TestTeamsRuntimeFingerprintSeparatesAccountsAndBinaryRevisions(t *testing.T) {
	command := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(command, []byte("first"), 0o700); err != nil {
		t.Fatal(err)
	}
	first := teamsCodexRuntimeContract{Runtime: resolvedCodexRuntime{Command: command, WrapperCommand: command, Environment: []string{"PATH=/bin", "CODEX_HOME=/account-a/.codex"}}}
	firstFingerprint := teamsCodexRuntimeFingerprint(first)
	second := first
	second.Runtime.Environment = []string{"PATH=/bin", "CODEX_HOME=/account-b/.codex"}
	if secondFingerprint := teamsCodexRuntimeFingerprint(second); secondFingerprint == firstFingerprint {
		t.Fatal("different account CODEX_HOME values shared an official-model cache fingerprint")
	}
	if err := os.WriteFile(command, []byte("second-revision"), 0o700); err != nil {
		t.Fatal(err)
	}
	if revised := teamsCodexRuntimeFingerprint(first); revised == firstFingerprint {
		t.Fatal("changed Codex binary shared an official-model cache fingerprint")
	}
}

func TestTeamsCodexUpgradeInvalidatesOfficialCatalogCache(t *testing.T) {
	teamsOfficialModelCache.Lock()
	teamsOfficialModelCache.byPath = map[string]teamsOfficialModelCacheEntry{
		"old-runtime": {models: []teamsOfficialModel{{Slug: "gpt-old"}}},
	}
	teamsOfficialModelCache.Unlock()
	invalidateTeamsOfficialModelCache()
	teamsOfficialModelCache.Lock()
	defer teamsOfficialModelCache.Unlock()
	if len(teamsOfficialModelCache.byPath) != 0 {
		t.Fatalf("official model cache survived Codex upgrade invalidation: %#v", teamsOfficialModelCache.byPath)
	}
}

func TestBeaconDoctorDoesNotRequireNakedCodexForManagedRuntime(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH fixture uses POSIX semantics")
	}
	t.Setenv("PATH", t.TempDir())
	t.Setenv("CODEX_NPM_PREFIX", t.TempDir())
	doctor := runBeaconWorkerDoctor("", filepath.Join(t.TempDir(), "beacon.json"))
	if !doctor.CodexAvailable {
		t.Fatal("Beacon doctor rejected a worker before the managed runtime resolver could install Codex")
	}
}

func TestTeamsRuntimeWiringGuard(t *testing.T) {
	source, err := os.ReadFile("teams.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{
		"officialRuntimeResolver := newTeamsCodexRuntimeResolver",
		"newTeamsModelProfileManagerWithRuntime(root, officialRuntimeResolver)",
		"newTeamsModelProfileResolverWithRuntime(root, officialRuntimeResolver)",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("Teams runtime wiring lost required shared-contract expression %q", required)
		}
	}
	for _, forbidden := range []string{
		"ModelProfileManager:                newTeamsModelProfileManager(root, codexPath)",
		"ModelProfileResolver:               newTeamsModelProfileResolver(root, codexPath)",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Teams runtime wiring regressed to a raw Codex path: %q", forbidden)
		}
	}
}

func TestTeamsBareCodexAuditHasNoUnreviewedConsumers(t *testing.T) {
	// These are the only reviewed compatibility defaults. They do not execute
	// the Teams service's model/auth/catalog path: the app-server starter
	// replaces the executor placeholder, the doctor probe is explicitly
	// user-selected, and the legacy constructors are retained for direct unit
	// callers. Any added occurrence requires a runtime-contract review.
	expected := map[string]map[string]int{
		"teams.go": {
			`command = "codex"`:      1,
			`exec.LookPath("codex")`: 0,
		},
		"teams_codex_runner.go": {
			`command = "codex"`:      1,
			`codexPath := "codex"`:   1,
			`exec.LookPath("codex")`: 0,
		},
		"teams_model_profile.go": {
			`path := "codex"`:        1,
			`exec.LookPath("codex")`: 0,
		},
		"beacon.go": {
			`exec.LookPath("codex")`: 0,
		},
	}
	for name, checks := range expected {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(source)
		for expression, want := range checks {
			if got := strings.Count(text, expression); got != want {
				t.Fatalf("%s contains %d occurrences of %q, want reviewed count %d; route new Codex consumers through teamsCodexRuntimeContract", name, got, expression, want)
			}
		}
	}
}
