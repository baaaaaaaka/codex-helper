package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateTestJSONOutputAcceptsRequiredTestPass(t *testing.T) {
	data := []byte(`{"Action":"start","Package":"example.test"}
{"Action":"run","Package":"example.test","Test":"TestRecovery"}
{"Action":"output","Package":"example.test","Test":"TestRecovery","Output":"=== RUN   TestRecovery\\n"}
{"Action":"pass","Package":"example.test","Test":"TestRecovery"}
`)
	if err := validateTestJSONOutput(data, "TestRecovery"); err != nil {
		t.Fatalf("validateTestJSONOutput() error = %v", err)
	}
}

func TestValidateTestJSONOutputRejectsSkippedRequiredTest(t *testing.T) {
	data := []byte(`{"Action":"run","Package":"example.test","Test":"TestRecovery"}
{"Action":"skip","Package":"example.test","Test":"TestRecovery"}
`)
	if err := validateTestJSONOutput(data, "TestRecovery"); err == nil {
		t.Fatal("validateTestJSONOutput() accepted skipped required test")
	}
}

func TestValidateTestJSONOutputRejectsMissingRequiredTest(t *testing.T) {
	data := []byte(`{"Action":"pass","Package":"example.test","Test":"TestOther"}
`)
	if err := validateTestJSONOutput(data, "TestRecovery"); err == nil {
		t.Fatal("validateTestJSONOutput() accepted output without required test")
	}
}

func TestValidateTestJSONOutputRejectsSkippedRequiredSubtest(t *testing.T) {
	data := []byte(`{"Action":"run","Package":"example.test","Test":"TestRecovery"}
{"Action":"run","Package":"example.test","Test":"TestRecovery/sqlite"}
{"Action":"skip","Package":"example.test","Test":"TestRecovery/sqlite"}
{"Action":"pass","Package":"example.test","Test":"TestRecovery"}
`)
	if err := validateTestJSONOutput(data, "TestRecovery"); err == nil {
		t.Fatal("validateTestJSONOutput() accepted skipped required subtest")
	}
}

func TestValidateTestJSONOutputRequiresEveryDeclaredBackend(t *testing.T) {
	data := []byte(`{"Action":"run","Package":"example.test","Test":"TestRecovery"}
{"Action":"run","Package":"example.test","Test":"TestRecovery/json"}
{"Action":"pass","Package":"example.test","Test":"TestRecovery/json"}
{"Action":"pass","Package":"example.test","Test":"TestRecovery"}
`)
	if err := validateTestJSONOutput(data, "TestRecovery", []string{"json", "sqlite"}); err == nil {
		t.Fatal("validateTestJSONOutput() accepted a manifest with an unexecuted backend")
	}
}

func TestValidateTestJSONOutputAcceptsBooleanBackendSubtests(t *testing.T) {
	data := []byte(`{"Action":"run","Package":"example.test","Test":"TestRecovery"}
{"Action":"run","Package":"example.test","Test":"TestRecovery/sqlite=false"}
{"Action":"pass","Package":"example.test","Test":"TestRecovery/sqlite=false"}
{"Action":"run","Package":"example.test","Test":"TestRecovery/sqlite=true"}
{"Action":"pass","Package":"example.test","Test":"TestRecovery/sqlite=true"}
{"Action":"pass","Package":"example.test","Test":"TestRecovery"}
`)
	if err := validateTestJSONOutput(data, "TestRecovery", []string{"json", "sqlite"}); err != nil {
		t.Fatalf("validateTestJSONOutput() rejected boolean backend subtests: %v", err)
	}
}

func TestRunManifestTestsForcesOwnershipStressStrictMode(t *testing.T) {
	previous, wasSet := os.LookupEnv(teamsOwnershipStressStrictEnv)
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv(teamsOwnershipStressStrictEnv, previous)
			return
		}
		_ = os.Unsetenv(teamsOwnershipStressStrictEnv)
	})
	if err := os.Setenv(teamsOwnershipStressStrictEnv, "0"); err != nil {
		t.Fatalf("set non-strict parent environment: %v", err)
	}
	env := manifestChildEnvironment()
	want := teamsOwnershipStressStrictEnv + "=1"
	count := 0
	for _, entry := range env {
		if strings.HasPrefix(entry, teamsOwnershipStressStrictEnv+"=") {
			count++
			if entry != want {
				t.Fatalf("manifest child environment entry = %q, want %q", entry, want)
			}
		}
	}
	if count != 1 {
		t.Fatalf("manifest child environment contains %d strict entries, want exactly one", count)
	}
}

func TestManifestChildEnvironmentRemovesStalePhaseIdentity(t *testing.T) {
	t.Setenv(teamsOwnershipStressStrictEnv, "0")
	t.Setenv(manifestPhaseFileEnv, filepath.Join(t.TempDir(), "stale.jsonl"))
	t.Setenv(manifestTestNameEnv, "TestStale")
	env := manifestChildEnvironment()
	for _, entry := range env {
		if strings.HasPrefix(entry, manifestPhaseFileEnv+"=") || strings.HasPrefix(entry, manifestTestNameEnv+"=") {
			t.Fatalf("child environment leaked phase identity: %q", entry)
		}
	}
	for _, entry := range env {
		if entry == teamsOwnershipStressStrictEnv+"=1" {
			return
		}
	}
	t.Fatalf("child environment did not force %s=1", teamsOwnershipStressStrictEnv)
}

func TestInspectManifestPhaseFileReportsValidEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	data := []byte(`{"schema":1,"seq":1,"phase":"owner_ready","test":"TestRecovery"}
{"schema":1,"seq":2,"phase":"admission_committed","test":"TestRecovery"}
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write phase file: %v", err)
	}
	count, phaseErr := inspectManifestPhaseFile(path, "TestRecovery")
	if count != 2 || phaseErr != "" {
		t.Fatalf("inspect phase file = count %d err %q, want 2 and no error", count, phaseErr)
	}
}

func TestInspectManifestPhaseFileReportsMalformedEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.WriteFile(path, []byte(`{"schema":1,"seq":1,"phase":"owner_ready"}
not-json
`), 0o600); err != nil {
		t.Fatalf("write phase file: %v", err)
	}
	count, phaseErr := inspectManifestPhaseFile(path, "TestRecovery")
	if count != 1 || !strings.Contains(phaseErr, "decode phase event") {
		t.Fatalf("inspect malformed phase file = count %d err %q", count, phaseErr)
	}
}

func TestManifestTestWorkerCountForSerializesWindows(t *testing.T) {
	if got := manifestTestWorkerCountFor(true, "windows", 8); got != 1 {
		t.Fatalf("Windows race manifest workers = %d, want 1", got)
	}
	if got := manifestTestWorkerCountFor(false, "windows", 8); got != 1 {
		t.Fatalf("Windows normal manifest workers = %d, want 1", got)
	}
	if got := manifestTestWorkerCountFor(true, "linux", 8); got != maxManifestTestWorkers {
		t.Fatalf("Linux race manifest workers = %d, want %d", got, maxManifestTestWorkers)
	}
	if got := manifestTestWorkerCountFor(true, "linux", 0); got != 1 {
		t.Fatalf("zero-GOMAXPROCS manifest workers = %d, want 1", got)
	}
}

func TestManifestRunBudgetAccountsForDeclaredBackends(t *testing.T) {
	if got, want := manifestRunBudget(manifestTest{MaxSeconds: 20, Backends: []string{"json", "sqlite"}}), 40*time.Second; got != want {
		t.Fatalf("two-backend manifest budget = %s, want %s", got, want)
	}
	if got, want := manifestRunBudget(manifestTest{MaxSeconds: 20, Backends: []string{"json"}}), 20*time.Second; got != want {
		t.Fatalf("single-backend manifest budget = %s, want %s", got, want)
	}
	if got, want := manifestRunBudget(manifestTest{MaxSeconds: 20}), 20*time.Second; got != want {
		t.Fatalf("metadata-free manifest budget = %s, want %s", got, want)
	}
}

func TestSplitManifestTestsKeepsExclusiveFixturesOutOfParallelPool(t *testing.T) {
	tests := []manifestTest{
		{Name: "parallel-a"},
		{Name: "exclusive-a", Exclusive: true},
		{Name: "parallel-b"},
		{Name: "exclusive-b", Exclusive: true},
	}
	parallel, exclusive := splitManifestTests(tests)
	if got, want := []string{parallel[0].Name, parallel[1].Name}, []string{"parallel-a", "parallel-b"}; !equalStrings(got, want) {
		t.Fatalf("parallel tests = %v, want %v", got, want)
	}
	if got, want := []string{exclusive[0].Name, exclusive[1].Name}, []string{"exclusive-a", "exclusive-b"}; !equalStrings(got, want) {
		t.Fatalf("exclusive tests = %v, want %v", got, want)
	}
}

func TestPartitionManifestTestsCoversEachEntryExactlyOnce(t *testing.T) {
	tests := []manifestTest{
		{Name: "test-0"},
		{Name: "test-1"},
		{Name: "test-2"},
		{Name: "test-3"},
		{Name: "test-4"},
	}
	seen := make(map[string]int)
	for partition := 0; partition < 2; partition++ {
		for _, item := range partitionManifestTests(tests, 2, partition) {
			seen[item.Name]++
		}
	}
	if len(seen) != len(tests) {
		t.Fatalf("partition union contains %d entries, want %d: %#v", len(seen), len(tests), seen)
	}
	for _, item := range tests {
		if seen[item.Name] != 1 {
			t.Fatalf("manifest entry %q appears %d times across partitions, want once", item.Name, seen[item.Name])
		}
	}
}

func TestRecoveryManifestDeclaresResourceContract(t *testing.T) {
	m, err := readManifest("teams_recovery_tests.json")
	if err != nil {
		t.Fatalf("read recovery manifest: %v", err)
	}
	if m.Version != 2 {
		t.Fatalf("recovery manifest version = %d, want 2", m.Version)
	}
	for _, item := range m.Tests {
		if strings.TrimSpace(item.ResourceClass) == "" || !manifestResourceClassAllowed(item.ResourceClass) {
			t.Fatalf("test %q has invalid resource class %q", item.Name, item.ResourceClass)
		}
	}
}

func TestRecoveryManifestVersionTwoRequiresResourceClass(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	data, err := json.Marshal(manifest{Version: 2, Tests: []manifestTest{{Name: "TestRecovery"}}})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, err := readManifest(path); err == nil || !strings.Contains(err.Error(), "missing resource_class") {
		t.Fatalf("read manifest error = %v, want missing resource_class", err)
	}
}

func TestManifestResourceClassDerivesFailSafeDefaults(t *testing.T) {
	cases := []struct {
		name string
		item manifestTest
		want string
	}{
		{name: "exclusive", item: manifestTest{Exclusive: true}, want: "host_exclusive"},
		{name: "sqlite", item: manifestTest{Backends: []string{"sqlite"}}, want: "sqlite_fsync"},
		{name: "listener", item: manifestTest{RealListener: true}, want: "listener_async"},
		{name: "pure", item: manifestTest{}, want: "pure_cpu"},
		{name: "unknown explicit", item: manifestTest{ResourceClass: "future"}, want: "future"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := manifestResourceClass(tc.item); got != tc.want {
				t.Fatalf("resource class = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestManifestResourceLimiterUsesHostAndModeCaps(t *testing.T) {
	cases := []struct {
		name string
		race bool
		goos string
		want map[string]int
	}{
		{name: "linux normal", goos: "linux", want: map[string]int{
			"pure_cpu": 4, "sqlite_fsync": 2, "listener_async": 2, "host_exclusive": 1,
		}},
		{name: "linux race", race: true, goos: "linux", want: map[string]int{
			"pure_cpu": 4, "sqlite_fsync": 1, "listener_async": 1, "host_exclusive": 1,
		}},
		{name: "windows normal", goos: "windows", want: map[string]int{
			"pure_cpu": 1, "sqlite_fsync": 1, "listener_async": 1, "host_exclusive": 1,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limiter := newManifestResourceLimiter(tc.race, tc.goos, 4)
			for class, want := range tc.want {
				if got := limiter.limit[class]; got != want {
					t.Errorf("%s limit = %d, want %d", class, got, want)
				}
			}
			release := limiter.acquire("undeclared_future_class")
			limiter.mu.Lock()
			unknown := cap(limiter.sems["undeclared_future_class"])
			limiter.mu.Unlock()
			release()
			if unknown != 1 {
				t.Fatalf("undeclared resource cap = %d, want fail-safe 1", unknown)
			}
		})
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}
