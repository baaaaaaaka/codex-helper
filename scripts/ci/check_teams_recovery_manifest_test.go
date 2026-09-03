package main

import (
	"os"
	"strings"
	"testing"
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

func TestManifestTestWorkerCountForSerializesWindowsRaceOnly(t *testing.T) {
	if got := manifestTestWorkerCountFor(true, "windows", 8); got != 1 {
		t.Fatalf("Windows race manifest workers = %d, want 1", got)
	}
	if got := manifestTestWorkerCountFor(false, "windows", 8); got != maxManifestTestWorkers {
		t.Fatalf("Windows normal manifest workers = %d, want %d", got, maxManifestTestWorkers)
	}
	if got := manifestTestWorkerCountFor(true, "linux", 8); got != maxManifestTestWorkers {
		t.Fatalf("Linux race manifest workers = %d, want %d", got, maxManifestTestWorkers)
	}
	if got := manifestTestWorkerCountFor(true, "linux", 0); got != 1 {
		t.Fatalf("zero-GOMAXPROCS manifest workers = %d, want 1", got)
	}
}
