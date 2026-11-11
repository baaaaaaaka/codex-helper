//go:build ci_runner_test

package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestJobStreamsAndRetainsFailure(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("RUNNER_TEMP", dir)
	t.Setenv("GO111MODULE", "off")
	fixture := filepath.Join(dir, "fixture")
	if err := os.Mkdir(fixture, 0700); err != nil {
		t.Fatal(err)
	}
	data := `package fixture
import ("fmt"; "strings"; "testing"; "time")
func TestFailure(t *testing.T) { fmt.Println("STREAM-MARKER", strings.Repeat("x", 100000)); time.Sleep(3*time.Second); t.Fatal("intentional failure") }
`
	if err := os.WriteFile(filepath.Join(fixture, "fixture_test.go"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan jobResult, 1)
	go func() {
		done <- executeJob(testJob{label: "fixture", args: []string{"-C", fixture, "test", "-v", "."}}, time.Minute)
	}()
	deadline := time.After(90 * time.Second)
	for {
		select {
		case result := <-done:
			t.Fatalf("job completed before live output was observed: %v", result.err)
		case <-deadline:
			t.Fatal("no live output observed")
		case <-time.After(50 * time.Millisecond):
			files, err := filepath.Glob(filepath.Join(dir, "cxp-ci-job-*.log"))
			if err != nil {
				t.Fatal(err)
			}
			if len(files) == 0 {
				continue
			}
			b, err := os.ReadFile(files[0])
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(b), "STREAM-MARKER") {
				continue
			}
			result := <-done
			if result.err == nil {
				t.Fatal("test failure was lost")
			}
			b, err = os.ReadFile(files[0])
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(b), "intentional failure") || len(b) < 100000 {
				t.Fatal("failure or long output missing from log")
			}
			return
		}
	}
}

func TestJobRejectsUnavailableLogDirectory(t *testing.T) {
	t.Setenv("RUNNER_TEMP", filepath.Join(t.TempDir(), "missing"))
	result := executeJob(testJob{label: "bad-log", args: []string{"version"}}, time.Minute)
	if result.err == nil || !strings.Contains(result.err.Error(), "create job log") {
		t.Fatalf("expected log creation error, got %v", result.err)
	}
}

func TestJobReportsCompilerFailure(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("RUNNER_TEMP", dir)
	t.Setenv("GO111MODULE", "off")
	if err := os.WriteFile(filepath.Join(dir, "bad.go"), []byte("package bad\nfunc broken(\n"), 0600); err != nil {
		t.Fatal(err)
	}
	result := executeJob(testJob{label: "compiler", args: []string{"-C", dir, "test", "."}}, time.Minute)
	if result.err == nil {
		t.Fatal("compiler failure was ignored")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "cxp-ci-job-*.log"))
	if len(files) != 1 {
		t.Fatalf("expected one diagnostic log, got %v", files)
	}
	contents, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "syntax error") {
		t.Fatalf("missing compiler diagnostic: %s", contents)
	}
}

func TestJobReportsTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX fake Go command")
	}
	dir := t.TempDir()
	t.Setenv("RUNNER_TEMP", dir)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte("#!/bin/sh\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	result := executeJob(testJob{label: "timeout", args: []string{"test"}}, -3*time.Minute+time.Second)
	if result.err == nil || !strings.Contains(result.err.Error(), "outer timeout") {
		t.Fatalf("expected timeout, got %v", result.err)
	}
}

func TestJobReportsPanic(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("RUNNER_TEMP", dir)
	t.Setenv("GO111MODULE", "off")
	source := "package panictest\nimport \"testing\"\nfunc TestPanic(t *testing.T) { panic(\"deliberate panic\") }\n"
	if err := os.WriteFile(filepath.Join(dir, "panic_test.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	result := executeJob(testJob{label: "panic", args: []string{"-C", dir, "test", "-v", "."}}, time.Minute)
	if result.err == nil {
		t.Fatal("panic was ignored")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "cxp-ci-job-*.log"))
	if len(files) != 1 {
		t.Fatalf("expected one diagnostic log, got %v", files)
	}
	contents, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "deliberate panic") {
		t.Fatalf("missing panic diagnostic: %s", contents)
	}
}

// An early failure must be visible while another independent job is still
// running, and must not prevent the other job from finishing its diagnostics.
func TestJobEarlyFailureDoesNotStopOtherJob(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("RUNNER_TEMP", dir)
	t.Setenv("GO111MODULE", "off")
	fast, slow := filepath.Join(dir, "fast"), filepath.Join(dir, "slow")
	for _, d := range []string{fast, slow} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	release := filepath.Join(dir, "release")
	started := filepath.Join(dir, "slow-started")
	defer os.WriteFile(release, []byte("release"), 0600)
	fastCode := "package fast\nimport \"testing\"\nfunc TestFast(t *testing.T) { t.Fatal(\"intentional early failure\") }\n"
	slowCode := "package slow\nimport (\"os\"; \"testing\"; \"time\")\nfunc TestSlow(t *testing.T) { if err := os.WriteFile(" + strconv.Quote(started) + ", []byte(\"started\"), 0600); err != nil { t.Fatal(err) }; for { if _, err := os.Stat(" + strconv.Quote(release) + "); err == nil { return }; time.Sleep(50*time.Millisecond) } }\n"
	if err := os.WriteFile(filepath.Join(fast, "fast_test.go"), []byte(fastCode), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(slow, "slow_test.go"), []byte(slowCode), 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan []jobResult, 1)
	go func() {
		done <- runParallelJobs([]testJob{
			{label: "fast", args: []string{"-C", fast, "test", "-v", "."}},
			{label: "slow", args: []string{"-C", slow, "test", "-v", "."}},
		}, time.Minute)
	}()
	deadline := time.After(90 * time.Second)
	for {
		select {
		case <-done:
			t.Fatal("job group ended before slow job was released")
		case <-deadline:
			t.Fatal("early failure was not streamed")
		case <-time.After(50 * time.Millisecond):
			files, _ := filepath.Glob(filepath.Join(dir, "cxp-ci-job-*.log"))
			seen := false
			for _, file := range files {
				b, _ := os.ReadFile(file)
				if strings.Contains(string(b), "--- FAIL: TestFast") {
					seen = true
				}
			}
			if _, err := os.Stat(started); !seen || err != nil {
				continue
			}
			if err := os.WriteFile(release, []byte("release"), 0600); err != nil {
				t.Fatal(err)
			}
			results := <-done
			got := map[string]error{}
			for _, result := range results {
				got[result.label] = result.err
			}
			if len(got) != 2 || got["fast"] == nil || got["slow"] != nil {
				t.Fatalf("unexpected job results: %v", got)
			}
			return
		}
	}
}
