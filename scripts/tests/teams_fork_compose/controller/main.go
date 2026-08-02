package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	stateDir := firstNonEmpty(os.Getenv("STATE_DIR"), "/state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		fatal(err)
	}
	initial := []string{
		"sut-a.done",
		"sut-a.crashed",
		"sut-a.failed",
	}
	marker, err := waitAny(stateDir, initial, 10*time.Minute)
	if err != nil {
		writeFailure(stateDir, err)
		fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "run-b"), []byte(marker+"\n"), 0o600); err != nil {
		fatal(err)
	}
	second, err := waitAny(stateDir, []string{"sut-b.done", "sut-b.failed"}, 10*time.Minute)
	if err != nil {
		writeFailure(stateDir, err)
		fatal(err)
	}
	if strings.HasSuffix(second, ".failed") {
		writeFailure(stateDir, fmt.Errorf("%s reported failure", second))
		fatal(fmt.Errorf("%s reported failure", second))
	}
	if err := os.WriteFile(filepath.Join(stateDir, "controller.done"), []byte("controller completed A to B recovery\n"), 0o600); err != nil {
		fatal(err)
	}
}

func waitAny(stateDir string, names []string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, name := range names {
			if _, err := os.Stat(filepath.Join(stateDir, name)); err == nil {
				return name, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("timed out waiting for %v", names)
}

func writeFailure(stateDir string, err error) {
	_ = os.WriteFile(filepath.Join(stateDir, "controller.failed"), []byte(err.Error()+"\n"), 0o600)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func fatal(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "controller: %v\n", err)
	os.Exit(1)
}
