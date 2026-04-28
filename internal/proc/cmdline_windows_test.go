//go:build windows

package proc

import (
	"os"
	"strings"
	"testing"
)

func TestIsAlive(t *testing.T) {
	if IsAlive(0) {
		t.Fatalf("expected pid 0 to be dead")
	}
	if IsAlive(-1) {
		t.Fatalf("expected negative pid to be dead")
	}
	if !IsAlive(os.Getpid()) {
		t.Fatalf("expected current pid to be alive")
	}
}

func TestCommandLine(t *testing.T) {
	t.Run("invalid pid", func(t *testing.T) {
		if _, err := CommandLine(0); err == nil {
			t.Fatal("expected invalid pid error")
		}
	})

	t.Run("current process", func(t *testing.T) {
		cmdline, err := CommandLine(os.Getpid())
		if err != nil {
			t.Fatalf("CommandLine(current pid): %v", err)
		}
		if strings.TrimSpace(cmdline) == "" {
			t.Fatal("expected non-empty command line")
		}
	})
}
