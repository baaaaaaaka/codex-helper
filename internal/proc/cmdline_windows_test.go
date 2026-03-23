//go:build windows

package proc

import (
	"os"
	"strings"
	"testing"
)

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
