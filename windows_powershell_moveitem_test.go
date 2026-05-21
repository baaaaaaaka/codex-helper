package installtest

import (
	"os"
	"strings"
	"testing"
)

func TestWindowsPowerShellMoveItemCommandsAreTerminatingStress(t *testing.T) {
	files := []string{
		"install.ps1",
		"internal/cli/codex_install.go",
		"internal/cli/teams_update_activation.go",
		"internal/update/replace_windows.go",
	}
	for i := 0; i < 64; i++ {
		for _, file := range files {
			t.Run(file, func(t *testing.T) {
				assertPowerShellMoveItemsAreTerminating(t, file)
			})
		}
	}
}

func TestWindowsPowerShellCriticalMovesVerifySourceRemoval(t *testing.T) {
	for _, tc := range []struct {
		file string
		want string
	}{
		{"install.ps1", "Pending codex-proxy binary still exists after Move-Item"},
		{"internal/cli/codex_install.go", "managed Node.js source still exists after Move-Item"},
		{"internal/cli/teams_update_activation.go", "pending helper still exists after Move-Item"},
		{"internal/update/replace_windows.go", "pending helper still exists after Move-Item"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			data, err := os.ReadFile(tc.file)
			if err != nil {
				t.Fatalf("read %s: %v", tc.file, err)
			}
			if !strings.Contains(string(data), tc.want) {
				t.Fatalf("%s missing post-Move-Item source removal check %q", tc.file, tc.want)
			}
		})
	}
}

func assertPowerShellMoveItemsAreTerminating(t *testing.T, file string) {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	found := false
	for lineNo, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.Contains(trimmed, "Move-Item ") || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "//") {
			continue
		}
		found = true
		if !strings.Contains(trimmed, "-ErrorAction Stop") {
			t.Fatalf("%s:%d Move-Item must use -ErrorAction Stop:\n%s", file, lineNo+1, trimmed)
		}
	}
	if !found {
		t.Fatalf("%s has no Move-Item command; update this stress test if the source moved", file)
	}
}
