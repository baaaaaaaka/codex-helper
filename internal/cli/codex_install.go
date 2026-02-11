package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func ensureCodexInstalled(_ context.Context, codexPath string, out io.Writer) (string, error) {
	if strings.TrimSpace(codexPath) != "" {
		if executableExists(codexPath) {
			return codexPath, nil
		}
		return "", fmt.Errorf("codex not found at %s", codexPath)
	}

	if path, err := exec.LookPath("codex"); err == nil {
		return path, nil
	}

	return "", fmt.Errorf("codex CLI not found in PATH. Install with: npm install -g @openai/codex")
}

func executableExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	path = filepath.Clean(path)
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return true
}
