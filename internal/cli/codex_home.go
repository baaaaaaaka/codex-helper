package cli

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

const (
	envCodexHome       = "CODEX_HOME"
	envCodexSQLiteHome = "CODEX_SQLITE_HOME"
)

func resolveCodexHome(override string, workingDir string) (string, error) {
	paths, err := resolveEffectivePaths("", override, workingDir)
	if err != nil {
		return "", err
	}
	return paths.CodexDir, nil
}

func resolveCodexHomePath(raw string, workingDir string) (string, error) {
	path := filepath.Clean(os.ExpandEnv(strings.TrimSpace(raw)))
	if filepath.IsAbs(path) {
		return path, nil
	}
	if strings.TrimSpace(workingDir) != "" {
		return filepath.Clean(filepath.Join(workingDir, path)), nil
	}
	return filepath.Abs(path)
}

func codexHomeEnv(codexHome string) []string {
	if strings.TrimSpace(codexHome) == "" {
		return nil
	}
	return []string{
		codexhistory.EnvCodexDir + "=" + codexHome,
		envCodexHome + "=" + codexHome,
	}
}
