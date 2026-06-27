package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestPublishedCLIContainsNoRetiredExecutionSignals(t *testing.T) {
	forbidden := []string{
		"yolo",
		"dangerously-bypass-approvals-and-sandbox",
		"danger-full-access",
		"bypasspermissions",
		"codex-patched",
		"patch_history",
	}
	var text strings.Builder
	var collect func(*cobra.Command)
	collect = func(command *cobra.Command) {
		text.WriteString(command.CommandPath())
		text.WriteString("\n")
		text.WriteString(command.Use)
		text.WriteString("\n")
		text.WriteString(command.Short)
		text.WriteString("\n")
		text.WriteString(command.Long)
		text.WriteString("\n")
		text.WriteString(command.Flags().FlagUsages())
		text.WriteString(command.PersistentFlags().FlagUsages())
		for _, child := range command.Commands() {
			collect(child)
		}
	}
	collect(newRootCmd())
	lower := strings.ToLower(text.String())
	for _, signal := range forbidden {
		if strings.Contains(lower, signal) {
			t.Fatalf("published CLI contains retired execution signal %q", signal)
		}
	}
}

func TestRuntimeSourcesContainNoRetiredExecutionSignals(t *testing.T) {
	forbidden := []string{
		"--yolo",
		"dangerously-bypass-approvals-and-sandbox",
		"danger-full-access",
		"codex-patched-",
		"patch_history.json",
		"yolo-auth",
	}
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(repoRoot, top), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if filepath.Clean(path) == filepath.Join(repoRoot, "internal", "migration") {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			lower := strings.ToLower(string(data))
			for _, signal := range forbidden {
				if strings.Contains(lower, signal) {
					t.Errorf("runtime source %s contains retired execution signal %q", path, signal)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
