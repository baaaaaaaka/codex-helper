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

func TestLegacyExecutionFlagsRemainHiddenCompatibilityOnly(t *testing.T) {
	runFlag := newRunCmd(nil).Flags().Lookup("yolo")
	if runFlag == nil || !runFlag.Hidden {
		t.Fatalf("legacy run flag = %#v, want hidden compatibility flag", runFlag)
	}
	storePath := ""
	for _, command := range []*cobra.Command{newBeaconWorkerRunOnceCmd(&storePath), newBeaconWorkerServeCmd(&storePath)} {
		flag := command.Flags().Lookup("no-yolo")
		if flag == nil || !flag.Hidden {
			t.Fatalf("%s legacy worker flag = %#v, want hidden compatibility flag", command.Name(), flag)
		}
	}
}

func TestAAAFlagIsPublicAndDefaultsOff(t *testing.T) {
	flag := newRunCmd(nil).Flags().Lookup("aaa")
	if flag == nil || flag.Hidden || flag.DefValue != "false" {
		t.Fatalf("AAA flag = %#v, want public default-off flag", flag)
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
		topPath := filepath.Join(repoRoot, top)
		if _, err := os.Stat(topPath); err != nil {
			if os.IsNotExist(err) {
				t.Skipf("runtime source tree is unavailable in this binary-only smoke environment: %s", topPath)
			}
			t.Fatalf("stat runtime source tree %s: %v", topPath, err)
		}
		err := filepath.WalkDir(topPath, func(path string, entry os.DirEntry, err error) error {
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
