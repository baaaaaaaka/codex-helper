package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

const realCodexYoloHelperEnv = "CODEX_PROXY_REAL_YOLO_HELPER"

func TestRunCodexNewSessionRealCodexYoloIntegration(t *testing.T) {
	if os.Getenv(realCodexYoloHelperEnv) == "1" {
		runRealCodexYoloHelper(t)
		return
	}

	if os.Getenv("CODEX_PATCH_TEST") != "1" {
		t.Skip("CODEX_PATCH_TEST not set")
	}
	if os.Getenv("CODEX_YOLO_PATCH_TEST") != "1" {
		t.Skip("CODEX_YOLO_PATCH_TEST not set")
	}

	codexPath, err := exec.LookPath("codex")
	if err != nil {
		t.Fatalf("codex not found in PATH: %v", err)
	}

	projectDir := t.TempDir()
	codexDir := t.TempDir()
	original := writeTestAuthJSON(t, codexDir, true)
	cachePath := filepath.Join(codexDir, "cloud-requirements-cache.json")
	if err := os.WriteFile(cachePath, []byte("{\"stale\":true}\n"), 0o600); err != nil {
		t.Fatalf("write cloud requirements cache: %v", err)
	}

	configPath := filepath.Join(t.TempDir(), "config.json")
	cmd := exec.Command(os.Args[0], "-test.run=^TestRunCodexNewSessionRealCodexYoloIntegration$")
	cmd.Env = append(os.Environ(),
		realCodexYoloHelperEnv+"=1",
		"CODEX_REAL_YOLO_CONFIG_PATH="+configPath,
		"CODEX_REAL_YOLO_CODEX_PATH="+codexPath,
		"CODEX_REAL_YOLO_CODEX_DIR="+codexDir,
		"CODEX_REAL_YOLO_PROJECT_DIR="+projectDir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper process failed: %v\noutput:\n%s", err, string(out))
	}

	text := string(out)
	for _, needle := range []string{
		"yolo patch active; launching patched codex binary:",
		"yolo auth override active; temporarily masking workspace plan in:",
		"HELPER_ERR:",
	} {
		if !strings.Contains(text, needle) {
			t.Fatalf("helper output missing %q\noutput:\n%s", needle, text)
		}
	}
	if strings.Contains(text, "failed to load your workspace-managed config") {
		t.Fatalf("workspace-managed config failure should be bypassed\noutput:\n%s", text)
	}

	restored, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read restored auth.json: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("auth.json should be restored after real codex launch")
	}
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatalf("cloud requirements cache should be deleted, stat err=%v", err)
	}
}

func runRealCodexYoloHelper(t *testing.T) {
	t.Helper()

	store, err := config.NewStore(os.Getenv("CODEX_REAL_YOLO_CONFIG_PATH"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = runCodexNewSession(
		context.Background(),
		&rootOptions{},
		store,
		nil, nil,
		os.Getenv("CODEX_REAL_YOLO_PROJECT_DIR"),
		os.Getenv("CODEX_REAL_YOLO_CODEX_PATH"),
		os.Getenv("CODEX_REAL_YOLO_CODEX_DIR"),
		false,
		true,
		os.Stderr,
	)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "HELPER_ERR: %v\n", err)
		return
	}

	_, _ = fmt.Fprintln(os.Stdout, "HELPER_OK")
}
