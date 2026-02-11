package cloudgate

import (
	"os"
	"runtime"
	"testing"
)

func TestSetup(t *testing.T) {
	dir := t.TempDir()

	cfg, err := Setup(dir)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if cfg == nil {
		t.Fatal("config is nil")
	}
	defer cfg.Cleanup()

	// Should always have hosts.
	if !cfg.ShouldIntercept("chatgpt.com") {
		t.Error("expected chatgpt.com to be intercepted")
	}

	if runtime.GOOS == "linux" && systemCABundlePath() != "" {
		if cfg.MITM == nil {
			t.Error("expected MITM config on Linux with system CA bundle")
		}
	}
}

func TestSetupFingerprintOnly(t *testing.T) {
	cfg := SetupFingerprintOnly()
	if cfg == nil {
		t.Fatal("config is nil")
	}
	if cfg.MITM != nil {
		t.Error("expected no MITM in fingerprint-only mode")
	}
	if !cfg.ShouldIntercept("chatgpt.com") {
		t.Error("expected chatgpt.com to be intercepted")
	}
}

func TestSetupCleanup(t *testing.T) {
	if systemCABundlePath() == "" {
		t.Skip("no system CA bundle")
	}
	if runtime.GOOS != "linux" {
		t.Skip("MITM only on Linux")
	}

	dir := t.TempDir()
	cfg, err := Setup(dir)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if cfg.MITM == nil {
		t.Skip("MITM not available")
	}

	bundlePath := cfg.MITM.BundlePath
	if _, err := os.Stat(bundlePath); err != nil {
		t.Fatalf("bundle should exist before cleanup: %v", err)
	}

	cfg.Cleanup()

	if _, err := os.Stat(bundlePath); err == nil {
		t.Error("bundle should be removed after cleanup")
	}
}
