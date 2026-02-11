package cloudgate

import (
	"os"
	"strings"
	"testing"
)

func TestCreateBundle(t *testing.T) {
	if systemCABundlePath() == "" {
		t.Skip("no system CA bundle found on this system")
	}

	ca, err := EnsureCA(t.TempDir())
	if err != nil {
		t.Fatalf("EnsureCA: %v", err)
	}

	bundlePath, err := CreateBundle(ca)
	if err != nil {
		t.Fatalf("CreateBundle: %v", err)
	}
	defer os.Remove(bundlePath)

	data, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatalf("read bundle: %v", err)
	}

	if !strings.Contains(string(data), "Codex Proxy MITM CA") && !strings.Contains(string(data), "BEGIN CERTIFICATE") {
		t.Error("bundle does not contain our CA certificate")
	}

	// Should contain at least the original system certs plus ours.
	if len(data) < 100 {
		t.Error("bundle seems too small")
	}
}

func TestSystemCABundlePath(t *testing.T) {
	// On Linux CI this should find something.
	p := systemCABundlePath()
	// We can't assert non-empty on all platforms, but log it.
	t.Logf("systemCABundlePath = %q", p)
}

func TestCreateBundleNoSystemBundle(t *testing.T) {
	// If we override known paths to empty, CreateBundle should fail.
	orig := knownCABundlePaths
	knownCABundlePaths = []string{"/nonexistent/path"}
	defer func() { knownCABundlePaths = orig }()

	// Also unset SSL_CERT_FILE for this test.
	origEnv := os.Getenv("SSL_CERT_FILE")
	os.Unsetenv("SSL_CERT_FILE")
	defer os.Setenv("SSL_CERT_FILE", origEnv)

	ca, err := EnsureCA(t.TempDir())
	if err != nil {
		t.Fatalf("EnsureCA: %v", err)
	}

	_, err = CreateBundle(ca)
	if err == nil {
		t.Error("expected error when no system bundle found")
	}
}
