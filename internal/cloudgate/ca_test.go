package cloudgate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureCA(t *testing.T) {
	dir := t.TempDir()

	ca, err := EnsureCA(dir)
	if err != nil {
		t.Fatalf("EnsureCA: %v", err)
	}
	if ca.Cert == nil {
		t.Fatal("cert is nil")
	}
	if ca.Key == nil {
		t.Fatal("key is nil")
	}
	if len(ca.CertPEM) == 0 {
		t.Fatal("CertPEM is empty")
	}
	if len(ca.KeyPEM) == 0 {
		t.Fatal("KeyPEM is empty")
	}
	if !ca.Cert.IsCA {
		t.Error("cert is not CA")
	}

	// Verify files exist.
	certPath := filepath.Join(dir, "mitm-ca.pem")
	keyPath := filepath.Join(dir, "mitm-ca-key.pem")
	if _, err := os.Stat(certPath); err != nil {
		t.Errorf("cert file missing: %v", err)
	}
	if _, err := os.Stat(keyPath); err != nil {
		t.Errorf("key file missing: %v", err)
	}
}

func TestEnsureCAReusesExisting(t *testing.T) {
	dir := t.TempDir()

	ca1, err := EnsureCA(dir)
	if err != nil {
		t.Fatalf("first EnsureCA: %v", err)
	}

	ca2, err := EnsureCA(dir)
	if err != nil {
		t.Fatalf("second EnsureCA: %v", err)
	}

	// Should be the same certificate.
	if ca1.Cert.SerialNumber.Cmp(ca2.Cert.SerialNumber) != 0 {
		t.Error("expected same serial number on reload")
	}
}

func TestEnsureCACreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "config")

	ca, err := EnsureCA(dir)
	if err != nil {
		t.Fatalf("EnsureCA with nested dir: %v", err)
	}
	if ca.Cert == nil {
		t.Fatal("cert is nil")
	}
}
