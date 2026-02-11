package cloudgate

import (
	"sync"
	"testing"
)

func TestGetCert(t *testing.T) {
	ca, err := EnsureCA(t.TempDir())
	if err != nil {
		t.Fatalf("EnsureCA: %v", err)
	}

	cc := NewCertCache(ca)
	cert, err := cc.GetCert("example.com")
	if err != nil {
		t.Fatalf("GetCert: %v", err)
	}
	if cert == nil {
		t.Fatal("cert is nil")
	}
	if cert.Leaf == nil {
		t.Fatal("leaf is nil")
	}
	// Check SAN.
	found := false
	for _, name := range cert.Leaf.DNSNames {
		if name == "example.com" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected SAN example.com, got %v", cert.Leaf.DNSNames)
	}
}

func TestGetCertCaches(t *testing.T) {
	ca, err := EnsureCA(t.TempDir())
	if err != nil {
		t.Fatalf("EnsureCA: %v", err)
	}

	cc := NewCertCache(ca)
	cert1, err := cc.GetCert("example.com")
	if err != nil {
		t.Fatalf("GetCert 1: %v", err)
	}
	cert2, err := cc.GetCert("example.com")
	if err != nil {
		t.Fatalf("GetCert 2: %v", err)
	}

	if cert1 != cert2 {
		t.Error("expected same pointer for cached cert")
	}
}

func TestGetCertConcurrent(t *testing.T) {
	ca, err := EnsureCA(t.TempDir())
	if err != nil {
		t.Fatalf("EnsureCA: %v", err)
	}

	cc := NewCertCache(ca)
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cc.GetCert("concurrent.example.com")
			if err != nil {
				t.Errorf("GetCert: %v", err)
			}
		}()
	}

	wg.Wait()
}

func TestGetCertDifferentHosts(t *testing.T) {
	ca, err := EnsureCA(t.TempDir())
	if err != nil {
		t.Fatalf("EnsureCA: %v", err)
	}

	cc := NewCertCache(ca)
	cert1, err := cc.GetCert("host1.example.com")
	if err != nil {
		t.Fatalf("GetCert host1: %v", err)
	}
	cert2, err := cc.GetCert("host2.example.com")
	if err != nil {
		t.Fatalf("GetCert host2: %v", err)
	}

	if cert1 == cert2 {
		t.Error("expected different certs for different hosts")
	}
}
