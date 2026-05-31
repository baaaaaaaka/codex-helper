package responsesadapter

import (
	"strings"
	"testing"
)

func TestKeyFingerprintStableSaltedAndRedacted(t *testing.T) {
	key := "sk-test-secret-value"
	a := KeyFingerprint(key, "salt-a")
	b := KeyFingerprint(key, "salt-a")
	c := KeyFingerprint(key, "salt-b")
	if a != b {
		t.Fatalf("fingerprint not stable: %q vs %q", a, b)
	}
	if a == c {
		t.Fatalf("different salts produced same fingerprint %q", a)
	}
	if strings.Contains(a, key) || strings.Contains(a, "secret") {
		t.Fatalf("fingerprint leaks key material: %q", a)
	}
	if got := KeyFingerprint("", "salt-a"); got != "no-key" {
		t.Fatalf("empty key fingerprint = %q", got)
	}
}

func TestBaseURLHashNormalizesCaseQueryAndTrailingSlash(t *testing.T) {
	a := BaseURLHash("HTTPS://API.EXAMPLE.COM/v1/?x=1#frag")
	b := BaseURLHash("https://api.example.com/v1")
	c := BaseURLHash("https://api.example.com/v2")
	if a != b {
		t.Fatalf("normalized hashes differ: %q vs %q", a, b)
	}
	if a == c {
		t.Fatalf("different base URLs produced same hash: %q", a)
	}
}
