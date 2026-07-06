package userpath

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultResolverExplicitAndServiceModes(t *testing.T) {
	resolver := DefaultResolver{}
	for _, test := range []struct {
		name   string
		policy Policy
		env    []string
		want   string
	}{
		{name: "explicit", policy: Policy{Mode: ModeExplicit, ExplicitPath: "/user/bin:/usr/bin"}, want: "/user/bin:/usr/bin"},
		{name: "captured", policy: Policy{Mode: ModeCapturedTerminal, ExplicitPath: "/venv/bin:/usr/bin"}, want: "/venv/bin:/usr/bin"},
		{name: "service-last-wins", policy: Policy{Mode: ModeService}, env: []string{"PATH=/old", "PATH=/service"}, want: "/service"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolver.Resolve(context.Background(), Request{Policy: test.policy, ServiceEnvironment: test.env})
			if err != nil {
				t.Fatal(err)
			}
			if got.Path != test.want || got.Fingerprint == "" {
				t.Fatalf("result = %#v, want path %q and fingerprint", got, test.want)
			}
		})
	}
}

func TestDefaultResolverRejectsIncompleteAndUnknownPolicies(t *testing.T) {
	resolver := DefaultResolver{}
	for _, policy := range []Policy{{Mode: ModeExplicit}, {Mode: ModeCapturedTerminal}, {Mode: "mystery"}, {Mode: ModeExplicit, ExplicitPath: "/bin\x00/usr/bin"}} {
		if _, err := resolver.Resolve(context.Background(), Request{Policy: policy}); err == nil {
			t.Fatalf("Resolve(%#v) succeeded, want error", policy)
		}
	}
}

func TestEnvironmentValueWindowsCaseInsensitive(t *testing.T) {
	environment := []string{"Path=C:\\Windows", "PATH=C:\\Tools"}
	if got, ok := EnvironmentValue(environment, "Path", true); !ok || got != "C:\\Tools" {
		t.Fatalf("case-insensitive Path = %q,%v", got, ok)
	}
	if got, ok := EnvironmentValue(environment, "Path", false); !ok || got != "C:\\Windows" {
		t.Fatalf("case-sensitive Path = %q,%v", got, ok)
	}
}

func TestPathFingerprintIsStableAndOrderSensitive(t *testing.T) {
	a := PathFingerprint("/a" + string(os.PathListSeparator) + "/b")
	b := PathFingerprint("/b" + string(os.PathListSeparator) + "/a")
	if a == "" || b == "" || a == b {
		t.Fatalf("fingerprints = %q %q", a, b)
	}
	if PathFingerprint(filepath.Join("/a", "bin")) != PathFingerprint(filepath.Join("/a", "bin")) {
		t.Fatal("fingerprint is not stable")
	}
}
