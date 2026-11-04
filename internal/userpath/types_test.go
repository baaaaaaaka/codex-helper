package userpath

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultResolverExplicitMode(t *testing.T) {
	resolver := DefaultResolver{}
	for _, test := range []struct {
		name   string
		policy Policy
		env    []string
		want   string
	}{
		{name: "explicit", policy: Policy{Mode: ModeExplicit, ExplicitPath: "/user/bin:/usr/bin"}, want: "/user/bin:/usr/bin"},
		// A terminal PATH captured by an earlier release was saved like an
		// explicit one and keeps working.
		{name: "legacy-captured", policy: Policy{Mode: "captured-terminal", ExplicitPath: "/venv/bin:/usr/bin"}, want: "/venv/bin:/usr/bin"},
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

// The service mode of earlier releases resolves the account default.
func TestLegacyModesNormalize(t *testing.T) {
	for in, want := range map[Mode]Mode{"service": ModeAccountDefault, "captured-terminal": ModeExplicit, "": ModeAccountDefault, " Explicit ": ModeExplicit} {
		if got, err := normalizeMode(in); err != nil || got != want {
			t.Fatalf("normalizeMode(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestDefaultResolverRejectsIncompleteAndUnknownPolicies(t *testing.T) {
	resolver := DefaultResolver{}
	for _, policy := range []Policy{{Mode: ModeExplicit}, {Mode: "captured-terminal"}, {Mode: "mystery"}, {Mode: ModeExplicit, ExplicitPath: "/bin\x00/usr/bin"}} {
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
