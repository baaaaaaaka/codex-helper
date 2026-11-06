package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// `cxp teams send-file` reads relative paths from the outbound root only,
// as v0.1.20's resolveOutboundPath did.
func TestResolveTeamsSendFilePath(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	mustWrite := func(p string) {
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite(filepath.Join(root, "reports", "notes.txt"))
	mustWrite(filepath.Join(outside, "secret.txt"))
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		path    string
		allow   bool
		want    string // resolved path, or "" when refused
		wantErr string
	}{
		{"reports/notes.txt", false, filepath.Join(root, "reports", "notes.txt"), ""},
		{"reports/../reports/notes.txt", false, filepath.Join(root, "reports", "notes.txt"), ""},
		{"../x.txt", false, "", "path must stay under Teams outbound root"},
		{"..", false, "", "path must stay under Teams outbound root"},
		{filepath.Join(outside, "secret.txt"), false, "", "absolute paths require --allow-local-path"},
		{filepath.Join(outside, "secret.txt"), true, filepath.Join(outside, "secret.txt"), ""},
		{"escape/secret.txt", false, "", "path must stay under Teams outbound root"},
		{"link.txt", false, "", "path must stay under Teams outbound root"},
		{"reports", false, "", "refusing to upload non-regular file: reports"},
		{"missing.txt", false, "", "no such file"},
		{"  ", false, "", "file path is required"},
	} {
		got, err := resolveTeamsSendFilePath(c.path, root, c.allow)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%q allow=%v: got %q, %v; want error %q", c.path, c.allow, got, err, c.wantErr)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%q allow=%v: got %q, %v; want %q", c.path, c.allow, got, err, c.want)
		}
	}
	// A symlink given as an explicit local path is refused too.
	if _, err := resolveTeamsSendFilePath(filepath.Join(root, "link.txt"), root, true); err == nil || !strings.Contains(err.Error(), "refusing to upload symlink") {
		t.Errorf("explicit symlink: %v", err)
	}
}

func TestTeamsSendFileLineFilterHidesMessageID(t *testing.T) {
	var b strings.Builder
	f := &lineFilter{out: &b, drop: func(l string) bool { return strings.HasPrefix(l, "Teams message id: ") }}
	for _, l := range []string{"Sent Teams file attachment: a.txt", "Target session: s001", "Teams message id: 42"} {
		if _, err := f.Write([]byte(l + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := b.String(), "Sent Teams file attachment: a.txt\nTarget session: s001\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
