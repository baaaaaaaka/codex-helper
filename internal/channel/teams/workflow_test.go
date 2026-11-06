package teams

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The webhook URL is a pre-signed secret: only a private, absolute,
// non-symlinked file under private parents holding an https URL is used.
func TestReadWebhookURLFileSafety(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "webhook-url")
	if err := os.WriteFile(path, []byte("https://workflow.example.test/hook\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if u, err := ReadWebhookURLFile(path); err != nil || u != "https://workflow.example.test/hook" {
		t.Fatalf("private file: %q, %v", u, err)
	}
	if _, err := ReadWebhookURLFile("webhook-url"); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("relative path: %v", err)
	}

	link := path + ".link"
	if err := os.Symlink(path, link); err == nil {
		if _, err := ReadWebhookURLFile(link); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("symlink: %v", err)
		}
	} else if runtime.GOOS != "windows" {
		t.Fatalf("create symlink: %v", err)
	}

	if runtime.GOOS != "windows" {
		open := path + ".open"
		if err := os.WriteFile(open, []byte("https://workflow.example.test/hook"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadWebhookURLFile(open); err == nil || !strings.Contains(err.Error(), "permissions") {
			t.Fatalf("readable by others: %v", err)
		}
		openDir := filepath.Join(t.TempDir(), "open-dir")
		if err := os.Mkdir(openDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(openDir, 0o777); err != nil {
			t.Fatal(err)
		}
		inOpen := filepath.Join(openDir, "webhook-url")
		if err := os.WriteFile(inOpen, []byte("https://workflow.example.test/hook"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadWebhookURLFile(inOpen); err == nil || !strings.Contains(err.Error(), "parent directories") {
			t.Fatalf("writable parent: %v", err)
		}
	}

	for _, bad := range []string{"http://workflow.example.test/hook", "https://user@workflow.example.test/hook", "https://workflow.example.test/hook#x", "/relative"} {
		p := filepath.Join(dir, "bad")
		if err := os.WriteFile(p, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadWebhookURLFile(p); err == nil || !strings.Contains(err.Error(), "https URL") {
			t.Fatalf("%q: %v", bad, err)
		}
	}
}
