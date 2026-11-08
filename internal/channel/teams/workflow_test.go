package teams

import (
	"context"
	"encoding/json"
	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"
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

// The old event heading stays concise; chat, request and action are separate.
func TestOwnerNoticeCard(t *testing.T) {
	var calls int
	var raw []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; raw, _ = io.ReadAll(r.Body); w.WriteHeader(500) }))
	defer srv.Close()
	notice := channel.OwnerNotice{Heading: "✅ Codex finished", ChatTitle: "Login fixes", Request: "Repair authentication", Hint: "Open the answer.", Button: "Open answer", URL: "https://teams.example/chat"}
	err := NewWorkflowNotifier(srv.URL, srv.Client()).Notify(context.Background(), notice)
	if err == nil || calls != 1 {
		t.Fatalf("delivery retried: %d %v", calls, err)
	}
	var payload struct {
		Attachments []struct {
			Content struct {
				Body []struct {
					Text string `json:"text"`
				} `json:"body"`
				Actions []struct {
					Title string `json:"title"`
				} `json:"actions"`
			} `json:"content"`
		} `json:"attachments"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	card := payload.Attachments[0].Content
	if card.Body[0].Text != "✅ Codex finished" || card.Actions[0].Title != "Open answer" {
		t.Fatalf("%s", raw)
	}
	for _, field := range []string{"Chat", "Login fixes", "Request", "Repair authentication"} {
		if !strings.Contains(string(raw), field) {
			t.Fatalf("missing %q: %s", field, raw)
		}
	}
	if got := cardText(strings.Repeat("中", 1000), 160); len(got) > 160 || !utf8.ValidString(got) {
		t.Fatal("invalid bounded UTF-8")
	}
}
