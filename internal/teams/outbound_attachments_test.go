package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPrepareOutboundAttachmentRestrictsToRootAndAllowedFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "result.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	file, err := PrepareOutboundAttachment("result.txt", OutboundAttachmentOptions{
		Root:          root,
		GeneratedName: "upload.txt",
	})
	if err != nil {
		t.Fatalf("PrepareOutboundAttachment error: %v", err)
	}
	if file.Name != "result.txt" || file.UploadName != "upload.txt" || !strings.HasPrefix(file.ContentType, "text/plain") || string(file.Bytes) != "ok" {
		t.Fatalf("unexpected prepared file: %#v", file)
	}

	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "absolute", path: filepath.Join(root, "result.txt")},
		{name: "escape", path: filepath.Join("..", "result.txt")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := PrepareOutboundAttachment(tc.path, OutboundAttachmentOptions{Root: root}); err == nil {
				t.Fatalf("expected %s path to be rejected", tc.name)
			}
		})
	}
}

func TestPrepareOutboundAttachmentRejectsSymlinkAndDisallowedExtension(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "secret.sh"), []byte("no"), 0o600); err != nil {
		t.Fatalf("write script fixture: %v", err)
	}
	if _, err := PrepareOutboundAttachment("secret.sh", OutboundAttachmentOptions{Root: root}); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected disallowed extension error, got %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "target.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatalf("write target fixture: %v", err)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(filepath.Join(root, "target.txt"), link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := PrepareOutboundAttachment("link.txt", OutboundAttachmentOptions{Root: root}); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestPrepareOutboundAttachmentRejectsSymlinkDirectoryEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatalf("write outside fixture: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	_, err := PrepareOutboundAttachment(filepath.Join("linked", "secret.txt"), OutboundAttachmentOptions{Root: root})
	if err == nil || !strings.Contains(err.Error(), "path must stay under Teams outbound root") {
		t.Fatalf("expected symlink directory escape rejection, got %v", err)
	}
}

func TestPrepareOutboundAttachmentSanitizesMissingPathError(t *testing.T) {
	root := t.TempDir()
	_, err := PrepareOutboundAttachment("missing.txt", OutboundAttachmentOptions{Root: root})
	if err == nil {
		t.Fatal("expected missing file error")
	}
	if strings.Contains(err.Error(), root) || strings.Contains(err.Error(), "missing.txt") {
		t.Fatalf("error leaked local path: %v", err)
	}
}

func TestPrepareOutboundAttachmentFixesOutboundRootPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatalf("chmod root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "result.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if _, err := PrepareOutboundAttachment("result.txt", OutboundAttachmentOptions{Root: root}); err != nil {
		t.Fatalf("PrepareOutboundAttachment error: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("root mode = %03o, want 700", got)
	}
}

func TestOutboundUploadNameUsesSubsecondStamp(t *testing.T) {
	when := time.Date(2026, 4, 30, 8, 16, 19, 123456789, time.UTC)
	got := outboundUploadName("result.txt", when)
	if !strings.Contains(got, "20260430T081619.123456789") {
		t.Fatalf("upload name should include nanoseconds, got %q", got)
	}
}

func TestSendOutboundAttachmentUploadsMetadataAndMessage(t *testing.T) {
	graph, sent := newOutboundAttachmentGraph(t)
	result, err := SendOutboundAttachment(context.Background(), graph, "chat-1", OutboundAttachmentFile{
		Name:        "report.txt",
		UploadName:  "upload-report.txt",
		ContentType: "text/plain",
		Bytes:       []byte("report"),
		Size:        6,
	}, OutboundAttachmentOptions{Message: "report ready"})
	if err != nil {
		t.Fatalf("SendOutboundAttachment error: %v", err)
	}
	if result.Item.ID != "item-1" || result.Message.ID != "message-1" {
		t.Fatalf("unexpected result: %#v", result)
	}
	if len(*sent) != 1 || !strings.Contains((*sent)[0].Content, "attachment") {
		t.Fatalf("sent messages = %#v", *sent)
	}
}

func newOutboundAttachmentGraph(t *testing.T) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.EscapedPath(), ":/content"):
			_, _ = fmt.Fprint(w, `{"id":"item-1","name":"upload-report.txt"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/me/drive/items/item-1":
			_, _ = fmt.Fprint(w, `{"id":"item-1","name":"upload-report.txt","eTag":"\"{1176C944-0CB9-4304-974C-5837185EFD6A},1\"","webDavUrl":"https://contoso.sharepoint.com/upload-report.txt"}`)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			sent = append(sent, bridgeSentMessage{ChatID: "chat-1", Content: body.Body.Content})
			_, _ = fmt.Fprint(w, `{"id":"message-1","messageType":"message"}`)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(server.Close)
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &sent
}
