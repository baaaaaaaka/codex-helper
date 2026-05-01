package teams

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHostedContentIDsFromHTMLDedupesAndBounds(t *testing.T) {
	html := `<p><img src="https://graph.microsoft.com/v1.0/chats/c/messages/m/hostedContents/a/$value">` +
		`<img src="../hostedContents/b/$value"><img src="../hostedContents/a/$value">` +
		`<img src="../hostedContents/c/$value"><img src="../hostedContents/d/$value">` +
		`<img src="../hostedContents/e/$value"><img src="../hostedContents/f/$value"></p>`
	got := HostedContentIDsFromHTML(html)
	want := []string{"a", "b", "c", "d", "e"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("hosted content ids = %#v, want %#v", got, want)
	}
}

func TestPromptWithLocalAttachments(t *testing.T) {
	prompt := PromptWithLocalAttachments("look", []LocalAttachment{{
		Path:        "/tmp/image.png",
		ContentType: "image/png",
	}})
	if !strings.Contains(prompt, "look") || !strings.Contains(prompt, "/tmp/image.png") || !strings.Contains(prompt, "image/png") {
		t.Fatalf("prompt missing attachment details:\n%s", prompt)
	}
}

func TestPromptWithLocalAttachmentsPrefersAlias(t *testing.T) {
	prompt := PromptWithLocalAttachments("look", []LocalAttachment{{
		Path:        "/tmp/private/image.png",
		PromptPath:  ".codex-helper/teams-attachments/message/image.png",
		ContentType: "image/png",
	}})
	if strings.Contains(prompt, "/tmp/private") || !strings.Contains(prompt, ".codex-helper/teams-attachments/message/image.png") {
		t.Fatalf("prompt should use alias instead of private temp path:\n%s", prompt)
	}
}

func TestSupportedReferenceAttachmentValidation(t *testing.T) {
	valid := MessageAttachment{ContentType: "reference", ContentURL: "https://contoso.sharepoint.com/sites/team/file.docx"}
	if !isSupportedReferenceAttachment(valid) {
		t.Fatal("expected SharePoint reference attachment to be supported")
	}
	rejected := []MessageAttachment{
		{ContentType: "image/png", ContentURL: "https://contoso.sharepoint.com/file.png"},
		{ContentType: "reference", ContentURL: "http://contoso.sharepoint.com/file.docx"},
		{ContentType: "reference", ContentURL: "https://user:pass@contoso.sharepoint.com/file.docx"},
		{ContentType: "reference", ContentURL: "https://example.com/file.docx"},
		{ContentType: "forwardedMessageReference", ContentURL: "https://contoso.sharepoint.com/file.docx"},
	}
	for _, attachment := range rejected {
		if isSupportedReferenceAttachment(attachment) {
			t.Fatalf("attachment should be rejected: %#v", attachment)
		}
	}
}

func TestSupportedReferenceAttachmentHonorsConfiguredSharePointAllowlist(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS", "nvidia.sharepoint.com,.trusted.sharepoint.com")
	allowedExact := MessageAttachment{ContentType: "reference", ContentURL: "https://nvidia.sharepoint.com/sites/team/file.docx"}
	allowedSuffix := MessageAttachment{ContentType: "reference", ContentURL: "https://dept.trusted.sharepoint.com/sites/team/file.docx"}
	rejected := MessageAttachment{ContentType: "reference", ContentURL: "https://contoso.sharepoint.com/sites/team/file.docx"}
	if !isSupportedReferenceAttachment(allowedExact) || !isSupportedReferenceAttachment(allowedSuffix) {
		t.Fatal("expected configured SharePoint hosts to be supported")
	}
	if isSupportedReferenceAttachment(rejected) {
		t.Fatal("unexpected unconfigured SharePoint host support")
	}
}

func TestDownloadReferenceFileAttachmentsRequiresFileScope(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_READ_SCOPES", "openid profile offline_access User.Read Chat.Read")
	bridge := &Bridge{}
	files, cleanup, message, err := bridge.downloadReferenceFileAttachments(context.Background(), &Session{ID: "s001"}, ChatMessage{
		Attachments: []MessageAttachment{{
			ContentType: "reference",
			ContentURL:  "https://contoso.sharepoint.com/sites/team/file.txt",
		}},
	})
	defer cleanup()
	if err != nil {
		t.Fatalf("downloadReferenceFileAttachments error: %v", err)
	}
	if len(files) != 0 || !strings.Contains(message, "Files.Read") {
		t.Fatalf("expected file-scope unsupported message, files=%#v message=%q", files, message)
	}
}

func TestDownloadReferenceFileAttachmentsWritesPrivateFile(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", tmp)
	t.Setenv("CODEX_HELPER_TEAMS_READ_SCOPES", "openid profile offline_access User.Read Chat.Read Files.Read")
	graph, requested := newAttachmentGraph(t)
	bridge := &Bridge{readGraph: graph}
	var msg ChatMessage
	msg.ID = "message-1"
	msg.Attachments = []MessageAttachment{{
		ID:          "file-1",
		ContentType: "reference",
		ContentURL:  "https://contoso.sharepoint.com/sites/team/file.txt",
		Name:        "file.txt",
	}}
	files, cleanup, message, err := bridge.downloadReferenceFileAttachments(context.Background(), &Session{ID: "s001"}, msg)
	defer cleanup()
	if err != nil {
		t.Fatalf("downloadReferenceFileAttachments error: %v", err)
	}
	if message != "" || len(files) != 1 {
		t.Fatalf("unexpected result files=%#v message=%q", files, message)
	}
	if !*requested {
		t.Fatal("expected Graph shared drive item content request")
	}
	data, err := os.ReadFile(files[0].Path)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if string(data) != "file-bytes" {
		t.Fatalf("downloaded file bytes = %q", string(data))
	}
}

func TestDownloadHostedContentUsesWorkspaceRelativePromptAlias(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/hostedContents/content-1/$value") {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("image-bytes"))
	}))
	defer server.Close()
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	bridge := &Bridge{graph: graph}
	var msg ChatMessage
	msg.ID = "message-hosted"
	msg.Body.Content = `<img src="https://graph.microsoft.com/v1.0/chats/chat-1/messages/message-hosted/hostedContents/content-1/$value">`
	session := &Session{ID: "s001", Cwd: filepath.Join(tmp, "workspace")}
	if err := os.MkdirAll(session.Cwd, 0o700); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	files, cleanup, warning, err := bridge.downloadHostedContentAttachments(context.Background(), session, "chat-1", msg)
	defer cleanup()
	if err != nil {
		t.Fatalf("downloadHostedContentAttachments error: %v", err)
	}
	if warning != "" {
		t.Fatalf("unexpected hosted content warning: %s", warning)
	}
	if len(files) != 1 {
		t.Fatalf("files = %#v, want one", files)
	}
	if strings.Contains(files[0].PromptPath, session.Cwd) || !strings.HasPrefix(files[0].PromptPath, ".codex-helper/teams-attachments/") {
		t.Fatalf("prompt alias = %q, want workspace-relative hidden path", files[0].PromptPath)
	}
	if _, err := os.Stat(filepath.Join(session.Cwd, filepath.FromSlash(files[0].PromptPath))); err != nil {
		t.Fatalf("workspace-relative attachment path is not readable: %v", err)
	}
}

func newAttachmentGraph(t *testing.T) (*GraphClient, *bool) {
	t.Helper()
	requested := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/shares/") || !strings.HasSuffix(r.URL.Path, "/driveItem/content") {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		requested = true
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("file-bytes"))
	}))
	t.Cleanup(server.Close)
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &requested
}
