package codex

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
)

func TestTitleValidation(t *testing.T) {
	for _, raw := range []string{`{}`, `{"title":""}`, `{"title":"a\nb"}`, `{"title":"x","extra":1}`, `{"title":"x"} {}`, `{"title":"` + strings.Repeat("中", 37) + `"}`, strings.Repeat("x", 8193)} {
		if v, err := ParseTitle(raw); err == nil {
			t.Errorf("accepted %q: %q", raw, v)
		}
	}
	if v, err := ParseTitle(`{"title":" 修复登录问题 "}`); err != nil || v != "修复登录问题" {
		t.Fatalf("%q %v", v, err)
	}
	prompt := TitlePrompt(strings.Repeat("中", 10000))
	var input string
	if err := json.Unmarshal([]byte(prompt[strings.LastIndex(prompt, "\n")+1:]), &input); err != nil || len(input) > 960 || !utf8.ValidString(input) {
		t.Fatalf("input bound: %d %v", len(input), err)
	}
}

func TestTemporaryTitleIsolationAndCleanup(t *testing.T) {
	requests := make(chan fakecodex.TitleRequest, 2)
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req fakecodex.TitleRequest
		if r.URL.Path != "/title" {
			http.Error(w, "unexpected request", 500)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		requests <- req
		_ = json.NewEncoder(w).Encode(fakecodex.TitleResult{Output: `{"title":"登录修复"}`})
	}))
	defer brain.Close()
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte("[mcp_servers.danger]\ncommand='danger'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Start(tctx(t), fakeConfig(t, brain.URL, home))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Kill()
	name, err := s.GenerateTitle(tctx(t), home, "test-model", "low", "Fix login")
	if err != nil || name != "登录修复" {
		t.Fatalf("%q %v", name, err)
	}
	req := <-requests
	config := req.Start["config"].(map[string]any)
	mcp := config["mcp_servers"].(map[string]any)["danger"].(map[string]any)
	if mcp["enabled"] != false || config["features.shell_tool"] != false || config["features.plugins"] != false || req.Start["ephemeral"] != true || req.Turn["outputSchema"] == nil {
		t.Fatalf("unsafe title parameters: %#v", req)
	}
	// No temporary thread or rollout is persisted; the native server unsubscribes.
	if files, _ := filepath.Glob(filepath.Join(home, "e2e-fakecodex", "threads", "*")); len(files) != 0 {
		t.Fatalf("persisted temporary threads: %v", files)
	}
	s.mu.Lock()
	n := len(s.temporary)
	subscribers := len(s.subs)
	s.mu.Unlock()
	if n != 0 || subscribers != 0 {
		t.Fatalf("retained hidden state: %d threads, %d subscribers", n, subscribers)
	}
	th, err := s.ThreadStart(tctx(t), ThreadStartParams{Cwd: home})
	if err != nil {
		t.Fatal(err)
	}
	if name, err = s.NameIfEmpty(tctx(t), th.ID, "First"); err != nil || name != "First" {
		t.Fatalf("set: %q %v", name, err)
	}
	if name, err = s.NameIfEmpty(tctx(t), th.ID, "Second"); err != nil || name != "First" {
		t.Fatalf("overwrote name: %q %v", name, err)
	}
}

func TestTemporaryTitleCancellation(t *testing.T) {
	started := make(chan struct{})
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer brain.Close()
	home := t.TempDir()
	s, err := Start(tctx(t), fakeConfig(t, brain.URL, home))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Kill()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.GenerateTitle(ctx, home, "test-model", "low", "hello"); done <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("not started")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("accepted cancelled title")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish")
	}
	if _, err := s.ThreadStart(tctx(t), ThreadStartParams{Cwd: home}); err != nil {
		t.Fatalf("title cancellation damaged server: %v", err)
	}
}

func BenchmarkTitlePromptBounded(b *testing.B) {
	text := strings.Repeat("标题数据", 250000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = TitlePrompt(text)
	}
}

func TestTitleVersionGate(t *testing.T) {
	for _, agent := range []string{"", "unknown", "codex_vscode/0.131.0 (Linux)", "codex_vscode/0.165.9", "codex_vscode/not-a-version"} {
		srv := &Server{userAgent: agent}
		if srv.SupportsTitle() {
			t.Errorf("accepted %q", agent)
		}
		// No transport is installed: a regression would fail instead of silently
		// sending an unsafe request to a version that ignores tool overrides.
		if _, err := srv.GenerateTitle(context.Background(), "", "model", "low", "topic"); err != ErrTitleUnsupported {
			t.Fatalf("%q: %v", agent, err)
		}
	}
	for _, agent := range []string{"codex_vscode/0.166.0 (Linux)", "codex_vscode/0.167.0", "codex_vscode/1.0.0"} {
		if !supportsTitle(agent) {
			t.Errorf("rejected %q", agent)
		}
	}
}
