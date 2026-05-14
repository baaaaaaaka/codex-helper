package teams

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/skills"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestHandleSkillsCommandQueuesListPushReviewAndLocalOnlyRefusal(t *testing.T) {
	mgr := newTeamsSkillsTestManager(t)
	source := skills.Source{
		ID:         "source-1",
		Name:       "acme",
		RemoteURL:  "https://github.com/acme/skills.git",
		Ref:        "main",
		TargetKind: skills.TargetCodexHome,
		TargetRoot: filepath.Join(mgr.CodexDir, "skills"),
		AutoSync:   true,
	}
	target := filepath.Join(source.TargetRoot, "acme__review")
	writeTeamsSkillExport(t, target, source)
	if err := mgr.Store.Update(func(cfg *skills.Config, st *skills.State) error {
		cfg.Sources = []skills.Source{source}
		st.Sources = []skills.SourceState{{
			ID:         source.ID,
			Status:     skills.StatusReady,
			LastCommit: "0123456789abcdef",
			InstalledSkills: []skills.InstalledSkill{{
				Name:       "review",
				ExportName: "acme__review",
				SourcePath: "skills/review",
				TargetPath: target,
				Files:      []skills.FileManifest{{RelPath: "SKILL.md", SHA256: "placeholder"}},
			}},
		}}
		return nil
	}); err != nil {
		t.Fatalf("seed skills store: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "SKILL.md"), []byte("---\nname: review\ndescription: Local\n---\nlocal\n"), 0o644); err != nil {
		t.Fatalf("modify skill: %v", err)
	}

	prev := newTeamsSkillsManagerForCommand
	newTeamsSkillsManagerForCommand = func() (*skills.Manager, error) { return mgr, nil }
	t.Cleanup(func() { newTeamsSkillsManagerForCommand = prev })

	store, err := teamstore.Open(filepath.Join(t.TempDir(), "teams-state.json"))
	if err != nil {
		t.Fatalf("open teams store: %v", err)
	}
	graphServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			t.Fatalf("unexpected graph request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"m1"}`))
	}))
	t.Cleanup(graphServer.Close)
	bridge := &Bridge{
		graph:   newTestGraphClient(&fakeGraphAuth{token: "token"}, graphServer, nil),
		store:   store,
		scope:   teamstore.ScopeIdentity{ID: "scope"},
		machine: teamstore.MachineRecord{ID: "machine"},
	}
	ctx := context.Background()
	for _, cmd := range []string{"list", "push", "add"} {
		if err := bridge.handleSkillsCommand(ctx, "chat-1", cmd); err != nil {
			t.Fatalf("handle skills %s: %v", cmd, err)
		}
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load teams state: %v", err)
	}
	body := joinedOutboxBodies(state.OutboxMessages)
	for _, want := range []string{"Skills", "acme", "Skills Push Review", "cxp skills push", "Use local"} {
		if !strings.Contains(body, want) {
			t.Fatalf("outbox missing %q:\n%s", want, body)
		}
	}
}

func TestSkillsCommandDispatchesThroughControlAndWorkChats(t *testing.T) {
	mgr := newTeamsSkillsTestManager(t)
	prev := newTeamsSkillsManagerForCommand
	newTeamsSkillsManagerForCommand = func() (*skills.Manager, error) { return mgr, nil }
	t.Cleanup(func() { newTeamsSkillsManagerForCommand = prev })

	store, err := teamstore.Open(filepath.Join(t.TempDir(), "teams-state.json"))
	if err != nil {
		t.Fatalf("open teams store: %v", err)
	}
	graphServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected graph request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"m1"}`))
	}))
	t.Cleanup(graphServer.Close)
	bridge := &Bridge{
		graph: newTestGraphClient(&fakeGraphAuth{token: "token"}, graphServer, nil),
		store: store,
		reg: Registry{
			ControlChatID: "control-chat",
			Sessions: []Session{{
				ID:     "session-1",
				ChatID: "work-chat",
				Status: "active",
				Cwd:    t.TempDir(),
			}},
		},
		user:    User{ID: "user-1", DisplayName: "Test User"},
		scope:   teamstore.ScopeIdentity{ID: "scope"},
		machine: teamstore.MachineRecord{ID: "machine"},
	}
	ctx := context.Background()
	if err := bridge.handleControlMessage(ctx, teamsSkillCommandMessage("control-msg", "user-1", "helper skills list"), "helper skills list"); err != nil {
		t.Fatalf("control skills dispatch: %v", err)
	}
	if err := bridge.handleSessionMessage(ctx, "work-chat", teamsSkillCommandMessage("work-msg", "user-1", "helper skills list"), "helper skills list"); err != nil {
		t.Fatalf("work skills dispatch: %v", err)
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load teams state: %v", err)
	}
	bodiesByChat := map[string]string{}
	for _, msg := range state.OutboxMessages {
		bodiesByChat[msg.TeamsChatID] += msg.Body + "\n"
	}
	for _, chatID := range []string{"control-chat", "work-chat"} {
		if !strings.Contains(bodiesByChat[chatID], "No skill subscriptions.") {
			t.Fatalf("%s outbox missing skills list response:\n%s", chatID, bodiesByChat[chatID])
		}
	}
}

func TestNewTeamsSkillsManagerUsesCodexProxyStoreAndCodexEnv(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	configBase := filepath.Join(root, "config")
	cacheBase := filepath.Join(root, "cache")
	codexDir := filepath.Join(root, "codex-dir")
	codexHome := filepath.Join(root, "codex-home")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CONFIG_HOME", configBase)
	t.Setenv("XDG_CACHE_HOME", cacheBase)
	t.Setenv("APPDATA", configBase)
	t.Setenv("LOCALAPPDATA", cacheBase)
	t.Setenv("CODEX_DIR", codexDir)
	t.Setenv("CODEX_HOME", codexHome)

	expectedConfigBase, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("user config dir: %v", err)
	}
	expectedCacheBase, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("user cache dir: %v", err)
	}
	mgr, err := newTeamsSkillsManager()
	if err != nil {
		t.Fatalf("newTeamsSkillsManager with CODEX_DIR: %v", err)
	}
	if mgr.ConfigDir != filepath.Join(expectedConfigBase, "codex-proxy") {
		t.Fatalf("ConfigDir = %q, want %q", mgr.ConfigDir, filepath.Join(expectedConfigBase, "codex-proxy"))
	}
	if mgr.CacheDir != filepath.Join(expectedCacheBase, "codex-proxy", "skill-subscriptions") {
		t.Fatalf("CacheDir = %q, want %q", mgr.CacheDir, filepath.Join(expectedCacheBase, "codex-proxy", "skill-subscriptions"))
	}
	if mgr.CodexDir != filepath.Clean(codexDir) {
		t.Fatalf("CodexDir = %q, want CODEX_DIR %q", mgr.CodexDir, filepath.Clean(codexDir))
	}

	t.Setenv("CODEX_DIR", "")
	mgr, err = newTeamsSkillsManager()
	if err != nil {
		t.Fatalf("newTeamsSkillsManager with CODEX_HOME: %v", err)
	}
	if mgr.CodexDir != filepath.Clean(codexHome) {
		t.Fatalf("CodexDir = %q, want CODEX_HOME %q", mgr.CodexDir, filepath.Clean(codexHome))
	}

	t.Setenv("CODEX_HOME", "")
	mgr, err = newTeamsSkillsManager()
	if err != nil {
		t.Fatalf("newTeamsSkillsManager with default codex home: %v", err)
	}
	if mgr.CodexDir != filepath.Join(filepath.Clean(home), ".codex") {
		t.Fatalf("CodexDir = %q, want home default %q", mgr.CodexDir, filepath.Join(filepath.Clean(home), ".codex"))
	}
}

func TestFormatTeamsSkillPushReviewKeepsPushInLocalTerminal(t *testing.T) {
	body := formatTeamsSkillPushReview([]skills.LocalChange{{
		Kind:       skills.ChangeModified,
		SourcePath: "skills/review/SKILL.md",
		Source:     skills.Source{Name: "acme"},
	}})
	for _, want := range []string{
		"Run `cxp skills push` locally.",
		"MODIFIED",
		"skills/review/SKILL.md",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("push review missing %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{"Push now?", "helper skills push --confirm", "pushed"} {
		if strings.Contains(strings.ToLower(body), strings.ToLower(forbidden)) {
			t.Fatalf("push review should not offer Teams push action %q:\n%s", forbidden, body)
		}
	}
}

func TestFormatTeamsSkillSyncResultsShowsFailuresAndSuccesses(t *testing.T) {
	body := formatTeamsSkillSyncResults([]skills.SyncResult{
		{
			Source:    skills.Source{Name: "acme"},
			Commit:    "0123456789abcdef",
			Installed: []skills.InstalledSkill{{Name: "review"}},
		},
		{
			Source: skills.Source{Name: "private"},
			Error:  errString("authentication hint: run gh auth login"),
		},
	})
	for _, want := range []string{"acme", "synced 1 skill", "0123456789ab", "private", "failed"} {
		if !strings.Contains(body, want) {
			t.Fatalf("sync results missing %q:\n%s", want, body)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func teamsSkillCommandMessage(id string, userID string, text string) ChatMessage {
	var msg ChatMessage
	msg.ID = id
	msg.MessageType = "message"
	msg.From.User = &struct {
		ID          string `json:"id"`
		DisplayName string `json:"displayName"`
	}{ID: userID, DisplayName: "Test User"}
	msg.Body.ContentType = "text"
	msg.Body.Content = text
	return msg
}

func newTeamsSkillsTestManager(t *testing.T) *skills.Manager {
	t.Helper()
	root := t.TempDir()
	mgr, err := skills.NewManager(skills.ManagerOptions{
		ConfigDir: filepath.Join(root, "config"),
		CacheDir:  filepath.Join(root, "cache"),
		CodexDir:  filepath.Join(root, "codex"),
		HomeDir:   filepath.Join(root, "home"),
	})
	if err != nil {
		t.Fatalf("new skills manager: %v", err)
	}
	return mgr
}

func writeTeamsSkillExport(t *testing.T, target string, source skills.Source) {
	t.Helper()
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir export: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "SKILL.md"), []byte("---\nname: review\ndescription: Review\n---\nbody\n"), 0o644); err != nil {
		t.Fatalf("write skill: %v", err)
	}
	manifest := `{
  "version": 1,
  "source_id": "` + source.ID + `",
  "source_name": "` + source.Name + `",
  "remote_url": "` + source.RemoteURL + `",
  "commit": "0123456789abcdef",
  "skill_name": "review",
  "source_path": "skills/review",
  "export_name": "acme__review",
  "files": [
    {
      "rel_path": "SKILL.md",
      "sha256": "b195d23672a91d025fe029082bfe8a3e3db50476014f1d969022868e1bc9988e",
      "size": 49,
      "mode": 420
    }
  ]
}
`
	if err := os.WriteFile(filepath.Join(target, ".cxp-skill-manifest.json"), []byte(manifest), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

func joinedOutboxBodies(messages map[string]teamstore.OutboxMessage) string {
	var b strings.Builder
	for _, msg := range messages {
		b.WriteString(msg.Body)
		b.WriteByte('\n')
	}
	return b.String()
}
