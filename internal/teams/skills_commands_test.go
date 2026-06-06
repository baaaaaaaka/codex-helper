package teams

import (
	"context"
	stdhtml "html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	newTeamsSkillsManagerForCommand = func(context.Context) (*skills.Manager, string, error) { return mgr, "", nil }
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
	for _, cmd := range []string{"list", "push", "remove"} {
		if err := bridge.handleSkillsCommand(ctx, "chat-1", cmd); err != nil {
			t.Fatalf("handle skills %s: %v", cmd, err)
		}
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load teams state: %v", err)
	}
	body := joinedOutboxBodies(state.OutboxMessages)
	for _, want := range []string{"Skills", "acme", "Skills Push Review", "helper skills push confirm", "review target", "Use local"} {
		if !strings.Contains(body, want) {
			t.Fatalf("outbox missing %q:\n%s", want, body)
		}
	}
	if _, ok := state.SkillPushReviews[teamsSkillPushReviewKey("chat-1")]; !ok {
		t.Fatalf("pending skill push review was not recorded: %#v", state.SkillPushReviews)
	}
}

func TestHandleSkillsCommandPushConfirmPushesReviewBranch(t *testing.T) {
	repo := initTeamsSkillRepo(t)
	mgr := newTeamsSkillsTestManager(t)
	ctx := context.Background()
	_, result, err := mgr.Add(ctx, repo, skills.AddOptions{Name: "acme", Ref: "HEAD", Path: "skills/review"})
	if err != nil {
		t.Fatalf("add source: %v", err)
	}
	if len(result.Installed) != 1 {
		t.Fatalf("installed len = %d, want 1", len(result.Installed))
	}
	if err := os.WriteFile(filepath.Join(result.Installed[0].TargetPath, "SKILL.md"), []byte("---\nname: review\ndescription: Teams updated\n---\nfrom teams\n"), 0o644); err != nil {
		t.Fatalf("modify skill: %v", err)
	}

	prev := newTeamsSkillsManagerForCommand
	newTeamsSkillsManagerForCommand = func(context.Context) (*skills.Manager, string, error) { return mgr, "", nil }
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
	if err := bridge.handleSkillsCommand(ctx, "chat-1", "push"); err != nil {
		t.Fatalf("handle skills push review: %v", err)
	}
	if err := bridge.handleSkillsCommand(ctx, "chat-1", "push confirm"); err != nil {
		t.Fatalf("handle skills push confirm: %v", err)
	}

	branch := singleTeamsReviewBranch(t, repo)
	gotSkill := teamsGitRun(t, repo, "show", branch+":skills/review/SKILL.md")
	if !strings.Contains(gotSkill, "from teams") {
		t.Fatalf("review branch SKILL.md was not updated:\n%s", gotSkill)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load teams state: %v", err)
	}
	body := joinedOutboxBodies(state.OutboxMessages)
	for _, want := range []string{"Skills Push Review", "Skills Push", "pushed `HEAD:refs/heads/skill/"} {
		if !strings.Contains(body, want) {
			t.Fatalf("push confirm response missing %q:\n%s", want, body)
		}
	}
	if _, ok := state.SkillPushReviews[teamsSkillPushReviewKey("chat-1")]; ok {
		t.Fatalf("pending skill push review was not cleared: %#v", state.SkillPushReviews)
	}
}

func TestHandleSkillsCommandAddsSourceAndInstallsSkills(t *testing.T) {
	mgr := newTeamsSkillsTestManager(t)
	mgr.Git = teamsSkillsAddGitRunner{}
	prev := newTeamsSkillsManagerForCommand
	newTeamsSkillsManagerForCommand = func(context.Context) (*skills.Manager, string, error) { return mgr, "", nil }
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
	if err := bridge.handleSkillsCommand(ctx, "chat-1", "add <https://github.com/acme/skills/tree/main/skills/review>"); err != nil {
		t.Fatalf("handle skills add: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load teams state: %v", err)
	}
	body := joinedOutboxBodies(state.OutboxMessages)
	for _, want := range []string{"Skills Add", "acme-skills", "path: `skills/review`", "Installed 1 skill", "`review` -> `acme-skills__review`"} {
		if !strings.Contains(body, want) {
			t.Fatalf("skills add response missing %q:\n%s", want, body)
		}
	}
	entries, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("list after add: %v", err)
	}
	if len(entries) != 1 || entries[0].Source.Name != "acme-skills" || len(entries[0].State.InstalledSkills) != 1 {
		t.Fatalf("entries after add = %#v", entries)
	}
}

func TestHandleSkillsCommandAddUsesTeamsHyperlinkHref(t *testing.T) {
	mgr := newTeamsSkillsTestManager(t)
	mgr.Git = teamsSkillsAddGitRunner{}
	prev := newTeamsSkillsManagerForCommand
	newTeamsSkillsManagerForCommand = func(context.Context) (*skills.Manager, string, error) { return mgr, "", nil }
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
		reg:     Registry{ControlChatID: "chat-1"},
		user:    User{ID: "user-1", DisplayName: "Test User"},
		scope:   teamstore.ScopeIdentity{ID: "scope"},
		machine: teamstore.MachineRecord{ID: "machine"},
	}

	rawURL := "https://github.com/acme/skills/tree/main/skills/review"
	displayURL := "https://github.com/acme/skills/tree/main/skills/..."
	msg := teamsSkillHTMLCommandMessage("control-add-href", "user-1", `<p>helper skills add <a href="`+stdhtml.EscapeString(rawURL)+`">`+stdhtml.EscapeString(displayURL)+`</a></p>`)
	if err := bridge.handleControlMessage(context.Background(), msg, "helper skills add "+displayURL); err != nil {
		t.Fatalf("handle skills add href: %v", err)
	}

	entries, err := mgr.List(context.Background())
	if err != nil {
		t.Fatalf("list after add: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries len = %d, want 1: %#v", len(entries), entries)
	}
	source := entries[0].Source
	if source.Name != "acme-skills" || source.RemoteURL != "https://github.com/acme/skills.git" || source.Ref != "main" || source.Path != "skills/review" {
		t.Fatalf("source = %#v, want parsed href source", source)
	}
	if len(entries[0].State.InstalledSkills) != 1 {
		t.Fatalf("installed skills = %#v, want one skill from href source", entries[0].State.InstalledSkills)
	}
}

func TestTeamsSkillAddURLFromTeamsMessagePrefersSafeMatchingHref(t *testing.T) {
	msg := teamsSkillHTMLCommandMessage("m1", "user-1", `<p>helper skills add <a href="https://github.com/acme/skills/tree/main/skills/review">Open source</a></p>`)
	if got, want := teamsSkillAddURLFromTeamsMessage(msg, "Open source"), "https://github.com/acme/skills/tree/main/skills/review"; got != want {
		t.Fatalf("href URL = %q, want %q", got, want)
	}

	msg.Body.Content = `<p>helper skills add <a href="mailto:owner@example.test">Open source</a></p>`
	if got, want := teamsSkillAddURLFromTeamsMessage(msg, "Open source"), "Open source"; got != want {
		t.Fatalf("unsafe href URL = %q, want visible text %q", got, want)
	}

	safeLinked := "https://github.com/acme/skills/tree/main/skills/review"
	msg.Body.Content = `<p>helper skills add <a href="https://nam12.safelinks.protection.outlook.com/?url=` + stdhtml.EscapeString(url.QueryEscape(safeLinked)) + `&amp;data=ignored">Open source</a></p>`
	if got, want := teamsSkillAddURLFromTeamsMessage(msg, "Open source"), safeLinked; got != want {
		t.Fatalf("safelink href URL = %q, want unwrapped %q", got, want)
	}

	msg.Body.Content = `<p>helper skills add <a href="https://nam12.safelinks.protection.outlook.com/?url=mailto%3Aowner%40example.test">Open source</a></p>`
	if got, want := teamsSkillAddURLFromTeamsMessage(msg, "Open source"), "Open source"; got != want {
		t.Fatalf("unsafe safelink URL = %q, want visible text %q", got, want)
	}

	msg.Body.Content = `<blockquote><a href="https://github.com/quoted/skills.git">Open source</a></blockquote><p>helper skills add Open source</p>`
	if got, want := teamsSkillAddURLFromTeamsMessage(msg, "Open source"), "Open source"; got != want {
		t.Fatalf("quoted href URL = %q, want visible text %q", got, want)
	}

	msg.Body.Content = `<p>helper skills add https://github.com/plain/skills.git <a href="https://github.com/acme/skills/tree/main/skills/review">github.com</a></p>`
	if got, want := teamsSkillAddURLFromTeamsMessage(msg, "https://github.com/plain/skills.git"), "https://github.com/plain/skills.git"; got != want {
		t.Fatalf("explicit raw URL = %q, want %q", got, want)
	}
}

func TestParseTeamsSkillAddArgsRejectsFlags(t *testing.T) {
	if _, err := parseTeamsSkillAddArgs("--target codex-home https://github.com/acme/skills.git"); err == nil {
		t.Fatal("parse add args accepted --target")
	}
	got, err := parseTeamsSkillAddArgs("https://github.com/acme/skills.git")
	if err != nil {
		t.Fatalf("parse add args: %v", err)
	}
	if got.RawURL != "https://github.com/acme/skills.git" {
		t.Fatalf("args = %#v, want URL", got)
	}
}

func TestHandleSkillsCommandAddReportsAuthHint(t *testing.T) {
	mgr := newTeamsSkillsTestManager(t)
	mgr.Git = teamsSkillsAuthFailGitRunner{}
	prev := newTeamsSkillsManagerForCommand
	newTeamsSkillsManagerForCommand = func(context.Context) (*skills.Manager, string, error) { return mgr, "", nil }
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
	if err := bridge.handleSkillsCommand(ctx, "chat-1", "add https://github.com/acme/private/tree/main/skills"); err != nil {
		t.Fatalf("handle skills add auth failure: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load teams state: %v", err)
	}
	body := joinedOutboxBodies(state.OutboxMessages)
	for _, want := range []string{"Skills Add", "private", "status: `auth_required`", "Authentication hint: run `gh auth login`"} {
		if !strings.Contains(body, want) {
			t.Fatalf("skills add auth response missing %q:\n%s", want, body)
		}
	}
}

func TestRedactTeamsSkillURLRedactsHTTPSecretsButKeepsSSHUser(t *testing.T) {
	got := redactTeamsSkillURL("https://token:secret@git.example.com/acme/private.git")
	if strings.Contains(got, "token:secret") || !strings.Contains(got, "https://<redacted>@git.example.com/acme/private.git") {
		t.Fatalf("HTTP secret redaction = %q", got)
	}
	got = redactTeamsSkillURL("ssh://git@gitlab-master.nvidia.com:12051/jawei/fgx_tin_skill.git")
	if got != "ssh://git@gitlab-master.nvidia.com:12051/jawei/fgx_tin_skill.git" {
		t.Fatalf("SSH URL redaction = %q", got)
	}
}

func TestSkillsCommandDispatchesThroughControlAndWorkChats(t *testing.T) {
	mgr := newTeamsSkillsTestManager(t)
	prev := newTeamsSkillsManagerForCommand
	newTeamsSkillsManagerForCommand = func(context.Context) (*skills.Manager, string, error) { return mgr, "", nil }
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
	mgr, _, err := newTeamsSkillsManager(context.Background())
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
	mgr, _, err = newTeamsSkillsManager(context.Background())
	if err != nil {
		t.Fatalf("newTeamsSkillsManager with CODEX_HOME: %v", err)
	}
	if mgr.CodexDir != filepath.Clean(codexHome) {
		t.Fatalf("CodexDir = %q, want CODEX_HOME %q", mgr.CodexDir, filepath.Clean(codexHome))
	}

	t.Setenv("CODEX_HOME", "")
	mgr, _, err = newTeamsSkillsManager(context.Background())
	if err != nil {
		t.Fatalf("newTeamsSkillsManager with default codex home: %v", err)
	}
	if mgr.CodexDir != filepath.Join(filepath.Clean(home), ".codex") {
		t.Fatalf("CodexDir = %q, want home default %q", mgr.CodexDir, filepath.Join(filepath.Clean(home), ".codex"))
	}
}

func TestFormatTeamsSkillPushReviewOffersTeamsConfirm(t *testing.T) {
	body := formatTeamsSkillPushReview(teamstore.SkillPushReview{
		Sources: []teamstore.SkillPushReviewSource{{
			SourceName:    "acme",
			RemoteURL:     "https://github.com/acme/skills.git",
			Ref:           "main",
			ReviewRefSpec: "HEAD:refs/heads/skill/acme-20260521-010203-0123456789ab",
			Changes: []teamstore.SkillPushReviewChange{{
				Kind:       string(skills.ChangeModified),
				SourcePath: "skills/review/SKILL.md",
			}},
		}},
	})
	for _, want := range []string{
		"helper skills push confirm",
		"helper skills push --direct confirm",
		"review target",
		"MODIFIED",
		"skills/review/SKILL.md",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("push review missing %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{"Run `cxp skills push` locally.", "Push now?", "helper skills push --confirm", "pushed"} {
		if strings.Contains(strings.ToLower(body), strings.ToLower(forbidden)) {
			t.Fatalf("push review contains forbidden text %q:\n%s", forbidden, body)
		}
	}
}

func TestParseTeamsSkillPushArgsRejectsNamedConfirm(t *testing.T) {
	for _, input := range []string{"acme confirm", "confirm acme", "--direct acme confirm"} {
		if _, err := parseTeamsSkillPushArgs(input); err == nil {
			t.Fatalf("parseTeamsSkillPushArgs(%q) succeeded, want usage error", input)
		}
	}
}

func TestFilterTeamsSkillPushReviewChangesIgnoresUnreviewedSources(t *testing.T) {
	now := time.Date(2026, 5, 21, 1, 2, 3, 0, time.UTC)
	sourceA := skills.Source{ID: "source-a", Name: "alpha", RemoteURL: "repo-a"}
	sourceB := skills.Source{ID: "source-b", Name: "bravo", RemoteURL: "repo-b"}
	changeA := teamsSkillPushTestChange(sourceA, "skills/alpha/SKILL.md")
	changeB := teamsSkillPushTestChange(sourceB, "skills/bravo/SKILL.md")
	review, err := buildTeamsSkillPushReview("chat-1", "", false, []skills.LocalChange{changeB}, now)
	if err != nil {
		t.Fatalf("buildTeamsSkillPushReview: %v", err)
	}

	filtered := filterTeamsSkillPushReviewChanges(review, []skills.LocalChange{changeA, changeB})
	if len(filtered) != 1 || filtered[0].Source.ID != sourceB.ID {
		t.Fatalf("filtered changes = %#v, want only source-b", filtered)
	}
	if err := ensureTeamsSkillPushReviewStillMatches(review, filtered); err != nil {
		t.Fatalf("review should still match filtered source changes: %v", err)
	}
}

func teamsSkillPushTestChange(source skills.Source, sourcePath string) skills.LocalChange {
	return skills.LocalChange{
		Source: source,
		Skill: skills.InstalledSkill{
			Name:       "review",
			ExportName: source.Name + "__review",
			SourcePath: strings.TrimSuffix(sourcePath, "/SKILL.md"),
		},
		Kind:       skills.ChangeModified,
		RelPath:    "SKILL.md",
		SourcePath: sourcePath,
		Commit:     "0123456789abcdef0123456789abcdef01234567",
		OldSHA256:  "old",
		NewSHA256:  "new",
		OldMode:    0o644,
		NewMode:    0o644,
		Size:       10,
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

func teamsSkillHTMLCommandMessage(id string, userID string, content string) ChatMessage {
	msg := teamsSkillCommandMessage(id, userID, "")
	msg.Body.ContentType = "html"
	msg.Body.Content = content
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

func initTeamsSkillRepo(t *testing.T) string {
	t.Helper()
	requireTeamsGit(t)
	repo := t.TempDir()
	teamsGitRun(t, repo, "init")
	teamsGitRun(t, repo, "config", "user.name", "Skill Test")
	teamsGitRun(t, repo, "config", "user.email", "skill-test@example.invalid")
	writeTeamsTestFile(t, filepath.Join(repo, "skills", "review", "SKILL.md"), "---\nname: review\ndescription: Review code\n---\nbody\n", 0o644)
	writeTeamsTestFile(t, filepath.Join(repo, "skills", "review", "scripts", "check.sh"), "#!/bin/sh\necho ok\n", 0o755)
	teamsGitRun(t, repo, "add", "-A")
	teamsGitRun(t, repo, "commit", "-m", "initial skill")
	return repo
}

func singleTeamsReviewBranch(t *testing.T, repo string) string {
	t.Helper()
	out := teamsGitRun(t, repo, "for-each-ref", "--format=%(refname:short)", "refs/heads/skill")
	lines := strings.Fields(out)
	if len(lines) != 1 {
		t.Fatalf("review branches = %v, want 1", lines)
	}
	return lines[0]
}

func teamsGitRun(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, string(out))
	}
	return string(out)
}

func requireTeamsGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
}

func writeTeamsTestFile(t *testing.T, path string, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
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

type teamsSkillsAddGitRunner struct{}

func (teamsSkillsAddGitRunner) Run(_ context.Context, _ string, _ []string, args ...string) ([]byte, error) {
	joined := strings.Join(args, "\x00")
	switch {
	case len(args) >= 1 && args[0] == "init":
		return nil, nil
	case len(args) >= 1 && args[0] == "config":
		return nil, nil
	case len(args) == 3 && args[0] == "ls-remote" && args[1] == "--heads":
		return []byte("0123456789abcdef0123456789abcdef01234567\trefs/heads/main\n"), nil
	case len(args) == 4 && args[0] == "ls-remote" && args[1] == "--symref":
		return []byte("ref: refs/heads/main\tHEAD\n0123456789abcdef0123456789abcdef01234567\tHEAD\n"), nil
	case strings.HasPrefix(joined, "fetch\x00"):
		return nil, nil
	case len(args) >= 1 && args[0] == "rev-parse":
		return []byte("0123456789abcdef0123456789abcdef01234567\n"), nil
	case len(args) >= 1 && args[0] == "ls-tree":
		return []byte("100644 blob skillmd\tskills/review/SKILL.md\x00"), nil
	case len(args) == 3 && args[0] == "cat-file" && args[1] == "blob" && args[2] == "skillmd":
		return []byte("---\nname: review\ndescription: Review\n---\nbody\n"), nil
	default:
		return nil, errString("unexpected git args: " + strings.Join(args, " "))
	}
}

type teamsSkillsAuthFailGitRunner struct{}

func (teamsSkillsAuthFailGitRunner) Run(_ context.Context, _ string, _ []string, args ...string) ([]byte, error) {
	switch {
	case len(args) >= 1 && args[0] == "init":
		return nil, nil
	case len(args) >= 1 && args[0] == "config":
		return nil, nil
	case len(args) >= 1 && args[0] == "ls-remote":
		return nil, &skills.GitError{Args: args, Output: "fatal: repository not found", Err: errString("exit status 128")}
	case len(args) >= 1 && args[0] == "fetch":
		return nil, &skills.GitError{Args: args, Output: "fatal: repository not found", Err: errString("exit status 128")}
	default:
		return nil, errString("unexpected git args: " + strings.Join(args, " "))
	}
}
