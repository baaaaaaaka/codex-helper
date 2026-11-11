package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/tuilease"
)

type sharedTUIFixture struct {
	thread    string
	newThread string
	released  chan struct{}
	done      chan struct{}
}

func (f *sharedTUIFixture) Attach(_ context.Context, thread string) (engine.TUIEndpoint, func(), <-chan struct{}, error) {
	f.thread = thread
	return engine.TUIEndpoint{URL: "ws://127.0.0.1:34567", Token: "existing-backend-capability"}, func() { close(f.released) }, f.done, nil
}

func (f *sharedTUIFixture) AttachNew(ctx context.Context, cwd string) (engine.TUIEndpoint, func(), <-chan struct{}, error) {
	ep, release, done, err := f.Attach(ctx, "")
	ep.ThreadID = f.newThread
	return ep, release, done, err
}
func TestRunCodexSessionAttachesBeforeOpeningAnotherWriter(t *testing.T) {
	testSharedTUILaunch(t, "resume")
}
func TestRunCodexNewSessionUsesSharedBackend(t *testing.T) { testSharedTUILaunch(t, "new") }
func TestRunCodexNewSessionIgnoresDifferentHelperHome(t *testing.T) {
	testSharedTUILaunch(t, "different-home")
}
func TestRunCodexNewSessionRefusesLegacyHelperHandoff(t *testing.T) {
	testSharedTUILaunch(t, "legacy-new")
}
func TestRunCodexSessionRefusesMissingLiveHelperEndpoint(t *testing.T) {
	testSharedTUILaunch(t, "missing")
}
func TestRunCodexForkAttachesAfterCreation(t *testing.T) { testSharedTUILaunch(t, "fork") }
func testSharedTUILaunch(t *testing.T, mode string) {
	fixture := writeCodexTUIBrokerFixture(t)
	cfg := newCodexOpenTestStore(t)
	t.Setenv("CODEX_HELPER_STATE_DIR", t.TempDir())
	anchor, _, err := teamsNewCoreCLIStorePath()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(anchor), 0700); err != nil {
		t.Fatal(err)
	}
	st, err := corestore.Open(anchor)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	host, _ := teamsHostname()
	holder, err := lease.Acquire(context.Background(), lease.Config{Anchor: anchor, Self: lease.Local(host, teamsBootID())})
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()
	if err = holder.Activate(); err != nil {
		t.Fatal(err)
	}
	target := &sharedTUIFixture{released: make(chan struct{}), done: make(chan struct{})}
	if mode == "legacy-new" {
		target.newThread = "pre-created-thread"
	}
	if mode != "missing" {
		helperHome := os.Getenv("CODEX_HOME")
		if mode == "different-home" {
			helperHome = t.TempDir()
		}
		server, err := tuilease.Listen(teamsTUIEndpointPath(anchor, host), helperHome, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()
	}

	// The existing backend has its own SQLite home. The TUI fixture waits
	// for this file, but must never start another app-server or migration.
	if err = os.WriteFile(fixture.appSQLiteHome, []byte("existing backend"), 0600); err != nil {
		t.Fatal(err)
	}
	originalProxy := codexAppEnsureProxyURLFn
	codexAppEnsureProxyURLFn = func(context.Context, *config.Store, config.Profile, []config.Instance, io.Writer) (string, error) {
		t.Fatal("shared attachment must not start an unused local proxy")
		return "", nil
	}
	t.Cleanup(func() { codexAppEnsureProxyURLFn = originalProxy })
	thread := "11111111-2222-3333-4444-555555555555"
	if mode == "new" || mode == "legacy-new" || mode == "different-home" {
		thread = ""
	}
	if thread == "" {
		err = runCodexNewSession(context.Background(), &rootOptions{configPath: cfg.Path()}, cfg, &config.Profile{Name: "shared"}, nil, fixture.workDir, fixture.path, "", mode != "different-home", io.Discard)
	} else if mode == "fork" {
		err = runCodexTUIInvocationViaBrokerWithHook(context.Background(), &rootOptions{configPath: cfg.Path()}, cfg, nil, nil, fixture.workDir, fixture.path, "", false, false, nil, nil, nil, func(context.Context, codexrunner.AppServerTransportStarter, codexrunner.AppServerLaunchContext) ([]string, error) {
			return []string{"resume", thread}, nil
		}, io.Discard)
	} else {
		err = runCodexSession(context.Background(), &rootOptions{configPath: cfg.Path()}, cfg, &config.Profile{Name: "shared"}, nil, codexhistory.Session{SessionID: thread, ProjectPath: fixture.workDir}, codexhistory.Project{Path: fixture.workDir}, fixture.path, "", true, io.Discard)
	}
	if mode == "missing" || mode == "legacy-new" {
		want := "compatible TUI endpoint"
		if mode == "legacy-new" {
			want = "legacy New chat handoff"
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("missing live endpoint fell back: %v", err)
		}
		for _, path := range []string{fixture.migrationArgs, fixture.appServerArgs, fixture.tuiArgs} {
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("opened another writer: %s %v", path, err)
			}
		}
		if mode == "legacy-new" {
			select {
			case <-target.released:
			case <-time.After(3 * time.Second):
				t.Fatal("legacy TUI lease leaked")
			}
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if mode == "different-home" {
		assertStandardBrokerLaunch(t, fixture)
		assertNoCodexRolloutMigration(t, fixture)
		select {
		case <-target.released:
			t.Fatal("attached to a different Codex home")
		default:
		}
		return
	}
	if target.thread != thread {
		t.Fatalf("attached %q", target.thread)
	}
	args := readArgLines(t, fixture.tuiArgs)
	wantArgs := 8
	if mode == "new" {
		wantArgs = 6
	}
	if len(args) != wantArgs || args[3] != "ws://127.0.0.1:34567" || (mode != "new" && args[7] != thread) {
		t.Fatal(args)
	}
	token, err := os.ReadFile(fixture.tuiAuthToken)
	if err != nil || string(token) != "existing-backend-capability\n" {
		t.Fatalf("capability not forwarded: %v", err)
	}
	for _, path := range []string{fixture.migrationArgs, fixture.appServerArgs} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("opened another writer: %s %v", path, err)
		}
	}
	select {
	case <-target.released:
	case <-time.After(3 * time.Second):
		t.Fatal("TUI lease was not released")
	}
}
