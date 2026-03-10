package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

type fakeSSHOps struct {
	probeErrs   []error
	probes      []config.Profile
	keyPath     string
	generateErr error
	installErr  error
	installed   []string
}

func (f *fakeSSHOps) probe(_ context.Context, prof config.Profile, _ bool) error {
	f.probes = append(f.probes, prof)
	if len(f.probeErrs) == 0 {
		return nil
	}
	err := f.probeErrs[0]
	f.probeErrs = f.probeErrs[1:]
	return err
}

func (f *fakeSSHOps) generateKeypair(context.Context, *config.Store, config.Profile) (string, error) {
	if f.generateErr != nil {
		return "", f.generateErr
	}
	return f.keyPath, nil
}

func (f *fakeSSHOps) installPublicKey(_ context.Context, _ config.Profile, pubKeyPath string) error {
	f.installed = append(f.installed, pubKeyPath)
	return f.installErr
}

func TestInitProfileInteractiveWithDepsDirectSSHSuccess(t *testing.T) {
	store := newTempStore(t)
	reader := bufio.NewReader(strings.NewReader("\nexample.com\n0\n70000\n2222\n\nalice\n"))
	ops := &fakeSSHOps{}
	var out bytes.Buffer

	prof, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, &out)
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v", err)
	}

	if prof.Host != "example.com" || prof.Port != 2222 || prof.User != "alice" {
		t.Fatalf("unexpected profile: %+v", prof)
	}
	if prof.Name != "alice@example.com" {
		t.Fatalf("unexpected profile name %q", prof.Name)
	}
	if prof.ID == "" || prof.CreatedAt.IsZero() {
		t.Fatalf("expected generated id and creation time, got %+v", prof)
	}
	if len(ops.probes) != 1 {
		t.Fatalf("expected 1 probe, got %d", len(ops.probes))
	}
	if len(ops.installed) != 0 {
		t.Fatalf("expected no key installation, got %v", ops.installed)
	}
	if !strings.Contains(out.String(), "Proxy mode uses an SSH tunnel") {
		t.Fatalf("expected intro text, got %q", out.String())
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].ID != prof.ID {
		t.Fatalf("expected saved profile, got %+v", cfg.Profiles)
	}
}

func TestInitProfileInteractiveWithDepsFallsBackToManagedKey(t *testing.T) {
	store := newTempStore(t)
	reader := bufio.NewReader(strings.NewReader("host.example\n22\ncarol\n"))
	ops := &fakeSSHOps{
		probeErrs: []error{errors.New("direct ssh failed"), nil},
		keyPath:   filepath.Join(t.TempDir(), "id_ed25519_test"),
	}
	var out bytes.Buffer

	prof, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, &out)
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v", err)
	}

	if len(ops.probes) != 2 {
		t.Fatalf("expected 2 probes, got %d", len(ops.probes))
	}
	if len(ops.installed) != 1 || ops.installed[0] != ops.keyPath+".pub" {
		t.Fatalf("expected installed pubkey %q, got %v", ops.keyPath+".pub", ops.installed)
	}
	if len(prof.SSHArgs) != 2 || prof.SSHArgs[0] != "-i" || prof.SSHArgs[1] != ops.keyPath {
		t.Fatalf("expected SSHArgs to use generated key, got %v", prof.SSHArgs)
	}
	if !strings.Contains(out.String(), "creating a dedicated codex-proxy key") {
		t.Fatalf("expected fallback message, got %q", out.String())
	}
}

func TestInitProfileInteractiveWithDepsReturnsWrappedKeyProbeError(t *testing.T) {
	store := newTempStore(t)
	reader := bufio.NewReader(strings.NewReader("host.example\n22\ndana\n"))
	ops := &fakeSSHOps{
		probeErrs: []error{errors.New("direct ssh failed"), errors.New("still blocked")},
		keyPath:   filepath.Join(t.TempDir(), "id_ed25519_test"),
	}

	_, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, ioDiscard{})
	if err == nil {
		t.Fatal("expected key-based probe error")
	}
	if !strings.Contains(err.Error(), "key-based ssh probe failed") {
		t.Fatalf("expected wrapped key probe error, got %v", err)
	}

	cfg, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("load config: %v", loadErr)
	}
	if len(cfg.Profiles) != 0 {
		t.Fatalf("expected no saved profiles after failure, got %+v", cfg.Profiles)
	}
}

func TestEnsureProfileAutoInitUsesInteractiveInitializer(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	prevInit := initProfileInteractiveFunc
	t.Cleanup(func() { initProfileInteractiveFunc = prevInit })

	initProfileInteractiveFunc = func(_ context.Context, s *config.Store) (config.Profile, error) {
		prof := config.Profile{ID: "p-auto", Name: "dev", Host: "host", Port: 22, User: "alice"}
		if err := s.Update(func(cfg *config.Config) error {
			cfg.UpsertProfile(prof)
			return nil
		}); err != nil {
			return config.Profile{}, err
		}
		return prof, nil
	}

	prof, cfg, err := ensureProfile(context.Background(), store, "", true, nil)
	if err != nil {
		t.Fatalf("ensureProfile error: %v", err)
	}
	if prof.ID != "p-auto" {
		t.Fatalf("expected created profile, got %+v", prof)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].ID != "p-auto" {
		t.Fatalf("expected loaded config to include created profile, got %+v", cfg.Profiles)
	}
}

func TestEnsureProfileReturnsCreatedProfileWhenRequestedRefStillMissing(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	prevInit := initProfileInteractiveFunc
	t.Cleanup(func() { initProfileInteractiveFunc = prevInit })

	initProfileInteractiveFunc = func(context.Context, *config.Store) (config.Profile, error) {
		return config.Profile{ID: "created", Name: "new"}, nil
	}

	prof, _, err := ensureProfile(context.Background(), store, "missing", true, nil)
	if err != nil {
		t.Fatalf("ensureProfile error: %v", err)
	}
	if prof.ID != "created" {
		t.Fatalf("expected created profile fallback, got %+v", prof)
	}
}

func TestNewInitCmdUsesInteractiveInitializer(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	prevInit := initProfileInteractiveFunc
	t.Cleanup(func() { initProfileInteractiveFunc = prevInit })

	initProfileInteractiveFunc = func(context.Context, *config.Store) (config.Profile, error) {
		return config.Profile{ID: "p1", Name: "dev@host"}, nil
	}

	cmd := newInitCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute init cmd: %v", err)
	}
	if !strings.Contains(out.String(), `Saved profile "dev@host" (p1)`) {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
