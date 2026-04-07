//go:build !windows

package cli

import (
	"context"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

type fakeOwnerStat struct {
	Uid uint32
	Gid uint32
}

type fakeOwnerFileInfo struct{}

func (fakeOwnerFileInfo) Name() string       { return "home" }
func (fakeOwnerFileInfo) Size() int64        { return 0 }
func (fakeOwnerFileInfo) Mode() os.FileMode  { return 0o700 }
func (fakeOwnerFileInfo) ModTime() time.Time { return time.Time{} }
func (fakeOwnerFileInfo) IsDir() bool        { return true }
func (fakeOwnerFileInfo) Sys() any           { return &fakeOwnerStat{Uid: 1001, Gid: 1002} }

func TestApplyExecIdentitySetsCommandCredentialAndEnv(t *testing.T) {
	lockCLITestHooks(t)

	cmd := exec.Command("true")
	envVars, err := applyExecIdentity(cmd, []string{"PATH=/usr/bin"}, &execIdentity{
		UID:         1000,
		GID:         1001,
		Groups:      []uint32{1002, 1003},
		GroupsKnown: true,
		Username:    "alice",
		Home:        "/home/alice",
	})
	if err != nil {
		t.Fatalf("applyExecIdentity: %v", err)
	}
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.Credential == nil {
		t.Fatal("expected command credential to be set")
	}
	if cmd.SysProcAttr.Credential.Uid != 1000 || cmd.SysProcAttr.Credential.Gid != 1001 {
		t.Fatalf("unexpected credential: %+v", cmd.SysProcAttr.Credential)
	}
	if !slices.Equal(cmd.SysProcAttr.Credential.Groups, []uint32{1002, 1003}) {
		t.Fatalf("unexpected supplementary groups: %v", cmd.SysProcAttr.Credential.Groups)
	}
	if !slices.Contains(envVars, "HOME=/home/alice") {
		t.Fatalf("expected HOME in env, got %v", envVars)
	}
	if !slices.Contains(envVars, "USER=alice") {
		t.Fatalf("expected USER in env, got %v", envVars)
	}
	if !slices.Contains(envVars, "LOGNAME=alice") {
		t.Fatalf("expected LOGNAME in env, got %v", envVars)
	}
}

func TestExecIdentityForUserResolvesSupplementaryGroups(t *testing.T) {
	lockCLITestHooks(t)

	prevLookupGroups := execIdentityLookupUserGroups
	t.Cleanup(func() { execIdentityLookupUserGroups = prevLookupGroups })

	execIdentityLookupUserGroups = func(u *user.User) ([]string, error) {
		if u == nil || u.Username != "alice" {
			t.Fatalf("unexpected user: %+v", u)
		}
		return []string{"1001", "1002", "1002", "1003"}, nil
	}

	identity, err := execIdentityForUser(&user.User{
		Uid:      "1000",
		Gid:      "1001",
		Username: "alice",
		HomeDir:  "/home/alice",
	}, "")
	if err != nil {
		t.Fatalf("execIdentityForUser: %v", err)
	}
	if identity == nil {
		t.Fatal("expected identity")
	}
	if !identity.GroupsKnown {
		t.Fatal("expected GroupsKnown=true")
	}
	if !slices.Equal(identity.Groups, []uint32{1002, 1003}) {
		t.Fatalf("unexpected supplementary groups: %v", identity.Groups)
	}
}

func TestExecIdentityForHomeRejectsUnknownNonRootOwner(t *testing.T) {
	lockCLITestHooks(t)

	prevStat := execIdentityStat
	prevLookupByID := execIdentityLookupUserByID
	t.Cleanup(func() {
		execIdentityStat = prevStat
		execIdentityLookupUserByID = prevLookupByID
	})

	execIdentityStat = func(string) (os.FileInfo, error) {
		return fakeOwnerFileInfo{}, nil
	}
	execIdentityLookupUserByID = func(string) (*user.User, error) {
		return nil, os.ErrNotExist
	}

	identity, err := execIdentityForHome("/home/alice")
	if err != nil {
		t.Fatalf("execIdentityForHome: %v", err)
	}
	if identity != nil {
		t.Fatalf("expected nil identity for unknown owner, got %+v", identity)
	}
}

func TestPrepareYoloAuthOverrideReownsAuthFilesForExecIdentity(t *testing.T) {
	lockCLITestHooks(t)

	prevChown := execIdentityChown
	t.Cleanup(func() { execIdentityChown = prevChown })

	codexDir := t.TempDir()
	writeTestAuthJSON(t, codexDir, true)

	var chowned []string
	execIdentityChown = func(path string, uid, gid int) error {
		chowned = append(chowned, path)
		if uid != 1000 || gid != 1001 {
			t.Fatalf("unexpected chown target %d:%d", uid, gid)
		}
		return nil
	}

	override, err := prepareYoloAuthOverride(codexDir, &execIdentity{
		UID:      1000,
		GID:      1001,
		Username: "alice",
		Home:     filepath.Dir(codexDir),
	})
	if err != nil {
		t.Fatalf("prepareYoloAuthOverride: %v", err)
	}
	if override == nil {
		t.Fatal("expected auth override")
	}
	if len(chowned) < 3 {
		t.Fatalf("expected backup/auth/lease ownership fixes, got %v", chowned)
	}
	override.Cleanup()
}

func TestPrepareCodexSelfUpdateGuardEnvReownsWrapperForExecIdentity(t *testing.T) {
	lockCLITestHooks(t)

	prevDetect := codexSelfUpdateDetectSource
	prevLookPath := codexSelfUpdateLookPath
	prevExecutable := codexSelfUpdateExecutable
	prevChown := execIdentityChown
	t.Cleanup(func() {
		codexSelfUpdateDetectSource = prevDetect
		codexSelfUpdateLookPath = prevLookPath
		codexSelfUpdateExecutable = prevExecutable
		execIdentityChown = prevChown
	})

	codexSelfUpdateDetectSource = func(context.Context, string, []string) (codexUpgradeSource, error) {
		return codexUpgradeSource{
			origin:    codexInstallOriginSystem,
			codexPath: "/tmp/codex",
			npmPrefix: "/tmp/npm-global",
		}, nil
	}
	codexSelfUpdateLookPath = func(string) (string, error) { return "/usr/bin/npm", nil }
	codexSelfUpdateExecutable = func() (string, error) { return "/usr/bin/codex-proxy", nil }

	var chowned []string
	execIdentityChown = func(path string, uid, gid int) error {
		chowned = append(chowned, filepath.Clean(path))
		if uid != 1000 || gid != 1001 {
			t.Fatalf("unexpected chown target %d:%d", uid, gid)
		}
		return nil
	}

	updated, cleanup, err := prepareCodexSelfUpdateGuardEnv(
		context.Background(),
		"/tmp/codex",
		[]string{"PATH=/usr/bin:/bin"},
		&execIdentity{UID: 1000, GID: 1001, Username: "alice", Home: "/home/alice"},
	)
	if err != nil {
		t.Fatalf("prepare guard env: %v", err)
	}
	t.Cleanup(cleanup)

	wrapperDir := filepath.Clean(filepath.SplitList(envValue(updated, "PATH"))[0])
	wrapperName := "npm"
	if len(chowned) < 2 {
		t.Fatalf("expected wrapper dir and wrapper file ownership fixes, got %v", chowned)
	}
	if !slices.Contains(chowned, wrapperDir) {
		t.Fatalf("expected wrapper dir in chown list, got %v", chowned)
	}
	if !slices.Contains(chowned, filepath.Join(wrapperDir, wrapperName)) {
		t.Fatalf("expected wrapper path in chown list, got %v", chowned)
	}
}
