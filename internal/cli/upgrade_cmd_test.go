package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func TestUpgradeCmdAlreadyUpToDateSkipsDownload(t *testing.T) {
	lockCLITestHooks(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(_ context.Context, opts update.CheckOptions) update.Status {
		if opts.Repo != "owner/name" {
			t.Fatalf("expected repo owner/name, got %q", opts.Repo)
		}
		if opts.InstalledVersion == "" {
			t.Fatal("expected installed version")
		}
		if opts.Timeout != 8*time.Second {
			t.Fatalf("expected 8s check timeout, got %s", opts.Timeout)
		}
		return update.Status{Supported: true, UpdateAvailable: false}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("PerformUpdate should not run when latest is already installed")
		return update.ApplyResult{}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--repo", "owner/name"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if !strings.Contains(out.String(), "Already up to date.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdExplicitVersionCallsPerformUpdate(t *testing.T) {
	lockCLITestHooks(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--repo", "owner/name", "--version", "v1.2.3", "--install-path", "/tmp/codex-proxy"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}

	if got.Repo != "owner/name" || got.Version != "v1.2.3" || got.InstallPath != "/tmp/codex-proxy" {
		t.Fatalf("unexpected update options: %+v", got)
	}
	if got.Timeout != 120*time.Second {
		t.Fatalf("expected 120s update timeout, got %s", got.Timeout)
	}
	if !strings.Contains(out.String(), "Updated to v1.2.3.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdRestartRequiredMessage(t *testing.T) {
	lockCLITestHooks(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		return update.Status{Supported: true, UpdateAvailable: true}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{Version: "1.2.3", RestartRequired: true}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if !strings.Contains(out.String(), "Update scheduled for v1.2.3. Please restart `codex-proxy`.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdPropagatesUpdateError(t *testing.T) {
	lockCLITestHooks(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		return update.Status{Supported: false, Error: "network unavailable"}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{}, errors.New("download failed")
	}

	cmd := newUpgradeCmd(&rootOptions{})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "download failed") {
		t.Fatalf("expected update error, got %v", err)
	}
}
