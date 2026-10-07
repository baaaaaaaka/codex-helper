package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsInstallIdentityRequiresSameNormalUser(t *testing.T) {
	expected := windowsInstallIdentity{SID: "S-1-5-21-1", Session: 2, Profile: "/user/profile", Cache: "/user/cache", Elevated: true}
	normal := expected
	normal.Elevated = false
	if err := validateWindowsInstallIdentity(expected, normal); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"SID", "session", "profile", "cache", "elevated", "empty"} {
		t.Run(name, func(t *testing.T) {
			actual := normal
			switch name {
			case "SID":
				actual.SID = "another-account"
			case "session":
				actual.Session++
			case "profile":
				actual.Profile = "/other/profile"
			case "cache":
				actual.Cache = "/other/cache"
			case "elevated":
				actual.Elevated = true
			case "empty":
				actual.Profile = ""
			}
			if err := validateWindowsInstallIdentity(expected, actual); err == nil {
				t.Fatal("unsafe installation identity accepted")
			}
		})
	}
}

func TestWindowsInstallDelegationNeverWritesInElevatedParent(t *testing.T) {
	lockCLITestHooks(t)
	previousGOOS, previousElevation := codexAppGOOS, codexAppTokenElevationFn
	previousCheck, previousDelegate := windowsInstallDelegationCheck, windowsInstallDelegate
	t.Cleanup(func() {
		codexAppGOOS, codexAppTokenElevationFn = previousGOOS, previousElevation
		windowsInstallDelegationCheck, windowsInstallDelegate = previousCheck, previousDelegate
	})
	codexAppGOOS = func() string { return "windows" }
	codexAppTokenElevationFn = func() (bool, error) { return true, nil }
	root := filepath.Join(t.TempDir(), "not-created")
	windowsInstallDelegationCheck = func() error { return nil }
	for _, refresh := range []bool{false, true} {
		called := false
		failure := errors.New("child failed")
		windowsInstallDelegate = func(_ context.Context, target string, opts codexDesktopAppOptions, actualRefresh bool) (codexWindowsManagedInstallState, bool, error) {
			called = true
			if target != root || actualRefresh != refresh || opts.ProxyURL != "http://127.0.0.1:8608" {
				t.Fatal("delegation changed installation arguments")
			}
			return codexWindowsManagedInstallState{}, false, failure
		}
		_, _, err := ensureCodexWindowsManagedInstallWithRefresh(context.Background(), root, codexDesktopAppOptions{ProxyURL: "http://127.0.0.1:8608"}, refresh)
		if !called || !errors.Is(err, failure) {
			t.Fatalf("worker failure was not propagated: %v", err)
		}
		if _, err := os.Stat(root); !os.IsNotExist(err) {
			t.Fatalf("elevated parent wrote the managed root: %v", err)
		}
	}
	windowsInstallDelegationCheck = func() error { return errors.New("no same-user normal context") }
	windowsInstallDelegate = func(context.Context, string, codexDesktopAppOptions, bool) (codexWindowsManagedInstallState, bool, error) {
		t.Fatal("delegation was attempted without a verified normal token")
		return codexWindowsManagedInstallState{}, false, nil
	}
	_, err := ensureCodexWindowsManagedInstall(context.Background(), root, codexDesktopAppOptions{})
	if err == nil || !strings.Contains(err.Error(), "without Administrator privileges") {
		t.Fatalf("missing safe fallback instruction: %v", err)
	}
}

func TestWindowsInstallWorkerRejectsRequestsBeforeWrite(t *testing.T) {
	lockCLITestHooks(t)
	previousGOOS, previousQuery, previousRoot := codexAppGOOS, windowsInstallIdentityQuery, codexAppWindowsManagedRootFn
	t.Cleanup(func() {
		codexAppGOOS, windowsInstallIdentityQuery, codexAppWindowsManagedRootFn = previousGOOS, previousQuery, previousRoot
	})
	codexAppGOOS = func() string { return "windows" }
	root := filepath.Join(t.TempDir(), "not-created")
	identity := windowsInstallIdentity{SID: "same-user", Session: 2, Profile: "/profile", Cache: "/cache"}
	windowsInstallIdentityQuery = func() (windowsInstallIdentity, error) { return identity, nil }
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return root, nil }
	for _, name := range []string{"account", "session", "profile", "cache", "elevated", "target", "relative", "malformed", "unknown", "trailing", "oversized"} {
		t.Run(name, func(t *testing.T) {
			request := windowsInstallRequest{Identity: identity, Root: root}
			switch name {
			case "account":
				request.Identity.SID = "other-user"
			case "session":
				request.Identity.Session++
			case "profile":
				request.Identity.Profile = "/other"
			case "cache":
				request.Identity.Cache = "/other"
			case "elevated":
				original := identity
				identity.Elevated = true
				defer func() { identity = original }()
			case "target":
				request.Root = root + "-other"
			case "relative":
				request.Root = "relative"
			}
			encoded, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "malformed":
				encoded = []byte("{")
			case "unknown":
				encoded = []byte(`{"Unknown":true}`)
			case "trailing":
				encoded = append(encoded, []byte(" {}")...)
			case "oversized":
				encoded = append(encoded, []byte(strings.Repeat(" ", 64*1024))...)
			}
			var output, diagnostics bytes.Buffer
			code, handled := HandleWindowsAppInstallWorker([]string{"cxp", windowsInstallWorkerArgument}, bytes.NewReader(encoded), &output, &diagnostics)
			if !handled || code != 1 || output.Len() != 0 || diagnostics.Len() == 0 {
				t.Fatalf("unsafe request accepted: code=%d handled=%t output=%s", code, handled, output.String())
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("worker wrote before verifying request: %v", err)
			}
		})
	}
}

func TestWindowsInstallWorkerDoesNotCaptureOrdinaryArguments(t *testing.T) {
	for _, args := range [][]string{{"cxp"}, {"cxp", "app"}, {"cxp", "--upgrade-codex-app"}, {"cxp", windowsInstallWorkerArgument, "extra"}} {
		if _, handled := HandleWindowsAppInstallWorker(args, nil, nil, nil); handled {
			t.Fatalf("internal worker captured ordinary CLI arguments: %v", args)
		}
	}
}

func TestWindowsInstallWorkerPublishesVerifiedState(t *testing.T) {
	lockCLITestHooks(t)
	previousGOOS, previousQuery, previousRoot := codexAppGOOS, windowsInstallIdentityQuery, codexAppWindowsManagedRootFn
	previousDownload, previousOutput := codexAppDownloadPackageFn, codexAppCommandOutput
	t.Cleanup(func() {
		codexAppGOOS, windowsInstallIdentityQuery, codexAppWindowsManagedRootFn = previousGOOS, previousQuery, previousRoot
		codexAppDownloadPackageFn, codexAppCommandOutput = previousDownload, previousOutput
	})
	codexAppGOOS = func() string { return "windows" }
	root := filepath.Join(t.TempDir(), "managed")
	identity := windowsInstallIdentity{SID: "same-user", Session: 2, Profile: "/profile", Cache: "/cache"}
	windowsInstallIdentityQuery = func() (windowsInstallIdentity, error) { return identity, nil }
	codexAppWindowsManagedRootFn = func(context.Context) (string, error) { return root, nil }
	packagePath := filepath.Join(t.TempDir(), "fixture.msix")
	writeTestCodexWindowsManagedMSIX(t, packagePath, "CN=TestPublisher", "app/ChatGPT.exe", []byte("inert fixture"))
	codexAppDownloadPackageFn = func(_ context.Context, opts codexAppDownloadOptions) error {
		if opts.ProxyURL != "http://127.0.0.1:8608" {
			t.Fatal("worker did not preserve download proxy")
		}
		data, err := os.ReadFile(packagePath)
		if err != nil {
			return err
		}
		return os.WriteFile(opts.Path, data, 0o600)
	}
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("CN=TestPublisher\n"), nil
	}
	for _, refresh := range []bool{false, true} {
		request, err := json.Marshal(windowsInstallRequest{Identity: identity, Root: root, ProxyURL: "http://127.0.0.1:8608", Refresh: refresh})
		if err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		if err := runWindowsAppInstallWorker(bytes.NewReader(request), &output, io.Discard); err != nil {
			t.Fatal(err)
		}
		var result windowsInstallResult
		if err := json.Unmarshal(output.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		state, valid, err := readValidCodexWindowsManagedState(root)
		if err != nil || !valid || state != result.State || result.Changed == refresh {
			t.Fatalf("unverified worker result: result=%+v valid=%t err=%v", result, valid, err)
		}
	}
}
