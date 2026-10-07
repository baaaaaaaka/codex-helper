package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
)

const windowsInstallWorkerArgument = "--cxp-internal-windows-app-install"

type windowsInstallIdentity struct {
	SID      string
	Session  uint32
	Profile  string
	Cache    string
	Elevated bool
}

type windowsInstallRequest struct {
	Identity windowsInstallIdentity
	Root     string
	ProxyURL string
	Refresh  bool
}

type windowsInstallResult struct {
	State   codexWindowsManagedInstallState
	Changed bool
}

var windowsInstallDelegationCheck = checkWindowsInstallDelegation
var windowsInstallDelegate = delegateWindowsInstall
var windowsInstallIdentityQuery = currentWindowsInstallIdentity

func validateWindowsInstallIdentity(expected, actual windowsInstallIdentity) error {
	if actual.Elevated || expected.SID == "" || actual.SID != expected.SID || actual.Session != expected.Session {
		return errors.New("installation worker must use the same user and session with a non-elevated token")
	}
	for _, paths := range [][2]string{{expected.Profile, actual.Profile}, {expected.Cache, actual.Cache}} {
		if paths[0] == "" || paths[1] == "" || !strings.EqualFold(filepath.Clean(paths[0]), filepath.Clean(paths[1])) {
			return errors.New("installation worker profile or LocalAppData differs from the invoking user")
		}
	}
	return nil
}

func ensureWindowsInstallDelegationAvailable() error {
	if err := windowsInstallDelegationCheck(); err != nil {
		return fmt.Errorf("cannot safely install with the same user's normal Windows token; rerun CXP without Administrator privileges: %w", err)
	}
	return nil
}

func HandleWindowsAppInstallWorker(args []string, input io.Reader, output, diagnostics io.Writer) (int, bool) {
	if len(args) != 2 || args[1] != windowsInstallWorkerArgument {
		return 0, false
	}
	err := runWindowsAppInstallWorker(input, output, diagnostics)
	if err != nil {
		_, _ = fmt.Fprintf(diagnostics, "Error: managed app installation worker: %v\n", err)
		return 1, true
	}
	return 0, true
}

func runWindowsAppInstallWorker(input io.Reader, output, diagnostics io.Writer) error {
	if codexAppGOOS() != "windows" {
		return errors.New("installation worker requires native Windows")
	}
	requestBytes, err := io.ReadAll(io.LimitReader(input, 64*1024+1))
	if err != nil {
		return fmt.Errorf("read installation request: %w", err)
	}
	if len(requestBytes) > 64*1024 {
		return errors.New("installation request exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(requestBytes))
	decoder.DisallowUnknownFields()
	var request windowsInstallRequest
	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("decode installation request: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("installation request has trailing data")
	}
	identity, err := windowsInstallIdentityQuery()
	if err != nil {
		return err
	}
	if err := validateWindowsInstallIdentity(request.Identity, identity); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	root, err := codexAppWindowsManagedRootFn(ctx)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(request.Root) || !strings.EqualFold(filepath.Clean(root), filepath.Clean(request.Root)) {
		return errors.New("installation worker target differs from the invoking user's managed root")
	}
	state, changed, err := ensureCodexWindowsManagedInstallWithRefresh(ctx, root, codexDesktopAppOptions{ProxyURL: request.ProxyURL, Log: diagnostics}, request.Refresh)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(windowsInstallResult{State: state, Changed: changed})
}
