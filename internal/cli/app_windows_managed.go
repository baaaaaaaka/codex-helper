package cli

// The managed Windows backend keeps a private, unpacked copy of the official
// ChatGPT MSIX. Store package files live below WindowsApps and cannot be
// launched with Start-Process, while an unpacked copy can inherit the proxy
// and CODEX_HOME environment that cxp needs to provide.

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/proc"
)

const (
	codexWindowsManagedDownloadsDir   = ".downloads"
	codexWindowsManagedStagingDir     = ".staging"
	codexWindowsManagedVersionsDir    = "versions"
	codexWindowsManagedLockDir        = "install.lockdir"
	codexWindowsManagedCurrentState   = "current.json"
	codexWindowsManagedArch           = "x64"
	codexWindowsManagedLockOwner      = "owner.json"
	codexWindowsManagedLockStaleAfter = 10 * time.Minute
)

var (
	codexWindowsManagedNow          = time.Now
	codexWindowsManagedProcessAlive = proc.IsAlive
)

type codexWindowsManagedLaunchState uint8

const (
	codexWindowsManagedLaunchNotStarted codexWindowsManagedLaunchState = iota
	codexWindowsManagedLaunchReady
	codexWindowsManagedLaunchStartedUncertain
	codexWindowsManagedLaunchNoFallback
)

type codexWindowsManagedInstallState struct {
	PackageName      string `json:"packageName"`
	PackageVersion   string `json:"packageVersion"`
	Architecture     string `json:"architecture"`
	Publisher        string `json:"publisher"`
	PackageSHA256    string `json:"packageSha256"`
	ExecutableSHA256 string `json:"executableSha256"`
	RuntimeRelative  string `json:"runtime"`
	InstalledAt      string `json:"installedAt"`
}

type codexWindowsManagedLockMetadata struct {
	PID       int       `json:"pid"`
	CreatedAt time.Time `json:"createdAt"`
}

// reclaimStaleCodexWindowsManagedLock only reclaims a lock that has a
// well-formed owner record, is older than the lease window, and belongs to a
// process that is no longer alive. Missing/corrupt metadata is deliberately
// treated as unknown and left in place; a bounded caller context can then
// report the contention instead of deleting another process's lock.
func reclaimStaleCodexWindowsManagedLock(lockPath string) (bool, error) {
	data, err := os.ReadFile(filepath.Join(lockPath, codexWindowsManagedLockOwner))
	if err != nil {
		return false, nil
	}
	var owner codexWindowsManagedLockMetadata
	if err := json.Unmarshal(data, &owner); err != nil || owner.PID <= 0 || owner.CreatedAt.IsZero() {
		return false, nil
	}
	if codexWindowsManagedNow().Sub(owner.CreatedAt) < codexWindowsManagedLockStaleAfter || codexWindowsManagedProcessAlive(owner.PID) {
		return false, nil
	}
	reclaimPath := lockPath + fmt.Sprintf(".reclaim-%d-%d", os.Getpid(), codexWindowsManagedNow().UnixNano())
	if err := os.Rename(lockPath, reclaimPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		// Another process may have won the race and replaced the lock. Do not
		// remove or reinterpret its new owner record.
		return false, nil
	}
	if err := os.RemoveAll(reclaimPath); err != nil {
		return false, fmt.Errorf("remove stale managed ChatGPT install lock: %w", err)
	}
	return true, nil
}

type codexWindowsManagedManifest struct {
	PackageName    string
	PackageVersion string
	Architecture   string
	Publisher      string
	Executable     string
}

func defaultCodexAppWindowsManagedRoot(ctx context.Context) (string, error) {
	if configured := strings.TrimSpace(os.Getenv("CXP_WINDOWS_MANAGED_ROOT")); configured != "" {
		return filepath.Clean(configured), nil
	}
	if codexAppGOOS() == "windows" {
		base, err := os.UserCacheDir()
		if err != nil {
			return "", fmt.Errorf("resolve Windows LocalAppData: %w", err)
		}
		return filepath.Join(base, "cxp", "apps", "chatgpt"), nil
	}
	if codexAppGOOS() != "linux" || !codexAppIsWSL() {
		return "", fmt.Errorf("managed Windows ChatGPT runtime requires Windows or WSL")
	}

	name := teamsServicePowerShellExecutable()
	out, err := exec.CommandContext(ctx, name, "-NoProfile", "-NonInteractive", "-Command", "[Environment]::GetFolderPath('LocalApplicationData')").Output()
	if err != nil {
		return "", fmt.Errorf("resolve Windows LocalAppData through PowerShell: %w", err)
	}
	windowsPath := strings.TrimSpace(string(out))
	if windowsPath == "" {
		return "", errors.New("PowerShell returned an empty Windows LocalAppData path")
	}
	out, err = exec.CommandContext(ctx, "wslpath", "-u", windowsPath).Output()
	if err != nil {
		return "", fmt.Errorf("convert Windows LocalAppData path to WSL: %w", err)
	}
	root := strings.TrimSpace(string(out))
	if root == "" {
		return "", errors.New("wslpath returned an empty Windows LocalAppData path")
	}
	return filepath.Join(root, "cxp", "apps", "chatgpt"), nil
}

func launchCodexDesktopAppWindowsManaged(ctx context.Context, opts codexDesktopAppOptions) (codexWindowsManagedLaunchState, error) {
	if codexAppGOOS() == "windows" && !strings.EqualFold(codexAppGOARCH(), codexWindowsManagedArch) && !strings.EqualFold(codexAppGOARCH(), "amd64") {
		return codexWindowsManagedLaunchNotStarted, fmt.Errorf("managed Windows ChatGPT runtime supports x64 only; current architecture: %s", codexAppGOARCH())
	}
	if codexAppGOOS() != "windows" && !(codexAppGOOS() == "linux" && codexAppIsWSL()) {
		return codexWindowsManagedLaunchNotStarted, errors.New("managed Windows ChatGPT runtime requires native Windows or WSL")
	}
	if err := ctx.Err(); err != nil {
		return codexWindowsManagedLaunchNoFallback, err
	}

	if err := ensureWindowsManagedProxyReachable(ctx, opts); err != nil {
		if ctx.Err() != nil {
			return codexWindowsManagedLaunchNoFallback, ctx.Err()
		}
		return codexWindowsManagedLaunchNotStarted, err
	}

	root, err := codexAppWindowsManagedRootFn(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return codexWindowsManagedLaunchNoFallback, ctx.Err()
		}
		return codexWindowsManagedLaunchNotStarted, err
	}
	if _, err := codexAppLookPath(teamsServicePowerShellExecutable()); err != nil {
		if ctx.Err() != nil {
			return codexWindowsManagedLaunchNoFallback, ctx.Err()
		}
		return codexWindowsManagedLaunchNotStarted, fmt.Errorf("PowerShell is required for managed Windows ChatGPT launch: %w", err)
	}
	if err := checkWindowsManagedProcessConflict(ctx); err != nil {
		if ctx.Err() != nil {
			return codexWindowsManagedLaunchNoFallback, ctx.Err()
		}
		if strings.Contains(err.Error(), "CXP_MANAGED_CONFLICT") {
			return codexWindowsManagedLaunchNoFallback, err
		}
		return codexWindowsManagedLaunchNotStarted, err
	}
	install, err := ensureCodexWindowsManagedInstall(ctx, root, opts)
	if err != nil {
		if ctx.Err() != nil {
			return codexWindowsManagedLaunchNoFallback, ctx.Err()
		}
		return codexWindowsManagedLaunchNotStarted, err
	}
	exePath := filepath.Join(root, filepath.FromSlash(install.RuntimeRelative), "app", codexDesktopWindowsCurrentExecutable)
	if _, err := os.Stat(exePath); err != nil {
		if ctx.Err() != nil {
			return codexWindowsManagedLaunchNoFallback, ctx.Err()
		}
		return codexWindowsManagedLaunchNotStarted, fmt.Errorf("managed ChatGPT executable is unavailable: %w", err)
	}
	launchExe, err := codexWindowsManagedPathForLaunch(exePath)
	if err != nil {
		if ctx.Err() != nil {
			return codexWindowsManagedLaunchNoFallback, ctx.Err()
		}
		return codexWindowsManagedLaunchNotStarted, err
	}
	script := codexDesktopWindowsManagedLaunchScript(opts, launchExe)
	out, err := codexAppCommandOutput(ctx, teamsServicePowerShellExecutable(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	if err != nil {
		text := string(out)
		if strings.Contains(text, "CXP_MANAGED_CONFLICT") {
			return codexWindowsManagedLaunchNoFallback, fmt.Errorf("managed ChatGPT launch refused because ChatGPT or Codex is already running: %w", err)
		}
		if strings.Contains(text, "CXP_MANAGED_PID=") || strings.Contains(text, "CXP_MANAGED_STARTED_UNCERTAIN") {
			return codexWindowsManagedLaunchStartedUncertain, fmt.Errorf("managed ChatGPT launch may have started a process: %w", err)
		}
		if ctx.Err() != nil {
			return codexWindowsManagedLaunchNoFallback, ctx.Err()
		}
		return codexWindowsManagedLaunchNotStarted, err
	}
	if strings.Contains(string(out), "CXP_MANAGED_PID=") {
		return codexWindowsManagedLaunchReady, nil
	}
	return codexWindowsManagedLaunchStartedUncertain, errors.New("managed ChatGPT launcher did not report a process id")
}

func checkWindowsManagedProcessConflict(ctx context.Context) error {
	script := "$existing = @(Get-Process -Name 'ChatGPT','Codex' -ErrorAction SilentlyContinue); if ($existing.Count -gt 0) { Write-Output 'CXP_MANAGED_CONFLICT'; throw 'ChatGPT or Codex is already running; quit it before launching with cxp proxy or a model profile' }"
	out, err := codexAppCommandOutput(ctx, teamsServicePowerShellExecutable(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	if err != nil {
		if strings.Contains(string(out), "CXP_MANAGED_CONFLICT") {
			return fmt.Errorf("CXP_MANAGED_CONFLICT: ChatGPT or Codex is already running")
		}
		return fmt.Errorf("managed ChatGPT process preflight failed: %w", err)
	}
	return nil
}

func ensureCodexWindowsManagedInstall(ctx context.Context, root string, opts codexDesktopAppOptions) (codexWindowsManagedInstallState, error) {
	state, _, err := ensureCodexWindowsManagedInstallWithRefresh(ctx, root, opts, false)
	return state, err
}

// upgradeCodexWindowsManagedInstall fetches and verifies the current official
// MSIX even when current.json is valid. A refresh publishes a new immutable
// runtime directory and switches current.json only after extraction succeeds,
// so a running process can finish on the old runtime without being replaced.
func upgradeCodexWindowsManagedInstall(ctx context.Context, root string, opts codexDesktopAppOptions) (codexWindowsManagedInstallState, bool, error) {
	return ensureCodexWindowsManagedInstallWithRefresh(ctx, root, opts, true)
}

func ensureCodexWindowsManagedInstallWithRefresh(ctx context.Context, root string, opts codexDesktopAppOptions, refresh bool) (codexWindowsManagedInstallState, bool, error) {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "." || root == "" {
		return codexWindowsManagedInstallState{}, false, errors.New("managed Windows ChatGPT root is empty")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("create managed Windows ChatGPT root: %w", err)
	}
	currentState, currentOK, err := readValidCodexWindowsManagedState(root)
	if err != nil {
		return codexWindowsManagedInstallState{}, false, err
	} else if currentOK && !refresh {
		return currentState, false, nil
	}

	lockPath := filepath.Join(root, codexWindowsManagedLockDir)
	for {
		err := os.Mkdir(lockPath, 0o700)
		if err == nil {
			owner := codexWindowsManagedLockMetadata{PID: os.Getpid(), CreatedAt: codexWindowsManagedNow().UTC()}
			data, marshalErr := json.Marshal(owner)
			if marshalErr != nil {
				_ = os.RemoveAll(lockPath)
				return codexWindowsManagedInstallState{}, false, fmt.Errorf("encode managed ChatGPT install lock: %w", marshalErr)
			}
			if writeErr := os.WriteFile(filepath.Join(lockPath, codexWindowsManagedLockOwner), append(data, '\n'), 0o600); writeErr != nil {
				_ = os.RemoveAll(lockPath)
				return codexWindowsManagedInstallState{}, false, fmt.Errorf("write managed ChatGPT install lock: %w", writeErr)
			}
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return codexWindowsManagedInstallState{}, false, fmt.Errorf("acquire managed ChatGPT install lock: %w", err)
		}
		if reclaimed, reclaimErr := reclaimStaleCodexWindowsManagedLock(lockPath); reclaimErr != nil {
			return codexWindowsManagedInstallState{}, false, reclaimErr
		} else if reclaimed {
			continue
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return codexWindowsManagedInstallState{}, false, ctx.Err()
		case <-timer.C:
		}
	}
	defer func() {
		_ = os.Remove(filepath.Join(lockPath, codexWindowsManagedLockOwner))
		_ = os.Remove(lockPath)
	}()
	currentState, currentOK, err = readValidCodexWindowsManagedState(root)
	if err != nil {
		return codexWindowsManagedInstallState{}, false, err
	} else if currentOK && !refresh {
		return currentState, false, nil
	}

	installID := fmt.Sprintf("%d", codexWindowsManagedNow().UnixNano())
	stagingRoot := filepath.Join(root, codexWindowsManagedStagingDir, installID)
	packagePath := filepath.Join(root, codexWindowsManagedDownloadsDir, installID+".msix")
	if err := os.MkdirAll(stagingRoot, 0o700); err != nil {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("create managed ChatGPT staging directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(packagePath), 0o700); err != nil {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("create managed ChatGPT download directory: %w", err)
	}
	defer os.RemoveAll(stagingRoot)
	defer os.Remove(packagePath)

	if err := codexAppDownloadPackageFn(ctx, codexAppDownloadOptions{
		URL:      codexDesktopWindowsManagedDownloadURL,
		Path:     packagePath,
		ProxyURL: opts.ProxyURL,
		Log:      opts.Log,
	}); err != nil {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("download official ChatGPT MSIX: %w", err)
	}
	manifest, packageHash, err := verifyCodexWindowsManagedPackage(ctx, packagePath)
	if err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	if !strings.EqualFold(manifest.Architecture, codexWindowsManagedArch) {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("official ChatGPT MSIX architecture is %q, want %s", manifest.Architecture, codexWindowsManagedArch)
	}
	if !strings.EqualFold(manifest.PackageName, codexDesktopWindowsPackageName) {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("official ChatGPT MSIX package name is %q, want %s", manifest.PackageName, codexDesktopWindowsPackageName)
	}
	if refresh && currentOK && strings.EqualFold(currentState.PackageSHA256, packageHash) {
		return currentState, false, nil
	}
	if err := extractCodexWindowsManagedPackage(packagePath, stagingRoot); err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	exePath := filepath.Join(stagingRoot, "app", codexDesktopWindowsCurrentExecutable)
	exeHash, err := sha256File(exePath)
	if err != nil {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("hash extracted ChatGPT executable: %w", err)
	}
	version := strings.TrimSpace(manifest.PackageVersion)
	if version == "" {
		return codexWindowsManagedInstallState{}, false, errors.New("official ChatGPT MSIX has no package version")
	}
	versionDirBase := filepath.Join(root, codexWindowsManagedVersionsDir, version+"-"+packageHash[:16])
	versionDir := versionDirBase
	if refresh {
		// Never replace an existing runtime during an explicit refresh: it may
		// still be mapped by a running ChatGPT process. The content hash check
		// above avoids duplicate directories when the CDN has not changed.
		for attempt := 0; ; attempt++ {
			if attempt > 0 {
				versionDir = fmt.Sprintf("%s-%d", versionDirBase, attempt)
			}
			if _, err := os.Stat(versionDir); os.IsNotExist(err) {
				break
			} else if err != nil {
				return codexWindowsManagedInstallState{}, false, fmt.Errorf("inspect managed ChatGPT runtime destination: %w", err)
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(versionDir), 0o755); err != nil {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("create managed ChatGPT versions directory: %w", err)
	}
	if !refresh {
		if _, err := os.Stat(versionDir); err == nil {
			// A previous interrupted publish can leave the same immutable version
			// directory behind. Replace only this exact generated destination.
			if err := os.RemoveAll(versionDir); err != nil {
				return codexWindowsManagedInstallState{}, false, fmt.Errorf("replace stale managed ChatGPT runtime: %w", err)
			}
		} else if !os.IsNotExist(err) {
			return codexWindowsManagedInstallState{}, false, fmt.Errorf("inspect managed ChatGPT runtime destination: %w", err)
		}
	}
	if err := os.Rename(stagingRoot, versionDir); err != nil {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("publish managed ChatGPT runtime: %w", err)
	}
	state := codexWindowsManagedInstallState{
		PackageName:      manifest.PackageName,
		PackageVersion:   version,
		Architecture:     manifest.Architecture,
		Publisher:        manifest.Publisher,
		PackageSHA256:    packageHash,
		ExecutableSHA256: exeHash,
		RuntimeRelative:  filepath.ToSlash(filepath.Join(codexWindowsManagedVersionsDir, filepath.Base(versionDir))),
		InstalledAt:      time.Now().UTC().Format(time.RFC3339),
	}
	if err := writeCodexWindowsManagedState(root, state); err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	return state, true, nil
}

func readValidCodexWindowsManagedState(root string) (codexWindowsManagedInstallState, bool, error) {
	data, err := os.ReadFile(filepath.Join(root, codexWindowsManagedCurrentState))
	if os.IsNotExist(err) {
		return codexWindowsManagedInstallState{}, false, nil
	}
	if err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	var state codexWindowsManagedInstallState
	if err := json.Unmarshal(data, &state); err != nil {
		// current.json is an atomically published cache hint, not the source of
		// truth.  A torn/manual edit must trigger a fresh verified install rather
		// than turning a recoverable cache miss into a hard launch failure.
		return codexWindowsManagedInstallState{}, false, nil
	}
	relativeRuntime := filepath.ToSlash(strings.TrimSpace(state.RuntimeRelative))
	if state.PackageName != codexDesktopWindowsPackageName || strings.TrimSpace(state.PackageVersion) == "" || !strings.EqualFold(state.Architecture, codexWindowsManagedArch) || strings.TrimSpace(state.Publisher) == "" || !isCodexWindowsManagedSHA256(state.PackageSHA256) || !isCodexWindowsManagedSHA256(state.ExecutableSHA256) || relativeRuntime == "" || !strings.HasPrefix(relativeRuntime, codexWindowsManagedVersionsDir+"/") || filepath.IsAbs(filepath.FromSlash(relativeRuntime)) || strings.HasPrefix(relativeRuntime, "../") || strings.Contains(relativeRuntime, ":") {
		return codexWindowsManagedInstallState{}, false, nil
	}
	runtimePath := filepath.Join(root, filepath.FromSlash(state.RuntimeRelative))
	exePath := filepath.Join(runtimePath, "app", codexDesktopWindowsCurrentExecutable)
	info, err := os.Stat(exePath)
	if err != nil || info.IsDir() {
		return codexWindowsManagedInstallState{}, false, nil
	}
	actual, err := sha256File(exePath)
	if err != nil || !strings.EqualFold(actual, strings.TrimSpace(state.ExecutableSHA256)) {
		return codexWindowsManagedInstallState{}, false, nil
	}
	return state, true, nil
}

func isCodexWindowsManagedSHA256(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func writeCodexWindowsManagedState(root string, state codexWindowsManagedInstallState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(root, codexWindowsManagedCurrentState)
	if err := writeFileAtomically(path, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("publish managed ChatGPT state: %w", err)
	}
	return nil
}

func verifyCodexWindowsManagedPackage(ctx context.Context, packagePath string) (codexWindowsManagedManifest, string, error) {
	manifest, err := readCodexWindowsManagedManifest(packagePath)
	if err != nil {
		return codexWindowsManagedManifest{}, "", err
	}
	packageHash, err := sha256File(packagePath)
	if err != nil {
		return codexWindowsManagedManifest{}, "", fmt.Errorf("hash downloaded ChatGPT MSIX: %w", err)
	}
	if err := verifyCodexWindowsManagedAuthenticode(ctx, packagePath, manifest.Publisher); err != nil {
		return codexWindowsManagedManifest{}, "", err
	}
	return manifest, packageHash, nil
}

func readCodexWindowsManagedManifest(packagePath string) (codexWindowsManagedManifest, error) {
	archive, err := zip.OpenReader(packagePath)
	if err != nil {
		return codexWindowsManagedManifest{}, fmt.Errorf("open ChatGPT MSIX: %w", err)
	}
	defer archive.Close()
	for _, file := range archive.File {
		if !strings.EqualFold(strings.ReplaceAll(file.Name, "\\", "/"), "AppxManifest.xml") {
			continue
		}
		reader, err := file.Open()
		if err != nil {
			return codexWindowsManagedManifest{}, err
		}
		var manifest codexWindowsManagedManifest
		decoder := xml.NewDecoder(reader)
		for {
			token, tokenErr := decoder.Token()
			if tokenErr == io.EOF {
				break
			}
			if tokenErr != nil {
				_ = reader.Close()
				return codexWindowsManagedManifest{}, fmt.Errorf("parse ChatGPT AppxManifest.xml: %w", tokenErr)
			}
			start, ok := token.(xml.StartElement)
			if !ok {
				continue
			}
			if start.Name.Local == "Identity" {
				manifest.PackageName = xmlAttribute(start, "Name")
				manifest.PackageVersion = xmlAttribute(start, "Version")
				manifest.Architecture = xmlAttribute(start, "ProcessorArchitecture")
				manifest.Publisher = xmlAttribute(start, "Publisher")
			}
			if start.Name.Local == "Application" && manifest.Executable == "" {
				manifest.Executable = xmlAttribute(start, "Executable")
			}
		}
		_ = reader.Close()
		if manifest.PackageName == "" || manifest.PackageVersion == "" || manifest.Publisher == "" {
			return codexWindowsManagedManifest{}, errors.New("ChatGPT AppxManifest.xml is missing package identity")
		}
		if manifest.Executable == "" {
			manifest.Executable = "app/" + codexDesktopWindowsCurrentExecutable
		}
		return manifest, nil
	}
	return codexWindowsManagedManifest{}, errors.New("ChatGPT MSIX does not contain AppxManifest.xml")
}

func xmlAttribute(start xml.StartElement, name string) string {
	for _, attr := range start.Attr {
		if attr.Name.Local == name {
			return strings.TrimSpace(attr.Value)
		}
	}
	return ""
}

func verifyCodexWindowsManagedAuthenticode(ctx context.Context, packagePath, publisher string) error {
	launchPath, err := codexWindowsManagedPathForLaunch(packagePath)
	if err != nil {
		return err
	}
	script := "$signature = Get-AuthenticodeSignature -LiteralPath " + powershellSingleQuote(launchPath) + "; if ($signature.Status -ne 'Valid') { throw ('Authenticode status is ' + $signature.Status) }; if ($null -eq $signature.SignerCertificate) { throw 'Authenticode signer certificate is missing' }; Write-Output $signature.SignerCertificate.Subject"
	// Windows PowerShell 5.1 is the normal helper runtime, but some current
	// Windows runner images ship a broken Microsoft.PowerShell.Security module
	// there while PowerShell 7 can still load Get-AuthenticodeSignature. Try the
	// configured helper shell first and use pwsh.exe only when it is available;
	// this keeps older machines working without making signature verification
	// dependent on PowerShell 7.
	powershells := []string{teamsServicePowerShellExecutable()}
	if _, lookPathErr := codexAppLookPath("pwsh.exe"); lookPathErr == nil && !strings.EqualFold(filepath.Base(powershells[0]), "pwsh.exe") {
		powershells = append(powershells, "pwsh.exe")
	}
	var lastErr error
	for index, powershell := range powershells {
		out, commandErr := codexAppCommandOutput(ctx, powershell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
		if commandErr != nil {
			lastErr = commandErr
			if index+1 < len(powershells) && codexWindowsManagedAuthenticodeVerifierUnavailable(commandErr) {
				continue
			}
			return fmt.Errorf("verify ChatGPT MSIX Authenticode signature: %w", commandErr)
		}
		signer := strings.TrimSpace(string(out))
		if signer == "" || !strings.EqualFold(signer, strings.TrimSpace(publisher)) {
			return fmt.Errorf("ChatGPT MSIX signer %q does not match manifest publisher %q", signer, publisher)
		}
		return nil
	}
	return fmt.Errorf("verify ChatGPT MSIX Authenticode signature: %w", lastErr)
}

func codexWindowsManagedAuthenticodeVerifierUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, exec.ErrNotFound) {
		return true
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "couldnotautoloadmatchingmodule") {
		return true
	}
	if !strings.Contains(message, "get-authenticodesignature") {
		return false
	}
	return strings.Contains(message, "could not be loaded") ||
		strings.Contains(message, "commandnotfoundexception") ||
		strings.Contains(message, "not recognized")
}

func extractCodexWindowsManagedPackage(packagePath, destination string) error {
	archive, err := zip.OpenReader(packagePath)
	if err != nil {
		return fmt.Errorf("open ChatGPT MSIX for extraction: %w", err)
	}
	defer archive.Close()
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return err
	}
	for _, file := range archive.File {
		target, err := safeCodexWindowsManagedZipPath(destination, file.Name)
		if err != nil {
			return err
		}
		if err := rejectCodexWindowsManagedReparseParents(destination, target); err != nil {
			return err
		}
		if file.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("ChatGPT MSIX contains unsupported symlink entry %q", file.Name)
		}
		if file.FileInfo().IsDir() || strings.HasSuffix(strings.ReplaceAll(file.Name, "\\", "/"), "/") {
			if err := os.MkdirAll(target, 0o700); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		reader, err := file.Open()
		if err != nil {
			return err
		}
		output, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o700)
		var closeOutputErr error
		if err == nil {
			_, err = io.Copy(output, reader)
			closeOutputErr = output.Close()
		}
		closeReaderErr := reader.Close()
		if err != nil {
			return fmt.Errorf("extract ChatGPT MSIX entry %q: %w", file.Name, err)
		}
		if closeOutputErr != nil {
			return closeOutputErr
		}
		if closeReaderErr != nil {
			return closeReaderErr
		}
	}
	return nil
}

func rejectCodexWindowsManagedReparseParents(destination, target string) error {
	relative, err := filepath.Rel(destination, target)
	if err != nil {
		return err
	}
	current := filepath.Clean(destination)
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			// The remaining path will be created below; no existing reparse
			// point can be followed through it.
			return nil
		}
		if err != nil {
			return fmt.Errorf("inspect managed extraction path %q: %w", current, err)
		}
		reparse, err := codexWindowsManagedPathIsReparsePoint(current, info)
		if err != nil {
			return fmt.Errorf("inspect managed extraction reparse point %q: %w", current, err)
		}
		if reparse {
			return fmt.Errorf("ChatGPT MSIX extraction path uses a symlink or reparse point: %q", current)
		}
	}
	return nil
}

func safeCodexWindowsManagedZipPath(destination, name string) (string, error) {
	normalized := strings.ReplaceAll(name, "\\", "/")
	clean := path.Clean(normalized)
	if clean == "." || path.IsAbs(clean) || strings.HasPrefix(clean, "../") || strings.Contains(clean, ":") {
		return "", fmt.Errorf("ChatGPT MSIX contains unsafe path %q", name)
	}
	target := filepath.Join(destination, filepath.FromSlash(clean))
	baseAbs, err := filepath.Abs(destination)
	if err != nil {
		return "", err
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(baseAbs, targetAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("ChatGPT MSIX path escapes staging directory: %q", name)
	}
	return target, nil
}

func sha256File(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func codexWindowsManagedPathForLaunch(path string) (string, error) {
	if codexAppGOOS() == "linux" && codexAppIsWSL() {
		converted, err := codexAppWSLPathFn(path)
		if err != nil {
			return "", fmt.Errorf("convert managed Windows path for launch: %w", err)
		}
		return strings.TrimSpace(converted), nil
	}
	return path, nil
}

func ensureWindowsManagedProxyReachable(ctx context.Context, opts codexDesktopAppOptions) error {
	if strings.TrimSpace(opts.ProxyURL) == "" || !(codexAppGOOS() == "linux" && codexAppIsWSL()) {
		return nil
	}
	host, port, err := codexAppAuthProxyHostPort(opts.ProxyURL)
	if err != nil {
		return err
	}
	// A TCP connect alone is not sufficient: an unrelated listener on the
	// selected port would otherwise make WSL launch a GUI that silently bypasses
	// CXP. Probe the authenticated health JSON and a real CONNECT response from
	// the Windows side before installing or launching the managed runtime.
	script := codexAppAuthWindowsProxyReachabilityScript(host, port)
	if _, err := codexAppCommandOutput(ctx, teamsServicePowerShellExecutable(), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script); err != nil {
		return fmt.Errorf("check Windows reachability of CXP proxy %s:%s: %w", host, port, err)
	}
	return nil
}

func codexDesktopWindowsManagedLaunchScript(opts codexDesktopAppOptions, executable string) string {
	waitForExit := "$codexWaitForExit = $false"
	if opts.WaitForExit {
		waitForExit = "$codexWaitForExit = $true"
	}
	parts := []string{
		"$ErrorActionPreference = 'Stop'",
		codexDesktopWindowsRuntimeMarkerCleanupPowerShell(),
		codexDesktopWindowsEnvPowerShell(opts),
		codexDesktopWindowsManagedNoProxyCleanupPowerShell(opts),
		codexDesktopWindowsAppArgsPowerShell(opts),
		waitForExit,
		"$cwd = " + powershellSingleQuote(opts.Cwd),
		"$exe = " + powershellSingleQuote(executable),
		"$existing = @(Get-Process -Name 'ChatGPT','Codex' -ErrorAction SilentlyContinue)",
		"if ($existing.Count -gt 0) { Write-Output 'CXP_MANAGED_CONFLICT'; throw 'ChatGPT or Codex is already running; quit it before launching with cxp proxy or a model profile' }",
		"$start = @{ FilePath = $exe; WorkingDirectory = $cwd; PassThru = $true }",
		"if ($codexArgs.Count -gt 0) { $start.ArgumentList = $codexArgs }",
		"if ($codexWaitForExit) { $start.Wait = $true }",
		"$process = Start-Process @start",
		"$actualProcessPath = ''; try { $actualProcessPath = $process.MainModule.FileName } catch { }",
		"if ([string]::IsNullOrWhiteSpace($actualProcessPath) -or -not [String]::Equals([IO.Path]::GetFullPath($actualProcessPath), [IO.Path]::GetFullPath($exe), [StringComparison]::OrdinalIgnoreCase)) { Write-Output 'CXP_MANAGED_STARTED_UNCERTAIN'; throw ('managed ChatGPT process path mismatch: ' + $actualProcessPath) }",
		"Write-Output ('CXP_MANAGED_PID=' + $process.Id)",
		"Start-Sleep -Milliseconds 300",
		"$process.Refresh()",
		"if ($process.HasExited) { Write-Output 'CXP_MANAGED_STARTED_UNCERTAIN'; throw 'managed ChatGPT process exited immediately after launch' }",
	}
	filtered := parts[:0]
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			filtered = append(filtered, part)
		}
	}
	return strings.Join(filtered, "; ")
}

// NO_PROXY belongs to the parent shell and may contain a broad bypass list for
// unrelated workloads. A managed ChatGPT launch must route its own traffic
// through the selected CXP proxy, so clear both conventional spellings only in
// the child-launch PowerShell boundary; the host/global environment is never
// modified.
func codexDesktopWindowsManagedNoProxyCleanupPowerShell(opts codexDesktopAppOptions) string {
	if strings.TrimSpace(opts.ProxyURL) == "" {
		return ""
	}
	return "$env:NO_PROXY = ''; $env:no_proxy = ''"
}
