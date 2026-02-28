package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	codexPathCacheFile   = "codex_path"
	codexInstallLockName = "codex_install.lock"
	codexProbeTimeout    = 5 * time.Second
)

var (
	codexInstallLockPollDelay  = 200 * time.Millisecond
	codexInstallLockStaleAfter = 30 * time.Minute
	codexInstallLockMaxWait    = 30 * time.Second
)

type codexInstallOptions struct {
	installerEnv     []string
	withInstallerEnv func(context.Context, func([]string) error) error
}

type codexInstallCmd struct {
	path string
	args []string
}

const codexInstallBootstrap = `set -eu

min_major="${CODEX_NODE_MIN_MAJOR:-16}"
target_major="${CODEX_NODE_MAJOR:-22}"
home_dir="${HOME:-}"
if [ -z "$home_dir" ]; then
  home_dir="$(cd ~ 2>/dev/null && pwd || true)"
fi
if [ -z "$home_dir" ]; then
  home_dir="$(pwd)"
fi

parse_major() {
  raw="$1"
  raw="${raw#v}"
  raw="${raw%%.*}"
  case "$raw" in
    ''|*[!0-9]*) echo ""; return ;;
    *) echo "$raw"; return ;;
  esac
}

is_wsl=false
case "$(uname -r 2>/dev/null || true)" in
  *[Mm]icrosoft*|*WSL*) is_wsl=true ;;
esac

download_local_node() {
  os_name="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "$os_name" in
    linux|darwin) ;;
    *) echo "unsupported OS: $os_name" >&2; exit 1 ;;
  esac

  arch_raw="$(uname -m)"
  case "$arch_raw" in
    x86_64|amd64) node_arch="x64" ;;
    aarch64|arm64) node_arch="arm64" ;;
    *) echo "unsupported architecture: $arch_raw" >&2; exit 1 ;;
  esac

  node_root="${CODEX_NODE_INSTALL_ROOT:-$home_dir/.cache/codex-proxy/node}"
  install_dir="$node_root/v${target_major}-${os_name}-${node_arch}"
  node_path="$install_dir/bin/node"
  npm_path="$install_dir/bin/npm"
  installed_major=""
  if [ -x "$node_path" ]; then
    installed_major="$(parse_major "$("$node_path" -v 2>/dev/null || true)")"
  fi
  if [ -z "$installed_major" ] || [ "$installed_major" -lt "$min_major" ]; then
    tmp_dir="$(mktemp -d)"
    cleanup() {
      rm -rf "$tmp_dir"
    }
    trap cleanup EXIT INT TERM

    base_url="https://nodejs.org/dist/latest-v${target_major}.x"
    shasums_file="$tmp_dir/SHASUMS256.txt"
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL "$base_url/SHASUMS256.txt" -o "$shasums_file"
    elif command -v wget >/dev/null 2>&1; then
      wget -qO "$shasums_file" "$base_url/SHASUMS256.txt"
    else
      echo "need curl or wget to download Node.js" >&2
      exit 1
    fi

    tarball="$(
      awk -v major="$target_major" -v os="$os_name" -v arch="$node_arch" '
        $2 ~ ("^node-v" major "\\.[0-9]+\\.[0-9]+-" os "-" arch "\\.tar\\.xz$") {
          print $2
          exit
        }
      ' "$shasums_file"
    )"
    if [ -z "$tarball" ]; then
      echo "failed to resolve Node.js tarball for ${os_name}-${node_arch}" >&2
      exit 1
    fi

    expected_sha="$(awk -v target="$tarball" '$2 == target { print $1; exit }' "$shasums_file")"
    if [ -z "$expected_sha" ]; then
      echo "missing checksum for ${tarball}" >&2
      exit 1
    fi

    archive_path="$tmp_dir/$tarball"
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL "$base_url/$tarball" -o "$archive_path"
    else
      wget -qO "$archive_path" "$base_url/$tarball"
    fi

    if command -v sha256sum >/dev/null 2>&1; then
      actual_sha="$(sha256sum "$archive_path" | awk '{print $1}')"
    elif command -v shasum >/dev/null 2>&1; then
      actual_sha="$(shasum -a 256 "$archive_path" | awk '{print $1}')"
    else
      echo "missing checksum tool: need sha256sum or shasum" >&2
      exit 1
    fi
    if [ "$actual_sha" != "$expected_sha" ]; then
      echo "Node.js checksum mismatch for ${tarball}" >&2
      echo "expected=${expected_sha}" >&2
      echo "actual=${actual_sha}" >&2
      exit 1
    fi

    rm -rf "$install_dir"
    mkdir -p "$install_dir"
    tar -xJf "$archive_path" --strip-components=1 -C "$install_dir"
    rm -rf "$tmp_dir"
  fi

  if [ ! -x "$npm_path" ]; then
    echo "npm not found in local Node.js install: $npm_path" >&2
    exit 1
  fi
  node_bin_dir="$install_dir/bin"
  npm_cmd="$npm_path"
}

node_bin_dir=""
npm_cmd=""
used_system_node=false
if command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
  node_found="$(command -v node)"
  skip_system=false
  # On WSL, Windows binaries under /mnt/ are visible via PATH interop but
  # npm would install wrong platform-specific optional dependencies.
  if $is_wsl; then
    case "$node_found" in
      /mnt/*) skip_system=true ;;
    esac
  fi
  if ! $skip_system; then
    node_major="$(parse_major "$(node -v 2>/dev/null || true)")"
    if [ -n "$node_major" ] && [ "$node_major" -ge "$min_major" ]; then
      node_bin_dir="$(dirname "$node_found")"
      npm_cmd="$(command -v npm)"
      used_system_node=true
    fi
  fi
fi

if [ -z "$npm_cmd" ]; then
  download_local_node
fi

prefix="${CODEX_NPM_PREFIX:-$home_dir/.local/share/codex-proxy/npm-global}"
mkdir -p "$prefix"
PATH="$node_bin_dir:$prefix/bin:$PATH" "$npm_cmd" install -g --prefix "$prefix" @openai/codex

if ! "$prefix/bin/codex" --version >/dev/null 2>&1 && $used_system_node; then
  echo "codex installed with system node is not functional; retrying with local node..." >&2
  download_local_node
  PATH="$node_bin_dir:$prefix/bin:$PATH" "$npm_cmd" install -g --prefix "$prefix" @openai/codex
fi
`

const codexInstallBootstrapWindows = `$ErrorActionPreference = 'Stop'

$minMajorRaw = [Environment]::GetEnvironmentVariable('CODEX_NODE_MIN_MAJOR')
if ([string]::IsNullOrWhiteSpace($minMajorRaw)) { $minMajorRaw = '16' }
$targetMajorRaw = [Environment]::GetEnvironmentVariable('CODEX_NODE_MAJOR')
if ([string]::IsNullOrWhiteSpace($targetMajorRaw)) { $targetMajorRaw = '22' }
$minMajor = [int]$minMajorRaw
$targetMajor = [int]$targetMajorRaw

function Get-NodeMajor([string]$nodeExe) {
  try {
    $out = & $nodeExe -v 2>$null
    if (-not $out) { return -1 }
    $v = $out.Trim().TrimStart('v')
    $major = $v.Split('.')[0]
    return [int]$major
  } catch {
    return -1
  }
}

$baseDir = [Environment]::GetFolderPath('LocalApplicationData')
if ([string]::IsNullOrWhiteSpace($baseDir)) {
  $baseDir = [IO.Path]::GetTempPath()
}

$nodeRoot = [Environment]::GetEnvironmentVariable('CODEX_NODE_INSTALL_ROOT')
if ([string]::IsNullOrWhiteSpace($nodeRoot)) {
  $nodeRoot = Join-Path $baseDir 'codex-proxy\node'
}
$npmPrefix = [Environment]::GetEnvironmentVariable('CODEX_NPM_PREFIX')
if ([string]::IsNullOrWhiteSpace($npmPrefix)) {
  $npmPrefix = Join-Path $baseDir 'codex-proxy\npm-global'
}

function Install-LocalNode {
  $arch = 'x64'
  if ($env:PROCESSOR_ARCHITECTURE -match 'ARM64' -or $env:PROCESSOR_ARCHITEW6432 -match 'ARM64') {
    $arch = 'arm64'
  }
  $installDir = Join-Path $nodeRoot ("v{0}-win-{1}" -f $targetMajor, $arch)
  $nodeExe = Join-Path $installDir 'node.exe'

  if (-not (Test-Path $nodeExe) -or ((Get-NodeMajor $nodeExe) -lt $minMajor)) {
    $tmpDir = Join-Path ([IO.Path]::GetTempPath()) ("codex-node-" + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    try {
      $baseUrl = "https://nodejs.org/dist/latest-v$targetMajor.x"
      $shasumsPath = Join-Path $tmpDir 'SHASUMS256.txt'
      Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/SHASUMS256.txt" -OutFile $shasumsPath

      $pattern = " node-v$targetMajor\.\d+\.\d+-win-$arch\.zip$"
      $line = Get-Content $shasumsPath | Where-Object { $_ -match $pattern } | Select-Object -First 1
      if ([string]::IsNullOrWhiteSpace($line)) {
        throw "failed to resolve Node.js zip for win-$arch"
      }
      $parts = $line -split '\s+', 3
      $expected = $parts[0].ToLower()
      $zipName = $parts[1]
      $zipPath = Join-Path $tmpDir $zipName
      Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/$zipName" -OutFile $zipPath
      $actual = (Get-FileHash -Algorithm SHA256 -Path $zipPath).Hash.ToLower()
      if ($actual -ne $expected) {
        throw "Node.js checksum mismatch for $zipName"
      }

      $extractRoot = Join-Path $tmpDir 'extract'
      Expand-Archive -Path $zipPath -DestinationPath $extractRoot -Force
      $expanded = Get-ChildItem -Path $extractRoot -Directory | Select-Object -First 1
      if (-not $expanded) {
        throw "failed to extract Node.js archive"
      }
      if (Test-Path $installDir) {
        Remove-Item -Recurse -Force $installDir
      }
      New-Item -ItemType Directory -Force -Path (Split-Path -Parent $installDir) | Out-Null
      Move-Item -Path $expanded.FullName -Destination $installDir -Force
    } finally {
      Remove-Item -Recurse -Force $tmpDir -ErrorAction SilentlyContinue
    }
  }

  $script:nodeDir = $installDir
  $script:npmCmd = Join-Path $installDir 'npm.cmd'
}

$nodeDir = $null
$npmCmd = $null
$usedSystemNode = $false
$systemNode = Get-Command node -ErrorAction SilentlyContinue
$systemNpm = Get-Command npm -ErrorAction SilentlyContinue
if ($systemNode -and $systemNpm) {
  $major = Get-NodeMajor $systemNode.Source
  if ($major -ge $minMajor) {
    $nodeDir = Split-Path -Parent $systemNode.Source
    $npmCmd = $systemNpm.Source
    $usedSystemNode = $true
  }
}

if (-not $npmCmd) {
  Install-LocalNode
}

if (-not (Test-Path $npmCmd)) {
  throw "npm is not available for Codex install"
}

New-Item -ItemType Directory -Force -Path $npmPrefix | Out-Null
$prefixBin = Join-Path $npmPrefix 'bin'
$env:PATH = "$nodeDir;$npmPrefix;$prefixBin;$env:PATH"
& $npmCmd install -g --prefix $npmPrefix @openai/codex
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

$codexCmd = Join-Path $npmPrefix 'codex.cmd'
$probeOk = $false
try {
  & $codexCmd --version 2>$null | Out-Null
  if ($LASTEXITCODE -eq 0) { $probeOk = $true }
} catch {}
if (-not $probeOk -and $usedSystemNode) {
  Write-Host "codex installed with system node is not functional; retrying with local node..." -ForegroundColor Yellow
  Install-LocalNode
  if (-not (Test-Path $npmCmd)) {
    throw "npm is not available for Codex install"
  }
  $env:PATH = "$nodeDir;$npmPrefix;$prefixBin;$env:PATH"
  & $npmCmd install -g --prefix $npmPrefix @openai/codex
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }
}
`

// probeCodex runs a quick smoke test to verify the codex binary is functional.
// Returns true if `codex --version` exits 0 within 5 seconds.
func probeCodex(ctx context.Context, codexPath string) bool {
	ctx, cancel := context.WithTimeout(ctx, codexProbeTimeout)
	defer cancel()
	return exec.CommandContext(ctx, codexPath, "--version").Run() == nil
}

func ensureCodexInstalled(ctx context.Context, codexPath string, out io.Writer) (string, error) {
	return ensureCodexInstalledWithOptions(ctx, codexPath, out, codexInstallOptions{})
}

func ensureCodexInstalledWithOptions(ctx context.Context, codexPath string, out io.Writer, opts codexInstallOptions) (string, error) {
	ensureManagedNodeOnPath()

	if strings.TrimSpace(codexPath) != "" {
		resolvedPath := normalizeExecutablePath(codexPath)
		if executableExists(resolvedPath) && probeCodex(ctx, resolvedPath) {
			writeCachedCodexPath(resolvedPath)
			return resolvedPath, nil
		}
		if executableExists(resolvedPath) {
			return "", fmt.Errorf("codex at %s is not functional", resolvedPath)
		}
		return "", fmt.Errorf("codex not found at %s", codexPath)
	}

	if path, err := exec.LookPath("codex"); err == nil {
		path = normalizeExecutablePath(path)
		if probeCodex(ctx, path) {
			writeCachedCodexPath(path)
			return path, nil
		}
		if out != nil {
			_, _ = fmt.Fprintf(out, "codex at %s is not functional; installing a local copy...\n", path)
		}
	}

	if cached := strings.TrimSpace(readCachedCodexPath()); cached != "" {
		if !filepath.IsAbs(cached) {
			clearCachedCodexPath()
		} else if executableExists(cached) && probeCodex(ctx, cached) {
			writeCachedCodexPath(cached)
			return cached, nil
		} else {
			clearCachedCodexPath()
		}
	}

	if path, err := findInstalledCodexInCandidates(ctx); err == nil {
		writeCachedCodexPath(path)
		return path, nil
	}

	if out != nil {
		_, _ = fmt.Fprintln(out, "codex not found; installing...")
	}

	var installedPath string
	if err := withCodexInstallLock(ctx, out, func() error {
		// Another process may have installed Codex while we waited for the lock.
		if path, err := findInstalledCodex(ctx); err == nil {
			installedPath = path
			return nil
		}

		runInstall := func(installerEnv []string) error {
			return runCodexInstaller(ctx, out, installerEnv)
		}
		if opts.withInstallerEnv != nil {
			if err := opts.withInstallerEnv(ctx, runInstall); err != nil {
				return err
			}
		} else {
			if err := runInstall(opts.installerEnv); err != nil {
				return err
			}
		}

		path, err := findInstalledCodex(ctx)
		if err != nil {
			return fmt.Errorf("codex installation finished but binary not found in PATH")
		}
		installedPath = path
		return nil
	}); err != nil {
		return "", err
	}

	if installedPath != "" {
		writeCachedCodexPath(installedPath)
		return installedPath, nil
	}
	return "", fmt.Errorf("codex installation finished but binary not found in PATH")
}

func withCodexInstallLock(ctx context.Context, out io.Writer, fn func() error) error {
	lockPath := codexInstallLockPath()
	if lockPath == "" {
		return fn()
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return fmt.Errorf("create codex install lock dir: %w", err)
	}
	waitStart := time.Time{}
	for {
		err := os.Mkdir(lockPath, 0o700)
		if err == nil {
			defer func() { _ = os.Remove(lockPath) }()
			return fn()
		}
		if !codexInstallLockIsContended(lockPath, err) {
			return fmt.Errorf("acquire codex install lock: %w", err)
		}
		stale, staleErr := codexInstallLockStale(lockPath)
		if staleErr == nil && stale {
			if rmErr := os.Remove(lockPath); rmErr == nil || os.IsNotExist(rmErr) {
				continue
			}
		}
		if waitStart.IsZero() {
			waitStart = time.Now()
			if out != nil {
				_, _ = fmt.Fprintf(out, "codex installation lock is held by another process; waiting up to %s...\n", codexInstallLockMaxWait)
			}
		}
		if time.Since(waitStart) >= codexInstallLockMaxWait {
			if out != nil {
				_, _ = fmt.Fprintf(out, "codex installation lock still held after %s; continuing without lock.\n", codexInstallLockMaxWait)
			}
			return fn()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(codexInstallLockPollDelay):
		}
	}
}

func codexInstallLockIsContended(lockPath string, err error) bool {
	if err == nil {
		return false
	}
	if os.IsExist(err) || os.IsPermission(err) {
		return true
	}
	_, statErr := os.Stat(lockPath)
	return statErr == nil
}

func codexInstallLockStale(lockPath string) (bool, error) {
	info, err := os.Stat(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return time.Since(info.ModTime()) > codexInstallLockStaleAfter, nil
}

func codexInstallLockPath() string {
	cacheFile := cachedCodexPathFile()
	if cacheFile != "" {
		return filepath.Join(filepath.Dir(cacheFile), codexInstallLockName)
	}
	if tmp := strings.TrimSpace(os.TempDir()); tmp != "" {
		return filepath.Join(tmp, "codex-proxy", codexInstallLockName)
	}
	return ""
}

func normalizeExecutablePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if !filepath.IsAbs(path) {
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
	}
	return filepath.Clean(path)
}

func cachedCodexPathFile() string {
	base, err := os.UserCacheDir()
	if err != nil || strings.TrimSpace(base) == "" {
		home := preferredHomeDir()
		if home == "" {
			return ""
		}
		base = filepath.Join(home, ".cache")
	}
	return filepath.Join(base, "codex-proxy", codexPathCacheFile)
}

func readCachedCodexPath() string {
	cacheFile := cachedCodexPathFile()
	if cacheFile == "" {
		return ""
	}
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func writeCachedCodexPath(path string) {
	path = normalizeExecutablePath(path)
	if path == "" || !filepath.IsAbs(path) {
		return
	}
	cacheFile := cachedCodexPathFile()
	if cacheFile == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(cacheFile), 0o755); err != nil {
		return
	}
	_ = os.WriteFile(cacheFile, []byte(path+"\n"), 0o600)
}

func clearCachedCodexPath() {
	cacheFile := cachedCodexPathFile()
	if cacheFile == "" {
		return
	}
	_ = os.Remove(cacheFile)
}

func runCodexInstaller(ctx context.Context, out io.Writer, installerEnv []string) error {
	candidates := codexInstallerCandidates(runtime.GOOS)
	if len(candidates) == 0 {
		return fmt.Errorf("no supported installer available for %s", runtime.GOOS)
	}

	attemptErrors := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, err := exec.LookPath(candidate.path); err != nil {
			attemptErrors = append(attemptErrors, fmt.Sprintf("%s: not found in PATH", installerAttemptLabel(candidate)))
			continue
		}

		cmd := exec.CommandContext(ctx, candidate.path, candidate.args...)
		if len(installerEnv) > 0 {
			cmd.Env = installerEnv
		}
		cmd.Stdout = out
		cmd.Stderr = out
		cmd.Stdin = os.Stdin
		if err := cmd.Run(); err != nil {
			attemptErrors = append(attemptErrors, fmt.Sprintf("%s: %v", installerAttemptLabel(candidate), err))
			continue
		}
		return nil
	}

	if len(attemptErrors) == 0 {
		return fmt.Errorf("no supported installer available for %s", runtime.GOOS)
	}
	return fmt.Errorf("failed to install codex CLI for %s (%s)", runtime.GOOS, strings.Join(attemptErrors, "; "))
}

func codexInstallerCandidates(goos string) []codexInstallCmd {
	switch strings.ToLower(goos) {
	case "windows":
		return []codexInstallCmd{
			{path: "powershell", args: []string{"-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", codexInstallBootstrapWindows}},
			{path: "pwsh", args: []string{"-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", codexInstallBootstrapWindows}},
		}
	case "darwin", "linux":
		return []codexInstallCmd{
			{path: "bash", args: []string{"-c", codexInstallBootstrap}},
			{path: "sh", args: []string{"-c", codexInstallBootstrap}},
		}
	default:
		return nil
	}
}

func installerAttemptLabel(cmd codexInstallCmd) string {
	if len(cmd.args) == 0 {
		return cmd.path
	}
	return fmt.Sprintf("%s %s", cmd.path, cmd.args[0])
}

func findInstalledCodex(ctx context.Context) (string, error) {
	ensureManagedNodeOnPath()

	if path, err := exec.LookPath("codex"); err == nil {
		path = normalizeExecutablePath(path)
		if probeCodex(ctx, path) {
			return path, nil
		}
	}
	return findInstalledCodexInCandidates(ctx)
}

func findInstalledCodexInCandidates(ctx context.Context) (string, error) {
	for _, candidate := range codexBinaryCandidates() {
		if executableExists(candidate) && probeCodex(ctx, candidate) {
			return normalizeExecutablePath(candidate), nil
		}
	}
	return "", fmt.Errorf("codex binary not found")
}

func codexBinaryCandidates() []string {
	return codexBinaryCandidatesForEnv(
		runtime.GOOS,
		preferredHomeDir(),
		os.Getenv("CODEX_NPM_PREFIX"),
		os.Getenv("LOCALAPPDATA"),
		os.Getenv("APPDATA"),
		os.TempDir(),
	)
}

func ensureManagedNodeOnPath() {
	candidates := managedNodeBinCandidates()
	if len(candidates) == 0 {
		return
	}

	currentPath := os.Getenv("PATH")
	pathParts := filepath.SplitList(currentPath)
	seen := make(map[string]struct{}, len(pathParts)+len(candidates))
	normalize := func(path string) string {
		path = filepath.Clean(strings.TrimSpace(path))
		if runtime.GOOS == "windows" {
			path = strings.ToLower(path)
		}
		return path
	}
	for _, part := range pathParts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			seen[normalize(trimmed)] = struct{}{}
		}
	}

	prepend := make([]string, 0, len(candidates))
	for _, dir := range candidates {
		normalized := normalize(dir)
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		prepend = append(prepend, filepath.Clean(dir))
	}
	if len(prepend) == 0 {
		return
	}

	updated := append(prepend, pathParts...)
	_ = os.Setenv("PATH", strings.Join(updated, string(os.PathListSeparator)))
}

func managedNodeBinCandidates() []string {
	return managedNodeBinCandidatesForEnv(
		runtime.GOOS,
		runtime.GOARCH,
		preferredHomeDir(),
		os.Getenv("CODEX_NODE_INSTALL_ROOT"),
		os.Getenv("CODEX_NODE_MAJOR"),
		os.Getenv("LOCALAPPDATA"),
		os.TempDir(),
	)
}

func managedNodeBinCandidatesForEnv(goos, goarch, home, nodeRoot, nodeMajor, localAppData, tempDir string) []string {
	candidates := make([]string, 0, 8)
	seen := map[string]struct{}{}
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		path = filepath.Clean(path)
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		candidates = append(candidates, path)
	}

	nodeRoot = strings.TrimSpace(nodeRoot)
	isWindows := strings.EqualFold(goos, "windows")
	if nodeRoot == "" {
		if isWindows {
			baseDir := strings.TrimSpace(localAppData)
			if baseDir == "" {
				baseDir = strings.TrimSpace(tempDir)
			}
			if baseDir != "" {
				nodeRoot = filepath.Join(baseDir, "codex-proxy", "node")
			}
		} else {
			if home = strings.TrimSpace(home); home != "" {
				nodeRoot = filepath.Join(home, ".cache", "codex-proxy", "node")
			}
		}
	}
	if nodeRoot == "" {
		return candidates
	}

	major := strings.TrimPrefix(strings.TrimSpace(nodeMajor), "v")
	if major == "" {
		major = "22"
	}
	for _, ch := range major {
		if ch < '0' || ch > '9' {
			major = "22"
			break
		}
	}

	arch := nodeRuntimeArch(goarch)
	if isWindows {
		nodeFile := "node.exe"
		addInstallDir := func(installDir string) {
			installDir = strings.TrimSpace(installDir)
			if installDir == "" {
				return
			}
			if executableExists(filepath.Join(installDir, nodeFile)) {
				add(installDir)
			}
		}

		if arch != "" {
			addInstallDir(filepath.Join(nodeRoot, fmt.Sprintf("v%s-win-%s", major, arch)))
		}
		pattern := filepath.Join(nodeRoot, "v*-win-*")
		if arch != "" {
			pattern = filepath.Join(nodeRoot, fmt.Sprintf("v*-win-%s", arch))
		}
		if matches, err := filepath.Glob(pattern); err == nil {
			for _, match := range matches {
				addInstallDir(match)
			}
		}
		return candidates
	}

	osName := strings.ToLower(strings.TrimSpace(goos))
	if osName != "linux" && osName != "darwin" {
		return candidates
	}
	addInstallDir := func(installDir string) {
		installDir = strings.TrimSpace(installDir)
		if installDir == "" {
			return
		}
		binDir := filepath.Join(installDir, "bin")
		if executableExists(filepath.Join(binDir, "node")) {
			add(binDir)
		}
	}

	if arch != "" {
		addInstallDir(filepath.Join(nodeRoot, fmt.Sprintf("v%s-%s-%s", major, osName, arch)))
	}
	pattern := filepath.Join(nodeRoot, fmt.Sprintf("v*-%s-*", osName))
	if arch != "" {
		pattern = filepath.Join(nodeRoot, fmt.Sprintf("v*-%s-%s", osName, arch))
	}
	if matches, err := filepath.Glob(pattern); err == nil {
		for _, match := range matches {
			addInstallDir(match)
		}
	}
	return candidates
}

func nodeRuntimeArch(goarch string) string {
	switch strings.ToLower(strings.TrimSpace(goarch)) {
	case "amd64", "x86_64":
		return "x64"
	case "arm64", "aarch64":
		return "arm64"
	default:
		return ""
	}
}

func codexBinaryCandidatesForEnv(goos, home, npmPrefix, localAppData, appData, tempDir string) []string {
	candidates := make([]string, 0, 16)
	seen := map[string]struct{}{}
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		path = filepath.Clean(path)
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		candidates = append(candidates, path)
	}

	isWindows := strings.EqualFold(goos, "windows")

	if prefix := strings.TrimSpace(npmPrefix); prefix != "" {
		if isWindows {
			add(filepath.Join(prefix, "codex.cmd"))
			add(filepath.Join(prefix, "codex.exe"))
			add(filepath.Join(prefix, "bin", "codex.cmd"))
			add(filepath.Join(prefix, "bin", "codex.exe"))
		} else {
			add(filepath.Join(prefix, "bin", "codex"))
			add(filepath.Join(prefix, "codex"))
		}
	}

	if home = strings.TrimSpace(home); home != "" {
		add(filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex"))
		add(filepath.Join(home, ".npm-global", "bin", "codex"))
		add(filepath.Join(home, ".local", "bin", "codex"))
	}

	if isWindows {
		localAppData = strings.TrimSpace(localAppData)
		if localAppData == "" {
			localAppData = strings.TrimSpace(tempDir)
		}
		if localAppData != "" {
			add(filepath.Join(localAppData, "codex-proxy", "npm-global", "codex.cmd"))
			add(filepath.Join(localAppData, "codex-proxy", "npm-global", "bin", "codex.cmd"))
			add(filepath.Join(localAppData, "codex-proxy", "npm-global", "codex.exe"))
		}
		appData = strings.TrimSpace(appData)
		if appData != "" {
			add(filepath.Join(appData, "npm", "codex.cmd"))
			add(filepath.Join(appData, "npm", "codex.exe"))
		}
	}

	return candidates
}

func preferredHomeDir() string {
	if home := strings.TrimSpace(os.Getenv("HOME")); home != "" {
		return filepath.Clean(home)
	}
	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		return filepath.Clean(home)
	}
	if cwd, err := os.Getwd(); err == nil && strings.TrimSpace(cwd) != "" {
		return filepath.Clean(cwd)
	}
	return ""
}

func isCodexCommand(command string) bool {
	command = strings.TrimSpace(command)
	if command == "" {
		return false
	}
	base := strings.ToLower(filepath.Base(command))
	switch base {
	case "codex", "codex.exe", "codex.cmd", "codex.bat":
		return true
	default:
		return false
	}
}

func hasPathSeparator(command string) bool {
	return strings.Contains(command, "/") || strings.Contains(command, "\\")
}

func resolveRunCommand(ctx context.Context, cmdArgs []string, out io.Writer) ([]string, error) {
	return resolveRunCommandWithInstallOptions(ctx, cmdArgs, out, codexInstallOptions{})
}

func resolveRunCommandWithInstallOptions(ctx context.Context, cmdArgs []string, out io.Writer, installOpts codexInstallOptions) ([]string, error) {
	if len(cmdArgs) == 0 {
		return cmdArgs, nil
	}
	cmd := strings.TrimSpace(cmdArgs[0])
	if !isCodexCommand(cmd) {
		return cmdArgs, nil
	}

	var resolved string
	var err error
	if filepath.IsAbs(cmd) || hasPathSeparator(cmd) {
		resolved, err = ensureCodexInstalledWithOptions(ctx, cmd, out, installOpts)
	} else {
		if path, lookErr := exec.LookPath(cmd); lookErr == nil {
			path = normalizeExecutablePath(path)
			if probeCodex(ctx, path) {
				return cmdArgs, nil
			}
		}
		resolved, err = ensureCodexInstalledWithOptions(ctx, "", out, installOpts)
	}
	if err != nil {
		return nil, err
	}

	updated := make([]string, len(cmdArgs))
	copy(updated, cmdArgs)
	updated[0] = resolved
	return updated, nil
}

func executableExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	path = filepath.Clean(path)
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return true
}
