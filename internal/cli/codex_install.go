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
)

type codexInstallCmd struct {
	path string
	args []string
}

const codexInstallBootstrap = `set -eu

min_major="${CODEX_NODE_MIN_MAJOR:-16}"
target_major="${CODEX_NODE_MAJOR:-22}"
home_dir="${HOME:-}"
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

node_bin_dir=""
npm_cmd=""
if command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
  node_major="$(parse_major "$(node -v 2>/dev/null || true)")"
  if [ -n "$node_major" ] && [ "$node_major" -ge "$min_major" ]; then
    node_bin_dir="$(dirname "$(command -v node)")"
    npm_cmd="$(command -v npm)"
  fi
fi

if [ -z "$npm_cmd" ]; then
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
  fi

  if [ ! -x "$npm_path" ]; then
    echo "npm not found in local Node.js install: $npm_path" >&2
    exit 1
  fi
  node_bin_dir="$install_dir/bin"
  npm_cmd="$npm_path"
fi

prefix="${CODEX_NPM_PREFIX:-$home_dir/.local/share/codex-proxy/npm-global}"
mkdir -p "$prefix"
PATH="$node_bin_dir:$prefix/bin:$PATH" "$npm_cmd" install -g --prefix "$prefix" @openai/codex
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

$nodeDir = $null
$npmCmd = $null
$systemNode = Get-Command node -ErrorAction SilentlyContinue
$systemNpm = Get-Command npm -ErrorAction SilentlyContinue
if ($systemNode -and $systemNpm) {
  $major = Get-NodeMajor $systemNode.Source
  if ($major -ge $minMajor) {
    $nodeDir = Split-Path -Parent $systemNode.Source
    $npmCmd = $systemNpm.Source
  }
}

if (-not $npmCmd) {
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

  $nodeDir = $installDir
  $npmCmd = Join-Path $nodeDir 'npm.cmd'
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
`

func ensureCodexInstalled(ctx context.Context, codexPath string, out io.Writer) (string, error) {
	if strings.TrimSpace(codexPath) != "" {
		if executableExists(codexPath) {
			return codexPath, nil
		}
		return "", fmt.Errorf("codex not found at %s", codexPath)
	}

	if path, err := exec.LookPath("codex"); err == nil {
		return path, nil
	}

	if out != nil {
		_, _ = fmt.Fprintln(out, "codex not found; installing...")
	}
	if err := runCodexInstaller(ctx, out); err != nil {
		return "", err
	}

	if path, err := findInstalledCodex(); err == nil {
		return path, nil
	}
	return "", fmt.Errorf("codex installation finished but binary not found in PATH")
}

func runCodexInstaller(ctx context.Context, out io.Writer) error {
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

func findInstalledCodex() (string, error) {
	if path, err := exec.LookPath("codex"); err == nil {
		return path, nil
	}
	for _, candidate := range codexBinaryCandidates() {
		if executableExists(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("codex binary not found")
}

func codexBinaryCandidates() []string {
	candidates := make([]string, 0, 8)

	home, _ := os.UserHomeDir()
	if home != "" {
		candidates = append(candidates,
			filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex"),
			filepath.Join(home, ".npm-global", "bin", "codex"),
			filepath.Join(home, ".local", "bin", "codex"),
		)
	}

	if runtime.GOOS == "windows" {
		localAppData := os.Getenv("LOCALAPPDATA")
		if localAppData != "" {
			candidates = append(candidates,
				filepath.Join(localAppData, "codex-proxy", "npm-global", "codex.cmd"),
				filepath.Join(localAppData, "codex-proxy", "npm-global", "bin", "codex.cmd"),
				filepath.Join(localAppData, "codex-proxy", "npm-global", "codex.exe"),
			)
		}
		appData := os.Getenv("APPDATA")
		if appData != "" {
			candidates = append(candidates,
				filepath.Join(appData, "npm", "codex.cmd"),
				filepath.Join(appData, "npm", "codex.exe"),
			)
		}
	}

	return candidates
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
		resolved, err = ensureCodexInstalled(ctx, cmd, out)
	} else {
		if _, lookErr := exec.LookPath(cmd); lookErr == nil {
			return cmdArgs, nil
		}
		resolved, err = ensureCodexInstalled(ctx, "", out)
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
