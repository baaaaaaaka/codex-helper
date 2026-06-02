package cli

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

var (
	codexNativeHTTPClient      = http.DefaultClient
	codexNativeNodeDistBaseURL = "https://nodejs.org/dist"
	codexCmdJSRelPattern       = regexp.MustCompile(`(?i)(?:%~?dp0%[\\/]|%~dp0)([^"\r\n]+\.js)`)
)

type nativeWindowsCodexInstallConfig struct {
	minMajor    int
	targetMajor int
	arch        string
	nodeRoot    string
	npmPrefix   string
	npmCacheDir string
	tempDir     string
}

func runNativeWindowsCodexInstaller(ctx context.Context, out io.Writer, installerEnv []string) error {
	if runtime.GOOS != "windows" {
		return fmt.Errorf("native Windows installer is not available on %s", runtime.GOOS)
	}
	cfg, err := nativeWindowsCodexConfig(installerEnv)
	if err != nil {
		return err
	}

	nodeDir, npmCmd, usedSystemNode, usingManagedNode, err := nativeWindowsResolveNPM(ctx, out, cfg, installerEnv)
	if err != nil {
		return err
	}
	if err := nativeWindowsInstallCodexPackage(ctx, out, cfg, installerEnv, nodeDir, npmCmd); err != nil {
		return err
	}
	if usingManagedNode {
		if err := writeWindowsManagedCodexShims(cfg.npmPrefix, nodeDir); err != nil {
			return err
		}
	}

	codexCmd := filepath.Join(cfg.npmPrefix, "codex.cmd")
	probeErr := probeCodexVersionWithTimeout(ctx, codexCmd, codexInstallProbeTimeout)
	if probeErr == nil {
		return nil
	}
	if usedSystemNode {
		if out != nil {
			_, _ = fmt.Fprintln(out, "codex installed with system node is not functional; retrying with local node...")
		}
		nodeDir, npmCmd, err = nativeWindowsInstallManagedNode(ctx, out, cfg, installerEnv)
		if err != nil {
			return err
		}
		if err := nativeWindowsInstallCodexPackage(ctx, out, cfg, installerEnv, nodeDir, npmCmd); err != nil {
			return err
		}
		if err := writeWindowsManagedCodexShims(cfg.npmPrefix, nodeDir); err != nil {
			return err
		}
		probeErr = probeCodexVersionWithTimeout(ctx, codexCmd, codexInstallProbeTimeout)
		if probeErr == nil {
			return nil
		}
	}

	reason := fmt.Sprintf("codex installation finished but %s is not functional (%v)", codexCmd, probeErr)
	writeCodexInstallFailureBanner(out, reason)
	return fmt.Errorf("%s", reason)
}

func nativeWindowsCodexConfig(installerEnv []string) (nativeWindowsCodexInstallConfig, error) {
	arch := nodeRuntimeArch(runtime.GOARCH)
	if arch == "" {
		return nativeWindowsCodexInstallConfig{}, fmt.Errorf("unsupported Windows architecture for managed Node.js: %s", runtime.GOARCH)
	}
	baseDir := strings.TrimSpace(envValue(installerEnv, "LOCALAPPDATA"))
	if baseDir == "" {
		baseDir = strings.TrimSpace(envTempDir(installerEnv))
	}
	if baseDir == "" {
		return nativeWindowsCodexInstallConfig{}, fmt.Errorf("cannot resolve LOCALAPPDATA or temporary directory for managed Codex install")
	}

	nodeRoot := strings.TrimSpace(envValue(installerEnv, "CODEX_NODE_INSTALL_ROOT"))
	if nodeRoot == "" {
		nodeRoot = filepath.Join(baseDir, "codex-proxy", "node")
	}
	npmPrefix := strings.TrimSpace(envValue(installerEnv, "CODEX_NPM_PREFIX"))
	if npmPrefix == "" {
		npmPrefix = filepath.Join(baseDir, "codex-proxy", "npm-global")
	}
	npmCacheDir := strings.TrimSpace(envValue(installerEnv, "npm_config_cache"))
	if npmCacheDir == "" {
		npmCacheDir = strings.TrimSpace(envValue(installerEnv, "NPM_CONFIG_CACHE"))
	}
	if npmCacheDir == "" {
		npmCacheDir = filepath.Join(baseDir, "npm-cache")
	}

	return nativeWindowsCodexInstallConfig{
		minMajor:    positiveIntEnv(installerEnv, "CODEX_NODE_MIN_MAJOR", 16),
		targetMajor: positiveIntEnv(installerEnv, "CODEX_NODE_MAJOR", 22),
		arch:        arch,
		nodeRoot:    nodeRoot,
		npmPrefix:   npmPrefix,
		npmCacheDir: npmCacheDir,
		tempDir:     envTempDir(installerEnv),
	}, nil
}

func positiveIntEnv(installerEnv []string, key string, fallback int) int {
	raw := strings.TrimSpace(envValue(installerEnv, key))
	if raw == "" {
		return fallback
	}
	raw = strings.TrimPrefix(raw, "v")
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func nativeWindowsResolveNPM(ctx context.Context, out io.Writer, cfg nativeWindowsCodexInstallConfig, installerEnv []string) (nodeDir, npmCmd string, usedSystemNode, usingManagedNode bool, err error) {
	systemNode, nodeErr := exec.LookPath("node")
	systemNpm, npmErr := exec.LookPath("npm")
	if nodeErr == nil && npmErr == nil {
		systemNodeDir := filepath.Dir(systemNode)
		if nativeWindowsNodeMajor(ctx, systemNode, systemNodeDir, installerEnv) >= cfg.minMajor &&
			nativeWindowsNPMUsable(ctx, systemNpm, systemNodeDir, installerEnv) {
			return systemNodeDir, systemNpm, true, false, nil
		}
		if out != nil {
			_, _ = fmt.Fprintln(out, "system Node.js/npm is missing or not usable; installing managed Node.js/npm...")
		}
	}

	nodeDir, npmCmd, err = nativeWindowsInstallManagedNode(ctx, out, cfg, installerEnv)
	return nodeDir, npmCmd, false, true, err
}

func nativeWindowsInstallManagedNode(ctx context.Context, out io.Writer, cfg nativeWindowsCodexInstallConfig, installerEnv []string) (string, string, error) {
	installDir := filepath.Join(cfg.nodeRoot, fmt.Sprintf("v%d-win-%s", cfg.targetMajor, cfg.arch))
	nodeExe := filepath.Join(installDir, "node.exe")
	npmCmd := filepath.Join(installDir, "npm.cmd")
	if executableExists(nodeExe) && nativeWindowsNodeMajor(ctx, nodeExe, installDir, installerEnv) >= cfg.minMajor &&
		nativeWindowsNPMUsable(ctx, npmCmd, installDir, installerEnv) {
		return installDir, npmCmd, nil
	}
	if executableExists(nodeExe) && out != nil {
		_, _ = fmt.Fprintf(out, "managed Node.js/npm install is missing or broken; reinstalling: %s\n", installDir)
	}

	tmpDir, err := os.MkdirTemp(cfg.tempDir, "codex-node-")
	if err != nil {
		if diskErr := ensureCodexInstallDiskSpace(out, installerEnv, []codexInstallDiskTarget{{label: "temporary directory", path: cfg.tempDir}}); diskErr != nil {
			return "", "", diskErr
		}
		return "", "", fmt.Errorf("create temporary directory for Node.js download: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	versionDirURL := strings.TrimRight(codexNativeNodeDistBaseURL, "/") + fmt.Sprintf("/latest-v%d.x", cfg.targetMajor)
	shasumsPath := filepath.Join(tmpDir, "SHASUMS256.txt")
	if err := nativeWindowsDownloadFile(ctx, versionDirURL+"/SHASUMS256.txt", shasumsPath); err != nil {
		return "", "", fmt.Errorf("download Node.js checksums: %w", err)
	}
	expected, zipName, err := nativeWindowsResolveNodeZip(shasumsPath, cfg.targetMajor, cfg.arch)
	if err != nil {
		return "", "", err
	}

	zipPath := filepath.Join(tmpDir, zipName)
	if err := nativeWindowsDownloadFile(ctx, versionDirURL+"/"+zipName, zipPath); err != nil {
		return "", "", fmt.Errorf("download Node.js archive: %w", err)
	}
	actual, err := fileSHA256Hex(zipPath)
	if err != nil {
		return "", "", err
	}
	if !strings.EqualFold(actual, expected) {
		return "", "", fmt.Errorf("Node.js checksum mismatch for %s", zipName)
	}

	extractRoot := filepath.Join(tmpDir, "extract")
	expanded, err := extractSingleRootZip(zipPath, extractRoot)
	if err != nil {
		return "", "", fmt.Errorf("extract Node.js archive: %w", err)
	}
	if err := replaceDirectory(expanded, installDir); err != nil {
		if diskErr := ensureCodexInstallDiskSpace(out, installerEnv, []codexInstallDiskTarget{{label: "managed Node.js install root", path: cfg.nodeRoot}}); diskErr != nil {
			return "", "", diskErr
		}
		return "", "", fmt.Errorf("install managed Node.js: %w", err)
	}
	if !nativeWindowsNPMUsable(ctx, npmCmd, installDir, installerEnv) {
		return "", "", fmt.Errorf("npm is not usable in managed Node.js install: %s", npmCmd)
	}
	return installDir, npmCmd, nil
}

func nativeWindowsInstallCodexPackage(ctx context.Context, out io.Writer, cfg nativeWindowsCodexInstallConfig, installerEnv []string, nodeDir string, npmCmd string) error {
	if err := os.MkdirAll(cfg.npmPrefix, 0o755); err != nil {
		if diskErr := ensureCodexInstallDiskSpace(out, installerEnv, []codexInstallDiskTarget{{label: "managed npm prefix", path: cfg.npmPrefix}}); diskErr != nil {
			return diskErr
		}
		return fmt.Errorf("create managed npm prefix: %w", err)
	}
	env := codexNPMInstallEnv(installerEnv)
	env = nativeWindowsEnvWithPath(env, nodeDir, cfg.npmPrefix, filepath.Join(cfg.npmPrefix, "bin"))
	env = setEnvValue(env, "NPM_CONFIG_CACHE", cfg.npmCacheDir)

	cmd := exec.CommandContext(ctx, npmCmd, "install", "-g", "--prefix", cfg.npmPrefix, "--include=optional", "@openai/codex")
	cmd.Env = env
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.Stdin = codexInstallerCommandStdin()
	if err := cmd.Run(); err != nil {
		if diskErr := ensureCodexInstallDiskSpace(out, installerEnv, []codexInstallDiskTarget{
			{label: "managed npm prefix", path: cfg.npmPrefix},
			{label: "npm cache", path: cfg.npmCacheDir},
			{label: "temporary directory", path: cfg.tempDir},
		}); diskErr != nil {
			return diskErr
		}
		reason := "npm install -g @openai/codex failed"
		writeCodexInstallFailureBanner(out, reason)
		return fmt.Errorf("%s: %w", reason, err)
	}
	return nil
}

func nativeWindowsNodeMajor(ctx context.Context, nodeExe string, nodeDir string, installerEnv []string) int {
	cmd := exec.CommandContext(ctx, nodeExe, "-v")
	cmd.Env = nativeWindowsEnvWithPath(installerEnv, nodeDir)
	out, err := cmd.Output()
	if err != nil {
		return -1
	}
	raw := strings.TrimPrefix(strings.TrimSpace(string(out)), "v")
	major, _, _ := strings.Cut(raw, ".")
	parsed, err := strconv.Atoi(major)
	if err != nil {
		return -1
	}
	return parsed
}

func nativeWindowsNPMUsable(ctx context.Context, npmCmd string, nodeDir string, installerEnv []string) bool {
	if strings.TrimSpace(npmCmd) == "" {
		return false
	}
	env := nativeWindowsEnvWithPath(installerEnv, nodeDir)
	for _, args := range [][]string{{"--version"}, {"prefix", "-g"}} {
		cmd := exec.CommandContext(ctx, npmCmd, args...)
		cmd.Env = env
		if err := cmd.Run(); err != nil {
			return false
		}
	}
	return true
}

func nativeWindowsEnvWithPath(base []string, dirs ...string) []string {
	pathValue := envValue(base, "PATH")
	parts := make([]string, 0, len(dirs)+1)
	seen := map[string]struct{}{}
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		key := strings.ToLower(filepath.Clean(dir))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		parts = append(parts, filepath.Clean(dir))
	}
	if strings.TrimSpace(pathValue) != "" {
		parts = append(parts, pathValue)
	}
	return setEnvValue(base, "PATH", strings.Join(parts, string(os.PathListSeparator)))
}

func nativeWindowsDownloadFile(ctx context.Context, url string, path string) error {
	client := codexNativeHTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, resp.Body)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func nativeWindowsResolveNodeZip(shasumsPath string, targetMajor int, arch string) (expectedSHA256 string, zipName string, err error) {
	data, err := os.ReadFile(shasumsPath)
	if err != nil {
		return "", "", err
	}
	pattern := regexp.MustCompile(fmt.Sprintf(`(?m)^([A-Fa-f0-9]{64})\s+(node-v%d\.\d+\.\d+-win-%s\.zip)$`, targetMajor, regexp.QuoteMeta(arch)))
	match := pattern.FindSubmatch(data)
	if len(match) != 3 {
		return "", "", fmt.Errorf("failed to resolve Node.js zip for win-%s", arch)
	}
	return strings.ToLower(string(match[1])), string(match[2]), nil
}

func fileSHA256Hex(path string) (string, error) {
	file, err := os.Open(path)
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

func extractSingleRootZip(zipPath string, dest string) (string, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return "", err
	}

	root := ""
	for _, entry := range reader.File {
		name := strings.Trim(strings.ReplaceAll(entry.Name, "\\", "/"), "/")
		if name == "" {
			continue
		}
		parts := strings.Split(name, "/")
		for _, part := range parts {
			if part == "." || part == ".." {
				return "", fmt.Errorf("unsafe zip entry %q", entry.Name)
			}
		}
		if root == "" {
			root = parts[0]
		} else if root != parts[0] {
			return "", fmt.Errorf("Node.js archive contains multiple top-level entries: %s and %s", root, parts[0])
		}
		target := filepath.Join(append([]string{dest}, parts...)...)
		rel, err := filepath.Rel(dest, target)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return "", fmt.Errorf("unsafe zip entry %q", entry.Name)
		}
		if entry.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return "", err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return "", err
		}
		src, err := entry.Open()
		if err != nil {
			return "", err
		}
		mode := entry.FileInfo().Mode()
		if mode == 0 {
			mode = 0o644
		}
		dst, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
		if err != nil {
			_ = src.Close()
			return "", err
		}
		_, copyErr := io.Copy(dst, src)
		closeSrcErr := src.Close()
		closeDstErr := dst.Close()
		if copyErr != nil {
			return "", copyErr
		}
		if closeSrcErr != nil {
			return "", closeSrcErr
		}
		if closeDstErr != nil {
			return "", closeDstErr
		}
	}
	if root == "" {
		return "", fmt.Errorf("Node.js archive is empty")
	}
	return filepath.Join(dest, root), nil
}

func replaceDirectory(src string, dst string) error {
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	if err := copyDirectory(src, dst); err != nil {
		return err
	}
	return os.RemoveAll(src)
}

func copyDirectory(src string, dst string) error {
	return filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		dstFile, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode())
		if err != nil {
			_ = srcFile.Close()
			return err
		}
		_, copyErr := io.Copy(dstFile, srcFile)
		closeSrcErr := srcFile.Close()
		closeErr := dstFile.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeSrcErr != nil {
			return closeSrcErr
		}
		return closeErr
	})
}

func writeWindowsManagedCodexShims(npmPrefix string, nodeDir string) error {
	nodeLeaf := filepath.Base(filepath.Clean(nodeDir))
	codexCmd := filepath.Join(npmPrefix, "codex.cmd")
	codexJsRel := windowsManagedCodexJSRel(codexCmd)
	if err := os.WriteFile(codexCmd, []byte(buildWindowsManagedCodexCmdShim(nodeLeaf, codexJsRel)), 0o644); err != nil {
		return fmt.Errorf("update codex command shim: %w", err)
	}

	codexPS1 := filepath.Join(npmPrefix, "codex.ps1")
	if executableExists(codexPS1) {
		if err := os.WriteFile(codexPS1, []byte(buildWindowsManagedCodexPS1Shim(nodeLeaf, codexJsRel)), 0o644); err != nil {
			return fmt.Errorf("update codex PowerShell shim: %w", err)
		}
	}
	return nil
}

func windowsManagedCodexJSRel(codexCmd string) string {
	codexJsRel := `node_modules\@openai\codex\bin\codex.js`
	data, err := os.ReadFile(codexCmd)
	if err != nil {
		return codexJsRel
	}
	match := codexCmdJSRelPattern.FindSubmatch(data)
	if len(match) == 2 && strings.TrimSpace(string(match[1])) != "" {
		codexJsRel = string(match[1])
	}
	return windowsBackslashPath(codexJsRel)
}

func buildWindowsManagedCodexCmdShim(nodeLeaf string, codexJsRel string) string {
	nodeLeaf = windowsBackslashPath(nodeLeaf)
	codexJsRel = windowsBackslashPath(codexJsRel)
	lines := []string{
		"@echo off",
		"setlocal",
		fmt.Sprintf(`rem "%%~dp0%s"`, codexJsRel),
		`set "_nodeRoot=%CODEX_NODE_INSTALL_ROOT%"`,
		`if "%_nodeRoot%"=="" if not "%LOCALAPPDATA%"=="" set "_nodeRoot=%LOCALAPPDATA%\codex-proxy\node"`,
		`if "%_nodeRoot%"=="" set "_nodeRoot=%~dp0..\node"`,
		fmt.Sprintf(`set "_nodePath=%%_nodeRoot%%\%s\node.exe"`, nodeLeaf),
		`if not exist "%_nodePath%" (`,
		fmt.Sprintf(`  set "_fallbackNodePath=%%~dp0..\node\%s\node.exe"`, nodeLeaf),
		`  if exist "%_fallbackNodePath%" set "_nodePath=%_fallbackNodePath%"`,
		`)`,
		`if not exist "%_nodePath%" (`,
		`  echo Managed Node.js not found: %_nodePath% 1>&2`,
		`  exit /b 1`,
		`)`,
		fmt.Sprintf(`set "_scriptPath=%%~dp0%s"`, codexJsRel),
		`if not exist "%_scriptPath%" (`,
		`  echo Codex JS entrypoint not found: %_scriptPath% 1>&2`,
		`  exit /b 1`,
		`)`,
		`"%_nodePath%" "%_scriptPath%" %*`,
		`exit /b %ERRORLEVEL%`,
	}
	return strings.Join(lines, "\r\n") + "\r\n"
}

func buildWindowsManagedCodexPS1Shim(nodeLeaf string, codexJsRel string) string {
	nodeLeaf = windowsBackslashPath(nodeLeaf)
	codexJsRel = windowsBackslashPath(codexJsRel)
	lines := []string{
		"$basedir = Split-Path $MyInvocation.MyCommand.Definition -Parent",
		"$nodeLeaf = " + powershellSingleQuote(nodeLeaf),
		"$nodeRoot = $env:CODEX_NODE_INSTALL_ROOT",
		"if ([string]::IsNullOrWhiteSpace($nodeRoot) -and -not [string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {",
		"  $nodeRoot = Join-Path $env:LOCALAPPDATA 'codex-proxy\\node'",
		"}",
		"$nodePath = ''",
		"if (-not [string]::IsNullOrWhiteSpace($nodeRoot)) {",
		"  $nodePath = Join-Path (Join-Path $nodeRoot $nodeLeaf) 'node.exe'",
		"}",
		"if ([string]::IsNullOrWhiteSpace($nodePath) -or -not (Test-Path $nodePath)) {",
		"  $nodePath = Join-Path $basedir ('..\\node\\' + $nodeLeaf + '\\node.exe')",
		"}",
		"if (-not (Test-Path $nodePath)) {",
		"  Write-Error \"Managed Node.js not found: $nodePath\"",
		"  exit 1",
		"}",
		"$scriptPath = Join-Path $basedir " + powershellSingleQuote(codexJsRel),
		"if (-not (Test-Path $scriptPath)) {",
		"  Write-Error \"Codex JS entrypoint not found: $scriptPath\"",
		"  exit 1",
		"}",
		"if ($MyInvocation.ExpectingInput) {",
		"  $input | & $nodePath $scriptPath $args",
		"} else {",
		"  & $nodePath $scriptPath $args",
		"}",
		"exit $LASTEXITCODE",
	}
	return strings.Join(lines, "\r\n") + "\r\n"
}

func windowsBackslashPath(path string) string {
	path = strings.TrimSpace(path)
	path = strings.ReplaceAll(path, "/", `\`)
	return strings.TrimPrefix(path, `.\`)
}
