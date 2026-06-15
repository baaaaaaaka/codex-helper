package teams

import (
	"archive/tar"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	managedASRNativeCompatDirName           = ".cxp-native"
	managedASRNativeCompatProfileGlibc235   = "linux-x64-glibc-2.35-conda-runtime-v1"
	managedASRNativeCompatProfileGlibc239   = "linux-x64-glibc-2.39-conda-runtime-v1"
	managedASRNativeCompatDefaultProfile    = managedASRNativeCompatProfileGlibc235
	managedASRNativeCompatUnsupportedOSArch = "managed llama native compatibility bundle is not available for %s/%s"
)

var managedASRNativeCompatSymbolVersionRE = regexp.MustCompile(`\b(?:GLIBC|GLIBCXX|CXXABI|GCC|OPENSSL)_[0-9][A-Za-z0-9_.]*`)
var managedASRNativeCompatCannotOpenLibraryRE = regexp.MustCompile(`(?m)(?:^|\s)((?:/[^:\s]+/)?lib[^\s:'"]+\.so(?:\.[0-9]+)*)[^:\n]*:\s+cannot open shared object file`)
var managedASRNativeCompatNotFoundLibraryRE = regexp.MustCompile(`(?m)^\s*(\S+)\s+=>\s+not found\b`)

var managedASRNativeCompatRepairableLibraries = []string{
	"ld-linux-x86-64.so.2",
	"libc.so.6",
	"libm.so.6",
	"libpthread.so.0",
	"libdl.so.2",
	"librt.so.1",
	"libresolv.so.2",
	"libgcc_s.so.1",
	"libstdc++.so.6",
	"libgomp.so.1",
	"libssl.so.3",
	"libcrypto.so.3",
	"libnss_files.so.2",
	"libnss_dns.so.2",
}

type managedASRNativeCompatAsset struct {
	Name        string
	URL         string
	SHA256      string
	Size        int64
	ArchiveKind string
	ExtractMode string
}

type managedASRNativeCompatProfile struct {
	Version    string
	GLIBCMax   []int
	GLIBCXXMax []int
	CXXABIMax  []int
	OpenSSLMax []int
	GCCMax     []int
	Assets     []managedASRNativeCompatAsset
}

type managedASRNativeCompatRuntime struct {
	Root        string
	LibDir      string
	BinDir      string
	Interpreter string
	Patchelf    string
}

func managedASRLlamaNativeCompatCanRepair(err error) bool {
	_, ok := managedASRLlamaNativeCompatProfileForError(err)
	return ok
}

func managedASRLlamaNativeCompatProfileForError(err error) (managedASRNativeCompatProfile, bool) {
	if err == nil {
		return managedASRNativeCompatProfile{}, false
	}
	text := err.Error()
	profiles := managedASRNativeCompatProfiles()
	missingLibraries := managedASRNativeCompatMissingLibraries(text)
	for _, library := range missingLibraries {
		if !managedASRNativeCompatLibraryCanRepair(library) {
			return managedASRNativeCompatProfile{}, false
		}
	}
	versions := managedASRNativeCompatSymbolVersionRE.FindAllString(text, -1)
	if len(versions) > 0 {
		for _, profile := range profiles {
			if managedASRNativeCompatProfileCanRepairSymbolVersions(profile, versions) {
				return profile, true
			}
		}
		return managedASRNativeCompatProfile{}, false
	}
	if len(missingLibraries) > 0 || managedASRNativeCompatErrorMentionsRepairableLibrary(text) {
		return profiles[0], true
	}
	return managedASRNativeCompatProfile{}, false
}

func managedASRNativeCompatMissingLibraries(text string) []string {
	seen := map[string]bool{}
	var libraries []string
	for _, re := range []*regexp.Regexp{managedASRNativeCompatCannotOpenLibraryRE, managedASRNativeCompatNotFoundLibraryRE} {
		for _, match := range re.FindAllStringSubmatch(text, -1) {
			if len(match) < 2 {
				continue
			}
			library := strings.TrimSpace(match[1])
			if library == "" {
				continue
			}
			base := pathBase(library)
			if seen[base] {
				continue
			}
			seen[base] = true
			libraries = append(libraries, base)
		}
	}
	return libraries
}

func managedASRNativeCompatLibraryCanRepair(library string) bool {
	base := pathBase(library)
	for _, candidate := range managedASRNativeCompatRepairableLibraries {
		if base == candidate {
			return true
		}
	}
	return false
}

func managedASRNativeCompatErrorMentionsRepairableLibrary(text string) bool {
	lower := strings.ToLower(text)
	if !strings.Contains(lower, "not found") &&
		!strings.Contains(lower, "no such file") &&
		!strings.Contains(lower, "cannot open shared object file") {
		return false
	}
	for _, library := range managedASRNativeCompatRepairableLibraries {
		if strings.Contains(lower, strings.ToLower(library)) {
			return true
		}
	}
	return false
}

func managedASRNativeCompatSymbolVersionCanRepair(version string) bool {
	for _, profile := range managedASRNativeCompatProfiles() {
		if managedASRNativeCompatProfileCanRepairSymbolVersion(profile, version) {
			return true
		}
	}
	return false
}

func managedASRNativeCompatProfileCanRepairSymbolVersions(profile managedASRNativeCompatProfile, versions []string) bool {
	if len(versions) == 0 {
		return false
	}
	for _, version := range versions {
		if !managedASRNativeCompatProfileCanRepairSymbolVersion(profile, version) {
			return false
		}
	}
	return true
}

func managedASRNativeCompatProfileCanRepairSymbolVersion(profile managedASRNativeCompatProfile, version string) bool {
	switch {
	case strings.HasPrefix(version, "GLIBC_"):
		return managedASRNativeCompatVersionAtMost(version, "GLIBC_", profile.GLIBCMax)
	case strings.HasPrefix(version, "GLIBCXX_"):
		return managedASRNativeCompatVersionAtMost(version, "GLIBCXX_", profile.GLIBCXXMax)
	case strings.HasPrefix(version, "CXXABI_"):
		return managedASRNativeCompatVersionAtMost(version, "CXXABI_", profile.CXXABIMax)
	case strings.HasPrefix(version, "OPENSSL_"):
		return managedASRNativeCompatVersionAtMost(version, "OPENSSL_", profile.OpenSSLMax)
	case strings.HasPrefix(version, "GCC_"):
		return managedASRNativeCompatVersionAtMost(version, "GCC_", profile.GCCMax)
	default:
		return false
	}
}

func managedASRNativeCompatVersionAtMost(version string, prefix string, max []int) bool {
	got := managedASRNativeCompatVersionNumbers(strings.TrimPrefix(version, prefix))
	if len(got) == 0 || len(max) == 0 {
		return false
	}
	if len(got) > len(max) {
		for _, extra := range got[len(max):] {
			if extra != 0 {
				return false
			}
		}
		got = got[:len(max)]
	}
	for len(got) < len(max) {
		got = append(got, 0)
	}
	for i := 0; i < len(max); i++ {
		if got[i] < max[i] {
			return true
		}
		if got[i] > max[i] {
			return false
		}
	}
	return true
}

func managedASRNativeCompatVersionNumbers(value string) []int {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	var numbers []int
	for _, part := range strings.Split(value, ".") {
		part = strings.TrimSpace(part)
		if part == "" {
			break
		}
		end := 0
		for end < len(part) && part[end] >= '0' && part[end] <= '9' {
			end++
		}
		if end == 0 {
			break
		}
		n, err := strconv.Atoi(part[:end])
		if err != nil {
			return nil
		}
		numbers = append(numbers, n)
		if end != len(part) {
			break
		}
	}
	return numbers
}

func managedASRNativeCompatProfiles() []managedASRNativeCompatProfile {
	common := []managedASRNativeCompatAsset{
		{
			Name:        "libgomp-9.3.0-h5101ec6_17.tar.bz2",
			URL:         "https://repo.anaconda.com/pkgs/main/linux-64/libgomp-9.3.0-h5101ec6_17.tar.bz2",
			SHA256:      "fd8540c16e79afdead8c6dcfb4e7401294f4185522f41deb4dc12f27fb58b3a8",
			Size:        386800,
			ArchiveKind: "tar.bz2",
			ExtractMode: "conda-runtime",
		},
		{
			Name:        "patchelf-0.9-hf79760b_2.tar.bz2",
			URL:         "https://repo.anaconda.com/pkgs/main/linux-64/patchelf-0.9-hf79760b_2.tar.bz2",
			SHA256:      "9c62ea8c7aaa8743057464bdfc50aedd4285207898c848f36a6ce94b5d6c8361",
			Size:        70656,
			ArchiveKind: "tar.bz2",
			ExtractMode: "conda-runtime",
		},
	}
	withCommon := func(assets ...managedASRNativeCompatAsset) []managedASRNativeCompatAsset {
		out := append([]managedASRNativeCompatAsset{}, assets...)
		return append(out, common...)
	}
	return []managedASRNativeCompatProfile{
		{
			Version:    managedASRNativeCompatProfileGlibc235,
			GLIBCMax:   []int{2, 35},
			GLIBCXXMax: []int{3, 4, 30},
			CXXABIMax:  []int{1, 3, 13},
			OpenSSLMax: []int{3, 0, 0},
			GCCMax:     []int{12, 0, 0},
			Assets: withCommon(managedASRNativeCompatAsset{
				Name:        "ubuntu-base-22.04.5-base-amd64.tar.gz",
				URL:         "https://cdimage.ubuntu.com/ubuntu-base/releases/22.04/release/ubuntu-base-22.04.5-base-amd64.tar.gz",
				SHA256:      "242cd8898b33ea806ef5f13b1076ed7c76f9f989d18384452f7166692438ff1a",
				Size:        29832285,
				ArchiveKind: "tar.gz",
				ExtractMode: "ubuntu-base-glibc",
			}),
		},
		{
			Version:    managedASRNativeCompatProfileGlibc239,
			GLIBCMax:   []int{2, 39},
			GLIBCXXMax: []int{3, 4, 33},
			CXXABIMax:  []int{1, 3, 15},
			OpenSSLMax: []int{3, 0, 0},
			GCCMax:     []int{14, 0, 0},
			Assets: withCommon(managedASRNativeCompatAsset{
				Name:        "ubuntu-base-24.04.4-base-amd64.tar.gz",
				URL:         "https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release/ubuntu-base-24.04.4-base-amd64.tar.gz",
				SHA256:      "c1e67ef7b17a6300e136118bd1dc04725009cb376c1aad10abcf8cd453628d58",
				Size:        29989394,
				ArchiveKind: "tar.gz",
				ExtractMode: "ubuntu-base-glibc",
			}),
		},
	}
}

func managedASRNativeCompatProfileByVersion(version string) (managedASRNativeCompatProfile, bool) {
	for _, profile := range managedASRNativeCompatProfiles() {
		if profile.Version == version {
			return profile, true
		}
	}
	return managedASRNativeCompatProfile{}, false
}

func managedASRLlamaNativeCompatAssets(goos string, goarch string, profileVersion string) ([]managedASRNativeCompatAsset, error) {
	if goos != "linux" || goarch != "amd64" {
		return nil, fmt.Errorf(managedASRNativeCompatUnsupportedOSArch, goos, goarch)
	}
	if profileVersion == "" {
		profileVersion = managedASRNativeCompatDefaultProfile
	}
	profile, ok := managedASRNativeCompatProfileByVersion(profileVersion)
	if !ok {
		return nil, fmt.Errorf("managed llama native compatibility profile %q is not available", profileVersion)
	}
	return append([]managedASRNativeCompatAsset{}, profile.Assets...), nil
}

func ensureManagedASRLlamaNativeCompat(ctx context.Context, command string, profile managedASRNativeCompatProfile) (managedASRNativeCompatRuntime, error) {
	if runtime.GOOS != "linux" {
		return managedASRNativeCompatRuntime{}, fmt.Errorf("native compatibility patching is only supported on Linux")
	}
	command = strings.TrimSpace(command)
	if command == "" {
		return managedASRNativeCompatRuntime{}, fmt.Errorf("llama binary path is empty")
	}
	if profile.Version == "" {
		profile, _ = managedASRNativeCompatProfileByVersion(managedASRNativeCompatDefaultProfile)
	}
	root := filepath.Join(filepath.Dir(command), managedASRNativeCompatDirName)
	if rt, ok := managedASRNativeCompatFromMarker(root, profile.Version); ok {
		return rt, nil
	}
	assetsFn := managedASRLlamaNativeCompatAssetsFn
	if assetsFn == nil {
		assetsFn = managedASRLlamaNativeCompatAssets
	}
	assets, err := assetsFn(runtime.GOOS, runtime.GOARCH, profile.Version)
	if err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	parent := filepath.Dir(root)
	staging := filepath.Join(parent, ".native-compat-staging-"+fmt.Sprint(time.Now().UnixNano()))
	defer func() { _ = os.RemoveAll(staging) }()
	if err := os.RemoveAll(staging); err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	if err := os.MkdirAll(filepath.Join(staging, "lib"), 0o700); err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	if err := os.MkdirAll(filepath.Join(staging, "bin"), 0o700); err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	for _, asset := range assets {
		archivePath := filepath.Join(parent, ".native-"+safePathPart(asset.Name))
		if err := downloadManagedASRLlamaFile(ctx, managedASRLlamaFileAsset{
			Name:   asset.Name,
			URL:    asset.URL,
			SHA256: asset.SHA256,
			Size:   asset.Size,
		}, archivePath, "Teams ASR native compatibility "+asset.Name); err != nil {
			_ = os.Remove(archivePath)
			return managedASRNativeCompatRuntime{}, err
		}
		if err := extractManagedASRNativeCompatAsset(archivePath, staging, asset); err != nil {
			_ = os.Remove(archivePath)
			return managedASRNativeCompatRuntime{}, err
		}
		_ = os.Remove(archivePath)
	}
	if err := ensureManagedASRNativeCompatSymlinks(filepath.Join(staging, "lib")); err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	rt := managedASRNativeCompatRuntime{
		Root:        staging,
		LibDir:      filepath.Join(staging, "lib"),
		BinDir:      filepath.Join(staging, "bin"),
		Interpreter: filepath.Join(staging, "lib", "ld-linux-x86-64.so.2"),
		Patchelf:    filepath.Join(staging, "bin", "patchelf"),
	}
	if err := validateManagedASRNativeCompatRuntime(rt); err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	if err := writeManagedASRNativeCompatMarker(staging, profile.Version); err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	if err := managedASRPublishDir("native compatibility runtime", staging, root); err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	return managedASRNativeCompatRuntime{
		Root:        root,
		LibDir:      filepath.Join(root, "lib"),
		BinDir:      filepath.Join(root, "bin"),
		Interpreter: filepath.Join(root, "lib", "ld-linux-x86-64.so.2"),
		Patchelf:    filepath.Join(root, "bin", "patchelf"),
	}, nil
}

func ensureManagedASRNativeCompatSymlinks(libDir string) error {
	links := map[string]string{
		"libgomp.so.1": "libgomp.so.1.0.0",
	}
	for link, target := range links {
		linkPath := filepath.Join(libDir, link)
		if _, err := os.Lstat(linkPath); err == nil {
			continue
		}
		if info, err := os.Stat(filepath.Join(libDir, target)); err != nil || info.IsDir() {
			continue
		}
		if err := os.Symlink(target, linkPath); err != nil {
			return err
		}
	}
	return nil
}

func managedASRNativeCompatFromMarker(root string, expectedVersions ...string) (managedASRNativeCompatRuntime, bool) {
	data, err := os.ReadFile(filepath.Join(root, "runtime.json"))
	if err != nil {
		return managedASRNativeCompatRuntime{}, false
	}
	var marker struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(data, &marker); err != nil {
		return managedASRNativeCompatRuntime{}, false
	}
	if len(expectedVersions) > 0 {
		found := false
		for _, expected := range expectedVersions {
			if marker.Version == expected {
				found = true
				break
			}
		}
		if !found {
			return managedASRNativeCompatRuntime{}, false
		}
	} else if _, ok := managedASRNativeCompatProfileByVersion(marker.Version); !ok {
		return managedASRNativeCompatRuntime{}, false
	}
	rt := managedASRNativeCompatRuntime{
		Root:        root,
		LibDir:      filepath.Join(root, "lib"),
		BinDir:      filepath.Join(root, "bin"),
		Interpreter: filepath.Join(root, "lib", "ld-linux-x86-64.so.2"),
		Patchelf:    filepath.Join(root, "bin", "patchelf"),
	}
	if err := validateManagedASRNativeCompatRuntime(rt); err != nil {
		return managedASRNativeCompatRuntime{}, false
	}
	return rt, true
}

func writeManagedASRNativeCompatMarker(root string, profileVersion string) error {
	if profileVersion == "" {
		profileVersion = managedASRNativeCompatDefaultProfile
	}
	data := fmt.Sprintf("{\n  \"version\": %q,\n  \"updated_at\": %q\n}\n", profileVersion, time.Now().UTC().Format(time.RFC3339Nano))
	return writePrivateFileReplacing(filepath.Join(root, "runtime.json"), []byte(data), 0o600)
}

func validateManagedASRNativeCompatRuntime(rt managedASRNativeCompatRuntime) error {
	for _, path := range []string{
		rt.Interpreter,
		rt.Patchelf,
		filepath.Join(rt.LibDir, "libc.so.6"),
		filepath.Join(rt.LibDir, "libstdc++.so.6"),
		filepath.Join(rt.LibDir, "libgomp.so.1"),
	} {
		info, err := os.Stat(path)
		if err != nil || info.IsDir() || info.Size() == 0 {
			return fmt.Errorf("native compatibility bundle is missing %s", filepath.Base(path))
		}
	}
	return nil
}

func extractManagedASRNativeCompatAsset(archivePath string, dest string, asset managedASRNativeCompatAsset) error {
	switch asset.ArchiveKind {
	case "tar.gz":
		return extractManagedASRNativeCompatTar(archivePath, dest, asset, true)
	case "tar.bz2":
		return extractManagedASRNativeCompatTar(archivePath, dest, asset, false)
	default:
		return fmt.Errorf("unsupported native compatibility archive type %q", asset.ArchiveKind)
	}
}

func extractManagedASRNativeCompatTar(archivePath string, dest string, asset managedASRNativeCompatAsset, gzipArchive bool) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	var reader io.Reader = file
	if gzipArchive {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer gz.Close()
		reader = gz
	} else {
		reader = bzip2.NewReader(file)
	}
	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		target, keep := managedASRNativeCompatTarget(dest, asset.ExtractMode, header.Name)
		if !keep {
			continue
		}
		if err := extractManagedASRNativeCompatEntry(tr, header, target); err != nil {
			return err
		}
	}
}

func managedASRNativeCompatTarget(dest string, mode string, name string) (string, bool) {
	name = strings.TrimPrefix(filepath.ToSlash(strings.TrimSpace(name)), "./")
	switch mode {
	case "ubuntu-base-glibc":
		base := pathBase(name)
		if !managedASRNativeCompatUbuntuLib(base) {
			return "", false
		}
		return filepath.Join(dest, "lib", base), true
	case "conda-runtime":
		if strings.HasPrefix(name, "lib/") {
			base := pathBase(name)
			if !strings.Contains(base, ".so") {
				return "", false
			}
			return filepath.Join(dest, "lib", base), true
		}
		if name == "bin/patchelf" {
			return filepath.Join(dest, "bin", "patchelf"), true
		}
	}
	return "", false
}

func managedASRNativeCompatUbuntuLib(base string) bool {
	switch base {
	case "ld-linux-x86-64.so.2",
		"libc.so.6",
		"libm.so.6",
		"libpthread.so.0",
		"libdl.so.2",
		"librt.so.1",
		"libresolv.so.2",
		"libgcc_s.so.1",
		"libstdc++.so.6",
		"libstdc++.so.6.0.30",
		"libstdc++.so.6.0.33",
		"libssl.so.3",
		"libcrypto.so.3",
		"libnss_files.so.2",
		"libnss_dns.so.2":
		return true
	default:
		return false
	}
}

func extractManagedASRNativeCompatEntry(reader io.Reader, header *tar.Header, target string) error {
	if target == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	switch header.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		_ = os.Remove(target)
		mode := header.FileInfo().Mode() & 0o777
		if mode == 0 {
			mode = 0o600
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, reader)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	case tar.TypeSymlink:
		link := strings.TrimSpace(header.Linkname)
		if link == "" || filepath.IsAbs(link) || strings.Contains(filepath.ToSlash(link), "../") {
			return nil
		}
		_ = os.Remove(target)
		return os.Symlink(pathBase(link), target)
	default:
		return nil
	}
}

func applyManagedASRLlamaNativeCompat(ctx context.Context, command string, compat managedASRNativeCompatRuntime) error {
	if compat.Patchelf == "" || compat.Interpreter == "" || compat.LibDir == "" {
		return fmt.Errorf("native compatibility bundle is incomplete")
	}
	root := filepath.Dir(command)
	rpath := "$ORIGIN:$ORIGIN/lib:$ORIGIN/" + managedASRNativeCompatDirName + "/lib"
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == managedASRNativeCompatDirName {
				return filepath.SkipDir
			}
			return nil
		}
		if ok, err := managedASRFileLooksELF(path); err != nil || !ok {
			return err
		}
		if managedASRELFHasInterpreter(ctx, compat.Patchelf, path) {
			if err := runManagedASRPatchelf(ctx, compat.Patchelf, "--set-interpreter", compat.Interpreter, path); err != nil {
				return err
			}
		}
		return runManagedASRPatchelf(ctx, compat.Patchelf, "--force-rpath", "--set-rpath", rpath, path)
	})
}

func managedASRNativeCompatCommandUsesCurrentInterpreter(ctx context.Context, command string, compat managedASRNativeCompatRuntime) bool {
	if compat.Patchelf == "" || compat.Interpreter == "" {
		return false
	}
	ok, err := managedASRFileLooksELF(command)
	if err != nil {
		return false
	}
	if !ok {
		return true
	}
	interpreter, ok := managedASRELFInterpreter(ctx, compat.Patchelf, command)
	return ok && interpreter == compat.Interpreter
}

func managedASRELFHasInterpreter(ctx context.Context, patchelf string, path string) bool {
	interpreter, ok := managedASRELFInterpreter(ctx, patchelf, path)
	return ok && interpreter != ""
}

func managedASRELFInterpreter(ctx context.Context, patchelf string, path string) (string, bool) {
	validateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(validateCtx, patchelf, "--print-interpreter", path)
	cmd.Env = managedASRSetupBaseEnv()
	out, err := cmd.Output()
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(out)), true
}

func runManagedASRPatchelf(ctx context.Context, patchelf string, args ...string) error {
	validateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(validateCtx, patchelf, args...)
	cmd.Env = managedASRSetupBaseEnv()
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := runASRCommand(validateCtx, cmd); err != nil {
		detail := strings.TrimSpace(out.String())
		if detail != "" {
			return fmt.Errorf("%w: %s", err, shortenTeamsLine(detail, 400))
		}
		return err
	}
	return nil
}

func managedASRFileLooksELF(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	var magic [4]byte
	n, err := io.ReadFull(file, magic[:])
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	return n == 4 && magic == [4]byte{0x7f, 'E', 'L', 'F'}, nil
}

func pathBase(value string) string {
	value = strings.TrimSuffix(filepath.ToSlash(value), "/")
	if value == "" {
		return ""
	}
	idx := strings.LastIndex(value, "/")
	if idx >= 0 {
		return value[idx+1:]
	}
	return value
}
