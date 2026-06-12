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
	"runtime"
	"strings"
	"time"
)

const (
	managedASRNativeCompatDirName = ".cxp-native"
	managedASRNativeCompatVersion = "linux-x64-glibc-2.35-conda-runtime-v1"
)

type managedASRNativeCompatAsset struct {
	Name        string
	URL         string
	SHA256      string
	Size        int64
	ArchiveKind string
	ExtractMode string
}

type managedASRNativeCompatRuntime struct {
	Root        string
	LibDir      string
	BinDir      string
	Interpreter string
	Patchelf    string
}

func managedASRLlamaNativeCompatCanRepair(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{
		"glibc_",
		"glibcxx_",
		"cxxabi_",
		"version `",
		"version '",
		"cannot open shared object file",
		"=> not found",
		"libstdc++.so.6",
		"libgcc_s.so.1",
		"libgomp.so.1",
		"libssl.so.3",
		"libcrypto.so.3",
		"ld-linux-x86-64.so.2",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func managedASRLlamaNativeCompatAssets(goos string, goarch string) ([]managedASRNativeCompatAsset, error) {
	if goos != "linux" || goarch != "amd64" {
		return nil, fmt.Errorf("managed llama native compatibility bundle is not available for %s/%s", goos, goarch)
	}
	return []managedASRNativeCompatAsset{
		{
			Name:        "ubuntu-base-22.04.5-base-amd64.tar.gz",
			URL:         "https://cdimage.ubuntu.com/ubuntu-base/releases/22.04/release/ubuntu-base-22.04.5-base-amd64.tar.gz",
			SHA256:      "242cd8898b33ea806ef5f13b1076ed7c76f9f989d18384452f7166692438ff1a",
			Size:        29832285,
			ArchiveKind: "tar.gz",
			ExtractMode: "ubuntu-base-glibc",
		},
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
	}, nil
}

func ensureManagedASRLlamaNativeCompat(ctx context.Context, command string) (managedASRNativeCompatRuntime, error) {
	if runtime.GOOS != "linux" {
		return managedASRNativeCompatRuntime{}, fmt.Errorf("native compatibility patching is only supported on Linux")
	}
	command = strings.TrimSpace(command)
	if command == "" {
		return managedASRNativeCompatRuntime{}, fmt.Errorf("llama binary path is empty")
	}
	root := filepath.Join(filepath.Dir(command), managedASRNativeCompatDirName)
	if rt, ok := managedASRNativeCompatFromMarker(root); ok {
		return rt, nil
	}
	assetsFn := managedASRLlamaNativeCompatAssetsFn
	if assetsFn == nil {
		assetsFn = managedASRLlamaNativeCompatAssets
	}
	assets, err := assetsFn(runtime.GOOS, runtime.GOARCH)
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
	if err := writeManagedASRNativeCompatMarker(staging); err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	if err := os.RemoveAll(root); err != nil {
		return managedASRNativeCompatRuntime{}, err
	}
	if err := os.Rename(staging, root); err != nil {
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

func managedASRNativeCompatFromMarker(root string) (managedASRNativeCompatRuntime, bool) {
	data, err := os.ReadFile(filepath.Join(root, "runtime.json"))
	if err != nil {
		return managedASRNativeCompatRuntime{}, false
	}
	var marker struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(data, &marker); err != nil || marker.Version != managedASRNativeCompatVersion {
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

func writeManagedASRNativeCompatMarker(root string) error {
	data := fmt.Sprintf("{\n  \"version\": %q,\n  \"updated_at\": %q\n}\n", managedASRNativeCompatVersion, time.Now().UTC().Format(time.RFC3339Nano))
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
		"libssl.so.3",
		"libcrypto.so.3":
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

func managedASRELFHasInterpreter(ctx context.Context, patchelf string, path string) bool {
	validateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(validateCtx, patchelf, "--print-interpreter", path)
	cmd.Env = managedASRSetupBaseEnv()
	out, err := cmd.Output()
	return err == nil && strings.TrimSpace(string(out)) != ""
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
