package asr

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Exercise the real workflow script with an isolated fake gh executable.
// No credentials, actual release, or helper process is used.
func TestPublishASRMirrorVerifiesExistingAssetsWithoutOverwriting(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Ubuntu publishing script")
	}
	for _, bin := range []string{"bash", "sha256sum", "cmp", "mktemp"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s unavailable", bin)
		}
	}
	for _, mode := range []string{"new", "existing", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			release := filepath.Join(root, "release")
			mirror := filepath.Join(root, "mirror")
			bin := filepath.Join(root, "bin")
			for _, dir := range []string{release, mirror, bin} {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			data := []byte("pinned voice bytes")
			a := downloadAsset("", data)
			for name, body := range map[string][]byte{"asset.gguf": data, "SHA256SUMS": []byte(a.SHA256 + "  asset.gguf\n")} {
				if err := os.WriteFile(filepath.Join(release, name), body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			fake := `#!/usr/bin/env bash
set -euo pipefail
case "$2" in
  view) [[ "$MIRROR_MODE" != new ]] ;;
  create)
    [[ "$MIRROR_MODE" == new ]] || exit 90
    for arg in "$@"; do if [[ -f "$arg" ]]; then cp "$arg" "$MIRROR_DIR/"; fi; done ;;
  download)
    while [[ "$1" != --dir ]]; do shift; done
    dest=$2
    cp "$MIRROR_DIR/"* "$dest/"
    if [[ "$MIRROR_MODE" == corrupt ]]; then echo wrong > "$dest/asset.gguf"; fi ;;
  *) exit 91 ;;
esac
`
			if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(fake), 0700); err != nil {
				t.Fatal(err)
			}
			if mode != "new" {
				for _, name := range []string{"asset.gguf", "SHA256SUMS"} {
					b, _ := os.ReadFile(filepath.Join(release, name))
					if err := os.WriteFile(filepath.Join(mirror, name), b, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			cmd := exec.Command("bash", "../../scripts/ci/publish-asr-mirror.sh", release, "owner/mirror", "llama-asr-test-r1")
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				if key != "PATH" && key != "GH_TOKEN" && key != "RUNNER_TEMP" {
					cmd.Env = append(cmd.Env, entry)
				}
			}
			cmd.Env = append(cmd.Env, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "GH_TOKEN=fake-test-token", "RUNNER_TEMP="+root, "MIRROR_DIR="+mirror, "MIRROR_MODE="+mode)
			out, err := cmd.CombinedOutput()
			if mode == "corrupt" {
				if err == nil {
					t.Fatal("corrupt mirror accepted")
				}
			} else if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
		})
	}
}
