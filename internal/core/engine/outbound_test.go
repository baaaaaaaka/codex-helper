package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestManifestBlocksStripAndParse(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "a.csv"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("s"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}
	final := "Here you go.\n\n```codex-helper-artifacts\n" +
		`{"version":1,"files":[{"path":"sub/a.csv","name":"quarterly report.csv"},{"path":"../x","name":"bad"},{"path":"/etc/passwd"},{"path":"link.txt","name":"leak.txt"},{"path":"missing.bin"}]}` +
		"\n```\n"
	visible, blocks := manifestBlocks(final)
	if visible != "Here you go." || len(blocks) != 1 {
		t.Fatalf("visible %q blocks %d", visible, len(blocks))
	}
	arts, problems := parseManifest(blocks[0], dir)
	if len(arts) != 1 || arts[0].name != "quarterly report.csv" || arts[0].src != filepath.Join(dir, "sub", "a.csv") {
		t.Fatalf("artifacts %+v", arts)
	}
	if len(problems) != 4 {
		t.Fatalf("problems %q", problems)
	}
	for _, p := range problems {
		if strings.Contains(p, dir) || strings.Contains(p, "/etc") {
			t.Fatalf("problem leaks a path: %q", p)
		}
	}
	// The prompt placeholder is not a manifest.
	if a, p := parseManifest([]byte(`{"version":1,"files":[{"path":"relative/path.ext","name":"display-name.ext"}]}`), dir); a != nil || p != nil {
		t.Fatalf("placeholder parsed: %v %v", a, p)
	}
}

func TestCodexInputListsAttachments(t *testing.T) {
	dir := t.TempDir()
	voice := filepath.Join(dir, "v.ogg.txt")
	if err := os.WriteFile(voice, []byte("hello from voice"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := []store.InboundFile{
		{Idx: 0, Name: "pic.png", Kind: "image", State: "ready", LocalPath: "/x/0-pic.png"},
		{Idx: 1, Name: "design notes.txt", Kind: "file", State: "ready", LocalPath: "/x/1-design notes.txt", ExtRef: `{"t":"text/plain"}`},
		{Idx: 2, Name: "v.ogg", Kind: "voice", State: "ready", LocalPath: voice},
		{Idx: 3, Name: "gone.pdf", Kind: "file", State: "failed", Error: "the attachment could not be downloaded"},
	}
	text, images := codexInput("Please look", files, "/out")
	if len(images) != 1 || images[0] != "/x/0-pic.png" {
		t.Fatalf("images %v", images)
	}
	for _, want := range []string{"User message:\nPlease look", "hello from voice", "- /x/1-design notes.txt (original name: design notes.txt; text/plain)", "- gone.pdf: the attachment could not be downloaded"} {
		if !strings.Contains(text, want) {
			t.Fatalf("input lacks %q:\n%s", want, text)
		}
	}
}
