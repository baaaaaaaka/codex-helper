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
	arts, problems := parseManifest(blocks[0], artifactDirs{legacy: dir})
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
	if a, p := parseManifest([]byte(`{"version":1,"files":[{"path":"relative/path.ext","name":"display-name.ext"}]}`), artifactDirs{legacy: dir}); a != nil || p != nil {
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
		{Idx: 1, Name: "design notes.txt", Kind: "file", State: "ready", LocalPath: "/x/1-design notes.txt", Ref: store.FileReference{ContentType: "text/plain"}},
		{Idx: 2, Name: "v.ogg", Kind: "voice", State: "ready", LocalPath: voice},
		{Idx: 3, Name: "gone.pdf", Kind: "file", State: "failed", Error: "the attachment could not be downloaded"},
	}
	text, images := codexInput("Please look", files, "/out", teamsRequestSafety)
	if len(images) != 1 || images[0] != "/x/0-pic.png" {
		t.Fatalf("images %v", images)
	}
	for _, want := range []string{"User message:\nPlease look", "hello from voice", "- /x/1-design notes.txt (original name: design notes.txt; text/plain)", "- gone.pdf: the attachment could not be downloaded"} {
		if !strings.Contains(text, want) {
			t.Fatalf("input lacks %q:\n%s", want, text)
		}
	}
}

func TestArtifactVersionsUseOnlyTheirOwnDirectory(t *testing.T) {
	e := &Engine{cfg: Config{FilesDir: t.TempDir()}}
	tr := store.Turn{ID: 42, ConvID: 1, TurnKey: "retry", OutputKey: "original"}
	legacy := e.outboundDir(tr.OutputKey)
	current := e.conversationOutboundDir(tr.ConvID)
	other := e.conversationOutboundDir(2)
	for root, content := range map[string]string{legacy: "legacy", current: "current", other: "another chat"} {
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "report.txt"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ version, content string }{
		{"0", "legacy"}, {"1", "legacy"}, {"2", "current"},
	} {
		t.Run(tc.version, func(t *testing.T) {
			answer := "Done.\n```codex-helper-artifacts\n" +
				`{"version":` + tc.version + `,"files":[{"path":"report.txt"}]}` + "\n```"
			visible, msgs, problems := e.stageArtifacts(tr, answer)
			if visible != "Done." || len(problems) != 0 || len(msgs) != 1 || msgs[0].File == nil {
				t.Fatalf("visible=%q, attachments=%+v, errors=%v", visible, msgs, problems)
			}
			if data, err := os.ReadFile(msgs[0].File.LocalPath); err != nil || string(data) != tc.content {
				t.Fatalf("snapshot = %q, %v; expected %q", data, err, tc.content)
			}
		})
	}
	dirs := artifactDirs{legacy: legacy, chat: current}
	for _, block := range []string{
		`{"version":2,"files":[{"path":"../conversation-2/report.txt"}]}`,
		`{"version":2,"files":[{"path":"C:\\report.txt"}]}`,
		`{"version":2,"files":[{"path":"/tmp/report.txt"}]}`,
		`{"version":2,"files":[{"path":"missing.txt"}]}`,
		`{"version":3,"files":[{"path":"report.txt"}]}`,
	} {
		if artifacts, problems := parseManifest([]byte(block), dirs); len(artifacts) != 0 || len(problems) != 1 {
			t.Fatalf("%s: files=%v, errors=%v", block, artifacts, problems)
		}
	}
	for _, tc := range []struct{ root, target, file string }{
		{filepath.Join(current, "escape"), filepath.Join(other, "report.txt"), "escape"},
		{filepath.Join(t.TempDir(), "conversation-3"), other, "report.txt"},
	} {
		if err := os.Symlink(tc.target, tc.root); err != nil {
			t.Logf("symlink test unavailable: %v", err)
			continue
		}
		dir := current
		if tc.file == "report.txt" {
			dir = tc.root
		}
		block := `{"version":2,"files":[{"path":"` + tc.file + `"}]}`
		if artifacts, problems := parseManifest([]byte(block), artifactDirs{chat: dir}); len(artifacts) != 0 || len(problems) != 1 {
			t.Fatalf("cross-conversation symlink was accepted: %v, %v", artifacts, problems)
		}
	}
}
