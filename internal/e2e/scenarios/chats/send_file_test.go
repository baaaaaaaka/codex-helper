package chats

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// F-RND-10: `helper file <relative-path>` / `!file` (v0.1.20
// sendFileCommand) sends one file from the Teams outbound root through the
// normal upload path. The bare command shows the usage and the root; a
// path that leaves the root (`..`, absolute, or a symlink pointing outside)
// is rejected and nothing is uploaded.
func TestSendFileCommand(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "SEND-FILE", harness.Options{
		Guarantees: []string{"F-RND-10"},
		Title:      "`helper file <rel-path>` uploads a file from the outbound root; escapes are rejected; usage names the root",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "send-file")

	reply := func(text, sub string) (string, bool) {
		seq := s.Graph.LastSeq()
		steps.MustPost(s, work.ID, text)
		var got string
		ok := s.WaitFor(fmt.Sprintf("reply to %q", text), steps.CommandTimeout, func() (bool, string) {
			for _, m := range s.SUTMessages(work.ID) {
				if m.RequestSeq > seq && strings.Contains(m.PlainText(), sub) {
					got = m.PlainText()
					return true, ""
				}
			}
			return false, strings.Join(s.Transcript(work.ID, 3), " | ")
		})
		return got, ok
	}

	usage, okUsage := reply("helper file", "Outbound root:")
	root := ""
	if i := strings.Index(usage, "Outbound root:"); i >= 0 {
		root = strings.TrimSpace(strings.SplitN(usage[i+len("Outbound root:"):], "\n", 2)[0])
	}
	s.Check("F-RND-10 bare `helper file` shows the usage and the outbound root",
		okUsage && strings.Contains(usage, "helper file <relative-path>") && filepath.IsAbs(root), "usage + absolute root", harness.NormalizeSpace(usage))
	if !filepath.IsAbs(root) {
		return
	}
	tok := s.Token("file")
	if err := os.MkdirAll(filepath.Join(root, "reports"), 0o755); err != nil {
		t.Fatal(err)
	}
	content := "quarterly numbers " + tok + "\n"
	if err := os.WriteFile(filepath.Join(root, "reports", "summary.txt"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(filepath.Dir(root), "outside-"+tok+".txt")
	if err := os.WriteFile(outside, []byte("secret "+tok), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape.txt")); err != nil {
		t.Fatal(err)
	}

	_, sent := reply("helper file reports/summary.txt", "summary.txt")
	uploaded := s.WaitFor("the file is uploaded and attached", steps.CommandTimeout, func() (bool, string) {
		return len(s.Graph.OutboundFiles(work.ID)) > 0, "no attachment yet"
	})
	dots, okDots := reply("!file ../outside-"+tok+".txt", "Teams file upload rejected")
	abs, okAbs := reply("helper file "+outside, "Teams file upload rejected")
	link, okLink := reply("helper file escape.txt", "Teams file upload rejected")
	s.Settle(3*time.Second, 15*time.Second)

	files := s.Graph.OutboundFiles(work.ID)
	got := ""
	if len(files) > 0 {
		got = fmt.Sprintf("%s: %q", files[0].Name, string(files[0].Item.Data))
	}
	s.Check("F-RND-10 the file is sent once through the upload path, with its content",
		sent && uploaded && len(files) == 1 && files[0].Name == "summary.txt" && string(files[0].Item.Data) == content,
		fmt.Sprintf("summary.txt: %q", content), fmt.Sprintf("%d attachments; %s", len(files), got), s.Transcript(work.ID, 10)...)
	s.Check("F-RND-10 a path with .. is rejected", okDots && strings.Contains(dots, "stay inside the outbound root"), "rejected: must stay inside the outbound root", harness.NormalizeSpace(dots))
	s.Check("F-RND-10 an absolute path is rejected", okAbs && strings.Contains(abs, "must be relative"), "rejected: must be relative", harness.NormalizeSpace(abs))
	s.Check("F-RND-10 a symlink escaping the root is rejected", okLink && strings.Contains(link, "outside the outbound root"), "rejected: outside the outbound root", harness.NormalizeSpace(link))
	for _, f := range s.Graph.DriveItems() {
		if strings.Contains(string(f.Data), "secret "+tok) {
			s.Check("F-RND-10 nothing outside the root is uploaded", false, "no upload of the outside file", f.Name)
		}
	}
}
