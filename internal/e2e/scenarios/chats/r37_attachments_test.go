package chats

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-37: inbound attachments reach Codex intact. A pasted image (Teams hosted
// content) arrives as an image input whose bytes equal what the user pasted;
// a file (OneDrive reference) arrives as a local file with the original name
// and bytes. Neither is silently dropped and each message runs one turn.
// (Outbound `helper file` uploads are not covered here: see the M2 report.)
func TestR37_InboundAttachmentsReachCodex(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-37", harness.Options{
		Guarantees: []string{"R-37", "R-3"},
		Title:      "pasted image and attached file reach Codex byte-identical, with the original file name",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	png := tinyPNG()
	doc := bytes.Repeat([]byte("e2e attachment line\n"), 300)
	imgTok, fileTok := s.Token("image"), s.Token("file")

	type seen struct {
		images   []string // sha256 of each image input
		files    map[string]string
		fileErrs []string
	}
	var mu sync.Mutex
	got := map[string]*seen{}
	// "- <cwd-relative path> ([original name: X; ]<content type>)"
	fileRE := regexp.MustCompile(`(?m)^- (\S.*?) \((?:original name: ([^;]+); )?[^()]*\)$`)
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		// Runs when the turn starts, while the helper's temp files exist.
		v := &seen{files: map[string]string{}}
		for _, p := range c.Images {
			if data, err := os.ReadFile(p); err == nil {
				v.images = append(v.images, digest(data))
			} else {
				v.fileErrs = append(v.fileErrs, err.Error())
			}
		}
		for _, m := range fileRE.FindAllStringSubmatch(c.Prompt, -1) {
			p := m[1]
			if !filepath.IsAbs(p) {
				p = filepath.Join(c.Cwd, p)
			}
			name := m[2]
			if name == "" {
				name = filepath.Base(m[1])
			}
			if data, err := os.ReadFile(p); err == nil {
				v.files[name] = digest(data)
			} else {
				v.fileErrs = append(v.fileErrs, err.Error())
			}
		}
		if i := strings.Index(c.Prompt, "Attached files"); i >= 0 && len(v.files) == 0 {
			v.fileErrs = append(v.fileErrs, "unparsed prompt: "+c.Prompt[i:min(len(c.Prompt), i+300)])
		}
		mu.Lock()
		for _, tok := range c.Tokens {
			got[tok] = v
		}
		mu.Unlock()
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r37")
	if _, err := s.Graph.PostImageAsUser(work.ID, "What is in this picture? "+imgTok, png); err != nil {
		t.Fatal(err)
	}
	steps.WaitAnswered(s, work.ID, imgTok, 45*time.Second)
	if _, err := s.Graph.PostFileAsUser(work.ID, "Please review the attached notes "+fileTok, "design notes.txt", doc); err != nil {
		t.Fatal(err)
	}
	steps.WaitAnswered(s, work.ID, fileTok, 45*time.Second)
	s.Settle(3*time.Second, 10*time.Second)

	tr := s.Transcript(work.ID, 15)
	mu.Lock()
	img, f := got[imgTok], got[fileTok]
	mu.Unlock()
	s.Check("R-37/R-3 image message runs one turn", len(s.Codex.CallsWithToken(imgTok)) == 1, "1 turn", fmt.Sprint(len(s.Codex.CallsWithToken(imgTok))), tr...)
	s.Check("R-37 pasted image reaches Codex byte-identical", img != nil && len(img.images) == 1 && img.images[0] == digest(png),
		"1 image input, sha256 "+digest(png)[:12], fmt.Sprintf("%+v", img))
	s.Check("R-37/R-3 file message runs one turn", len(s.Codex.CallsWithToken(fileTok)) == 1, "1 turn", fmt.Sprint(len(s.Codex.CallsWithToken(fileTok))))
	s.Check("R-37 attached file reaches Codex with its name and bytes", f != nil && f.files["design notes.txt"] == digest(doc),
		"design notes.txt, sha256 "+digest(doc)[:12], fmt.Sprintf("%+v", f))
	var fetches []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "GET"}) {
		if strings.Contains(r.Route, "hostedContents") || strings.Contains(r.Route, "/shares/") {
			fetches = append(fetches, fmt.Sprintf("%s -> %d", r.Route, r.Status))
		}
	}
	s.Note("attachment fetches: %v", fetches)
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum[:])
}

// tinyPNG is a valid 1x1 PNG.
func tinyPNG() []byte {
	return []byte{
		0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
		0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0xf8, 0xcf, 0xc0, 0xf0,
		0x1f, 0x00, 0x05, 0x00, 0x01, 0xff, 0x89, 0x99, 0x3d, 0x1d, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
		0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
	}
}
