package chats

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// outboundDirRE finds the directory the helper tells Codex to write files
// for the Teams user into (SPEC F-RND-10: the artifact manifest contract).
var outboundDirRE = regexp.MustCompile(`write them under this local directory:\s*\n\s*(\S[^\n]*)\n`)

// R-37 (outbound half), F-RND-10: a file the agent asks to send arrives in
// the chat exactly once, byte-identical, under the name the agent gave it.
// Two files in one answer: a small one (simple drive upload) and a 12 MiB
// one (above the 10 MiB single-request limit, so an upload session).
func TestR37_AgentFilesReachChat(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-37-out", harness.Options{
		Guarantees: []string{"R-37", "F-RND-10", "R-1"},
		Title:      "files the agent lists in its artifact manifest reach the chat once, byte-identical, with the right names",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	tok := s.Token("artifacts")
	small := []byte(strings.Repeat("id,value\n", 200) + "end " + tok + "\n")
	large := make([]byte, 12<<20+12345)
	rand.New(rand.NewSource(37)).Read(large)
	const smallName, largeName = "quarterly report.csv", "dataset-large.bin"

	var mu sync.Mutex
	var promptDir, writeErr string
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if !c.HasToken(tok) {
			return harness.CodexBehavior{}
		}
		m := outboundDirRE.FindStringSubmatch(c.Prompt)
		mu.Lock()
		defer mu.Unlock()
		if m == nil {
			writeErr = "prompt has no outbound directory instruction"
			return harness.CodexBehavior{Answer: "E2EANSWER no outbound directory " + tok}
		}
		promptDir = strings.TrimSpace(m[1])
		sub := filepath.Join(promptDir, "r37-"+strings.ToLower(tok[len(tok)-6:]))
		if err := os.MkdirAll(sub, 0o700); err != nil {
			writeErr = err.Error()
		}
		for name, data := range map[string][]byte{"report.csv": small, "large.bin": large} {
			if err := os.WriteFile(filepath.Join(sub, name), data, 0o600); err != nil {
				writeErr = err.Error()
			}
		}
		rel := filepath.Base(sub)
		manifest, _ := json.Marshal(map[string]any{"version": 1, "files": []any{
			map[string]any{"path": rel + "/report.csv", "name": smallName},
			map[string]any{"path": rel + "/large.bin", "name": largeName},
		}})
		return harness.CodexBehavior{Answer: "E2EANSWER here are the two files you asked for " + tok +
			"\n\n```codex-helper-artifacts\n" + string(manifest) + "\n```\n"}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r37-out")
	steps.MustPost(s, work.ID, "Export the report and the dataset as files "+tok)
	matching := func(data []byte) []fakegraph.OutboundFile {
		var out []fakegraph.OutboundFile
		for _, f := range s.Graph.OutboundFiles(work.ID) {
			if bytes.Equal(f.Item.Data, data) {
				out = append(out, f)
			}
		}
		return out
	}
	s.WaitFor("both files attached in the chat", 90*time.Second, func() (bool, string) {
		return len(matching(small)) > 0 && len(matching(large)) > 0,
			fmt.Sprintf("%d attachments, drive: %d items", len(s.Graph.OutboundFiles(work.ID)), len(s.Graph.DriveItems()))
	})
	s.Settle(3*time.Second, 15*time.Second)

	mu.Lock()
	s.Note("outbound dir from prompt: %q; write error: %q", promptDir, writeErr)
	mu.Unlock()
	files := s.Graph.OutboundFiles(work.ID)
	var uploads []string
	for _, it := range s.Graph.DriveItems() {
		uploads = append(uploads, fmt.Sprintf("%s writes=%v fragments=%d size=%d", it.Path, it.Writes, it.Fragments, len(it.Data)))
	}
	s.Note("drive: %v", uploads)
	tr := s.Transcript(work.ID, 12)
	for _, c := range []struct {
		name string
		data []byte
		via  string
	}{{smallName, small, "put"}, {largeName, large, "session"}} {
		got := matching(c.data)
		s.Check("R-37 "+c.name+" arrives byte-identical, attached exactly once", len(got) == 1,
			fmt.Sprintf("1 attachment with %d bytes, sha256 %s", len(c.data), digest(c.data)[:12]),
			fmt.Sprintf("%d (all attachments: %d)", len(got), len(files)), tr...)
		name := "not attached"
		if len(got) > 0 {
			name = got[0].Name
		}
		s.Check("R-37 "+c.name+" is shown under the name the agent gave", len(got) > 0 && name == c.name, strconv.Quote(c.name), strconv.Quote(name))
		writes := 0
		if len(got) > 0 {
			writes = len(got[0].Item.Writes)
		}
		s.Check("R-37 "+c.name+" is uploaded once (no duplicate drive writes)", writes == 1, "1 write via "+c.via, fmt.Sprint(uploads))
	}
	s.Check("R-1 the visible answer is posted once", len(s.SUTMessagesContaining(work.ID, "here are the two files you asked for "+tok)) == 1,
		"1", fmt.Sprint(len(s.SUTMessagesContaining(work.ID, "here are the two files you asked for "+tok))), tr...)
	s.Check("F-RND-10 the manifest block is not shown to the user", len(s.SUTMessagesContaining(work.ID, `"version":1`)) == 0,
		"no manifest JSON in chat", fmt.Sprint(len(s.SUTMessagesContaining(work.ID, `"version":1`))))
}
