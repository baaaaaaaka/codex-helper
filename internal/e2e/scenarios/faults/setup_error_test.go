package faults

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

// F-TRN-16: a request Codex could not start because of the helper's own
// setup (here: the Codex launcher cannot find Node.js, as when a managed
// install lost its runtime) gets v0.1.20's setup notice for that class
// (missingNodeRuntimeFailureNotice): the request did not run, the helper
// does not retry it by itself, and `helper retry last` runs it once the
// setup is fixed. The notice carries the launcher's diagnostic.
//
// The SUT's --codex-path is a launcher that fails `codex app-server` the
// way /usr/bin/env does when node is missing while a flag file exists, and
// otherwise runs the fake Codex.
func TestSetupErrorNoticeAndRetry(t *testing.T) {
	dir := t.TempDir()
	flag := filepath.Join(dir, "node-missing")
	launcher := filepath.Join(dir, "codex")
	real := filepath.Join(filepath.Dir(harness.CLIBinary), "codex")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = app-server ] && [ -e '" + flag + "' ]; then\n" +
		"  echo \"/usr/bin/env: 'node': No such file or directory\" >&2\n" +
		"  exit 127\n" +
		"fi\n" +
		"exec '" + real + "' \"$@\"\n"
	if err := os.WriteFile(launcher, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	harness.Parallel(t)
	s := harness.NewScenario(t, "SETUP-ERR", harness.Options{
		Guarantees: []string{"F-TRN-16", "F-TRN-4"},
		Title:      "Codex cannot start (Node.js missing): one setup notice, no automatic retry; `helper retry last` runs it once fixed",
		Budget:     120 * time.Second,
		Args:       []string{"--codex-path", launcher},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "setup-err")
	if err := os.WriteFile(flag, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	tok := s.Token("setup")
	steps.MustPost(s, work.ID, "Build the report "+tok)
	notice, ok := s.WaitSUTMessage(work.ID, "could not find its Node.js runtime", 45*time.Second)
	text := harness.NormalizeSpace(notice.PlainText())
	time.Sleep(5 * time.Second) // room for a (wrong) automatic retry or a second notice
	s.Check("F-TRN-16 one setup notice of the Node.js class, saying nothing ran and naming `helper retry last`",
		ok && strings.Contains(text, "No user work ran or was duplicated.") && strings.Contains(text, "helper retry last") &&
			strings.Contains(text, "Diagnostic for the admin:") && strings.Contains(text, "'node': No such file or directory") &&
			len(s.SUTMessagesContaining(work.ID, "could not find its Node.js runtime")) == 1,
		"⚠️ Codex did not start because the helper process could not find its Node.js runtime. … Diagnostic …", text, s.Transcript(work.ID, 8)...)
	s.Check("F-TRN-16 the request did not reach Codex and is not retried by itself", len(s.Codex.CallsWithToken(tok)) == 0, "0 Codex calls",
		fmt.Sprint(len(s.Codex.CallsWithToken(tok))))
	leaks := steps.InternalLeaks(s, text)
	s.Check("F-TRN-16 the notice shows no internal ids or helper paths", len(leaks) == 0, "none", fmt.Sprint(leaks))

	if err := os.Remove(flag); err != nil {
		t.Fatal(err)
	}
	seq := s.Graph.LastSeq()
	steps.MustPost(s, work.ID, "helper retry last")
	retried := s.WaitFor("the retry reply", steps.CommandTimeout, func() (bool, string) {
		for _, m := range s.SUTMessages(work.ID) {
			if m.RequestSeq > seq && strings.Contains(m.PlainText(), "again as") {
				return true, ""
			}
		}
		return false, strings.Join(s.Transcript(work.ID, 3), " | ")
	})
	_, answered := steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout)
	s.Settle(3*time.Second, 15*time.Second)
	s.Check("F-TRN-4 once fixed, `helper retry last` runs the request once and it is answered once",
		retried && answered && len(s.Codex.CallsWithToken(tok)) == 1 && len(steps.AnswerPosts(s, work.ID, tok)) == 1,
		"1 Codex call, 1 answer", fmt.Sprintf("calls=%d answers=%d", len(s.Codex.CallsWithToken(tok)), len(steps.AnswerPosts(s, work.ID, tok))),
		s.Transcript(work.ID, 10)...)
}
