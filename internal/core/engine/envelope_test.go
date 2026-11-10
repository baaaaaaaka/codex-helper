package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// TestCodexInputMatchesV0120 compares the Codex input of fixed messages
// with v0.1.20's bytes (testdata/v0120_envelope.json: the output of
// v0.1.20 executionInputWithPreparedTeamsContext for the same inputs, its
// outbound root replaced by <ROOT>). The only differences are the owner's:
// the `helper reload now` line is gone and the delegation line names native
// subagents first (SPEC F-TRN-14, ae23659).
func TestCodexInputMatchesV0120(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "v0120_envelope.json"))
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]struct {
		Prompt string   `json:"prompt"`
		Images []string `json:"images"`
	}
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	owner := strings.NewReplacer(
		"- Tell the user to send `helper reload now` only for source-checkout development reloads when a local `codex-helper` source tree is available.\n", "",
		"- For cross-machine agent delegation,", "- Subagent requests use native Codex subagents. For cross-machine agent delegation,",
	)
	dir := t.TempDir()
	write := func(name, text string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	quote := func(idx int, qt quoteText) store.InboundFile {
		return store.InboundFile{Idx: idx, Name: "referenced message", Kind: "file", State: "ready", Quote: &store.QuoteReference{N: 1}, ResolvedQuote: &qt}
	}
	img := store.InboundFile{Idx: 0, Name: "attachment-1", Kind: "image", State: "ready", LocalPath: "/in/0-attachment-1.png", Ref: store.FileReference{Kind: 1, Ref: "x"}}
	doc := store.InboundFile{Idx: 1, Name: "design notes.txt", Kind: "file", State: "ready", LocalPath: "/in/1-design notes.txt", Ref: store.FileReference{Name: "design notes.txt", Ref: "y"}}
	long := strings.Repeat("long quoted line with words ", 100)
	voice := func(idx int, name, text string) store.InboundFile {
		return store.InboundFile{Idx: idx, Name: name, Kind: "voice", State: "ready", LocalPath: write(name+".txt", text), Ref: store.FileReference{Kind: 2, Name: name, Ref: "v"}}
	}
	cases := map[string]struct {
		body  string
		files []store.InboundFile
	}{
		"text":       {"fix the bug\n", nil},
		"image-only": {"", []store.InboundFile{img}},
		"files":      {"Please look", []store.InboundFile{img, doc}},
		"quotes": {"what about this?", []store.InboundFile{
			quote(0, quoteText{From: "Alice Example", Time: "2026-11-06T10:00:00.000Z", Text: "original text\nline two  ", Fetched: true}),
			quote(1, quoteText{From: "Bob Example", Text: "preview only"}),
		}},
		"quote-long": {"", []store.InboundFile{
			quote(0, quoteText{From: "Alice Example", Time: "2026-11-06T10:00:00.000Z", Text: long, Fetched: true, FullPath: "/in/referenced-message-001.txt"}),
		}},
		"voice":      {"", []store.InboundFile{voice(0, "voice.ogg", "hello from voice")}},
		"voice-text": {"and this", []store.InboundFile{voice(0, "a.ogg", "first clip"), voice(1, "b.ogg", "second\nclip")}},
	}
	if len(cases) != len(want) {
		t.Fatalf("%d cases, %d in the v0.1.20 dump", len(cases), len(want))
	}
	for name, c := range cases {
		w, ok := want[name]
		if !ok {
			t.Fatalf("case %s is not in the v0.1.20 dump", name)
		}
		got, images := codexInput(c.body, c.files, "<ROOT>")
		if exp := owner.Replace(w.Prompt); got != exp {
			t.Errorf("%s: prompt differs from v0.1.20\n--- got\n%s\n--- want\n%s", name, got, exp)
		}
		if !slices.Equal(images, w.Images) {
			t.Errorf("%s: images %v, v0.1.20 %v", name, images, w.Images)
		}
	}
}

func TestCodexInputWithoutRootIsUserSectionOnly(t *testing.T) {
	got, _ := codexInput("hello", nil, "")
	if got != "User message:\nhello" {
		t.Fatalf("got %q", got)
	}
	if got, _ := codexInput("", nil, ""); got != "" {
		t.Fatalf("empty prompt: %q", got)
	}
}

func TestCodexInputListsUnusableAttachmentsAndFailedQuotes(t *testing.T) {
	files := []store.InboundFile{
		{Idx: 0, Name: "gone.pdf", Kind: "file", State: "failed", Error: "the attachment could not be downloaded"},
		{Idx: 1, Name: "referenced message 1", Kind: "file", State: "failed", Quote: &store.QuoteReference{N: 1, ID: "m1", From: "Ann", Text: "preview text"}},
		{Idx: 2, Name: "referenced message 2", Kind: "file", State: "failed", Quote: &store.QuoteReference{N: 2, ID: "m2"}},
	}
	got, images := codexInput("Please look", files, "/out")
	if len(images) != 0 {
		t.Fatalf("images %v", images)
	}
	for _, want := range []string{
		"User message:\nPlease look\n\nAttachments that could not be used:\n- gone.pdf: the attachment could not be downloaded\n\nReferenced Teams messages for this turn.",
		"1. From: Ann; Source: Teams reference preview\n   preview text\n",
		"2. Source: Teams reference preview\n   Referenced message could not be read from Graph: the request failed\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("input lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "referenced message 1") {
		t.Fatalf("a quote is listed as an attachment:\n%s", got)
	}
}

func TestAdmittedFilesAppendsQuotes(t *testing.T) {
	m := channel.InboundMsg{
		Files:  []channel.FileRef{{Kind: channel.FileDoc, Name: "a.txt", Ref: "share:x"}},
		Quotes: []channel.Quote{{ID: "m1", FromName: "Ann", Text: "hi", Time: "2026-01-01T00:00:00Z"}, {ID: "m2", Forwarded: true}},
	}
	rows := admittedFiles(m)
	if len(rows) != 3 {
		t.Fatalf("rows %+v", rows)
	}
	if _, ok := quoteOf(rows[0]); ok {
		t.Fatal("a file row reads as a quote")
	}
	q, ok := quoteOf(rows[1])
	if !ok || rows[1].Idx != 1 || q.N != 1 || q.ID != "m1" || q.From != "Ann" || q.Time != "2026-01-01T00:00:00Z" || q.Fwd {
		t.Fatalf("first quote %+v %+v", rows[1], q)
	}
	if q, ok := quoteOf(rows[2]); !ok || rows[2].Idx != 2 || q.N != 2 || !q.Fwd || rows[2].State != "pending" {
		t.Fatalf("second quote %+v %+v", rows[2], q)
	}
}

// Ported from v0.1.20 TestModelAPIKeyPreflightRejectsRawKeysButAllowsRefs.
func TestContainsRawAPIKey(t *testing.T) {
	for _, text := range []string{
		"model setup mimo --api-key sk-test",
		"model setup mimo api_key=sk-test",
		"Authorization: Bearer sk-test",
		"sk-1234567890abcdef1234567890abcdef",
		"please use `sk-proj-ABCDEFGHIJKLMNOP` for this",
	} {
		if !containsRawAPIKey(text) {
			t.Fatalf("containsRawAPIKey(%q) = false", text)
		}
	}
	for _, text := range []string{
		"model setup mimo --api-key-env MIMO_API_KEY",
		"model setup mimo --api-key-stdin",
		"new /tmp/work --model mimo25",
		"the sk- prefix marks OpenAI keys",
		"sk-short",
	} {
		if containsRawAPIKey(text) {
			t.Fatalf("containsRawAPIKey(%q) = true", text)
		}
	}
}

func TestScreen(t *testing.T) {
	files := func(kind channel.FileKind, n int) []channel.FileRef {
		out := make([]channel.FileRef, n)
		for i := range out {
			out[i] = channel.FileRef{Kind: kind}
		}
		return out
	}
	prompt := func(m channel.InboundMsg) classified {
		return classified{msg: m, quoteID: "q", in: store.InboundMsg{Kind: store.InPrompt, Sender: store.SenderOwner, Body: m.Text}}
	}
	for _, tc := range []struct {
		name  string
		msg   channel.InboundMsg
		want  string
		quote string
	}{
		{"5 images", channel.InboundMsg{Files: files(channel.FileImage, 5)}, "", ""},
		{"6 media", channel.InboundMsg{Files: append(files(channel.FileImage, 4), files(channel.FileVoice, 2)...)}, textTooManyHosted, "q"},
		{"5 files", channel.InboundMsg{Files: append(files(channel.FileDoc, 5), files(channel.FileUnsupported, 3)...)}, "", ""},
		{"6 files", channel.InboundMsg{Files: files(channel.FileDoc, 6)}, textTooManyFiles, "q"},
		{"4 quotes", channel.InboundMsg{Quotes: make([]channel.Quote, 4)}, textTooManyQuotes, "q"},
		{"key", channel.InboundMsg{Text: "use sk-1234567890abcdef1234", Files: files(channel.FileImage, 9)}, textRawAPIKey, ""},
	} {
		c := prompt(tc.msg)
		screen(&c)
		if c.reject != tc.want || c.rejectQuote != tc.quote {
			t.Fatalf("%s: reject %q quote %q", tc.name, c.reject, c.rejectQuote)
		}
		if tc.want != "" && c.in.Kind != store.InCommand {
			t.Fatalf("%s: refused message kind %s", tc.name, c.in.Kind)
		}
		if tc.want == textRawAPIKey && c.in.Body != redactedBody {
			t.Fatalf("%s: body stored as %q", tc.name, c.in.Body)
		}
	}
	// Chatter, our own posts and delegation records are not screened.
	for _, c := range []classified{
		{msg: channel.InboundMsg{Text: "sk-1234567890abcdef1234"}, chatter: true, in: store.InboundMsg{Kind: store.InCommand, Sender: store.SenderOther}},
		{msg: channel.InboundMsg{Text: "sk-1234567890abcdef1234"}, in: store.InboundMsg{Kind: store.InPrompt, Sender: store.SenderSelf}},
		{msg: channel.InboundMsg{Text: "sk-1234567890abcdef1234"}, delegation: "d", in: store.InboundMsg{Kind: store.InPrompt, Sender: store.SenderOwner}},
	} {
		screen(&c)
		if c.reject != "" {
			t.Fatalf("screened %+v", c)
		}
	}
}
