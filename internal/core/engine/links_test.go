package engine

import (
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestNamedLinksAdmission(t *testing.T) {
	r := newRig(t)
	r.control()
	links := []channel.Link{{Text: "任务说明", URL: "https://example.com/task"}}
	for _, tc := range []struct {
		name, text, want             string
		control, group, queue, other bool
	}{
		{name: "ordinary", text: "看任务说明", want: "看任务说明"},
		{name: "queue", text: "helper queue 看任务说明", want: "看任务说明", queue: true},
		{name: "control ask", text: "ask 看任务说明", want: "看任务说明", control: true},
		{name: "control fallback", text: "看任务说明", want: "看任务说明", control: true},
		{name: "group", text: "@codex 看任务说明", want: "@codex 看任务说明", group: true},
		{name: "group queue", text: "@codex helper queue 看任务说明", want: "看任务说明", group: true, queue: true},
		{name: "coworker", text: "@codex 看任务说明", want: "@codex 看任务说明", group: true, other: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind := store.KindWork
			if tc.control {
				kind = store.KindControl
			}
			m := channel.InboundMsg{ID: tc.name, ByOwner: !tc.other, From: "person", Text: tc.text, Links: links}
			if err := r.e.update(func(tx *store.Tx) error {
				c, keep, err := r.e.classify(tx, 123, kind, tc.group, m)
				if err != nil {
					return err
				}
				screen(&c)
				want := channel.WithLinks(tc.want, links)
				if !keep || c.in.Kind != store.InPrompt || c.in.Body != want || c.in.Queue != tc.queue {
					t.Errorf("%+v want %q", c, want)
				}
				// Only the persisted body is needed by a restored or steered turn.
				prompt, _ := codexInput(c.in.Body, nil, "")
				if !strings.Contains(prompt, "任务说明: https://example.com/task") {
					t.Error(prompt)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNamedLinksKeepCommandsAndScreenSecrets(t *testing.T) {
	for _, tc := range []struct {
		name, body, command string
		kind                string
		secret              bool
	}{
		{"command", "helper cancel", cmdCancelName, store.InCommand, false},
		{"answer", "helper answer Q1 use docs", cmdAnswer, store.InCommand, false},
		{"skip", "helper answer Q1 --skip", cmdAnswer, store.InCommand, false},
		{"secret", "see docs", "", store.InPrompt, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := "https://example.com"
			c := classified{msg: channel.InboundMsg{Text: tc.body, Links: []channel.Link{{Text: "docs", URL: url}}}, in: store.InboundMsg{Sender: store.SenderOwner, Kind: tc.kind, Body: tc.body}, cmd: parseWork(tc.body)}
			if tc.secret {
				c.msg.Text = "use sk-" + strings.Repeat("a", 48)
			}
			screen(&c)
			if tc.secret {
				if c.in.Body != redactedBody || c.reject == "" {
					t.Fatal(c)
				}
				return
			}
			if c.cmd.name != tc.command {
				t.Fatal(c)
			}
			if tc.name == "answer" {
				if !strings.Contains(c.in.Body, url) {
					t.Fatal(c)
				}
			} else if c.in.Body != tc.body {
				t.Fatal(c)
			}
		})
	}
}
