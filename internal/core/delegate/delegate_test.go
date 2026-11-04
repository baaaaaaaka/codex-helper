package delegate

import (
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/teams/delegation"
)

func requestText(t *testing.T, machine string, now time.Time) (delegation.Record, string) {
	t.Helper()
	r, err := delegation.NewRequestRecord("sess-1", "turn-1", "", nil, machine,
		delegation.TaskSpec{Title: "Build", Objective: "check the build", Constraints: []string{"read only"}}, now)
	if err != nil {
		t.Fatal(err)
	}
	return r, delegation.RenderRecordHTML(r)
}

func TestRequestDecodedAndAnsweredInTheWireFormat(t *testing.T) {
	now := time.Now().UTC()
	p := New("m1")
	req, text := requestText(t, "m1", now)
	got, ok := p.Decode(text, now)
	if !ok || got.Kind != engine.DelegRequest || got.ID != req.DelegationID || !got.Mine || got.Expired {
		t.Fatalf("decode request: %+v %v", got, ok)
	}
	if other, _ := New("m2").Decode(text, now); other.Mine {
		t.Fatal("a request for m1 is mine on m2")
	}
	if late, _ := p.Decode(text, now.Add(delegation.DefaultDelegationTTL+time.Minute)); !late.Expired {
		t.Fatal("expired request not flagged")
	}
	prompt, err := p.Prompt(text)
	if err != nil || !strings.Contains(prompt, "check the build") || !strings.Contains(prompt, "- read only") {
		t.Fatalf("prompt %q, %v", prompt, err)
	}
	for _, c := range []struct {
		kind  string
		build func() (string, error)
	}{
		{engine.DelegClaim, func() (string, error) { return p.Claim(text, "w-1", now) }},
		{engine.DelegStatus, func() (string, error) { return p.Running(text, "w-1", now) }},
		{engine.DelegResult, func() (string, error) { return p.Result(text, "w-1", true, "all green", now) }},
	} {
		out, err := c.build()
		if err != nil {
			t.Fatal(err)
		}
		// The engine posts the record as a code span; the requester parses
		// it from the message text.
		r, ok := p.Decode("`"+out+"`", now)
		if !ok || r.Kind != c.kind || r.ID != req.DelegationID || !r.Mine || r.Worker != "w-1" {
			t.Fatalf("%s record decodes as %+v %v", c.kind, r, ok)
		}
	}
	res, _ := p.Result(text, "w-1", true, "all green", now)
	rec, _ := delegation.ParseRecordMessage(res)
	if rec.Status != delegation.StateComplete || rec.Body != "all green" {
		t.Fatalf("result %+v", rec)
	}
	// Claims are deterministic, so a rebuilt claim matches the posted one.
	c1, _ := p.Claim(text, "w-1", now)
	c2, _ := p.Claim(text, "w-1", now.Add(time.Minute))
	r1, _ := delegation.ParseRecordMessage(c1)
	r2, _ := delegation.ParseRecordMessage(c2)
	if r1.ClaimID == "" || r1.ClaimID != r2.ClaimID {
		t.Fatalf("claim ids %q %q", r1.ClaimID, r2.ClaimID)
	}
	failed, _ := p.Result(text, "w-1", false, "interrupted", now)
	if rec, _ := delegation.ParseRecordMessage(failed); rec.Status != delegation.StateBlocked {
		t.Fatalf("failed result status %q", rec.Status)
	}
	if _, ok := p.Decode("hello", now); ok {
		t.Fatal("plain text decoded as a record")
	}
}
