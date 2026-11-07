package harness

import (
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestFloatingMessagesMoveToTheEndOfTheirChat(t *testing.T) {
	msg := func(id string, o fakegraph.Origin, text string) fakegraph.Message {
		return fakegraph.Message{ID: id, Origin: o, ContentType: "text", Content: text}
	}
	o := GoldenOptions{Floating: []Float{{Text: "summary", Reason: "timing"}}}
	if err := o.validate(); err != nil {
		t.Fatal(err)
	}
	in := []fakegraph.Message{
		msg("1", fakegraph.OriginSUT, "migrated"),
		msg("2", fakegraph.OriginSUT, "summary 1"),
		msg("3", fakegraph.OriginUser, "new"),
		msg("4", fakegraph.OriginUser, "a summary the owner wrote"),
		msg("5", fakegraph.OriginSUT, "created"),
		msg("6", fakegraph.OriginSUT, "summary 2"),
	}
	out, notes := o.applyFloating(in)
	var got string
	for _, m := range out {
		got += m.ID
	}
	if got != "134526" || len(notes) != 1 {
		t.Fatalf("order %s, notes %q", got, notes)
	}
	if err := (GoldenOptions{Floating: []Float{{Text: "x"}}}).validate(); err == nil {
		t.Fatal("a floating message without a reason was accepted")
	}
}
