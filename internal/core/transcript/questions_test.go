package transcript

import (
	"strings"
	"testing"
)

func TestQuestionsAsyncItemsAreNotMirroredAsAnswers(t *testing.T) {
	line := []byte(`{"timestamp":"2026-11-08T00:00:00Z","type":"response_item","payload":{"type":"message","id":"question","role":"assistant","phase":"final_answer","delivery":"async","questions":[{"title":"Choose?"}],"content":[{"type":"output_text","text":"Choose?"}]}}`)
	if ev := Parse(line); ev.Kind != KindAsyncMessage {
		t.Fatal(ev)
	}
}

func TestQuestionsAsyncTaskCompleteFallback(t *testing.T) {
	async := strings.Replace(selAgent("q", "final_answer", "Choose?"), `"phase":"final_answer",`, `"phase":"final_answer","delivery":"async",`, 1)
	for _, tc := range []struct{ name, regular, last, want string }{
		{"async only", "", "Choose?", "U:task"},
		{"real final", selAgent("final", "final_answer", "Done"), "Done", "U:task | F:Done"},
		{"legacy final", "", "Done", "U:task | F:Done"},
		{"equal legitimate final", selAgent("final", "final_answer", "Choose?"), "Choose?", "U:task | F:Choose?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := selRead(t, nil, selTurnStart("turn"), selPrompt("prompt", "task"), selUserItem("turn", "native"), async, tc.regular, selComplete("turn", tc.last))
			if got := keys(out); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
