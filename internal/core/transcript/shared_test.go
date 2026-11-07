package transcript

import (
	"strings"
	"testing"
)

func TestSharedTurnMirrorsLocalInputsButNotDuplicateAnswers(t *testing.T) {
	for _, firstLocal := range []bool{true, false} {
		t.Run(map[bool]string{true: "tui-started", false: "teams-started"}[firstLocal], func(t *testing.T) {
			first, client := "teams original", "cxp-initial"
			if firstLocal {
				first, client = "tui original", "tui-initial"
			}
			lines := []string{selTurnStart("shared"), selPrompt("initial", first), selUserItem("shared", client),
				selPrompt("extra", "from teams"), selUserItem("shared", "cxp-steer-request"),
				selPrompt("local", "from tui"), selUserItem("shared", "tui-steer"),
				selAgent("answer", "final_answer", "one answer"), selComplete("shared", "one answer")}
			tr := NewTracker("", 0, 0, func(client, turn string) bool {
				return strings.HasPrefix(client, "cxp-") || client == "" && turn == "shared"
			})
			got := keys(readAll(t, writeLines(t, lines...), tr))
			want := "U:from tui"
			if firstLocal {
				want = "U:tui original | " + want
			}
			if got != want {
				t.Fatalf("%q, want %q", got, want)
			}
		})
	}
}

func TestSteerOnlyInputKeepsForeignGoalAnswer(t *testing.T) {
	lines := []string{selTurnStart("goal"), selPrompt("input", "from teams"), selUserItem("goal", "cxp-steer-request"), selAgent("answer", "final_answer", "goal answer"), selComplete("goal", "goal answer")}
	tr := NewTracker("", 0, 0, func(client, _ string) bool { return strings.HasPrefix(client, "cxp-") })
	if got := keys(readAll(t, writeLines(t, lines...), tr)); got != "F:goal answer" {
		t.Fatal(got)
	}
}
