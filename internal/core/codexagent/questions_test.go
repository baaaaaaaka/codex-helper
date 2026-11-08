package codexagent

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

func TestQuestionsNativeAsyncAnswerRecognition(t *testing.T) {
	for _, tc := range []struct {
		text string
		want int
	}{
		{`<send_user_message_question_reply>[{"questionItemId":"native-id","question":"Which?","answer":"A"}]</send_user_message_question_reply>`, 1},
		{`<send_user_message_question_reply>{"questionItemId":"native-id","question":"Which?","answer":"A"}</send_user_message_question_reply>`, 1},
		{`prefix <send_user_message_question_reply>[{"questionItemId":"native-id"}]</send_user_message_question_reply>`, 0},
		{`<send_user_message_question_reply>invalid</send_user_message_question_reply>`, 0},
	} {
		if ids := answeredIDs(tc.text); len(ids) != tc.want {
			t.Errorf("%s: %v", tc.text, ids)
		}
	}
}

func TestQuestionsNativeEventValidationAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]any, map[string]any)
		want   bool
	}{
		{"valid", func(map[string]any, map[string]any) {}, true},
		{"missing-thread", func(p, _ map[string]any) { delete(p, "threadId") }, false},
		{"missing-item", func(_, i map[string]any) { delete(i, "id") }, false},
		{"not-async", func(_, i map[string]any) { i["delivery"] = "default" }, false},
		{"no-questions", func(_, i map[string]any) { delete(i, "questions") }, false},
		{"empty-title", func(_, i map[string]any) { i["questions"] = []codex.AsyncQuestion{{Title: " "}} }, false},
		{"too-many-questions", func(_, i map[string]any) { i["questions"] = make([]codex.AsyncQuestion, 17) }, false},
		{"too-many-options", func(_, i map[string]any) {
			i["questions"] = []codex.AsyncQuestion{{Title: "Which?", Options: make([]string, 33)}}
		}, false},
		{"oversized", func(_, i map[string]any) { i["text"] = strings.Repeat("x", 128<<10) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := map[string]any{"type": "agentMessage", "id": "native-item", "delivery": "async", "questions": []codex.AsyncQuestion{{Title: "选择环境？", Options: []string{"测试", "生产"}}}}
			params := map[string]any{"threadId": "thread", "turnId": "turn", "item": item}
			tc.change(params, item)
			raw, err := json.Marshal(params)
			if err != nil {
				t.Fatal(err)
			}
			var events []engine.QuestionEvent
			a := &Agent{questionHandler: func(e engine.QuestionEvent) error { events = append(events, e); return nil }}
			a.observeQuestion(nil, codex.Notification{Method: "item/completed", Params: raw})
			if (len(events) == 1) != tc.want {
				t.Fatalf("events: %+v", events)
			}
			if tc.want {
				g := events[0].Group
				if g.Key != "async:thread:native-item" || g.ThreadID != "thread" || g.TurnID != "turn" || !g.Async || g.Questions[0].NativeID != `["request_user_input_async","native-item",0]` || g.Questions[0].Title != "选择环境？" || g.Questions[0].Options[1].Label != "生产" {
					t.Fatalf("native identity or original question changed: %+v", g)
				}
			}
		})
	}
}

func TestQuestionsNativeResolutionOnlyClosesMatchingRequest(t *testing.T) {
	srv := &codex.Server{}
	a := &Agent{questionEpoch: "epoch", questionRequests: map[string]questionRequest{}}
	key := a.requestKey(srv, json.RawMessage(`7`))
	a.questionRequests[key] = questionRequest{srv: srv, id: json.RawMessage(`7`)}
	var events []engine.QuestionEvent
	a.questionHandler = func(e engine.QuestionEvent) error { events = append(events, e); return nil }
	for _, id := range []int{8, 7, 7} {
		raw, _ := json.Marshal(map[string]any{"threadId": "thread", "requestId": id})
		a.observeQuestion(srv, codex.Notification{Method: "serverRequest/resolved", Params: raw})
	}
	if len(events) != 1 || events[0].Resolved != key || len(a.questionRequests) != 0 {
		t.Fatalf("duplicate/foreign resolution: %+v, pending=%d", events, len(a.questionRequests))
	}
}
