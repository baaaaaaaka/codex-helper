package teams

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type fakeASRTranscriber struct {
	calls []ASRTranscribeInput
	err   error
}

func (f *fakeASRTranscriber) TranscribeTeamsMedia(_ context.Context, input ASRTranscribeInput) (ASRTranscript, error) {
	f.calls = append(f.calls, input)
	if f.err != nil {
		return ASRTranscript{}, f.err
	}
	return ASRTranscript{
		Text:    "transcript for " + input.File.PromptPath,
		Model:   "qwen3-asr-0.6b",
		Backend: "test",
	}, nil
}

func TestPromptWithASRTranscriptsLabelsSpeechAsFallibleContext(t *testing.T) {
	prompt := promptWithASRTranscripts("typed request wins", []ASRTranscript{
		{
			SourceName: "voice-001.m4a",
			Text:       "测试 Qwen route",
			Language:   "auto",
			Speed:      "1.25x",
			Model:      "qwen3-asr-0.6b",
			Backend:    "cpu",
			Duration:   "12.4s",
		},
	})
	if !strings.HasPrefix(prompt, "typed request wins\n\nAutomatic local ASR transcript") {
		t.Fatalf("typed text should stay ahead of ASR context:\n%s", prompt)
	}
	for _, want := range []string{
		"may contain recognition errors",
		"mixed Chinese/English terms",
		"Voice/video clip: voice-001.m4a",
		"language=auto",
		"speed=1.25x",
		"model=qwen3-asr-0.6b",
		"backend=cpu",
		"duration=12.4s",
		"测试 Qwen route",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestASRTranscriptDisplayNameFallsBackToStableMediaLabel(t *testing.T) {
	if got := asrTranscriptDisplayName(ASRTranscript{SourceIndex: 7}); got != "media-007" {
		t.Fatalf("empty display name = %q, want media-007", got)
	}
	if got := asrTranscriptDisplayName(ASRTranscript{SourceIndex: 7, SourcePath: "/tmp/voice.m4a"}); got != "voice.m4a" {
		t.Fatalf("path display name = %q, want voice.m4a", got)
	}
}

func TestExecutionInputWithPreparedTeamsContextCombinesSpeechReferencesAndImages(t *testing.T) {
	input := executionInputWithPreparedTeamsContext(
		"please handle this",
		[]ReferencedMessage{{Sender: "Alex", Text: "quoted context", Fetched: true}},
		[]LocalAttachment{
			{Path: "/tmp/photo.jpg", PromptPath: ".codex-helper/photo.jpg", ContentType: "image/jpeg"},
			{Path: "/tmp/voice.m4a", PromptPath: ".codex-helper/voice.m4a", ContentType: "audio/mp4"},
		},
		[]ASRTranscript{{SourceName: "voice.m4a", Text: "speech text"}},
	)
	for _, want := range []string{
		"User message:",
		"please handle this",
		"Automatic local ASR transcript",
		"speech text",
		"Referenced Teams message for this turn",
		"quoted context",
		"Attached files saved locally for this turn",
		".codex-helper/voice.m4a",
	} {
		if !strings.Contains(input.Prompt, want) {
			t.Fatalf("prepared prompt missing %q:\n%s", want, input.Prompt)
		}
	}
	if !reflect.DeepEqual(input.ImagePaths, []string{"/tmp/photo.jpg"}) {
		t.Fatalf("image paths = %#v, want only the supported image", input.ImagePaths)
	}
}

func TestIsTeamsMediaAttachment(t *testing.T) {
	cases := []struct {
		name string
		file LocalAttachment
		want bool
	}{
		{name: "audio content type", file: LocalAttachment{Path: "/tmp/a.bin", ContentType: "audio/mp4; codecs=mp4a"}, want: true},
		{name: "video content type", file: LocalAttachment{Path: "/tmp/a.bin", ContentType: "video/mp4"}, want: true},
		{name: "extension fallback", file: LocalAttachment{Path: "/tmp/a.opus"}, want: true},
		{name: "prompt path extension fallback", file: LocalAttachment{PromptPath: ".codex-helper/a.webm"}, want: true},
		{name: "image", file: LocalAttachment{Path: "/tmp/a.jpg", ContentType: "image/jpeg"}, want: false},
		{name: "document", file: LocalAttachment{Path: "/tmp/a.txt", ContentType: "text/plain"}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTeamsMediaAttachment(tc.file); got != tc.want {
				t.Fatalf("isTeamsMediaAttachment(%#v) = %v, want %v", tc.file, got, tc.want)
			}
		})
	}
}

func TestTranscribeTeamsMediaAttachmentsFiltersAndDefaults(t *testing.T) {
	transcriber := &fakeASRTranscriber{}
	bridge := &Bridge{asrTranscriber: transcriber}
	files := []LocalAttachment{
		{Path: "/tmp/photo.jpg", PromptPath: ".codex-helper/photo.jpg", ContentType: "image/jpeg"},
		{Path: "/tmp/voice.m4a", PromptPath: ".codex-helper/voice.m4a", ContentType: "audio/mp4"},
		{Path: "/tmp/video.mp4", PromptPath: ".codex-helper/video.mp4", ContentType: "video/mp4"},
	}
	transcripts, err := bridge.transcribeTeamsMediaAttachments(context.Background(), &Session{ID: "s001"}, "turn-1", files)
	if err != nil {
		t.Fatalf("transcribeTeamsMediaAttachments error: %v", err)
	}
	if len(transcriber.calls) != 2 {
		t.Fatalf("transcriber calls = %d, want 2", len(transcriber.calls))
	}
	if transcriber.calls[0].SourceIndex != 1 || transcriber.calls[1].SourceIndex != 2 {
		t.Fatalf("source indexes = %d/%d, want 1/2", transcriber.calls[0].SourceIndex, transcriber.calls[1].SourceIndex)
	}
	for _, call := range transcriber.calls {
		if call.Language != defaultASRLanguage || call.Speed != defaultASRSpeed {
			t.Fatalf("call defaults = language %q speed %q", call.Language, call.Speed)
		}
	}
	if len(transcripts) != 2 || transcripts[0].SourceName != "voice.m4a" || transcripts[0].Language != defaultASRLanguage || transcripts[0].Speed != defaultASRSpeed {
		t.Fatalf("transcripts = %#v", transcripts)
	}
}

func TestASRProgressOutboxIsTransientAndSkippedOnInterrupt(t *testing.T) {
	store, err := teamstore.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close store: %v", err)
		}
	})
	ctx := context.Background()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Sessions["s001"] = teamstore.SessionContext{ID: "s001", TeamsChatID: "chat-1", Status: teamstore.SessionStatusActive}
		return nil
	}); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	turn, _, err := store.QueueTurn(ctx, teamstore.Turn{ID: "turn-asr", SessionID: "s001"})
	if err != nil {
		t.Fatalf("QueueTurn: %v", err)
	}
	outbox, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox-asr-progress",
		SessionID:   "s001",
		TurnID:      turn.ID,
		TeamsChatID: "chat-1",
		Kind:        teamsASRProgressKind,
		Body:        "transcribing",
	})
	if err != nil {
		t.Fatalf("QueueOutbox: %v", err)
	}
	if !outbox.UpgradeNonBlocking {
		t.Fatalf("ASR progress outbox should be upgrade non-blocking: %#v", outbox)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if teamstore.OutboxBlocksUpgrade(state, outbox, time.Now()) {
		t.Fatalf("ASR progress outbox blocks upgrade: %#v", outbox)
	}
	if _, err := store.MarkTurnInterrupted(ctx, turn.ID, "canceled by user"); err != nil {
		t.Fatalf("MarkTurnInterrupted: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after interrupt: %v", err)
	}
	if got := state.OutboxMessages[outbox.ID].Status; got != teamstore.OutboxStatusSkipped {
		t.Fatalf("ASR progress status = %q, want skipped", got)
	}
}
