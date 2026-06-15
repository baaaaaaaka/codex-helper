package teams

import (
	"context"
	"errors"
	"path/filepath"
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
		".codex-helper/photo.jpg",
	} {
		if !strings.Contains(input.Prompt, want) {
			t.Fatalf("prepared prompt missing %q:\n%s", want, input.Prompt)
		}
	}
	for _, forbidden := range []string{
		".codex-helper/voice.m4a",
		"audio/mp4",
	} {
		if strings.Contains(input.Prompt, forbidden) {
			t.Fatalf("prepared prompt exposed raw Teams media %q:\n%s", forbidden, input.Prompt)
		}
	}
	if !reflect.DeepEqual(input.ImagePaths, []string{"/tmp/photo.jpg"}) {
		t.Fatalf("image paths = %#v, want only the supported image", input.ImagePaths)
	}
}

func TestExecutionInputWithPreparedTeamsContextStressMixedInputsDoesNotExposeRawTeamsMedia(t *testing.T) {
	input := executionInputWithPreparedTeamsContext(
		"typed text with mixed inputs",
		[]ReferencedMessage{{Sender: "Dana", Text: "quoted context", Fetched: true}},
		[]LocalAttachment{
			{Path: "/tmp/photo.jpg", PromptPath: ".codex-helper/photo.jpg", ContentType: "image/jpeg"},
			{Path: "/tmp/spec.pdf", PromptPath: ".codex-helper/spec.pdf", ContentType: "application/pdf"},
			{Path: "/tmp/voice.m4a", PromptPath: ".codex-helper/voice.m4a", ContentType: "audio/mp4"},
			{Path: "/tmp/demo.mp4", PromptPath: ".codex-helper/demo.mp4", ContentType: "video/mp4"},
		},
		[]ASRTranscript{
			{SourceName: "voice.m4a", Text: "first speech transcript", ContentType: "audio/mp4"},
			{SourceName: "demo.mp4", Text: "second video transcript", ContentType: "video/mp4"},
		},
	)

	requirePlainTextInOrder(t, input.Prompt,
		"User message:",
		"typed text with mixed inputs",
		"Automatic local ASR transcript",
		"first speech transcript",
		"second video transcript",
		"Referenced Teams message for this turn",
		"quoted context",
		"Attached files saved locally for this turn",
		".codex-helper/photo.jpg",
		".codex-helper/spec.pdf",
	)
	for _, forbidden := range []string{
		".codex-helper/voice.m4a",
		".codex-helper/demo.mp4",
		"audio/mp4",
		"video/mp4",
	} {
		if strings.Contains(input.Prompt, forbidden) {
			t.Fatalf("mixed-input prompt exposed raw Teams media %q:\n%s", forbidden, input.Prompt)
		}
	}
	if !reflect.DeepEqual(input.ImagePaths, []string{"/tmp/photo.jpg"}) {
		t.Fatalf("image paths = %#v, want only photo", input.ImagePaths)
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

func TestTranscribeTeamsMediaAttachmentsRequiresConfiguredASRForMedia(t *testing.T) {
	bridge := &Bridge{}
	_, err := bridge.transcribeTeamsMediaAttachments(context.Background(), &Session{ID: "s001"}, "turn-1", []LocalAttachment{
		{Path: "/tmp/photo.jpg", PromptPath: ".codex-helper/photo.jpg", ContentType: "image/jpeg"},
		{Path: "/tmp/voice.m4a", PromptPath: ".codex-helper/voice.m4a", ContentType: "audio/mp4"},
	})
	if !errors.Is(err, errASRCommandNotConfigured) {
		t.Fatalf("transcribeTeamsMediaAttachments error = %v, want ASR not configured", err)
	}
}

func TestTranscribeTeamsMediaAttachmentsTreatsEmptyCommandASRAsUnconfigured(t *testing.T) {
	bridge := &Bridge{asrTranscriber: NewCommandASRTranscriber("", nil)}
	_, err := bridge.transcribeTeamsMediaAttachments(context.Background(), &Session{ID: "s001"}, "turn-1", []LocalAttachment{
		{Path: "/tmp/voice.m4a", PromptPath: ".codex-helper/voice.m4a", ContentType: "audio/mp4"},
	})
	if !errors.Is(err, errASRCommandNotConfigured) {
		t.Fatalf("transcribeTeamsMediaAttachments error = %v, want ASR not configured", err)
	}
}

func TestTeamsASRFailureUserMessageClassifiesActionableFailures(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantKind  teamsASRFailureKind
		wantParts []string
	}{
		{
			name:     "disk",
			err:      managedASRDiskSpaceError{Path: "/cache/asr", NeedBytes: 2 * 1024 * 1024 * 1024, FreeBytes: 128 * 1024 * 1024},
			wantKind: teamsASRFailureInsufficientDisk,
			wantParts: []string{
				"not enough free disk space",
				"Next:",
				"free space",
				"Diagnostic:",
			},
		},
		{
			name:     "setup busy",
			err:      managedASRSetupBusyError{Scope: "Teams speech recognition"},
			wantKind: teamsASRFailureSetupBusy,
			wantParts: []string{
				"already running",
				"retry",
				"Diagnostic:",
			},
		},
		{
			name:     "download integrity",
			err:      managedASRDownloadIntegrityError{Label: "llama.cpp runtime", Err: errors.New("sha256 bad")},
			wantKind: teamsASRFailureDownloadIntegrity,
			wantParts: []string{
				"failed integrity verification",
				"discard",
				"sha256 bad",
			},
		},
		{
			name:     "native compat exhausted",
			err:      managedASRLlamaValidationError{Err: errors.New("/lib64/libc.so.6: version `GLIBC_2.38' not found")},
			wantKind: teamsASRFailureNativeCompatFailed,
			wantParts: []string{
				"native library or loader compatibility",
				"GLIBC_2.38",
			},
		},
		{
			name:     "native symbol newer than managed profiles",
			err:      managedASRLlamaValidationError{Err: errors.New("/lib64/libc.so.6: version `GLIBC_2.40' not found")},
			wantKind: teamsASRFailureUnsupportedRuntime,
			wantParts: []string{
				"newer than the managed compatibility profiles",
				"GLIBC_2.40",
			},
		},
		{
			name:     "missing managed binary",
			err:      errors.New("run llama.cpp Teams speech recognition: fork/exec /home/user/.cache/codex-helper/teams-asr/qwen_qwen3-asr-0.6b/llama/runtime/llama-b9437/llama-mtmd-cli: no such file or directory"),
			wantKind: teamsASRFailureCachePreparation,
			wantParts: []string{
				"managed ASR runtime cache is incomplete",
				"reinstall the managed runtime",
				"llama-mtmd-cli",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			notice := classifyTeamsASRFailure(tc.err)
			if notice.Kind != tc.wantKind {
				t.Fatalf("kind = %q, want %q; notice=%#v", notice.Kind, tc.wantKind, notice)
			}
			message := teamsASRFailureUserMessage(tc.err)
			for _, want := range tc.wantParts {
				if !strings.Contains(message, want) {
					t.Fatalf("message missing %q:\n%s", want, message)
				}
			}
		})
	}
}

func TestTeamsASRStatusLineIsUserLevelAndDoesNotLeakConfiguration(t *testing.T) {
	cases := []struct {
		name        string
		transcriber ASRTranscriber
		want        string
	}{
		{name: "managed default", transcriber: NewManagedQwenASRTranscriber(), want: "automatic backend selection"},
		{name: "managed llama", transcriber: NewManagedQwenASRTranscriber(ManagedASRConfig{Backend: managedASRBackendLlama}), want: "llama.cpp opt-in backend"},
		{name: "managed auto", transcriber: NewManagedQwenASRTranscriber(ManagedASRConfig{Backend: managedASRBackendAuto}), want: "automatic backend selection"},
		{name: "developer override", transcriber: &CommandASRTranscriber{Command: "/tmp/asr"}, want: "developer override configured"},
		{name: "missing", transcriber: nil, want: "not ready"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := teamsASRStatusLine(tc.transcriber)
			if !strings.Contains(got, tc.want) {
				t.Fatalf("ASR status line = %q, want %q", got, tc.want)
			}
			for _, forbidden := range []string{"--asr-command", "--asr-arg", "CODEX_HELPER_TEAMS_ASR", "{input}", "{threads}"} {
				if strings.Contains(got, forbidden) {
					t.Fatalf("ASR status leaked implementation detail %q: %q", forbidden, got)
				}
			}
		})
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

func TestASRProgressStartsAfterDelayAndHeartbeatIsConcise(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	cacheRoot := t.TempDir()
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.asrTranscriber = NewManagedQwenASRTranscriber(ManagedASRConfig{CacheRoot: cacheRoot})
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}

	stopProgress := bridge.startTeamsASRProgressLoop(context.Background(), session, "turn-asr-delay", 1, LocalAttachment{
		Path:       filepath.Join(cacheRoot, "attachment-001.f4a"),
		PromptPath: "attachment-001.f4a",
	})
	time.Sleep(20 * time.Millisecond)
	stopProgress()
	if got := strings.TrimSpace(sentPlainJoined(*sent)); got != "" {
		t.Fatalf("ASR progress should not be sent before the heartbeat delay, got:\n%s", got)
	}

	if err := bridge.queueTeamsASRProgress(context.Background(), session, "turn-asr", 1, LocalAttachment{
		Path:       filepath.Join(cacheRoot, "attachment-001.f4a"),
		PromptPath: "attachment-001.f4a",
	}, 61*time.Second, 1); err != nil {
		t.Fatalf("queueTeamsASRProgress error: %v", err)
	}
	got := sentPlainJoined(*sent)
	for _, want := range []string{
		"Still transcribing Teams media before running Codex.",
		"Clip: attachment-001.f4a",
		"Elapsed: 1m",
		"Still running; Codex will start after transcription finishes.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("heartbeat progress missing %q:\n%s", want, got)
		}
	}
	for _, forbidden := range []string{
		"Local speech recognition runtime",
		"Cache:",
		"Disk preflight",
		"First-time setup",
		"private Python/ASR environment",
		"local Qwen speech model",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("heartbeat progress should not expose setup detail %q:\n%s", forbidden, got)
		}
	}
}

func TestPromptTextStripsVisibleASRTranscriptAnnotation(t *testing.T) {
	msg := ChatMessage{ID: "message-voice"}
	msg.Body.ContentType = "html"
	msg.Body.Content = `<p>please handle this voice clip</p><attachment id="audio-card-1"></attachment>`
	msg.Attachments = []MessageAttachment{{
		ID:          "audio-card-1",
		ContentType: "application/vnd.microsoft.card.audio",
		Content:     `{"media":[{"url":"https://graph.microsoft.com/v1.0/chats/chat-1/messages/message-voice/hostedContents/audio-1/$value"}]}`,
	}}
	body, ok := userAnnotatedASRTranscriptMessageHTML(msg, []ASRTranscript{{SourceName: "attachment-001.f4a", Text: "recognized speech"}})
	if !ok {
		t.Fatal("userAnnotatedASRTranscriptMessageHTML returned !ok")
	}
	if plain := PlainTextFromTeamsHTML(body); !strings.Contains(plain, teamsASRTranscriptAnnotationLabel) || !strings.Contains(plain, "recognized speech") {
		t.Fatalf("annotated body missing visible ASR transcript:\n%s", plain)
	}
	if got := promptTextFromTeamsMessageHTML(body); got != "please handle this voice clip" {
		t.Fatalf("promptTextFromTeamsMessageHTML() = %q, want original typed prompt only", got)
	}
	routed := ChatMessage{}
	routed.Body.Content = body
	if got := commandRouteTextFromTeamsMessage(routed, ""); got != "" {
		t.Fatalf("command route text = %q, want edited user-marker message ignored by router", got)
	}
}
