package teams

import (
	"context"
	"errors"
	"fmt"
	"html"
	"path/filepath"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	defaultASRSpeed              = "1.25x"
	defaultASRLanguage           = "auto"
	teamsASRProgressKind         = "asr-progress"
	teamsASRProgressNoticeAfter  = 60 * time.Second
	teamsASRProgressRepeatAfter  = 30 * time.Second
	teamsASRTranscriptPromptLead = "Automatic local ASR transcript from Microsoft Teams media."
)

const teamsASRTranscriptAnnotationLabel = "🎙️ Speech-to-text transcript (automatic; may contain recognition errors)"

type ASRTranscript struct {
	SourceIndex  int    `json:"source_index,omitempty"`
	SourceName   string `json:"source_name,omitempty"`
	SourcePath   string `json:"source_path,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	Duration     string `json:"duration,omitempty"`
	Text         string `json:"text,omitempty"`
	Language     string `json:"language,omitempty"`
	Speed        string `json:"speed,omitempty"`
	Model        string `json:"model,omitempty"`
	Backend      string `json:"backend,omitempty"`
	TranscriptID string `json:"transcript_id,omitempty"`
	SourceHash   string `json:"source_hash,omitempty"`
	Warning      string `json:"warning,omitempty"`
}

type ASRTranscribeInput struct {
	Session     *Session
	TurnID      string
	SourceIndex int
	File        LocalAttachment
	Language    string
	Speed       string
}

type ASRTranscriber interface {
	TranscribeTeamsMedia(ctx context.Context, input ASRTranscribeInput) (ASRTranscript, error)
}

func isTeamsMediaAttachment(file LocalAttachment) bool {
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(file.ContentType, ";")[0]))
	switch {
	case strings.HasPrefix(contentType, "audio/"), strings.HasPrefix(contentType, "video/"):
		return true
	}
	switch strings.ToLower(filepath.Ext(firstNonEmptyString(file.PromptPath, file.Path))) {
	case ".aac", ".aiff", ".flac", ".m4a", ".mka", ".mp3", ".mp4", ".oga", ".ogg", ".opus", ".wav", ".webm", ".mov", ".mkv":
		return true
	default:
		return false
	}
}

func asrTranscriptDisplayName(t ASRTranscript) string {
	if name := strings.TrimSpace(t.SourceName); name != "" {
		return name
	}
	if path := strings.TrimSpace(t.SourcePath); path != "" {
		base := filepath.Base(path)
		if base != "" && base != "." && base != string(filepath.Separator) {
			return base
		}
	}
	return fmt.Sprintf("media-%03d", t.SourceIndex)
}

func promptWithASRTranscripts(text string, transcripts []ASRTranscript) string {
	text = strings.TrimSpace(text)
	if len(transcripts) == 0 {
		return text
	}
	var b strings.Builder
	if text != "" {
		b.WriteString(text)
		b.WriteString("\n\n")
	}
	b.WriteString(teamsASRTranscriptPromptLead)
	b.WriteString(" The transcript may contain recognition errors, especially names, code identifiers, paths, commands, acronyms, and mixed Chinese/English terms. Prefer explicit typed text over ASR text when they conflict, and ask for clarification when a critical command or identifier is ambiguous.\n")
	for i, transcript := range transcripts {
		if i > 0 {
			b.WriteString("\n")
		}
		label := "Voice/video clip"
		if len(transcripts) > 1 {
			label = fmt.Sprintf("Voice/video clip %d", i+1)
		}
		b.WriteString("\n")
		b.WriteString(label)
		b.WriteString(": ")
		b.WriteString(asrTranscriptDisplayName(transcript))
		var attrs []string
		if transcript.Language != "" {
			attrs = append(attrs, "language="+transcript.Language)
		} else {
			attrs = append(attrs, "language="+defaultASRLanguage)
		}
		if transcript.Speed != "" {
			attrs = append(attrs, "speed="+transcript.Speed)
		} else {
			attrs = append(attrs, "speed="+defaultASRSpeed)
		}
		if transcript.Model != "" {
			attrs = append(attrs, "model="+transcript.Model)
		}
		if transcript.Backend != "" {
			attrs = append(attrs, "backend="+transcript.Backend)
		}
		if transcript.Duration != "" {
			attrs = append(attrs, "duration="+transcript.Duration)
		}
		if len(attrs) > 0 {
			b.WriteString(" (")
			b.WriteString(strings.Join(attrs, ", "))
			b.WriteString(")")
		}
		b.WriteString("\n")
		if strings.TrimSpace(transcript.Warning) != "" {
			b.WriteString("Warning: ")
			b.WriteString(strings.TrimSpace(transcript.Warning))
			b.WriteString("\n")
		}
		body := strings.TrimSpace(transcript.Text)
		if body == "" {
			body = "(empty transcript)"
		}
		for _, line := range strings.Split(body, "\n") {
			b.WriteString(strings.TrimRight(line, " \t"))
			b.WriteString("\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func executionInputWithPreparedTeamsContext(text string, refs []ReferencedMessage, files []LocalAttachment, transcripts []ASRTranscript) ExecutionInput {
	prompt := promptWithASRTranscripts(text, transcripts)
	prompt = PromptWithReferencedMessages(prompt, refs)
	return ExecutionInputWithLocalAttachments(TeamsCodexPrompt(prompt), nonTeamsMediaAttachments(files))
}

func nonTeamsMediaAttachments(files []LocalAttachment) []LocalAttachment {
	if len(files) == 0 {
		return nil
	}
	out := make([]LocalAttachment, 0, len(files))
	for _, file := range files {
		if isTeamsMediaAttachment(file) {
			continue
		}
		out = append(out, file)
	}
	return out
}

func (b *Bridge) transcribeTeamsMediaAttachments(ctx context.Context, session *Session, turnID string, files []LocalAttachment) ([]ASRTranscript, error) {
	var transcripts []ASRTranscript
	sourceIndex := 0
	for _, file := range files {
		if !isTeamsMediaAttachment(file) {
			continue
		}
		sourceIndex++
		if b == nil || !teamsASRTranscriberConfigured(b.asrTranscriber) {
			return nil, errASRCommandNotConfigured
		}
		stopProgress := b.startTeamsASRProgressLoop(ctx, session, turnID, sourceIndex, file)
		transcript, err := b.asrTranscriber.TranscribeTeamsMedia(ctx, ASRTranscribeInput{
			Session:     session,
			TurnID:      turnID,
			SourceIndex: sourceIndex,
			File:        file,
			Language:    defaultASRLanguage,
			Speed:       defaultASRSpeed,
		})
		stopProgress()
		if err != nil {
			return nil, fmt.Errorf("transcribe Teams media attachment %s: %w", firstNonEmptyString(file.PromptPath, file.Path), err)
		}
		if transcript.SourceIndex == 0 {
			transcript.SourceIndex = sourceIndex
		}
		if transcript.SourcePath == "" {
			transcript.SourcePath = firstNonEmptyString(file.PromptPath, file.Path)
		}
		if transcript.TranscriptID == "" {
			transcript.TranscriptID = strings.TrimSpace(file.SourceID)
		}
		if transcript.SourceName == "" {
			transcript.SourceName = filepath.Base(firstNonEmptyString(file.PromptPath, file.Path))
		}
		if transcript.ContentType == "" {
			transcript.ContentType = file.ContentType
		}
		if transcript.Language == "" {
			transcript.Language = defaultASRLanguage
		}
		if transcript.Speed == "" {
			transcript.Speed = defaultASRSpeed
		}
		transcripts = append(transcripts, transcript)
	}
	return transcripts, nil
}

func teamsASRTranscriberConfigured(transcriber ASRTranscriber) bool {
	if transcriber == nil {
		return false
	}
	if command, ok := transcriber.(*CommandASRTranscriber); ok {
		if command == nil {
			return false
		}
		return strings.TrimSpace(command.Command) != ""
	}
	return true
}

func teamsASRFailureUserMessage(err error) string {
	if errors.Is(err, errASRCommandNotConfigured) {
		return "Teams voice/video transcription is not ready on this helper yet. I received Teams media, but I did not send the raw audio/video to Codex. cxp should prepare local speech recognition automatically; please update/reload the helper and try again."
	}
	return "Teams media transcription failed: " + err.Error()
}

func teamsASRStatusLine(transcriber ASRTranscriber) string {
	switch value := transcriber.(type) {
	case *ManagedASRTranscriber:
		if value != nil {
			return "Speech recognition: managed local Qwen ASR; cxp sets it up automatically on the first Teams voice/video message."
		}
	case *CommandASRTranscriber:
		if value != nil && strings.TrimSpace(value.Command) != "" {
			return "Speech recognition: developer override configured."
		}
	}
	if teamsASRTranscriberConfigured(transcriber) {
		return "Speech recognition: configured."
	}
	return "Speech recognition: not ready in this helper process; update/reload the helper to enable automatic local ASR."
}

func (b *Bridge) startTeamsASRProgressLoop(ctx context.Context, session *Session, turnID string, sourceIndex int, file LocalAttachment) func() {
	if b == nil || b.store == nil || session == nil || strings.TrimSpace(session.ChatID) == "" || strings.TrimSpace(turnID) == "" {
		return func() {}
	}
	if teamsASRProgressNoticeAfter <= 0 || teamsASRProgressRepeatAfter <= 0 {
		return func() {}
	}
	progressCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(done)
		timer := time.NewTimer(teamsASRProgressNoticeAfter)
		defer timer.Stop()
		count := 0
		for {
			select {
			case <-progressCtx.Done():
				return
			case <-timer.C:
				count++
				_ = b.queueTeamsASRProgress(progressCtx, session, turnID, sourceIndex, file, time.Since(started), count)
				timer.Reset(teamsASRProgressRepeatAfter)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (b *Bridge) queueTeamsASRProgress(ctx context.Context, session *Session, turnID string, sourceIndex int, file LocalAttachment, elapsed time.Duration, count int) error {
	if b == nil || session == nil || strings.TrimSpace(session.ChatID) == "" || strings.TrimSpace(turnID) == "" {
		return nil
	}
	if count <= 0 {
		count = 1
	}
	name := asrTranscriptDisplayName(ASRTranscript{
		SourceIndex: sourceIndex,
		SourceName:  filepath.Base(firstNonEmptyString(file.PromptPath, file.Path)),
		SourcePath:  firstNonEmptyString(file.PromptPath, file.Path),
	})
	lines := []string{
		"Still transcribing Teams media before running Codex.",
		"",
		fmt.Sprintf("Clip: %s", name),
		fmt.Sprintf("Elapsed: %s", formatCodexQuietDuration(elapsed)),
		"Still running; Codex will start after transcription finishes.",
	}
	body := strings.Join(lines, "\n")
	return b.queueAndBestEffortSendOutbox(ctx, teamstore.OutboxMessage{
		ID:               fmt.Sprintf("outbox:%s:%s:%03d:%03d", strings.TrimSpace(turnID), teamsASRProgressKind, sourceIndex, count),
		SessionID:        session.ID,
		TurnID:           turnID,
		TeamsChatID:      session.ChatID,
		Kind:             teamsASRProgressKind,
		Body:             body,
		NotificationKind: "turn_progress",
	})
}

func stripASRTranscriptAnnotation(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	idx := strings.Index(text, teamsASRTranscriptAnnotationLabel)
	if idx < 0 {
		return text
	}
	return strings.TrimSpace(text[:idx])
}

func asrTranscriptAnnotationHTML(transcripts []ASRTranscript) string {
	if len(transcripts) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(`<p><strong>`)
	b.WriteString(html.EscapeString(teamsASRTranscriptAnnotationLabel))
	b.WriteString(`:</strong></p>`)
	for i, transcript := range transcripts {
		name := asrTranscriptDisplayName(transcript)
		label := "Clip"
		if len(transcripts) > 1 {
			label = fmt.Sprintf("Clip %d", i+1)
		}
		b.WriteString(`<p><em>`)
		b.WriteString(html.EscapeString(label + ": " + name))
		b.WriteString(`</em></p>`)
		text := strings.TrimSpace(transcript.Text)
		if text == "" {
			text = "(empty transcript)"
		}
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimRight(line, " \t")
			if strings.TrimSpace(line) == "" {
				continue
			}
			b.WriteString(`<p>`)
			b.WriteString(html.EscapeString(line))
			b.WriteString(`</p>`)
		}
		if warning := strings.TrimSpace(transcript.Warning); warning != "" {
			b.WriteString(`<p><em>Warning: `)
			b.WriteString(html.EscapeString(warning))
			b.WriteString(`</em></p>`)
		}
	}
	return b.String()
}
