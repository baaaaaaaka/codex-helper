// Package voice adapts internal/asr to the engine's Transcriber: managed
// local speech recognition (or an external --asr-command) for Teams voice
// messages. The engine itself never imports internal/asr.
package voice

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/asr"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

// Transcriber implements engine.Transcriber over an asr.Transcriber.
type Transcriber struct {
	asr      asr.Transcriber
	language string
}

// New wraps t. language "" means automatic detection.
func New(t asr.Transcriber, language string) *Transcriber {
	return &Transcriber{asr: t, language: language}
}

// Close stops the recognizer (setup and the resident server).
func (t *Transcriber) Close() error { return t.asr.Close() }

// Transcribe returns the transcript of the audio file at path. Setup still
// in progress (or an interrupted run) wraps engine.ErrTranscribeLater so
// the attachment stays pending; other failures carry a path-free UserText.
func (t *Transcriber) Transcribe(ctx context.Context, path string) (string, error) {
	res, err := t.asr.Transcribe(ctx, asr.Request{Path: path, Name: filepath.Base(path), Language: t.language})
	if err != nil {
		return "", &failure{err: err, later: asr.Retryable(err)}
	}
	return strings.TrimSpace(res.Text), nil
}

type failure struct {
	err   error
	later bool
}

func (f *failure) Error() string { return f.err.Error() }

func (f *failure) Unwrap() []error {
	if f.later {
		return []error{f.err, engine.ErrTranscribeLater}
	}
	return []error{f.err}
}

// UserText is asr's user message without its diagnostic line (which can
// name local paths).
func (f *failure) UserText() string {
	msg, _, _ := strings.Cut(asr.UserMessage(f.err), "\n")
	return strings.TrimSpace(msg)
}
