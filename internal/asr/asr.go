// Package asr turns voice and video attachments into text.
//
// There are exactly two backends:
//
//   - Command runs a user-configured external program (--asr-command).
//   - Managed downloads a pinned llama.cpp runtime plus the Qwen3-ASR GGUF
//     model into a private cache, verifies them, and keeps llama-server
//     resident for IdleUnload (30 min) after the last use.
//
// Every failure is returned as an *Error with a stable Kind, so the caller
// can keep the voice file, tell the user once, and decide whether to retry
// (see Retryable). The package never deletes or modifies the input file.
package asr

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

// MaxThreads caps the CPU threads any backend may use (owner requirement:
// at most four cores for speech recognition).
const MaxThreads = 4

// DefaultLanguage lets the model detect the spoken language.
const DefaultLanguage = "auto"

// Request describes one media file to transcribe.
type Request struct {
	Path        string      // local audio/video file; never modified or removed
	Name        string      // display name (defaults to the base name of Path)
	ContentType string      // MIME type as received, may be empty
	Language    string      // language hint; "" means DefaultLanguage
	Index       int         // position of the attachment in its message ({source_index})
	Notify      func(Event) // optional; receives progress events for this request
}

// Result is a finished transcription.
type Result struct {
	Text     string
	Language string // detected or hinted language
	Model    string
	Backend  string
}

// EventKind identifies a progress event.
type EventKind string

// EventPreparing is sent when a request has to wait for a first-run
// download, at most once per voice file (Request.Path) per setup, however
// often the caller retries after KindPreparing.
const EventPreparing EventKind = "preparing"

// Event reports progress that the user should see.
type Event struct {
	Kind          EventKind
	CacheDir      string // where the runtime and model are being installed
	DownloadBytes int64  // bytes still to download (0 if unknown)
}

// Transcriber is implemented by both backends.
type Transcriber interface {
	Transcribe(ctx context.Context, req Request) (Result, error)
	Close() error
}

func (r Request) displayName() string {
	if strings.TrimSpace(r.Name) != "" {
		return strings.TrimSpace(r.Name)
	}
	return filepath.Base(r.Path)
}

func (r Request) language() string {
	if l := strings.TrimSpace(r.Language); l != "" {
		return l
	}
	return DefaultLanguage
}

func (r Request) notify(e Event) {
	if r.Notify != nil {
		r.Notify(e)
	}
}

// PromptLead starts the block that carries transcripts into the Codex prompt.
// It is also the marker that transcript mirroring uses to hide the block again
// (fix 52621ec: the wrapper must never be echoed back into the chat).
const PromptLead = "Automatic local ASR transcript from Microsoft Teams media."

const promptCaveat = " The transcript may contain recognition errors, especially names, code identifiers, paths, commands, acronyms, and mixed Chinese/English terms. Prefer explicit typed text over ASR text when they conflict, and ask for clarification when a critical command or identifier is ambiguous."

// Clip is one transcribed attachment for PromptWithTranscripts.
type Clip struct {
	Name   string
	Result Result
}

// PromptWithTranscripts appends the transcripts to the user's typed text,
// labelled as fallible speech recognition.
func PromptWithTranscripts(text string, clips []Clip) string {
	text = strings.TrimSpace(text)
	if len(clips) == 0 {
		return text
	}
	var b strings.Builder
	if text != "" {
		b.WriteString(text)
		b.WriteString("\n\n")
	}
	b.WriteString(PromptLead)
	b.WriteString(promptCaveat)
	b.WriteString("\n")
	for i, c := range clips {
		label := "Voice/video clip"
		if len(clips) > 1 {
			label = fmt.Sprintf("Voice/video clip %d", i+1)
		}
		attrs := []string{"language=" + firstNonEmpty(c.Result.Language, DefaultLanguage)}
		if c.Result.Model != "" {
			attrs = append(attrs, "model="+c.Result.Model)
		}
		if c.Result.Backend != "" {
			attrs = append(attrs, "backend="+c.Result.Backend)
		}
		fmt.Fprintf(&b, "\n%s: %s (%s)\n", label, c.Name, strings.Join(attrs, ", "))
		body := strings.TrimSpace(c.Result.Text)
		if body == "" {
			body = "(empty transcript)"
		}
		b.WriteString(body)
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

// StripTranscripts returns the user's own text from a prompt built by
// PromptWithTranscripts, so mirrors of the Codex history never show the
// helper-only transcript block.
func StripTranscripts(prompt string) string {
	if i := strings.Index(prompt, PromptLead); i >= 0 {
		return strings.TrimSpace(prompt[:i])
	}
	return strings.TrimSpace(prompt)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
