package voice

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/asr"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

type fakeASR struct {
	res asr.Result
	err error
	req asr.Request
}

func (f *fakeASR) Transcribe(_ context.Context, req asr.Request) (asr.Result, error) {
	f.req = req
	return f.res, f.err
}
func (f *fakeASR) Close() error { return nil }

func TestTranscriptTrimmedAndRequestNamed(t *testing.T) {
	f := &fakeASR{res: asr.Result{Text: "  hi there \n"}}
	got, err := New(f, "zh").Transcribe(context.Background(), "/files/7/0-voice.ogg")
	if err != nil || got != "hi there" {
		t.Fatalf("%q %v", got, err)
	}
	if f.req.Name != "0-voice.ogg" || f.req.Language != "zh" {
		t.Fatalf("request %+v", f.req)
	}
}

func TestPreparingIsLater(t *testing.T) {
	f := &fakeASR{err: &asr.Error{Kind: asr.KindPreparing, Op: "setup"}}
	_, err := New(f, "").Transcribe(context.Background(), "/x.ogg")
	if !errors.Is(err, engine.ErrTranscribeLater) {
		t.Fatalf("preparing should be later: %v", err)
	}
}

func TestFailureUserTextHasNoDiagnostic(t *testing.T) {
	f := &fakeASR{err: &asr.Error{Kind: asr.KindMedia, Op: "decode", Detail: "/home/u/.cache/codex-helper/asr/x failed"}}
	_, err := New(f, "").Transcribe(context.Background(), "/x.ogg")
	if errors.Is(err, engine.ErrTranscribeLater) {
		t.Fatal("media failure must be final")
	}
	var ut interface{ UserText() string }
	if !errors.As(err, &ut) {
		t.Fatal("no UserText")
	}
	if txt := ut.UserText(); strings.Contains(txt, ".cache") || strings.Contains(txt, "Diagnostic") || !strings.Contains(txt, "could not be decoded") {
		t.Fatalf("user text %q", txt)
	}
}
