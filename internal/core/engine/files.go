package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Inbound attachments: admission records them as pending inbound_files
// rows; the turn does not start until every one is ready or failed
// (NextRunnable). asrLoop downloads them (and transcribes voice) outside
// any transaction, then records the outcome.

// fileRef is the persisted form of a channel.FileRef (inbound_files.ext_ref).
type fileRef struct {
	Kind        channel.FileKind `json:"k"`
	Name        string           `json:"n"`
	Size        int64            `json:"s,omitempty"`
	ContentType string           `json:"t,omitempty"`
	Ref         string           `json:"r"`
}

// inboundFiles turns a message's attachments into inbound_files rows.
// Unsupported attachments are recorded as failed with the adapter's note.
func inboundFiles(refs []channel.FileRef) []store.InboundFile {
	var out []store.InboundFile
	for i, r := range refs {
		data, _ := json.Marshal(fileRef{Kind: r.Kind, Name: r.Name, Size: r.Size, ContentType: r.ContentType, Ref: r.Ref})
		f := store.InboundFile{Idx: i, Name: r.Name, ExtRef: string(data), Kind: "file", State: "pending"}
		if f.Name == "" {
			f.Name = fmt.Sprintf("attachment-%d", i+1)
		}
		switch r.Kind {
		case channel.FileImage:
			f.Kind = "image"
		case channel.FileVoice:
			f.Kind = "voice"
		case channel.FileUnsupported:
			f.State, f.Error = "failed", firstNonEmpty(r.Note, "this attachment type is not supported")
		}
		out = append(out, f)
	}
	return out
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

// asrLoop downloads pending attachments and transcribes voice messages.
func (e *Engine) asrLoop(ctx context.Context) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		ids, err := e.st.PendingInboundFiles()
		if err != nil {
			e.logf("pending attachments: %v", err)
		}
		for _, id := range ids {
			if ctx.Err() != nil {
				return
			}
			e.fetchInbound(ctx, id)
		}
		select {
		case <-ctx.Done():
			return
		case <-e.asrWake:
		case <-tick.C:
		}
	}
}

// fetchInbound handles every pending attachment of one inbound row.
func (e *Engine) fetchInbound(ctx context.Context, inboundID int64) {
	var (
		in    store.Inbound
		files []store.InboundFile
	)
	if err := e.update(func(tx *store.Tx) error {
		var err error
		in, _, err = tx.Inbound(inboundID)
		if err != nil {
			return err
		}
		files, err = tx.InboundFiles(inboundID)
		return err
	}); err != nil {
		e.logf("attachments of %d: %v", inboundID, err)
		return
	}
	for _, f := range files {
		if f.State != "pending" || ctx.Err() != nil {
			continue
		}
		state, local, msg := e.fetchOne(ctx, inboundID, f)
		if ctx.Err() != nil {
			return
		}
		key := fmt.Sprintf("%d/%d", inboundID, f.Idx)
		if state == "" {
			e.mu.Lock()
			e.fileTries[key]++
			tries := e.fileTries[key]
			e.mu.Unlock()
			if tries < maxFileTries {
				continue // transient: retried on the next pass
			}
			state, msg = "failed", "the attachment could not be downloaded"
		}
		e.mu.Lock()
		delete(e.fileTries, key)
		e.mu.Unlock()
		if err := e.update(func(tx *store.Tx) error {
			return tx.SetInboundFile(inboundID, f.Idx, state, local, msg)
		}); err != nil {
			e.logf("record attachment %d/%d: %v", inboundID, f.Idx, err)
		}
	}
	e.wakeRunner(in.ConvID)
}

// maxFileTries bounds transient download failures of one attachment; the
// turn waits for it, so it must end in ready or failed.
const maxFileTries = 5

// fetchOne downloads (and for voice transcribes) one attachment. It returns
// the new state ("" = retry later), the local path and an error text.
func (e *Engine) fetchOne(ctx context.Context, inboundID int64, f store.InboundFile) (string, string, string) {
	var ref fileRef
	if err := json.Unmarshal([]byte(f.ExtRef), &ref); err != nil {
		return "failed", "", "the attachment reference is unreadable"
	}
	if e.cfg.FilesDir == "" {
		return "failed", "", "attachments are not enabled"
	}
	dir := filepath.Join(e.cfg.FilesDir, fmt.Sprint(inboundID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		e.logf("attachment dir: %v", err)
		return "", "", ""
	}
	dst := filepath.Join(dir, fmt.Sprintf("%d-%s", f.Idx, safeName(f.Name)))
	got, err := e.ch.FetchFile(ctx, channel.FileRef{Kind: ref.Kind, Name: ref.Name, Size: ref.Size, ContentType: ref.ContentType, Ref: ref.Ref}, dst)
	if err != nil {
		var rej *channel.Rejected
		if errors.As(err, &rej) && rej.Permanent || errors.Is(err, channel.ErrNotFound) {
			e.logf("attachment %d/%d: %v", inboundID, f.Idx, err)
			return "failed", "", "the attachment could not be downloaded"
		}
		e.logf("attachment %d/%d (will retry): %v", inboundID, f.Idx, err)
		return "", "", ""
	}
	_ = got
	if f.Kind != "voice" {
		return "ready", dst, ""
	}
	if e.cfg.Transcriber == nil {
		return "failed", dst, "voice messages cannot be transcribed on this machine"
	}
	text, err := e.cfg.Transcriber.Transcribe(ctx, dst)
	if err != nil {
		if ctx.Err() != nil {
			return "", "", ""
		}
		e.logf("transcribe %d/%d: %v", inboundID, f.Idx, err)
		return "failed", dst, "the voice message could not be transcribed"
	}
	txt := dst + ".txt"
	if err := os.WriteFile(txt, []byte(strings.TrimSpace(text)), 0o600); err != nil {
		return "", "", ""
	}
	return "ready", txt, ""
}

// safeName keeps a file name readable but safe as one path element.
func safeName(name string) string {
	name = filepath.Base(strings.ReplaceAll(strings.TrimSpace(name), `\`, "/"))
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || strings.ContainsRune(`/\:*?"<>|`, r) {
			return '_'
		}
		return r
	}, name)
	if name == "" || name == "." || name == ".." {
		return "attachment"
	}
	return name
}
