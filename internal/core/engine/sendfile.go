package engine

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// `helper file <relative-path>` / `!file` (F-RND-10, v0.1.20
// sendFileCommand B:9748): sends one file from the outbound root (the
// parent of every turn's output directory) to the chat, through the same
// staging and resumable upload as a manifest file (outbound.go). The path
// must be relative and stay inside the root with symlinks resolved.

// outboundRoot is the directory `helper file` reads from ("" when files
// are off).
func (e *Engine) outboundRoot() string {
	if e.cfg.FilesDir == "" {
		return ""
	}
	return filepath.Join(e.cfg.FilesDir, "outbound")
}

func textSendFileUsage(root string) string {
	return "usage: `helper file <relative-path>` or `!file <relative-path>`\nOutbound root: " + root
}

func textSendFileRejected(reason, root string) string {
	return fmt.Sprintf("Teams file upload rejected: %s. `helper file` only reads relative files under the Teams outbound root: %s", reason, root)
}

// cmdSendFile stages the file and enqueues it as the command's reply (the
// file message, "📎 <name>"), in one transaction.
func (e *Engine) cmdSendFile(j cmdJob, cs *convState, arg string) {
	root := e.outboundRoot()
	if root == "" {
		e.reply(j, "Teams file upload is not available in this helper.", nil)
		return
	}
	rel := strings.Trim(strings.TrimSpace(arg), "`\"'")
	if rel == "" {
		e.reply(j, textSendFileUsage(root), nil)
		return
	}
	clean, err := cleanManifestPath(rel)
	if err != nil {
		e.reply(j, textSendFileRejected(strings.ReplaceAll(err.Error(), "output directory", "outbound root"), root), nil)
		return
	}
	// The root is where Codex writes its outputs (one directory per turn);
	// it may not exist yet before the first turn.
	_ = os.MkdirAll(root, 0o700)
	src := filepath.Join(root, filepath.FromSlash(clean))
	if err := underDir(root, src); err != nil {
		e.reply(j, textSendFileRejected(strings.ReplaceAll(err.Error(), "output directory", "outbound root"), root), nil)
		return
	}
	name := safeName(path.Base(clean))
	staged := filepath.Join(e.cfg.FilesDir, "send", "cmd", fmt.Sprint(j.inbound), name)
	size, err := snapshot(src, staged)
	if err != nil {
		e.reply(j, textSendFileRejected(name+": "+err.Error(), root), nil)
		return
	}
	e.complete(j, func(tx *store.Tx) error {
		_, err := tx.Enqueue(cs.id, []store.OutMsg{{
			DeliveryKey: fmt.Sprintf("cmd:%d", j.inbound), Kind: store.OutControl,
			File: &store.OutFile{LocalPath: staged, Size: size},
		}})
		return err
	})
}
