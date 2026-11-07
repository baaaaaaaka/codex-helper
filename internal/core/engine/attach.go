package engine

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// TUIEndpoint is a private native transport capability, not a second RPC relay.
type TUIEndpoint struct {
	ThreadID string `json:"threadId,omitempty"`
	URL      string `json:"url"`
	Token    string `json:"token"`
}
type TUIAgent interface {
	Attach(context.Context, int64, string, string) (TUIEndpoint, func(), <-chan struct{}, error)
	Active() bool
}

var ErrUnmanagedThread = errors.New("thread is not managed by this helper")

// Attach keeps the helper, policy gateway and app-server alive until the
// caller releases its connection. Admission and update drain share e.mu.
func (e *Engine) Attach(ctx context.Context, thread string) (TUIEndpoint, func(), <-chan struct{}, error) {
	return e.attachTUI(ctx, thread, "")
}

// AttachNew starts a local-only session on the shared backend. Its Work chat
// is created by the existing mirror only after the first actual user input.
func (e *Engine) AttachNew(ctx context.Context, cwd string) (TUIEndpoint, func(), <-chan struct{}, error) {
	return e.attachTUI(ctx, "", cwd)
}

func (e *Engine) attachTUI(ctx context.Context, thread, cwd string) (TUIEndpoint, func(), <-chan struct{}, error) {
	a, ok := e.ag.(TUIAgent)
	if !ok {
		return TUIEndpoint{}, nil, nil, ErrUnmanagedThread
	}
	e.mu.Lock()
	if !e.attachReady {
		e.mu.Unlock()
		return TUIEndpoint{}, nil, nil, errors.New("helper startup is still in progress")
	}
	var cs *convState
	for _, c := range e.convs {
		if thread != "" && c.threadID == thread && c.kind == store.KindWork && c.state != store.ConvClosed && c.state != store.ConvHold {
			cs = c
			break
		}
	}
	if e.draining() || e.stopping.Load() || !e.readyLocked(cs) {
		e.mu.Unlock()
		return TUIEndpoint{}, nil, nil, errors.New("helper is not ready to attach; retry shortly")
	}
	e.tuiLeases++
	e.mu.Unlock()
	var once sync.Once
	releaseCount := func() { once.Do(func() { e.mu.Lock(); e.tuiLeases--; e.mu.Unlock(); wake(e.updateWake) }) }
	if cs == nil {
		var err error
		if thread == "" {
			cs, err = e.newTUIConversation(cwd)
		} else {
			cs, err = e.adoptLocalThread(ctx, thread)
		}
		if err != nil {
			releaseCount()
			return TUIEndpoint{}, nil, nil, err
		}
	}
	e.mu.Lock()
	convID, workDir := cs.id, cs.cwd
	closed := cs.state == store.ConvClosed || cs.state == store.ConvHold
	e.mu.Unlock()
	if closed {
		releaseCount()
		return TUIEndpoint{}, nil, nil, ErrUnmanagedThread
	}
	// An unfinished recovery must not resume an orphan native queue.
	var recovering bool
	err := e.update(func(tx *store.Tx) error {
		open, err := tx.OpenTurnsOf(cs.id)
		if err != nil {
			return err
		}
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, tr := range open {
			if e.orphans[tr.ID] {
				recovering = true
			}
		}
		return nil
	})
	if err != nil || recovering {
		releaseCount()
		if err == nil {
			err = errors.New("thread recovery is still in progress")
		}
		return TUIEndpoint{}, nil, nil, err
	}
	endpoint, release, done, err := a.Attach(ctx, convID, thread, workDir)
	if err == nil && thread == "" {
		if endpoint.ThreadID == "" {
			err = errors.New("backend omitted new thread id")
		} else {
			err = e.update(func(tx *store.Tx) error {
				if x := tx.Bind(store.Thread{ConvID: convID, ThreadID: endpoint.ThreadID, Cwd: workDir}); x != nil {
					return x
				}
				e.mu.Lock()
				cs.threadID = endpoint.ThreadID
				e.byThread[endpoint.ThreadID] = convID
				e.mu.Unlock()
				return nil
			})
		}
	}
	if err != nil {
		if release != nil {
			release()
		}
		if thread == "" {
			_ = e.update(func(tx *store.Tx) error {
				if x := tx.Close(convID); x != nil {
					return x
				}
				_, x := e.refresh(tx, convID)
				return x
			})
		}
		releaseCount()
		return TUIEndpoint{}, nil, nil, err
	}
	var leaseOnce sync.Once
	return endpoint, func() { leaseOnce.Do(func() { release(); releaseCount() }) }, done, nil
}

// A local session can be opened before the mirror has created its Work chat.
// Bind it now so subsequent discovery and Teams input use the same pool key.
// Search filenames only, once at attachment; never decode old conversation text.
func (e *Engine) adoptLocalThread(ctx context.Context, thread string) (*convState, error) {
	if e.cfg.SessionsDir == "" || len(thread) != 36 {
		return nil, ErrUnmanagedThread
	}
	var path string
	err := filepath.WalkDir(e.cfg.SessionsDir, func(p string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), thread+".jsonl") {
			path = p
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if path == "" {
		return nil, ErrUnmanagedThread
	}
	meta, err := e.readMeta(path)
	if err != nil {
		return nil, err
	}
	if meta.ID != thread || !meta.Interactive() || meta.Subagent() {
		return nil, ErrUnmanagedThread
	}
	info, err := e.statFile(path)
	if err != nil {
		return nil, err
	}
	machine := e.machineLabel()
	var id int64
	err = e.update(func(tx *store.Tx) error {
		ids, err := tx.ThreadConvs(thread)
		if err != nil {
			return err
		}
		if len(ids) > 0 {
			id, err = e.importBoundConv(tx, thread)
			if err != nil {
				return err
			}
			if id == 0 {
				return ErrUnmanagedThread
			}
			return nil
		}
		label, err := e.nextLabel(tx)
		if err != nil {
			return err
		}
		id, err = tx.CreateConversation(store.NewConversation{Label: label, Kind: store.KindWork, Channel: e.cfg.ChannelName, Title: workTitle(machine, "", "", meta.Cwd), TitleSource: "auto"})
		if err != nil {
			return err
		}
		if err = tx.Bind(store.Thread{ConvID: id, ThreadID: thread, Cwd: meta.Cwd}); err != nil {
			return err
		}
		// Opening a window does not import or post its earlier history. Existing
		// baseline cursors are preserved by UpsertSource.
		if _, err = tx.UpsertSource(store.TranscriptSource{Path: path, ThreadID: thread, ConvID: id, State: store.SrcTracking, Offset: info.Size, Size: info.Size, MTime: ms(info.MTime), Inode: info.Inode}); err != nil {
			return err
		}
		if err = tx.SetMeta(discoveredKey(id), discoveredPending); err != nil {
			return err
		}
		_, err = e.refresh(tx, id)
		if err == nil {
			e.mu.Lock()
			cs := e.convs[id]
			cs.threadID, cs.cwd = thread, meta.Cwd
			e.byThread[thread] = id
			e.mu.Unlock()
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return e.conv(id), nil
}

func (e *Engine) newTUIConversation(cwd string) (*convState, error) {
	if e.cfg.SessionsDir == "" {
		return nil, ErrUnmanagedThread
	}
	info, err := os.Stat(cwd)
	if err != nil || !filepath.IsAbs(cwd) || !info.IsDir() {
		return nil, errors.New("TUI working directory must be an existing absolute directory")
	}
	var id int64
	err = e.update(func(tx *store.Tx) error {
		label, err := e.nextLabel(tx)
		if err != nil {
			return err
		}
		id, err = tx.CreateConversation(store.NewConversation{Label: label, Kind: store.KindWork, Channel: e.cfg.ChannelName, Title: workTitle(e.machineLabel(), "", "", cwd), TitleSource: "auto"})
		if err != nil {
			return err
		}
		if err = tx.SetMeta(discoveredKey(id), discoveredPending); err != nil {
			return err
		}
		if _, err = e.refresh(tx, id); err != nil {
			return err
		}
		e.mu.Lock()
		e.convs[id].cwd = cwd
		e.mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return e.conv(id), nil
}
