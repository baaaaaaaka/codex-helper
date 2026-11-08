package codexagent

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

// startTitle reserves the single slot synchronously, then returns immediately.
// The durable original-thread binding is the only once boundary: a restart or
// failed attempt is deliberately never recovered as another naming request.
func (a *Agent) startTitle(ctx context.Context, req engine.TurnRequest, thread string, original *codex.Lease, apply func(context.Context, func(context.Context) (string, error)) error) {
	if strings.TrimSpace(req.TitleText) == "" || req.Model == "" || !original.Server().SupportsTitle() {
		return
	}
	// Do not retain the full first task or its attachments after the answer.
	n := min(len(req.TitleText), 960)
	for n < len(req.TitleText) && n > 0 && !utf8.RuneStart(req.TitleText[n]) {
		n--
	}
	text := strings.Clone(req.TitleText[:n])
	conv, cwd, model, effort := req.ConvID, req.Cwd, req.Model, req.Effort
	a.mu.Lock()
	if a.closed || a.titleCancel != nil {
		a.mu.Unlock()
		return
	}
	lease, err := original.Retain()
	if err != nil {
		a.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	a.titleCancel, a.titleConv = cancel, conv
	a.titleWG.Add(1)
	a.mu.Unlock()
	go func() {
		defer a.titleWG.Done()
		defer func() { cancel(); a.mu.Lock(); a.titleCancel = nil; a.titleConv = 0; a.mu.Unlock() }()
		defer lease.Release()
		srv := lease.Server()
		check, done := context.WithTimeout(ctx, 3*time.Second)
		existing, err := srv.ThreadRead(check, thread, false)
		done()
		if err != nil {
			return
		}
		name := existing.Name
		if name == "" {
			name, err = srv.GenerateTitle(ctx, cwd, model, effort, text)
		}
		if err == nil && ctx.Err() == nil {
			c, done := context.WithTimeout(ctx, 5*time.Second)
			err = apply(c, func(c context.Context) (string, error) { return srv.NameIfEmpty(c, thread, name) })
			done()
		}
		if err != nil && ctx.Err() == nil {
			fmt.Fprintln(a.cfg.Log, "codexagent: optional chat title was skipped")
		}
	}()
}

// CancelTitle does not wait or affect the user's running turn.
func (a *Agent) CancelTitle(conv int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.titleConv == conv && a.titleCancel != nil {
		a.titleCancel()
	}
}
