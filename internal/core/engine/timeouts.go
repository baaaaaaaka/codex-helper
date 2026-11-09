package engine

import (
	"context"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Bounds on one channel call (review M2). The adapter bounds each request
// more tightly; these are the engine's backstop for any channel, so a
// stalled call can never pin a chat's reading or sending flag, or a worker
// slot, until restart.
const (
	callTimeout     = 3 * time.Minute  // send, read-back, create, render
	receiveTimeout  = 30 * time.Minute // one whole receive window (up to MaxPages pages)
	transferTimeout = time.Hour        // file download or upload
)

// timedChannel applies the bounds to every call.
type timedChannel struct{ channel.Channel }

func bounded(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

func (c timedChannel) Render(ctx context.Context, doc channel.RichDoc) ([]channel.Part, error) {
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return c.Channel.Render(ctx, doc)
}

func (c timedChannel) Receive(ctx context.Context, stream, cursor string) (channel.Window, error) {
	ctx, cancel := bounded(ctx, receiveTimeout)
	defer cancel()
	return c.Channel.Receive(ctx, stream, cursor)
}

func (c timedChannel) Send(ctx context.Context, chat string, p channel.Part, token string) (channel.Sent, error) {
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return c.Channel.Send(ctx, chat, p, token)
}

func (c timedChannel) ReadBack(ctx context.Context, chat string, since time.Time) ([]channel.InboundMsg, error) {
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return c.Channel.ReadBack(ctx, chat, since)
}

func (c timedChannel) Get(ctx context.Context, chat, id string) (channel.InboundMsg, error) {
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return c.Channel.Get(ctx, chat, id)
}

func (c timedChannel) CreateChat(ctx context.Context, title, key string) (channel.ChatInfo, error) {
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return c.Channel.CreateChat(ctx, title, key)
}

func (c timedChannel) RenameChat(ctx context.Context, chat, title string) error {
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return c.Channel.RenameChat(ctx, chat, title)
}

func (c timedChannel) FetchFile(ctx context.Context, ref channel.FileRef, dst string) (channel.FileRef, error) {
	ctx, cancel := bounded(ctx, transferTimeout)
	defer cancel()
	return c.Channel.FetchFile(ctx, ref, dst)
}

func (c timedChannel) UploadFile(ctx context.Context, src, name string, sess *channel.UploadSession, save func(channel.UploadSession) error) (channel.FileRef, error) {
	ctx, cancel := bounded(ctx, transferTimeout)
	defer cancel()
	return c.Channel.UploadFile(ctx, src, name, sess, save)
}

func (c timedChannel) Edit(ctx context.Context, chat, id string, p channel.Part, token string) error {
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return c.Channel.Edit(ctx, chat, id, p, token)
}

func (c timedChannel) SetMessagePrefix(ctx context.Context, chat, id, prefix string, present bool) error {
	editor, ok := c.Channel.(channel.MessagePrefixEditor)
	if !ok {
		return channel.ErrNotEditable
	}
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return editor.SetMessagePrefix(ctx, chat, id, prefix, present)
}

func (c timedChannel) MarkUnread(ctx context.Context, chat string, readAt time.Time) error {
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return c.Channel.MarkUnread(ctx, chat, readAt)
}

// timedScanner bounds the activity scan.
type timedScanner struct{ channel.ActivityScanner }

func (s timedScanner) ScanActivity(ctx context.Context, since time.Time) ([]channel.ChatActivity, error) {
	ctx, cancel := bounded(ctx, callTimeout)
	defer cancel()
	return s.ActivityScanner.ScanActivity(ctx, since)
}
