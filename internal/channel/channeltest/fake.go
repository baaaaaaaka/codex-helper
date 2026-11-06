// Package channeltest provides an in-memory channel.Channel for core unit
// tests. It models what the core relies on: ordered streams with opaque
// cursors, self-echo of own posts, hidden markers (optionally stripped),
// injectable send/receive faults and simple files. It does not model any
// real service's formatting.
package channeltest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Owner is the id of the signed-in account in the fake.
const Owner = "owner"

// Fake is an in-memory channel. The zero value is not usable; call New.
type Fake struct {
	mu    sync.Mutex
	caps  channel.Caps
	seq   int64
	now   func() time.Time
	chats map[string]*chat
	keys  map[string]string // CreateChat key -> chat id
	files map[string][]byte // Ref -> content

	// StripMarkers makes the fake drop hidden markers like a service that
	// sanitises HTML comments.
	StripMarkers bool
	// SendFault, when set, runs before every Send. Returning a non-nil error
	// fails the send; deliver=true still stores the message first (the
	// "accepted but response lost" case, usually with a *channel.Ambiguous).
	SendFault func(chat string, p channel.Part) (deliver bool, err error)
	// ReceiveFault, when set, can fail Receive/ReadBack/Get (e.g. *channel.Throttled).
	ReceiveFault func(stream string) error
	// MarkUnreadFault, when set, can fail MarkUnread.
	MarkUnreadFault func(chat string) error
	// MaxWindow, when > 0, makes Receive fail with *channel.WindowTooLarge
	// when more than MaxWindow messages are pending.
	MaxWindow int
	// HistoryPageSize is the page size of HistoryPage (default 50).
	HistoryPageSize int

	historyPages int
}

type chat struct {
	info    channel.ChatInfo
	msgs    []stored
	readAt  time.Time
	unreads int
}

type stored struct {
	seq int64
	msg channel.InboundMsg
}

// New returns a fake with Teams-like capabilities and a deterministic clock
// that advances one millisecond per message.
func New() *Fake {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := &Fake{
		caps: channel.Caps{
			ReadBack: true, SelfEcho: true, Mentions: true, Quotes: true,
			Markers: true, HostedFiles: true, EditMessages: true, MarkUnread: true,
			MaxPartBytes: 4096,
		},
		chats: map[string]*chat{},
		keys:  map[string]string{},
		files: map[string][]byte{},
	}
	f.now = func() time.Time { return base.Add(time.Duration(f.seq) * time.Millisecond) }
	return f
}

// SetCaps replaces the capability flags (e.g. SelfEcho=false for a bot channel).
func (f *Fake) SetCaps(c channel.Caps) { f.mu.Lock(); f.caps = c; f.mu.Unlock() }

func (f *Fake) Caps() channel.Caps { f.mu.Lock(); defer f.mu.Unlock(); return f.caps }

// Render emits plain text "Label [part i/n]:\nbody" split on line boundaries
// so that every part's Body fits MaxPartBytes.
func (f *Fake) Render(_ context.Context, doc channel.RichDoc) ([]channel.Part, error) {
	limit := f.Caps().MaxPartBytes
	room := limit - len(doc.Label) - 32
	if room < 16 {
		return nil, fmt.Errorf("channeltest: MaxPartBytes %d too small", limit)
	}
	var chunks []string
	var cur strings.Builder
	for _, line := range strings.Split(doc.Markdown, "\n") {
		for len(line) > room {
			chunks = append(chunks, line[:room])
			line = line[room:]
		}
		if cur.Len() > 0 && cur.Len()+1+len(line) > room {
			chunks = append(chunks, cur.String())
			cur.Reset()
		}
		if cur.Len() > 0 {
			cur.WriteByte('\n')
		}
		cur.WriteString(line)
	}
	chunks = append(chunks, cur.String())
	parts := make([]channel.Part, len(chunks))
	for i, c := range chunks {
		label := doc.Label
		if len(chunks) > 1 {
			label += fmt.Sprintf(" [part %d/%d]", i+1, len(chunks))
		}
		body := label + ":\n" + c
		if i == len(chunks)-1 {
			if doc.Footer != "" {
				body += "\n" + doc.FooterLabel + ": " + doc.Footer
			}
			for _, fr := range doc.Files {
				body += "\n[file " + fr.Name + "]"
			}
		}
		p := channel.Part{Index: i + 1, Count: len(chunks), Body: body, Hash: channel.TextHash(body), Size: len(body)}
		if i == 0 {
			p.Mention, p.ReplyTo = doc.Mention, doc.ReplyTo
		}
		parts[i] = p
	}
	return parts, nil
}

func (f *Fake) Receive(_ context.Context, stream, cursor string) (channel.Window, error) {
	after := int64(0)
	if cursor != "" {
		n, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return channel.Window{}, &channel.Rejected{Permanent: true, Err: fmt.Errorf("bad cursor %q", cursor)}
		}
		after = n
	}
	if err := f.receiveFault(stream); err != nil {
		return channel.Window{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	w := channel.Window{Cursor: strconv.FormatInt(after, 10), Requests: 1}
	c := f.chats[stream]
	if c == nil {
		return w, nil
	}
	for _, s := range c.msgs {
		if s.seq <= after {
			continue
		}
		if s.msg.ByOwner && !f.caps.SelfEcho {
			continue
		}
		w.Msgs = append(w.Msgs, s.msg)
		w.Cursor = strconv.FormatInt(s.seq, 10)
	}
	if f.MaxWindow > 0 && len(w.Msgs) > f.MaxWindow {
		return channel.Window{}, &channel.WindowTooLarge{Pages: f.MaxWindow, Seen: len(w.Msgs), OldestSeen: w.Msgs[0].Modified, NewestSeen: w.Msgs[len(w.Msgs)-1].Modified, SkipCursor: w.Cursor}
	}
	return w, nil
}

func (f *Fake) Send(_ context.Context, chatID string, p channel.Part, token string) (channel.Sent, error) {
	if hook := f.sendFault(); hook != nil {
		deliver, err := hook(chatID, p)
		if err != nil {
			if deliver {
				f.store(chatID, p, token)
			}
			return channel.Sent{}, err
		}
	}
	f.mu.Lock()
	_, ok := f.chats[chatID]
	f.mu.Unlock()
	if !ok {
		return channel.Sent{}, &channel.Rejected{Permanent: true, Err: fmt.Errorf("chat %s: %w", chatID, channel.ErrNotFound)}
	}
	m := f.store(chatID, p, token)
	return channel.Sent{ID: m.ID, Time: m.Created}, nil
}

// Edit replaces the text of an owner message. Like Teams, the edited
// message is returned again by the next Receive (it moves to the end of the
// stream with a new modification time).
func (f *Fake) Edit(_ context.Context, chatID, id string, p channel.Part, token string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.chats[chatID]
	if c == nil {
		return &channel.Rejected{Permanent: true, Err: fmt.Errorf("chat %s: %w", chatID, channel.ErrNotFound)}
	}
	for i, s := range c.msgs {
		if s.msg.ID != id {
			continue
		}
		if !s.msg.ByOwner {
			return &channel.Rejected{Permanent: true, Err: fmt.Errorf("message %s was not posted by the owner", id)}
		}
		m := s.msg
		m.Text, m.Hash, m.Edited, m.Marker = p.Body, channel.TextHash(p.Body), true, ""
		if token != "" && !f.StripMarkers && f.caps.Markers {
			m.Marker = token
		}
		f.seq++
		m.Modified = f.now()
		c.msgs = append(append(c.msgs[:i:i], c.msgs[i+1:]...), stored{seq: f.seq, msg: m})
		return nil
	}
	return &channel.Rejected{Permanent: true, Err: fmt.Errorf("message %s/%s: %w", chatID, id, channel.ErrNotFound)}
}

// MarkUnread records readAt for the chat; see Unread.
func (f *Fake) MarkUnread(_ context.Context, chatID string, readAt time.Time) error {
	f.mu.Lock()
	hook := f.MarkUnreadFault
	f.mu.Unlock()
	if hook != nil {
		if err := hook(chatID); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.chats[chatID]
	if c == nil {
		return &channel.Rejected{Permanent: true, Err: fmt.Errorf("chat %s: %w", chatID, channel.ErrNotFound)}
	}
	c.readAt = readAt
	c.unreads++
	return nil
}

// Unread returns the last MarkUnread readAt of chat and how often it was called.
func (f *Fake) Unread(chatID string) (time.Time, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.chats[chatID]; c != nil {
		return c.readAt, c.unreads
	}
	return time.Time{}, 0
}

func (f *Fake) ReadBack(_ context.Context, chatID string, since time.Time) ([]channel.InboundMsg, error) {
	if err := f.receiveFault(chatID); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []channel.InboundMsg
	if c := f.chats[chatID]; c != nil {
		for _, s := range c.msgs {
			if s.msg.Modified.After(since) {
				out = append(out, s.msg)
			}
		}
	}
	return out, nil
}

// HistoryPage lists one page of the messages of chat modified after since,
// newest first (channel.HistoryPager); pos is the index of the page start.
func (f *Fake) HistoryPage(_ context.Context, chatID string, since time.Time, pos string) ([]channel.InboundMsg, string, error) {
	if err := f.receiveFault(chatID); err != nil {
		return nil, "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.historyPages++
	var all []channel.InboundMsg
	if c := f.chats[chatID]; c != nil {
		for i := len(c.msgs) - 1; i >= 0; i-- {
			if m := c.msgs[i].msg; m.Modified.After(since) {
				all = append(all, m)
			}
		}
	}
	size := f.HistoryPageSize
	if size <= 0 {
		size = 50
	}
	start, _ := strconv.Atoi(pos)
	if start >= len(all) {
		return nil, "", nil
	}
	end, next := min(start+size, len(all)), ""
	if end < len(all) {
		next = strconv.Itoa(end)
	}
	return append([]channel.InboundMsg(nil), all[start:end]...), next, nil
}

// HistoryPages is the number of HistoryPage requests served.
func (f *Fake) HistoryPages() int { f.mu.Lock(); defer f.mu.Unlock(); return f.historyPages }

// PartText is the visible text of a part body: the fake stores it as is.
func (f *Fake) PartText(body string) string { return body }

// SetClock makes the fake's clock start at base (it still advances one
// millisecond per message).
func (f *Fake) SetClock(base time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = func() time.Time { return base.Add(time.Duration(f.seq) * time.Millisecond) }
}

func (f *Fake) Get(_ context.Context, chatID, id string) (channel.InboundMsg, error) {
	if err := f.receiveFault(chatID); err != nil {
		return channel.InboundMsg{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.chats[chatID]; c != nil {
		for _, s := range c.msgs {
			if s.msg.ID == id {
				return s.msg, nil
			}
		}
	}
	return channel.InboundMsg{}, fmt.Errorf("message %s/%s: %w", chatID, id, channel.ErrNotFound)
}

func (f *Fake) CreateChat(_ context.Context, title, key string) (channel.ChatInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.keys[key]; ok && key != "" {
		return f.chats[id].info, nil
	}
	f.seq++
	id := "chat-" + strconv.FormatInt(f.seq, 10)
	info := channel.ChatInfo{ID: id, Title: title, URL: "fake://chat/" + id}
	f.chats[id] = &chat{info: info}
	if key != "" {
		f.keys[key] = id
	}
	return info, nil
}

func (f *Fake) RenameChat(_ context.Context, chatID, title string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.chats[chatID]
	if c == nil {
		return &channel.Rejected{Permanent: true, Err: channel.ErrNotFound}
	}
	c.info.Title = title
	return nil
}

func (f *Fake) FetchFile(_ context.Context, ref channel.FileRef, dst string) (channel.FileRef, error) {
	f.mu.Lock()
	data, ok := f.files[ref.Ref]
	f.mu.Unlock()
	if !ok {
		return ref, fmt.Errorf("file %s: %w", ref.Ref, channel.ErrNotFound)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		return ref, err
	}
	ref.Size = int64(len(data))
	return ref, nil
}

func (f *Fake) UploadFile(_ context.Context, src, name string, _ *channel.UploadSession, _ func(channel.UploadSession) error) (channel.FileRef, error) {
	data, err := os.ReadFile(src)
	if err != nil {
		return channel.FileRef{}, err
	}
	return f.AddFile(name, data), nil
}

// AddFile registers file content and returns a reference to it.
func (f *Fake) AddFile(name string, data []byte) channel.FileRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	ref := "file-" + strconv.FormatInt(f.seq, 10)
	f.files[ref] = append([]byte(nil), data...)
	return channel.FileRef{Kind: channel.FileDoc, Name: name, Size: int64(len(data)), Ref: ref}
}

// PostUser adds a message from the owner typed in the client (no marker).
func (f *Fake) PostUser(chatID, text string, files ...channel.FileRef) channel.InboundMsg {
	return f.add(chatID, channel.InboundMsg{From: Owner, FromName: "Owner", ByOwner: true, Text: text, Files: files})
}

// PostOtherHelper adds a message another helper instance posted as the owner.
func (f *Fake) PostOtherHelper(chatID, text, marker string) channel.InboundMsg {
	m := channel.InboundMsg{From: Owner, FromName: "Owner", ByOwner: true, Text: text}
	if !f.StripMarkers {
		m.Marker = marker
	}
	return f.add(chatID, m)
}

// PostCoworker adds a message from someone other than the owner.
func (f *Fake) PostCoworker(chatID, text string) channel.InboundMsg {
	return f.add(chatID, channel.InboundMsg{From: "coworker", FromName: "Coworker", Text: text})
}

// PostSystem adds a service event.
func (f *Fake) PostSystem(chatID, text string) channel.InboundMsg {
	return f.add(chatID, channel.InboundMsg{Kind: channel.KindSystem, Text: text})
}

// Messages returns a copy of every message in chat, oldest first.
func (f *Fake) Messages(chatID string) []channel.InboundMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []channel.InboundMsg
	if c := f.chats[chatID]; c != nil {
		for _, s := range c.msgs {
			out = append(out, s.msg)
		}
	}
	return out
}

// Chat returns the chat info for id.
func (f *Fake) Chat(id string) (channel.ChatInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.chats[id]
	if !ok {
		return channel.ChatInfo{}, false
	}
	return c.info, true
}

func (f *Fake) store(chatID string, p channel.Part, token string) channel.InboundMsg {
	m := channel.InboundMsg{From: Owner, FromName: "Owner", ByOwner: true, Text: p.Body, MentionsOwner: p.Mention}
	if token != "" && !f.StripMarkers && f.Caps().Markers {
		m.Marker = token
	}
	return f.add(chatID, m)
}

func (f *Fake) add(chatID string, m channel.InboundMsg) channel.InboundMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.chats[chatID]
	if c == nil {
		c = &chat{info: channel.ChatInfo{ID: chatID, Title: chatID}}
		f.chats[chatID] = c
	}
	f.seq++
	m.Chat = chatID
	m.ID = "msg-" + strconv.FormatInt(f.seq, 10)
	m.Created = f.now()
	m.Modified = m.Created
	m.Hash = channel.TextHash(m.Text)
	c.msgs = append(c.msgs, stored{seq: f.seq, msg: m})
	return m
}

func (f *Fake) sendFault() func(string, channel.Part) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.SendFault
}

func (f *Fake) receiveFault(stream string) error {
	f.mu.Lock()
	hook := f.ReceiveFault
	f.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(stream)
}

var _ channel.Channel = (*Fake)(nil)
