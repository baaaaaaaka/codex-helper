// Package channel is the boundary between the codex-helper core and a chat
// service (Microsoft Teams today, Telegram later).
//
// The core talks only to the Channel interface and the neutral types in this
// file. Ids, cursors and rendered payloads are opaque strings owned by the
// adapter; the core stores and compares them but never parses them.
// Service-specific reliability features (read-back, own-message recognition,
// hidden markers) are switched on through Caps.
package channel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
	"unicode"
)

// Channel is one chat service account (one signed-in identity).
//
// Methods are safe for concurrent use. No method retries a write internally:
// a write either succeeds or fails with *Rejected, *Ambiguous, *Throttled or
// *AuthRequired (errors.go), and the core decides what happens next.
type Channel interface {
	Caps() Caps

	// Render turns a neutral document into one or more parts that each fit
	// Caps.MaxPartBytes. The parts are deterministic for a given document and
	// renderer state, and are meant to be stored verbatim and sent later.
	Render(ctx context.Context, doc RichDoc) ([]Part, error)

	// Receive returns every message of the stream newer than cursor (the
	// whole window, all pages), in admission order (oldest first), plus the
	// new cursor. The cursor never moves backwards. Messages already returned
	// earlier may be returned again (overlap re-reads); the core dedupes by
	// (Chat, ID). An empty cursor means "from the beginning of the stream".
	Receive(ctx context.Context, stream, cursor string) (Window, error)

	// Send posts one part. token is a per-attempt id the adapter embeds as a
	// hidden marker when Caps.Markers is set, so ReadBack can recognise the
	// message even if the response was lost.
	Send(ctx context.Context, chat string, p Part, token string) (Sent, error)

	// ReadBack lists messages of chat modified after since, oldest first.
	// Only meaningful when Caps.ReadBack is set.
	ReadBack(ctx context.Context, chat string, since time.Time) ([]InboundMsg, error)

	// Get fetches one message by id (quotes, marker self-test).
	Get(ctx context.Context, chat, id string) (InboundMsg, error)

	// CreateChat creates the chat for key, or returns the one already
	// created for the same key (idempotent).
	CreateChat(ctx context.Context, title, key string) (ChatInfo, error)
	RenameChat(ctx context.Context, chat, title string) error

	// FetchFile downloads an inbound file to dst. Partial downloads are kept
	// next to dst (dst + ".part") and resumed by the next call.
	FetchFile(ctx context.Context, ref FileRef, dst string) (FileRef, error)

	// UploadFile uploads a local file and returns a FileRef that can be put
	// in RichDoc.Files. sess carries a resumable session from an earlier
	// attempt (nil for a fresh upload); save is called whenever the session
	// advances so the core can persist it.
	UploadFile(ctx context.Context, src, name string, sess *UploadSession, save func(UploadSession) error) (FileRef, error)

	// Edit replaces the content of a message this account posted with a
	// part rendered by Render (Caps.EditMessages). token is embedded as the
	// hidden marker like in Send; pass the marker of the original message to
	// keep it recognisable. Editing is idempotent: after *Ambiguous the core
	// may simply repeat the call.
	Edit(ctx context.Context, chat, id string, p Part, token string) error

	// MarkUnread makes chat show as unread for the owner (Caps.MarkUnread):
	// every message created after readAt counts as unread. A delegated-user
	// channel posts answers as the owner, so without this the chat looks
	// already read. To flag one sent answer pass Sent.Time minus one
	// millisecond (zero readAt: let the service choose). It is idempotent and
	// a notification nicety: a *Throttled from it has Scope == ScopeCall and
	// must never hold back sends.
	MarkUnread(ctx context.Context, chat string, readAt time.Time) error
}

// Caps describes what a channel supports. The core switches features on
// and off with these flags instead of checking the channel type.
type Caps struct {
	ReadBack     bool // ReadBack can resolve Ambiguous sends by listing recent messages
	SelfEcho     bool // Receive also returns messages this helper posted (delegated-user channels)
	Mentions     bool // RichDoc.Mention notifies the owner
	Quotes       bool // RichDoc.ReplyTo quotes an earlier message
	EditMessages bool // Edit works for messages this account posted
	MarkUnread   bool // MarkUnread works
	Markers      bool // Send can embed a hidden marker; whether the service keeps it must still be self-tested
	HostedFiles  bool // UploadFile and FetchFile work
	MaxPartBytes int  // hard limit of Part.Size
}

// RichDoc is the neutral outbound document.
type RichDoc struct {
	// Label is the visible header ("🤖 ✅ Codex answer"). When the document
	// is split the adapter appends " [part i/n]".
	Label string
	// Markdown is the body: CommonMark blocks and inlines plus GFM tables and
	// strikethrough.
	Markdown string
	// Math enables formula spans written as <m>TeX</m> inside Markdown. Set it
	// only for Codex-authored text. A formula the channel cannot render
	// faithfully is shown as a code block with the original TeX.
	Math bool
	// Mention notifies the owner. Only the first part carries the mention.
	Mention bool
	// ReplyTo quotes an earlier message (its ext id). First part only.
	ReplyTo string
	// Files are already-uploaded outbound files (from UploadFile). They are
	// attached to the last part.
	Files []FileRef
	// Footer is a closing line after the body of the last part, shown as
	// "FooterLabel: Footer" (v0.1.20's "🔧 Helper: ✅ Codex finished
	// responding." under an answer). Plain text.
	FooterLabel, Footer string
	// Notes are closing lines after the body of the last part and before the
	// footer, each shown as "Label: Text" (the active goal and model). A note
	// can request a visual separator before it.
	Notes []Note
}

// Note is one closing line of a document (RichDoc.Notes). SeparatorBefore
// inserts a visual divider before the note when the channel supports it.
type Note struct {
	Label, Text     string
	SeparatorBefore bool
}

// Part is one rendered, sendable message.
type Part struct {
	Index   int    // 1-based
	Count   int    // parts of the same document
	Body    string // channel payload, stored verbatim and sent as is (the marker is added at send time)
	Hash    string // TextHash of the visible text; equals InboundMsg.Hash of the echo
	Mention bool
	ReplyTo string
	Size    int // bytes counted against Caps.MaxPartBytes
}

// Sent is the outcome of an accepted Send.
type Sent struct {
	ID   string
	Time time.Time
}

// Window is the result of one Receive.
type Window struct {
	Msgs     []InboundMsg // oldest first
	Cursor   string       // pass to the next Receive; never behind the input cursor
	Requests int          // service requests used (for read budgets)
	// OtherParticipants is an authoritative current membership snapshot.
	// nil means this channel can only infer membership from messages.
	OtherParticipants *bool
}

// ParticipantReader distinguishes current membership from historical senders.
// An empty or incomplete member list must not be reported as owner-only.
type ParticipantReader interface {
	OtherParticipants(ctx context.Context, chat string) (bool, error)
}

// MsgKind classifies an inbound message.
type MsgKind int

const (
	KindMessage MsgKind = iota // a normal user-authored message
	KindSystem                 // service event (member added, meeting started, …)
	KindDeleted                // tombstone of a deleted message
)

func (k MsgKind) String() string {
	switch k {
	case KindSystem:
		return "system"
	case KindDeleted:
		return "deleted"
	default:
		return "message"
	}
}

// InboundMsg is one received message in neutral form.
type InboundMsg struct {
	Chat          string
	ID            string
	Created       time.Time // admission order key
	Modified      time.Time
	Kind          MsgKind
	From          string // sender id; "" for system messages
	FromName      string
	ByOwner       bool   // sent by the signed-in account (the helper posts as this account too)
	MentionsOwner bool   // the owner is @mentioned
	Edited        bool   // the author edited it after posting
	Text          string // plain text, "\n" line breaks
	Hash          string // TextHash(Text)
	Marker        string // hidden helper marker found in the message ("" if none or stripped)
	Files         []FileRef
	Quotes        []Quote
	// Attachments names everything the message carries besides its text,
	// as the service types it, in v0.1.20's order: each attachment's
	// content type (files, quotes, media cards, cards), then one
	// "Teams-hosted inline content" per inline image of the body. The
	// control chat, which takes text only, refuses a message by these.
	Attachments []string
}

// Quote is a message quoted or forwarded inside an inbound message.
type Quote struct {
	ID        string
	FromName  string
	Text      string // preview text as delivered by the service; fetch with Get for the full body
	Time      string // the original's send time as the service gives it ("" when not given)
	Forwarded bool   // a forward: the original may be in another chat and is not fetched
}

// FileKind classifies a file reference.
type FileKind int

const (
	FileDoc         FileKind = iota // ordinary file
	FileImage                       // inline image
	FileVoice                       // voice or video clip (ASR input)
	FileUnsupported                 // present but not fetchable; Note says why
)

// FileRef is an inbound or outbound file.
type FileRef struct {
	Kind         FileKind
	Name         string
	Size         int64 // 0 when unknown
	ContentType  string
	Ref          string // scalar adapter handle
	AttachmentID string // uploaded reference attachment identity
	URL          string // uploaded reference URL
	Note         string // reason for FileUnsupported
}

// UploadSession is resumable upload state the core persists between attempts.
type UploadSession struct {
	URL       string // service upload URL (a bearer secret; store it like a token)
	Target    string // adapter destination handle, used to find a finished upload
	ExpiresAt time.Time
	Offset    int64 // bytes the service has acknowledged
}

// ChatInfo describes a created chat.
type ChatInfo struct {
	ID    string // also the Receive stream for per-chat channels
	Title string
	URL   string // link the user can open
}

// TextHash is the content hash used for own-message recognition. It is
// computed over visible text with whitespace runs collapsed and pictographic
// symbols (emoji, their modifiers and joiners) removed, because services
// rewrite markup and emoji on storage (Teams stores emoji as <emoji> image
// tags) while the words stay the same.
func TextHash(text string) string {
	clean := strings.Map(func(r rune) rune {
		switch {
		case r < 0x80:
			return r // no ASCII rune is one of the below
		case unicode.Is(unicode.So, r), unicode.Is(unicode.Cf, r), unicode.Is(unicode.Sk, r) && r > 0xFF,
			r >= 0xFE00 && r <= 0xFE0F, r >= 0xE0100 && r <= 0xE01EF, r == 0x20E3:
			return ' '
		}
		return r
	}, text)
	sum := sha256.Sum256([]byte(strings.Join(strings.Fields(clean), " ")))
	return hex.EncodeToString(sum[:16])
}

// BodyHasher is implemented by channels that can recompute Part.Hash from
// a stored part body (used once to re-hash imported legacy outbox rows).
type BodyHasher interface {
	BodyHash(body string) string
}

// HistoryPager is implemented by channels that can list a chat's history
// one service request at a time, so the core can pace a long read.
type HistoryPager interface {
	// HistoryPage returns one page of the messages of chat modified after
	// since, newest first, and the position of the next page ("" after the
	// last one). pos "" starts the listing.
	HistoryPage(ctx context.Context, chat string, since time.Time, pos string) ([]InboundMsg, string, error)
}

// Annotator is implemented by channels that can annotate a message in
// place (Caps.EditMessages): what v0.1.20 added to the owner's messages
// (the user label, a voice clip's transcript) and to the newest message
// of a chat that stopped listening (the freeze notice). One call reads the
// message once and edits it at most once, keeping its attachments,
// mentions and quotes. It edits nothing and returns an error wrapping
// ErrNotEditable when the edit could lose part of the message or the
// message is not the signed-in account's, and nil when the annotation is
// already there.
type Annotator interface {
	Annotate(ctx context.Context, chat, id string, a Annotation) error
}

// MessagePrefixEditor adds or removes one first-line marker on a message
// posted by this account. It must preserve the rest of the message.
type MessagePrefixEditor interface {
	SetMessagePrefix(ctx context.Context, chat, id, prefix string, present bool) error
}

// Annotation is one in-place edit (Annotator). Label alone is the user
// marker; Clips adds a voice transcript (the label too, when missing);
// Notice appends a document below the message.
type Annotation struct {
	// Confirmation appends one already rendered initial prompt confirmation.
	// ConfirmationKey is stable across delivery attempts, separate from own-post markers.
	Confirmation    *Part
	ConfirmationKey string
	// Label heads the message as its own line ("🧑‍💻 User:") unless the
	// message already starts with it. Without Clips, a message carrying a
	// voice or video clip is not labelled (it gets its transcript).
	Label string
	// ClipsLabel heads the transcripts of the message's voice clips.
	ClipsLabel string
	Clips      []Clip
	// Notice is appended below the message. Present: the message already
	// shows the notice when its text contains every one of these strings.
	Notice  *RichDoc
	Present []string
}

// Clip is the transcript of one voice or video clip of a message.
type Clip struct {
	Name string
	Text string
}

// PartTexter is implemented by channels that can tell the visible text of a
// rendered part body, as InboundMsg.Text carries it when the part is read.
type PartTexter interface {
	PartText(body string) string
}

// ChatActivity is one chat's newest message as reported by an activity scan.
type ChatActivity struct {
	Chat   string
	LastID string    // id of the newest message ("" when the chat has none)
	Last   time.Time // its creation time (service clock)
	ByMe   bool      // the newest message was posted by this account
}

// ActivityScanner is implemented by channels that can list, in one cheap
// account-wide request, which chats received messages recently (Teams:
// GET /me/chats ordered by lastMessagePreview). The core then reads only
// the chats that changed instead of polling every idle chat.
type ActivityScanner interface {
	// ScanActivity returns the chats whose newest message is newer than
	// since, newest first, paging until it reaches older chats (all chats
	// with a message when since is zero, bounded by the adapter's page
	// limit). The order and times are the service's.
	ScanActivity(ctx context.Context, since time.Time) ([]ChatActivity, error)
}

// PartCombiner optionally combines complete rendered progress parts, preserving
// every visible block. It declines unsafe or oversized combinations. The core
// freezes accepted payloads and membership before attempting any channel write.
type PartCombiner interface {
	CombineProgress([]Part) (Part, bool)
}

// OwnerNotice is a small, transport-independent owner notification. Empty
// optional fields are omitted; the heading always describes the event.
type OwnerNotice struct {
	Heading, ChatTitle, Request, Hint, Button, URL string
}
