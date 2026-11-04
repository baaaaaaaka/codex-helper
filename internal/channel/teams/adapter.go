// Package teams is the Microsoft Teams adapter of the channel boundary,
// talking to Microsoft Graph with one delegated user token.
//
// The helper posts as the signed-in user, so its own messages come back in
// Receive (Caps.SelfEcho) and are recognised by the core through the outbox
// (message id, body hash) and, once the self-test shows Teams keeps HTML
// comments, through the hidden marker.
package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Config configures an Adapter. Only Tokens is required.
type Config struct {
	GraphURL string       // default https://graph.microsoft.com/v1.0
	HTTP     *http.Client // default: no whole-request timeout (uploads stream); requests use ctx deadlines
	Tokens   TokenSource
	TenantID string // for chat links

	// Math renders TeX to PNG. nil renders every formula as a code block.
	Math MathRenderer
	// SharePointHosts are the hosts inbound file links may point to. Entries
	// starting with "." are suffixes. Empty means ".sharepoint.com".
	SharePointHosts []string
	// Overlap is how far behind the cursor each Receive re-reads, to catch
	// messages Graph indexes late. Default 2 minutes.
	Overlap time.Duration
	// MaxPages bounds one Receive walk (50 messages per page). Default 1000.
	MaxPages int
	// UploadFolder is the OneDrive folder for outbound files.
	UploadFolder string
	// ChunkSize is the resumable upload chunk (multiple of 320 KiB). Default 10 MiB.
	ChunkSize int64

	Now   func() time.Time
	Sleep func(context.Context, time.Duration) error
}

// Adapter implements channel.Channel for Teams.
type Adapter struct {
	cfg Config
	g   *graph

	mu    sync.Mutex
	owner *identity
}

type identity struct {
	ID   string `json:"id"`
	Name string `json:"displayName"`
	UPN  string `json:"userPrincipalName"`
}

const (
	pageSize     = 50
	maxPartBytes = 32 << 10 // R-36 / N-15; Graph's own limit is ~100 KB
	markerPrefix = "codex-helper-outbox:"
)

var (
	markerRE      = regexp.MustCompile(`(?i)<!--\s*codex-helper-outbox:([A-Za-z0-9._:@%+=~-]{1,200})\s*-->`)
	markerTokenRE = regexp.MustCompile(`^[A-Za-z0-9._:@%+=~-]{1,200}$`)
)

// New returns an Adapter.
func New(cfg Config) (*Adapter, error) {
	if cfg.Tokens == nil {
		return nil, errors.New("teams: Config.Tokens is required")
	}
	if cfg.GraphURL == "" {
		cfg.GraphURL = defaultGraphURL
	}
	base, err := url.Parse(strings.TrimRight(cfg.GraphURL, "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("teams: bad GraphURL %q", cfg.GraphURL)
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{}
	}
	if cfg.Overlap <= 0 {
		cfg.Overlap = 2 * time.Minute
	}
	if cfg.MaxPages <= 0 {
		cfg.MaxPages = 1000
	}
	if cfg.UploadFolder == "" {
		cfg.UploadFolder = "Microsoft Teams Chat Files"
	}
	if cfg.ChunkSize <= 0 || cfg.ChunkSize%(320<<10) != 0 {
		cfg.ChunkSize = 10 << 20
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	return &Adapter{cfg: cfg, g: &graph{base: base, http: cfg.HTTP, tokens: cfg.Tokens, sleep: cfg.Sleep}}, nil
}

func (a *Adapter) Caps() channel.Caps {
	return channel.Caps{
		ReadBack:     true,
		SelfEcho:     true,
		Mentions:     true,
		Quotes:       true,
		EditMessages: true,
		MarkUnread:   true,
		Markers:      true,
		HostedFiles:  true,
		MaxPartBytes: maxPartBytes,
	}
}

// Owner returns the signed-in user (cached after the first call).
func (a *Adapter) Owner(ctx context.Context) (id, name string, err error) {
	a.mu.Lock()
	o := a.owner
	a.mu.Unlock()
	if o != nil {
		return o.ID, o.Name, nil
	}
	var me identity
	if err := a.g.getJSON(ctx, "/me?$select=id,displayName,userPrincipalName", smallBodyLimit, &me); err != nil {
		return "", "", err
	}
	if me.ID == "" {
		return "", "", &channel.Rejected{Err: errors.New("graph /me returned no id")}
	}
	a.mu.Lock()
	a.owner = &me
	a.mu.Unlock()
	return me.ID, me.Name, nil
}

func (a *Adapter) me(ctx context.Context) (identity, error) {
	if _, _, err := a.Owner(ctx); err != nil {
		return identity{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return *a.owner, nil
}

// Send posts one rendered part. token becomes the hidden outbox marker.
// The POST is never retried here; see graph.do for the outcome classes.
func (a *Adapter) Send(ctx context.Context, chat string, p channel.Part, token string) (channel.Sent, error) {
	msg, err := withMarker(p.Body, token)
	if err != nil {
		return channel.Sent{}, &channel.Rejected{Permanent: true, Err: err}
	}
	path := "/chats/" + url.PathEscape(chat) + "/messages"
	var in any = msg
	if p.ReplyTo != "" {
		path += "/replyWithQuote"
		in = map[string]any{"messageIds": []string{p.ReplyTo}, "replyMessage": msg}
	}
	var out struct {
		ID      string `json:"id"`
		Created string `json:"createdDateTime"`
	}
	if err := a.g.writeJSON(ctx, http.MethodPost, path, in, &out); err != nil {
		return channel.Sent{}, err
	}
	if out.ID == "" {
		return channel.Sent{}, &channel.Ambiguous{Err: errors.New("graph accepted the message but returned no id")}
	}
	t, _ := time.Parse(time.RFC3339Nano, out.Created)
	return channel.Sent{ID: out.ID, Time: t}, nil
}

// withMarker returns the stored part body with the marker comment
// prepended to body.content.
func withMarker(body, token string) (json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return nil, fmt.Errorf("part body is not a chatMessage: %w", err)
	}
	if token == "" {
		return json.RawMessage(body), nil
	}
	if !markerTokenRE.MatchString(token) {
		return nil, fmt.Errorf("marker token %q has unsupported characters", token)
	}
	var b struct {
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
	}
	if err := json.Unmarshal(m["body"], &b); err != nil {
		return nil, fmt.Errorf("part body has no body object: %w", err)
	}
	b.Content = "<!-- " + markerPrefix + token + " -->" + b.Content
	raw, _ := json.Marshal(b)
	m["body"] = raw
	return json.Marshal(m)
}

// ReadBack lists chat messages modified after since (no overlap), oldest
// first. Used to resolve Ambiguous sends: match by id, Marker or Hash.
func (a *Adapter) ReadBack(ctx context.Context, chat string, since time.Time) ([]channel.InboundMsg, error) {
	msgs, _, _, err := a.walk(ctx, chat, since)
	return msgs, err
}

// Get fetches one message.
func (a *Adapter) Get(ctx context.Context, chat, id string) (channel.InboundMsg, error) {
	me, err := a.me(ctx)
	if err != nil {
		return channel.InboundMsg{}, err
	}
	var gm graphMessage
	if err := a.g.getJSON(ctx, "/chats/"+url.PathEscape(chat)+"/messages/"+url.PathEscape(id), smallBodyLimit, &gm); err != nil {
		return channel.InboundMsg{}, err
	}
	return a.convert(chat, gm, me.ID), nil
}

// CreateChat creates (or finds) the meeting chat whose externalId is key.
// createOrGet is idempotent, so an Ambiguous result may be retried as is.
func (a *Adapter) CreateChat(ctx context.Context, title, key string) (channel.ChatInfo, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return channel.ChatInfo{}, &channel.Rejected{Permanent: true, Err: errors.New("CreateChat needs a key")}
	}
	me, err := a.me(ctx)
	if err != nil {
		return channel.ChatInfo{}, err
	}
	subject := SanitizeTopic(title)
	now := a.cfg.Now().UTC().Truncate(time.Second)
	in := map[string]any{
		"subject":       subject,
		"externalId":    key,
		"startDateTime": now.Format(time.RFC3339),
		"endDateTime":   now.Add(24 * time.Hour).Format(time.RFC3339),
		"participants": map[string]any{"attendees": []map[string]any{{
			"upn": me.UPN, "role": "attendee",
			"identity": map[string]any{"user": map[string]any{"id": me.ID}},
		}}},
		"allowMeetingChat":               "enabled",
		"shareMeetingChatHistoryDefault": "all",
	}
	var out struct {
		Subject  string `json:"subject"`
		ChatInfo struct {
			ThreadID string `json:"threadId"`
		} `json:"chatInfo"`
	}
	if err := a.g.writeJSON(ctx, http.MethodPost, "/me/onlineMeetings/createOrGet", in, &out); err != nil {
		return channel.ChatInfo{}, err
	}
	id := strings.TrimSpace(out.ChatInfo.ThreadID)
	if id == "" {
		return channel.ChatInfo{}, &channel.Ambiguous{Err: errors.New("onlineMeeting response has no chatInfo.threadId")}
	}
	return channel.ChatInfo{ID: id, Title: first(SanitizeTopic(out.Subject), subject), URL: ChatURL(id, a.cfg.TenantID)}, nil
}

// RenameChat sets the chat topic. PATCH is idempotent.
func (a *Adapter) RenameChat(ctx context.Context, chat, title string) error {
	return a.g.writeJSON(ctx, http.MethodPatch, "/chats/"+url.PathEscape(chat), map[string]string{"topic": SanitizeTopic(title)}, nil)
}

// ChatURL is the Teams web link of a chat.
func ChatURL(chatID, tenantID string) string {
	link := "https://teams.microsoft.com/l/chat/" + url.QueryEscape(chatID) + "/0"
	if tenantID != "" {
		link += "?tenantId=" + url.QueryEscape(tenantID)
	}
	return link
}

// SanitizeTopic makes a title acceptable as a Teams chat topic.
func SanitizeTopic(topic string) string {
	var b strings.Builder
	for _, r := range topic {
		switch {
		case unicode.IsControl(r):
		case strings.ContainsRune(`:<>"/\|?*`, r):
			b.WriteByte('-')
		default:
			b.WriteRune(r)
		}
	}
	clean := strings.Trim(strings.Join(strings.Fields(b.String()), " "), " -.")
	if rs := []rune(clean); len(rs) > 120 {
		clean = strings.TrimSpace(string(rs[:120]))
	}
	if clean == "" {
		return "codex session"
	}
	return clean
}

var _ channel.Channel = (*Adapter)(nil)
