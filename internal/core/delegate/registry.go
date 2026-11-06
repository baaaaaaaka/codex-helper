package delegate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/delegate/machineregistry"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

// MeetingChannel is the part of the Teams channel adapter the registry
// uses (*chteams.Adapter implements it).
type MeetingChannel interface {
	Send(ctx context.Context, chat string, p channel.Part, token string) (channel.Sent, error)
	Edit(ctx context.Context, chat, id string, p channel.Part, token string) error
	CreateMeetingChat(ctx context.Context, title, key string, start, end time.Time) (channel.ChatInfo, chteams.Meeting, error)
	GetMeeting(ctx context.Context, id string) (chteams.Meeting, error)
	RenewMeeting(ctx context.Context, id string, start, end time.Time) (chteams.Meeting, error)
	Owner(ctx context.Context) (id, name string, err error)
}

// RegistryConfig configures the machine card publisher.
type RegistryConfig struct {
	Channel  MeetingChannel
	TenantID string
	// MachineID is the registry id (ResolveMachineID); Label is the name
	// people see; Host and Profile become aliases.
	MachineID, Label, Host, Profile string
	Version                         string
	// CachePath is the registry cache shared with `cxp delegate`
	// (machineregistry.DefaultCachePath()).
	CachePath string
	// Interval is the heartbeat (the card's TTL is three of them).
	Interval time.Duration
	Now      func() time.Time
}

// Registry is engine.Registrar over the cross-machine registry of the old
// core (internal/core/delegate/machineregistry): the same registry chat, card
// format and inbox addressing, so `cxp delegate` requesters of either core
// find this machine.
type Registry struct {
	cfg   RegistryConfig
	store machineregistry.Store

	mu    sync.Mutex
	cache machineregistry.Cache
	owner string
	ready bool
}

var _ engine.Registrar = (*Registry)(nil)

// NewRegistry returns the publisher.
func NewRegistry(cfg RegistryConfig) *Registry {
	if cfg.Interval <= 0 {
		cfg.Interval = machineregistry.DefaultHeartbeatInterval
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Registry{cfg: cfg, store: machineregistry.Store{Graph: graph{cfg.Channel}, CachePath: cfg.CachePath,
		Now: func() time.Time { return cfg.Now().UTC() }}}
}

var _ engine.WritePaced = (*Registry)(nil)

// PaceWrites makes every Graph write of the registry (card posts and
// edits, meeting chat creation and renewal) wait for wait first: the
// engine's account write pacing, which the card shares with every other
// write of the helper. The engine calls it before it starts.
func (r *Registry) PaceWrites(wait func(ctx context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := pacedChannel{MeetingChannel: r.cfg.Channel, wait: wait}
	r.cfg.Channel = ch
	r.store.Graph = graph{ch}
}

// pacedChannel is a MeetingChannel whose writes wait for the pacing first.
type pacedChannel struct {
	MeetingChannel
	wait func(ctx context.Context) error
}

func (c pacedChannel) Send(ctx context.Context, chat string, p channel.Part, token string) (channel.Sent, error) {
	if err := c.wait(ctx); err != nil {
		return channel.Sent{}, err
	}
	return c.MeetingChannel.Send(ctx, chat, p, token)
}

func (c pacedChannel) Edit(ctx context.Context, chat, id string, p channel.Part, token string) error {
	if err := c.wait(ctx); err != nil {
		return err
	}
	return c.MeetingChannel.Edit(ctx, chat, id, p, token)
}

func (c pacedChannel) CreateMeetingChat(ctx context.Context, title, key string, start, end time.Time) (channel.ChatInfo, chteams.Meeting, error) {
	if err := c.wait(ctx); err != nil {
		return channel.ChatInfo{}, chteams.Meeting{}, err
	}
	return c.MeetingChannel.CreateMeetingChat(ctx, title, key, start, end)
}

func (c pacedChannel) RenewMeeting(ctx context.Context, id string, start, end time.Time) (chteams.Meeting, error) {
	if err := c.wait(ctx); err != nil {
		return chteams.Meeting{}, err
	}
	return c.MeetingChannel.RenewMeeting(ctx, id, start, end)
}

// InboxKey is the channel key (meeting externalId) of the machine's inbox
// chat: requesters open the inbox by this key (the card's inbox_ref).
func InboxKey(tenantID, ownerID, machineID string) string {
	return machineregistry.InboxExternalID(machineregistry.RegistryKey(tenantID, ownerID), machineID)
}

// Publish writes the card: it makes sure the registry chat exists (and its
// meeting window is renewed), keeps the inbox meeting window open so
// requesters can still open the inbox by its key, and edits this machine's
// card message (or posts one).
func (r *Registry) Publish(ctx context.Context, card engine.RegistryCard) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.owner == "" {
		id, _, err := r.cfg.Channel.Owner(ctx)
		if err != nil {
			return err
		}
		r.owner = id
	}
	cache := r.cache
	if !r.ready || cache.RegistryChatID == "" || cache.MeetingID == "" {
		ensured, err := r.store.Ensure(ctx, r.cfg.TenantID, r.owner)
		if err != nil {
			return err
		}
		cache, r.ready = ensured.Cache, true
	}
	if !cache.NextRefreshAt.IsZero() && !r.now().Before(cache.NextRefreshAt) {
		next, _, err := r.store.RefreshMeeting(ctx, cache)
		if err != nil {
			return err
		}
		cache = next
	}
	next, err := r.keepInbox(ctx, cache, card)
	if err != nil {
		return err
	}
	cache = next
	next, _, _, err = r.store.Publish(ctx, cache, r.card(cache, card.Draining))
	if err != nil {
		return err
	}
	r.cache = next
	return nil
}

// keepInbox records the inbox in the cache and renews its meeting window
// when it is due (the engine created the chat with a short window).
func (r *Registry) keepInbox(ctx context.Context, cache machineregistry.Cache, card engine.RegistryCard) (machineregistry.Cache, error) {
	key := machineregistry.InboxExternalID(cache.RegistryKey, r.cfg.MachineID)
	now := r.now()
	if cache.InboxExternalID == key && cache.InboxChatID == card.InboxChat && cache.InboxMeetingID != "" &&
		!cache.InboxNextRefreshAt.IsZero() && now.Before(cache.InboxNextRefreshAt) {
		return cache, nil
	}
	// createOrGet with the inbox key returns the engine's chat and its
	// meeting (it creates nothing new).
	info, mt, err := r.cfg.Channel.CreateMeetingChat(ctx, card.InboxTitle, key, now.Add(-5*time.Minute), now.Add(machineregistry.DefaultWindowDuration))
	if err != nil {
		return cache, statusError(err)
	}
	if info.ID != card.InboxChat {
		return cache, fmt.Errorf("registry: the inbox key opens chat %s, not the inbox %s", info.ID, card.InboxChat)
	}
	if mt.End.Before(now.Add(machineregistry.DefaultWindowDuration - machineregistry.DefaultRefreshInterval)) {
		if mt, err = r.cfg.Channel.RenewMeeting(ctx, mt.ID, now.Add(-5*time.Minute), now.Add(machineregistry.DefaultWindowDuration)); err != nil {
			return cache, statusError(err)
		}
	}
	next := cache
	next.SchemaVersion = 1
	next.InboxMachineID = r.cfg.MachineID
	next.InboxExternalID = key
	next.InboxChatID = info.ID
	next.InboxMeetingID = mt.ID
	next.InboxMeetingEndDateTime = mt.End.UTC().Format(time.RFC3339)
	next.InboxGeneration = shortHash("inbox-generation:"+mt.ID+":"+info.ID, 12)
	next.InboxValidatedAt = now
	next.InboxRefreshedAt = now
	next.InboxNextRefreshAt = now.Add(machineregistry.DefaultRefreshInterval)
	if _, err := machineregistry.SaveCache(r.cfg.CachePath, next); err != nil {
		return cache, err
	}
	return next, nil
}

func (r *Registry) card(cache machineregistry.Cache, draining bool) machineregistry.MachineCard {
	now := r.now()
	ttl := 3 * r.cfg.Interval
	caps := []string{"cxp", "codex", "teams-helper", "teams-registry"}
	var aliases []string
	for _, a := range []string{r.cfg.Host, r.cfg.Profile} {
		if a = strings.TrimSpace(a); a != "" {
			aliases = append(aliases, a)
		}
	}
	label := strings.TrimSpace(r.cfg.Label)
	if label == "" {
		label = r.cfg.MachineID
	}
	return machineregistry.MachineCard{
		Kind:                     machineregistry.CardKind,
		RegistryKey:              cache.RegistryKey,
		MachineID:                r.cfg.MachineID,
		InstanceID:               fmt.Sprintf("worker_%s_%d", r.cfg.MachineID, os.Getpid()),
		MachineLabel:             label,
		HostLabel:                strings.TrimSpace(r.cfg.Host),
		Aliases:                  aliases,
		HelperProfile:            strings.TrimSpace(r.cfg.Profile),
		CXPVersion:               strings.TrimSpace(r.cfg.Version),
		Platform:                 machineregistry.MachinePlatform{OS: runtime.GOOS, Arch: runtime.GOARCH},
		Capabilities:             caps,
		CapabilityFingerprint:    machineregistry.CapabilityFingerprint(caps),
		Skills:                   []string{"cxp"},
		ProtocolVersions:         []string{"cxp-delegation-v1"},
		InboxRef:                 cache.InboxExternalID,
		InboxGeneration:          cache.InboxGeneration,
		Accepting:                !draining,
		Draining:                 draining,
		Revision:                 int(now.Unix()),
		Sequence:                 int(now.Unix()),
		HeartbeatIntervalSeconds: int(r.cfg.Interval.Seconds()),
		TTLSeconds:               int(ttl.Seconds()),
		PublishedAt:              now.Format(time.RFC3339Nano),
		ExpiresAt:                now.Add(ttl).Format(time.RFC3339Nano),
	}
}

func (r *Registry) now() time.Time { return r.cfg.Now().UTC() }

// graph is machineregistry.Graph over the channel adapter. The publisher
// does not list; requesters (`cxp delegate`) list through a channel that
// reads a bounded tail (*chteams.Adapter.Recent).
type graph struct{ ch MeetingChannel }

// TailReader reads a chat's most recently modified messages, newest first.
type TailReader interface {
	Recent(ctx context.Context, chat string, limit int) ([]channel.InboundMsg, error)
}

// NewGraph is machineregistry.Graph over ch for requesters: listing needs
// ch to be a TailReader. Messages are listed as plain text (records and
// cards are marker + base64 tokens, which survive the conversion).
func NewGraph(ch MeetingChannel) machineregistry.Graph { return graph{ch} }

func (g graph) SendHTML(ctx context.Context, chat, html string) (machineregistry.ChatMessage, error) {
	sent, err := g.ch.Send(ctx, chat, chteams.HTMLPart(html), "")
	if err != nil {
		return machineregistry.ChatMessage{}, statusError(err)
	}
	return machineregistry.ChatMessage{ID: sent.ID}, nil
}

func (g graph) UpdateChatMessageHTML(ctx context.Context, chat, id, html string) error {
	return statusError(g.ch.Edit(ctx, chat, id, chteams.HTMLPart(html), ""))
}

func (g graph) ListMessages(ctx context.Context, chat string, top int) ([]machineregistry.ChatMessage, error) {
	r, ok := g.ch.(TailReader)
	if !ok {
		return nil, errors.New("registry: listing is not supported by this channel")
	}
	msgs, err := r.Recent(ctx, chat, top)
	if err != nil {
		return nil, statusError(err)
	}
	out := make([]machineregistry.ChatMessage, 0, len(msgs))
	for _, m := range msgs {
		// Code spans arrive as backticks around the token.
		text := strings.ReplaceAll(m.Text, "`", " ")
		out = append(out, machineregistry.ChatMessage{ID: m.ID,
			CreatedDateTime:      m.Created.UTC().Format(time.RFC3339Nano),
			LastModifiedDateTime: m.Modified.UTC().Format(time.RFC3339Nano),
			Body:                 machineregistry.ChatMessageBody{Content: html.EscapeString(text)}})
	}
	return out, nil
}

func (g graph) CreateOrGetMeetingChatWindow(ctx context.Context, topic, key string, start, end time.Time) (machineregistry.Chat, machineregistry.OnlineMeeting, error) {
	info, mt, err := g.ch.CreateMeetingChat(ctx, topic, key, start, end)
	if err != nil {
		return machineregistry.Chat{}, machineregistry.OnlineMeeting{}, statusError(err)
	}
	return machineregistry.Chat{ID: info.ID, Topic: info.Title, ChatType: "meeting", WebURL: info.URL}, meeting(mt), nil
}

func (g graph) GetOnlineMeeting(ctx context.Context, id string) (machineregistry.OnlineMeeting, error) {
	mt, err := g.ch.GetMeeting(ctx, id)
	if err != nil {
		return machineregistry.OnlineMeeting{}, statusError(err)
	}
	return meeting(mt), nil
}

func (g graph) UpdateOnlineMeetingWindow(ctx context.Context, id string, start, end time.Time) (machineregistry.OnlineMeeting, error) {
	mt, err := g.ch.RenewMeeting(ctx, id, start, end)
	if err != nil {
		return machineregistry.OnlineMeeting{}, statusError(err)
	}
	return meeting(mt), nil
}

func meeting(m chteams.Meeting) machineregistry.OnlineMeeting {
	out := machineregistry.OnlineMeeting{ID: m.ID, Subject: m.Subject, JoinWebURL: m.JoinURL, ChatThreadID: m.ChatID}
	if !m.Start.IsZero() {
		out.StartDateTime = m.Start.UTC().Format(time.RFC3339)
	}
	if !m.End.IsZero() {
		out.EndDateTime = m.End.UTC().Format(time.RFC3339)
	}
	return out
}

// statusError gives machineregistry the HTTP class it repairs on (404: a
// lost card message or meeting) and keeps the channel error inside, so the
// engine still sees a Retry-After.
func statusError(err error) error {
	if err == nil {
		return nil
	}
	var th *channel.Throttled
	switch {
	case errors.Is(err, channel.ErrNotFound):
		return &machineregistry.StatusError{StatusCode: http.StatusNotFound, Err: err}
	case errors.As(err, &th):
		return &machineregistry.StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: th.RetryAfter, Err: err}
	}
	return err
}

func shortHash(v string, n int) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:n])
}

// Configure turns on the cross-machine registry of cfg (cfg.Registry): the
// worker protocol under this machine's registry id, the inbox key requesters
// open the inbox by, and the card publisher. cachePath is the registry cache
// shared with `cxp delegate` ("" = machineregistry.DefaultCachePath()).
func Configure(ctx context.Context, cfg *engine.Config, ch MeetingChannel, tenantID, host, profile, cachePath string) error {
	id, err := ResolveIdentity(ctx, cfg.Store, ch, tenantID)
	if err != nil {
		return err
	}
	if cachePath == "" {
		if cachePath, err = machineregistry.DefaultCachePath(); err != nil {
			return err
		}
	}
	cfg.Delegations = New(id.MachineID)
	cfg.InboxKey = id.InboxKey
	cfg.Registrar = NewRegistry(RegistryConfig{Channel: ch, TenantID: tenantID, MachineID: id.MachineID,
		Label: cfg.Machine, Host: host, Profile: profile, Version: cfg.Version, CachePath: cachePath,
		Interval: cfg.Timing.Registry, Now: cfg.Now})
	return nil
}
