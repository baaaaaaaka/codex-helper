package machineregistry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/teamshtml"
)

const (
	CardMarker = "CXP_REGISTRY_CARD_V1"
	CardKind   = "cxp.machine-card.v1"

	DefaultHeartbeatInterval = 5 * time.Minute
	DefaultOnlineTTL         = 15 * time.Minute
	DefaultStaleTTL          = 2 * time.Hour
	DefaultWindowDuration    = 45 * 24 * time.Hour
	DefaultRefreshInterval   = 7 * 24 * time.Hour
	DefaultSlotRotation      = 30 * 24 * time.Hour
)

type MachineCard struct {
	Kind                     string          `json:"kind"`
	RegistryKey              string          `json:"registry_key"`
	MachineID                string          `json:"machine_id"`
	InstanceID               string          `json:"instance_id,omitempty"`
	MachineLabel             string          `json:"machine_label"`
	HostLabel                string          `json:"host_label,omitempty"`
	Aliases                  []string        `json:"aliases,omitempty"`
	HelperProfile            string          `json:"helper_profile,omitempty"`
	CXPVersion               string          `json:"cxp_version,omitempty"`
	Platform                 MachinePlatform `json:"platform,omitempty"`
	Capabilities             []string        `json:"capabilities,omitempty"`
	CapabilityFingerprint    string          `json:"capability_fingerprint,omitempty"`
	Workspaces               []WorkspaceRef  `json:"workspaces,omitempty"`
	Skills                   []string        `json:"skills,omitempty"`
	ModelProfiles            []string        `json:"model_profiles,omitempty"`
	ProtocolVersions         []string        `json:"protocol_versions,omitempty"`
	InboxRef                 string          `json:"inbox_ref,omitempty"`
	InboxGeneration          string          `json:"inbox_generation,omitempty"`
	Load                     MachineLoad     `json:"load,omitempty"`
	Accepting                bool            `json:"accepting"`
	Draining                 bool            `json:"draining,omitempty"`
	Revision                 int             `json:"revision,omitempty"`
	Sequence                 int             `json:"sequence"`
	HeartbeatIntervalSeconds int             `json:"heartbeat_interval_seconds"`
	TTLSeconds               int             `json:"ttl_seconds"`
	PublishedAt              string          `json:"published_at"`
	ExpiresAt                string          `json:"expires_at"`
}

type MachinePlatform struct {
	OS   string `json:"os,omitempty"`
	Arch string `json:"arch,omitempty"`
	WSL  bool   `json:"wsl,omitempty"`
}

type WorkspaceRef struct {
	Kind     string `json:"kind,omitempty"`
	Name     string `json:"name,omitempty"`
	RootHash string `json:"root_hash,omitempty"`
}

type MachineLoad struct {
	RunningTurns int `json:"running_turns,omitempty"`
	QueuedTurns  int `json:"queued_turns,omitempty"`
}

type MachineStatus struct {
	MachineID             string      `json:"machine_id"`
	InstanceID            string      `json:"instance_id,omitempty"`
	MachineLabel          string      `json:"machine_label"`
	HostLabel             string      `json:"host_label,omitempty"`
	Aliases               []string    `json:"aliases,omitempty"`
	State                 string      `json:"state"`
	Accepting             bool        `json:"accepting"`
	Draining              bool        `json:"draining,omitempty"`
	InboxRef              string      `json:"inbox_ref,omitempty"`
	InboxGeneration       string      `json:"inbox_generation,omitempty"`
	AgeSeconds            int         `json:"age_seconds"`
	Capabilities          []string    `json:"capabilities,omitempty"`
	CapabilityFingerprint string      `json:"capability_fingerprint,omitempty"`
	Skills                []string    `json:"skills,omitempty"`
	ProtocolVersions      []string    `json:"protocol_versions,omitempty"`
	Load                  MachineLoad `json:"load,omitempty"`
	Revision              int         `json:"revision,omitempty"`
	Sequence              int         `json:"sequence"`
}

type ChatMessage struct {
	ID                   string
	CreatedDateTime      string
	LastModifiedDateTime string
	Body                 ChatMessageBody
}

type MessageWindow struct {
	Messages  []ChatMessage
	Truncated bool
	NextPath  string
}

type ChatMessageBody struct {
	Content string
}

type Chat struct {
	ID       string
	Topic    string
	ChatType string
	WebURL   string
}

type OnlineMeeting struct {
	ID             string
	Subject        string
	JoinWebURL     string
	StartDateTime  string
	EndDateTime    string
	ExpiryDateTime string
	ChatThreadID   string
}

type StatusError struct {
	StatusCode int
	RetryAfter time.Duration
	Err        error
}

func (e *StatusError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("registry graph HTTP %d", e.StatusCode)
}

func (e *StatusError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type HeartbeatGraph interface {
	SendHTML(ctx context.Context, chatID string, html string) (ChatMessage, error)
	UpdateChatMessageHTML(ctx context.Context, chatID string, messageID string, html string) error
}

type MessageLister interface {
	ListMessages(ctx context.Context, chatID string, top int) ([]ChatMessage, error)
}

type MessageWindowLister interface {
	ListMessagesWindow(ctx context.Context, chatID string, top int) (MessageWindow, error)
	ListMessagesWindowFromPath(ctx context.Context, path string) (MessageWindow, error)
}

type Graph interface {
	HeartbeatGraph
	MessageLister
	CreateOrGetMeetingChatWindow(ctx context.Context, topic string, externalID string, start time.Time, end time.Time) (Chat, OnlineMeeting, error)
	GetOnlineMeeting(ctx context.Context, meetingID string) (OnlineMeeting, error)
	UpdateOnlineMeetingWindow(ctx context.Context, meetingID string, start time.Time, end time.Time) (OnlineMeeting, error)
}

type HeartbeatPublisher struct {
	Graph         HeartbeatGraph
	ChatID        string
	SlotMessageID string
}

type PublishResult struct {
	SlotMessageID string `json:"slot_message_id"`
	Mode          string `json:"mode"`
}

type Cache struct {
	SchemaVersion           int       `json:"schema_version"`
	TenantIDHash            string    `json:"tenant_id_hash,omitempty"`
	UserIDHash              string    `json:"user_id_hash,omitempty"`
	RegistryKey             string    `json:"registry_key,omitempty"`
	ExternalID              string    `json:"external_id,omitempty"`
	RegistryChatID          string    `json:"registry_chat_id,omitempty"`
	MeetingID               string    `json:"meeting_id,omitempty"`
	MeetingEndDateTime      string    `json:"meeting_end_date_time,omitempty"`
	RegistryGeneration      string    `json:"registry_generation,omitempty"`
	SlotMessageID           string    `json:"slot_message_id,omitempty"`
	SlotMachineID           string    `json:"slot_machine_id,omitempty"`
	InboxMachineID          string    `json:"inbox_machine_id,omitempty"`
	InboxExternalID         string    `json:"inbox_external_id,omitempty"`
	InboxChatID             string    `json:"inbox_chat_id,omitempty"`
	InboxMeetingID          string    `json:"inbox_meeting_id,omitempty"`
	InboxMeetingEndDateTime string    `json:"inbox_meeting_end_date_time,omitempty"`
	InboxGeneration         string    `json:"inbox_generation,omitempty"`
	InboxValidatedAt        time.Time `json:"inbox_validated_at,omitempty"`
	InboxRefreshedAt        time.Time `json:"inbox_refreshed_at,omitempty"`
	InboxNextRefreshAt      time.Time `json:"inbox_next_refresh_at,omitempty"`
	SlotCreatedAt           time.Time `json:"slot_created_at,omitempty"`
	NextSlotRotationAt      time.Time `json:"next_slot_rotation_at,omitempty"`
	CapabilityFingerprint   string    `json:"capability_fingerprint,omitempty"`
	ValidatedAt             time.Time `json:"validated_at,omitempty"`
	RefreshedAt             time.Time `json:"refreshed_at,omitempty"`
	NextRefreshAt           time.Time `json:"next_refresh_at,omitempty"`
}

type Store struct {
	Graph           Graph
	CachePath       string
	Subject         string
	WindowDuration  time.Duration
	RefreshInterval time.Duration
	SlotRotation    time.Duration
	Now             func() time.Time
}

type EnsureResult struct {
	Chat         Chat
	Meeting      OnlineMeeting
	Cache        Cache
	CacheWritten bool
}

func DefaultCachePath() (string, error) {
	return appdirs.StatePath("teams", "machine-registry.json")
}

func RegistryKey(tenantID string, userID string) string {
	return "cxp-registry-" + shortHash("registry-key:"+strings.TrimSpace(tenantID)+":"+strings.TrimSpace(userID), 16)
}

func ExternalID(registryKey string) string {
	return "cxp-registry-" + shortHash("registry-external-id:"+strings.TrimSpace(registryKey), 16)
}

func InboxExternalID(registryKey string, machineID string) string {
	return "cxp-inbox-" + shortHash("inbox-external-id:"+strings.TrimSpace(registryKey)+":"+strings.TrimSpace(machineID), 16)
}

func RenderCardHTML(card MachineCard) string {
	if strings.TrimSpace(card.Kind) == "" {
		card.Kind = CardKind
	}
	raw, _ := json.Marshal(card)
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	return "<p>" + CardMarker + " " + html.EscapeString(encoded) + "</p>"
}

func ParseCardMessage(content string) (MachineCard, bool) {
	text := teamshtml.PlainTextFromTeamsHTML(content)
	fields := strings.Fields(text)
	for i, field := range fields {
		if field != CardMarker || i+1 >= len(fields) {
			continue
		}
		raw, err := base64.RawURLEncoding.DecodeString(fields[i+1])
		if err != nil {
			continue
		}
		var card MachineCard
		if err := json.Unmarshal(raw, &card); err != nil {
			continue
		}
		return card, true
	}
	return MachineCard{}, false
}

func Observe(ctx context.Context, graph MessageLister, chatID string, registryKey string, top int, now time.Time) ([]MachineStatus, error) {
	if graph == nil {
		return nil, fmt.Errorf("registry message lister is required")
	}
	if windowLister, ok := graph.(MessageWindowLister); ok {
		return ObserveWindowed(ctx, windowLister, chatID, registryKey, top, now)
	}
	messages, err := graph.ListMessages(ctx, chatID, top)
	if err != nil {
		return nil, err
	}
	return ObserveMessages(messages, registryKey, now), nil
}

func ObserveWindowed(ctx context.Context, graph MessageWindowLister, chatID string, registryKey string, top int, now time.Time) ([]MachineStatus, error) {
	if graph == nil {
		return nil, fmt.Errorf("registry message window lister is required")
	}
	if top <= 0 {
		top = 50
	}
	var messages []ChatMessage
	nextPath := ""
	for page := 0; page < 20; page++ {
		var (
			window MessageWindow
			err    error
		)
		if page == 0 {
			window, err = graph.ListMessagesWindow(ctx, chatID, top)
		} else {
			window, err = graph.ListMessagesWindowFromPath(ctx, nextPath)
		}
		if err != nil {
			return nil, err
		}
		messages = append(messages, window.Messages...)
		if !window.Truncated || strings.TrimSpace(window.NextPath) == "" {
			break
		}
		nextPath = window.NextPath
	}
	return ObserveMessages(messages, registryKey, now), nil
}

func ObserveMessages(messages []ChatMessage, registryKey string, now time.Time) []MachineStatus {
	type observedCard struct {
		card       MachineCard
		observedAt time.Time
	}
	latest := map[string]observedCard{}
	for _, msg := range messages {
		card, ok := ParseCardMessage(msg.Body.Content)
		if !ok || card.Kind != CardKind || card.RegistryKey != registryKey || strings.TrimSpace(card.MachineID) == "" {
			continue
		}
		observedAt := firstNonZeroTime(parseTime(msg.LastModifiedDateTime), parseTime(msg.CreatedDateTime), parseTime(card.PublishedAt))
		prev, exists := latest[card.MachineID]
		if !exists || cardNewer(card, prev.card) || (!observedAt.IsZero() && observedAt.After(prev.observedAt)) {
			latest[card.MachineID] = observedCard{card: card, observedAt: observedAt}
		}
	}
	statuses := make([]MachineStatus, 0, len(latest))
	for _, observed := range latest {
		card := observed.card
		published := parseTime(card.PublishedAt)
		expires := parseTime(card.ExpiresAt)
		heartbeatAt := firstNonZeroTime(observed.observedAt, published)
		if card.TTLSeconds > 0 && !heartbeatAt.IsZero() {
			expires = heartbeatAt.Add(time.Duration(card.TTLSeconds) * time.Second)
		}
		state := "stale"
		if !expires.IsZero() && now.Before(expires) {
			state = "online"
		}
		age := 0
		if !heartbeatAt.IsZero() {
			age = int(now.Sub(heartbeatAt).Seconds())
			if age < 0 {
				age = 0
			}
		}
		statuses = append(statuses, MachineStatus{
			MachineID:             card.MachineID,
			InstanceID:            strings.TrimSpace(card.InstanceID),
			MachineLabel:          firstNonEmptyString(card.MachineLabel, card.MachineID),
			HostLabel:             strings.TrimSpace(card.HostLabel),
			Aliases:               append([]string(nil), card.Aliases...),
			State:                 state,
			Accepting:             card.Accepting && !card.Draining,
			Draining:              card.Draining,
			InboxRef:              strings.TrimSpace(card.InboxRef),
			InboxGeneration:       strings.TrimSpace(card.InboxGeneration),
			AgeSeconds:            age,
			Capabilities:          append([]string(nil), card.Capabilities...),
			CapabilityFingerprint: firstNonEmptyString(card.CapabilityFingerprint, CapabilityFingerprint(card.Capabilities)),
			Skills:                append([]string(nil), card.Skills...),
			ProtocolVersions:      append([]string(nil), card.ProtocolVersions...),
			Load:                  card.Load,
			Revision:              card.Revision,
			Sequence:              card.Sequence,
		})
	}
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].MachineID < statuses[j].MachineID
	})
	return statuses
}

func SplitStatuses(statuses []MachineStatus) ([]MachineStatus, []MachineStatus) {
	var online []MachineStatus
	var stale []MachineStatus
	for _, status := range statuses {
		if status.State == "online" {
			online = append(online, status)
		} else {
			stale = append(stale, status)
		}
	}
	return online, stale
}

func (p *HeartbeatPublisher) Publish(ctx context.Context, card MachineCard) (PublishResult, error) {
	if p == nil || p.Graph == nil {
		return PublishResult{}, fmt.Errorf("heartbeat graph is required")
	}
	chatID := strings.TrimSpace(p.ChatID)
	if chatID == "" {
		return PublishResult{}, fmt.Errorf("registry chat id is required")
	}
	html := RenderCardHTML(card)
	if strings.TrimSpace(p.SlotMessageID) != "" {
		if err := p.Graph.UpdateChatMessageHTML(ctx, chatID, p.SlotMessageID, html); err == nil {
			return PublishResult{SlotMessageID: p.SlotMessageID, Mode: "patch"}, nil
		} else if !permanentSlotLoss(err) {
			return PublishResult{}, err
		}
	}
	msg, err := p.Graph.SendHTML(ctx, chatID, html)
	if err != nil {
		return PublishResult{}, err
	}
	p.SlotMessageID = strings.TrimSpace(msg.ID)
	if p.SlotMessageID == "" {
		return PublishResult{}, fmt.Errorf("registry heartbeat response did not include message id")
	}
	return PublishResult{SlotMessageID: p.SlotMessageID, Mode: "append-slot"}, nil
}

func (s Store) Ensure(ctx context.Context, tenantID string, userID string) (EnsureResult, error) {
	if s.Graph == nil {
		return EnsureResult{}, fmt.Errorf("registry graph is required")
	}
	now := s.now()
	cache, err := LoadCache(s.CachePath)
	if err != nil {
		return EnsureResult{}, err
	}
	registryKey := RegistryKey(tenantID, userID)
	externalID := ExternalID(registryKey)
	tenantHash := shortHash("tenant:"+strings.TrimSpace(tenantID), 16)
	userHash := shortHash("user:"+strings.TrimSpace(userID), 16)
	if cacheMatchesLocator(cache, registryKey, externalID, tenantHash, userHash) {
		if chatID := strings.TrimSpace(cache.RegistryChatID); chatID != "" && strings.TrimSpace(cache.MeetingID) != "" {
			meeting, err := s.Graph.GetOnlineMeeting(ctx, cache.MeetingID)
			if err == nil {
				if threadID := meetingThreadID(meeting); threadID == "" || threadID == chatID {
					next := cache
					next.SchemaVersion = 1
					next.ValidatedAt = firstNonZeroTime(next.ValidatedAt, now)
					written, err := SaveCache(s.CachePath, next)
					if err != nil {
						return EnsureResult{}, err
					}
					return EnsureResult{
						Chat:         Chat{ID: chatID, Topic: s.subject(), ChatType: "meeting"},
						Meeting:      meeting,
						Cache:        next,
						CacheWritten: written,
					}, nil
				}
			} else if !cacheValidationRepairable(err) {
				return EnsureResult{}, err
			}
		}
	}
	start := now.Add(-5 * time.Minute)
	end := now.Add(s.windowDuration())
	chat, meeting, err := s.Graph.CreateOrGetMeetingChatWindow(ctx, s.subject(), externalID, start, end)
	if err != nil {
		return EnsureResult{}, err
	}
	next := cache
	next.SchemaVersion = 1
	next.TenantIDHash = tenantHash
	next.UserIDHash = userHash
	next.RegistryKey = registryKey
	next.ExternalID = externalID
	next.RegistryChatID = strings.TrimSpace(chat.ID)
	next.MeetingID = strings.TrimSpace(meeting.ID)
	next.MeetingEndDateTime = strings.TrimSpace(meeting.EndDateTime)
	next.RegistryGeneration = registryGeneration(next.MeetingID, next.RegistryChatID)
	if !sameCache(cache, next) {
		next.ValidatedAt = now
		next.RefreshedAt = now
		next.NextRefreshAt = now.Add(s.refreshInterval())
	}
	written, err := SaveCache(s.CachePath, next)
	if err != nil {
		return EnsureResult{}, err
	}
	return EnsureResult{Chat: chat, Meeting: meeting, Cache: next, CacheWritten: written}, nil
}

func (s Store) EnsureInbox(ctx context.Context, cache Cache, machineID string) (Cache, Chat, OnlineMeeting, bool, error) {
	if s.Graph == nil {
		return cache, Chat{}, OnlineMeeting{}, false, fmt.Errorf("registry graph is required")
	}
	machineID = strings.TrimSpace(machineID)
	if machineID == "" {
		return cache, Chat{}, OnlineMeeting{}, false, fmt.Errorf("machine id is required")
	}
	registryKey := strings.TrimSpace(cache.RegistryKey)
	if registryKey == "" {
		return cache, Chat{}, OnlineMeeting{}, false, fmt.Errorf("registry key is required")
	}
	now := s.now()
	externalID := InboxExternalID(registryKey, machineID)
	if cacheMatchesInbox(cache, machineID, externalID) {
		if chatID := strings.TrimSpace(cache.InboxChatID); chatID != "" && strings.TrimSpace(cache.InboxMeetingID) != "" {
			meeting, err := s.Graph.GetOnlineMeeting(ctx, cache.InboxMeetingID)
			if err == nil {
				if threadID := meetingThreadID(meeting); threadID == "" || threadID == chatID {
					next := cache
					next.SchemaVersion = 1
					next.InboxValidatedAt = firstNonZeroTime(next.InboxValidatedAt, now)
					written, err := SaveCache(s.CachePath, next)
					if err != nil {
						return cache, Chat{}, OnlineMeeting{}, false, err
					}
					return next, Chat{ID: chatID, Topic: s.inboxSubject(machineID), ChatType: "meeting"}, meeting, written, nil
				}
			} else if !cacheValidationRepairable(err) {
				return cache, Chat{}, OnlineMeeting{}, false, err
			}
		}
	}
	start := now.Add(-5 * time.Minute)
	end := now.Add(s.windowDuration())
	chat, meeting, err := s.Graph.CreateOrGetMeetingChatWindow(ctx, s.inboxSubject(machineID), externalID, start, end)
	if err != nil {
		return cache, Chat{}, OnlineMeeting{}, false, err
	}
	next := cache
	next.SchemaVersion = 1
	next.InboxMachineID = machineID
	next.InboxExternalID = externalID
	next.InboxChatID = strings.TrimSpace(chat.ID)
	next.InboxMeetingID = strings.TrimSpace(meeting.ID)
	next.InboxMeetingEndDateTime = strings.TrimSpace(meeting.EndDateTime)
	next.InboxGeneration = inboxGeneration(next.InboxMeetingID, next.InboxChatID)
	next.InboxValidatedAt = now
	next.InboxRefreshedAt = now
	next.InboxNextRefreshAt = now.Add(s.refreshInterval())
	written, err := SaveCache(s.CachePath, next)
	if err != nil {
		return cache, Chat{}, OnlineMeeting{}, false, err
	}
	return next, chat, meeting, written, nil
}

func (s Store) OpenInboxRef(ctx context.Context, inboxRef string) (Chat, OnlineMeeting, error) {
	if s.Graph == nil {
		return Chat{}, OnlineMeeting{}, fmt.Errorf("registry graph is required")
	}
	inboxRef = strings.TrimSpace(inboxRef)
	if inboxRef == "" {
		return Chat{}, OnlineMeeting{}, fmt.Errorf("inbox ref is required")
	}
	now := s.now()
	return s.Graph.CreateOrGetMeetingChatWindow(ctx, s.inboxSubject(""), inboxRef, now.Add(-5*time.Minute), now.Add(s.windowDuration()))
}

func (s Store) RefreshMeeting(ctx context.Context, cache Cache) (Cache, bool, error) {
	if s.Graph == nil {
		return cache, false, fmt.Errorf("registry graph is required")
	}
	meetingID := strings.TrimSpace(cache.MeetingID)
	if meetingID == "" {
		return cache, false, fmt.Errorf("registry meeting id is required")
	}
	now := s.now()
	if !cache.NextRefreshAt.IsZero() && now.Before(cache.NextRefreshAt) {
		return cache, false, nil
	}
	start := now.Add(-5 * time.Minute)
	end := now.Add(s.windowDuration())
	meeting, err := s.Graph.UpdateOnlineMeetingWindow(ctx, meetingID, start, end)
	if err != nil {
		return cache, false, err
	}
	next := cache
	next.SchemaVersion = 1
	next.MeetingID = firstNonEmptyString(strings.TrimSpace(meeting.ID), meetingID)
	next.MeetingEndDateTime = strings.TrimSpace(meeting.EndDateTime)
	next.RefreshedAt = now
	next.NextRefreshAt = now.Add(s.refreshInterval())
	written, err := SaveCache(s.CachePath, next)
	return next, written, err
}

func (s Store) Publish(ctx context.Context, cache Cache, card MachineCard) (Cache, PublishResult, bool, error) {
	if s.Graph == nil {
		return cache, PublishResult{}, false, fmt.Errorf("registry graph is required")
	}
	chatID := strings.TrimSpace(cache.RegistryChatID)
	if chatID == "" {
		return cache, PublishResult{}, false, fmt.Errorf("registry chat id is required")
	}
	if strings.TrimSpace(card.RegistryKey) == "" {
		card.RegistryKey = cache.RegistryKey
	}
	slotMessageID := cache.SlotMessageID
	if shouldRotateSlot(cache, s.now(), s.slotRotation()) {
		slotMessageID = ""
	}
	publisher := HeartbeatPublisher{Graph: s.Graph, ChatID: chatID, SlotMessageID: slotMessageID}
	result, err := publisher.Publish(ctx, card)
	if err != nil {
		return cache, PublishResult{}, false, err
	}
	next := cache
	next.SlotMessageID = publisher.SlotMessageID
	next.SlotMachineID = strings.TrimSpace(card.MachineID)
	next.CapabilityFingerprint = CapabilityFingerprint(card.Capabilities)
	if result.Mode == "append-slot" {
		now := s.now()
		next.SlotCreatedAt = now
		next.NextSlotRotationAt = now.Add(s.slotRotation())
	}
	written := false
	if !sameCache(cache, next) {
		var err error
		written, err = SaveCache(s.CachePath, next)
		if err != nil {
			return cache, PublishResult{}, false, err
		}
	}
	return next, result, written, nil
}

func (s Store) Observe(ctx context.Context, cache Cache, top int, now time.Time) ([]MachineStatus, error) {
	if s.Graph == nil {
		return nil, fmt.Errorf("registry graph is required")
	}
	chatID := strings.TrimSpace(cache.RegistryChatID)
	if chatID == "" {
		return nil, fmt.Errorf("registry chat id is required")
	}
	if now.IsZero() {
		now = s.now()
	}
	return Observe(ctx, s.Graph, chatID, cache.RegistryKey, top, now)
}

func LoadCache(path string) (Cache, error) {
	if strings.TrimSpace(path) == "" {
		return Cache{}, nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return Cache{}, nil
	}
	if err != nil {
		return Cache{}, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return Cache{}, nil
	}
	var cache Cache
	if err := json.Unmarshal(raw, &cache); err != nil {
		return Cache{}, err
	}
	return cache, nil
}

func SaveCache(path string, cache Cache) (bool, error) {
	if strings.TrimSpace(path) == "" {
		return false, nil
	}
	raw, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return false, err
	}
	raw = append(raw, '\n')
	if existing, err := os.ReadFile(path); err == nil && bytes.Equal(existing, raw) {
		return false, nil
	} else if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, err
	}
	if err := writeCacheFileAtomically(path, raw, 0o600); err != nil {
		return false, err
	}
	return true, nil
}

func (s Store) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s Store) windowDuration() time.Duration {
	if s.WindowDuration > 0 {
		return s.WindowDuration
	}
	return DefaultWindowDuration
}

func (s Store) refreshInterval() time.Duration {
	if s.RefreshInterval > 0 {
		return s.RefreshInterval
	}
	return DefaultRefreshInterval
}

func (s Store) slotRotation() time.Duration {
	if s.SlotRotation > 0 {
		return s.SlotRotation
	}
	return DefaultSlotRotation
}

func (s Store) subject() string {
	if subject := strings.TrimSpace(s.Subject); subject != "" {
		return subject
	}
	return "CXP Machine Registry"
}

func (s Store) inboxSubject(machineID string) string {
	machineID = strings.TrimSpace(machineID)
	if machineID == "" {
		return "CXP Machine Inbox"
	}
	return "CXP Machine Inbox " + machineID
}

func cardNewer(left MachineCard, right MachineCard) bool {
	if left.Revision != 0 || right.Revision != 0 {
		if left.Revision != right.Revision {
			return left.Revision > right.Revision
		}
	}
	leftTime := parseTime(left.PublishedAt)
	rightTime := parseTime(right.PublishedAt)
	if !leftTime.IsZero() && !rightTime.IsZero() && !leftTime.Equal(rightTime) {
		return leftTime.After(rightTime)
	}
	return left.Sequence > right.Sequence
}

func parseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}
	}
	return t
}

func registryGeneration(meetingID string, chatID string) string {
	return shortHash("registry-generation:"+meetingID+":"+chatID, 12)
}

func inboxGeneration(meetingID string, chatID string) string {
	return shortHash("inbox-generation:"+meetingID+":"+chatID, 12)
}

func cacheMatchesLocator(cache Cache, registryKey string, externalID string, tenantHash string, userHash string) bool {
	if strings.TrimSpace(cache.RegistryKey) != registryKey || strings.TrimSpace(cache.ExternalID) != externalID {
		return false
	}
	if strings.TrimSpace(cache.TenantIDHash) != "" && strings.TrimSpace(cache.TenantIDHash) != tenantHash {
		return false
	}
	if strings.TrimSpace(cache.UserIDHash) != "" && strings.TrimSpace(cache.UserIDHash) != userHash {
		return false
	}
	return true
}

func cacheMatchesInbox(cache Cache, machineID string, externalID string) bool {
	return strings.TrimSpace(cache.InboxMachineID) == strings.TrimSpace(machineID) &&
		strings.TrimSpace(cache.InboxExternalID) == strings.TrimSpace(externalID)
}

func meetingThreadID(meeting OnlineMeeting) string {
	return strings.TrimSpace(meeting.ChatThreadID)
}

func cacheValidationRepairable(err error) bool {
	var graphErr *StatusError
	if errors.As(err, &graphErr) {
		return graphErr.StatusCode == http.StatusNotFound || graphErr.StatusCode == http.StatusGone
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "not found") || strings.Contains(text, "gone")
}

func permanentSlotLoss(err error) bool {
	var graphErr *StatusError
	if errors.As(err, &graphErr) {
		return graphErr.StatusCode == http.StatusNotFound || graphErr.StatusCode == http.StatusGone
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "message not found") || strings.Contains(text, "slot disappeared")
}

func shouldRotateSlot(cache Cache, now time.Time, interval time.Duration) bool {
	if strings.TrimSpace(cache.SlotMessageID) == "" || now.IsZero() || interval <= 0 {
		return false
	}
	if !cache.NextSlotRotationAt.IsZero() {
		return !now.Before(cache.NextSlotRotationAt)
	}
	if !cache.SlotCreatedAt.IsZero() {
		return !now.Before(cache.SlotCreatedAt.Add(interval))
	}
	return false
}

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func CapabilityFingerprint(capabilities []string) string {
	if len(capabilities) == 0 {
		return ""
	}
	values := append([]string(nil), capabilities...)
	sort.Strings(values)
	return shortHash("capabilities:"+strings.Join(values, "\x00"), 12)
}

func shortHash(value string, bytes int) string {
	if bytes <= 0 || bytes > sha256.Size {
		bytes = 12
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:bytes])
}

func sameCache(left Cache, right Cache) bool {
	leftRaw, _ := json.Marshal(left)
	rightRaw, _ := json.Marshal(right)
	return bytes.Equal(leftRaw, rightRaw)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
