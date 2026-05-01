package teams

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const DefaultSendLeaseDuration = 2 * time.Minute

type SendScheduleStatus string

const (
	SendStatusQueued   SendScheduleStatus = "queued"
	SendStatusLeased   SendScheduleStatus = "leased"
	SendStatusSent     SendScheduleStatus = "sent"
	SendStatusPoisoned SendScheduleStatus = "poisoned"
)

var (
	ErrScheduledSendInvalid       = errors.New("scheduled send item is invalid")
	ErrScheduledSendItemNotFound  = errors.New("scheduled send item not found")
	ErrScheduledSendLeaseNotHeld  = errors.New("scheduled send lease is not held")
	ErrScheduledSendChatIDMissing = errors.New("scheduled send chat id is required")
)

type OutboundSendPayload struct {
	Kind        string
	Body        string
	ContentType string
}

type ScheduledSendItem struct {
	ID              string
	ChatID          string
	Sequence        int64
	Payload         OutboundSendPayload
	Status          SendScheduleStatus
	Attempts        int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	LeaseID         string
	LeaseUntil      time.Time
	LastError       string
	PoisonReason    string
	SentMessageID   string
	SentAt          time.Time
	SourceBatchID   string
	SourcePartIndex int
}

type SendLease struct {
	LeaseID   string
	ItemID    string
	ChatID    string
	ExpiresAt time.Time
	Item      ScheduledSendItem
}

type SendFailure struct {
	Reason     string
	RetryAfter time.Duration
	BlockUntil time.Time
	Poison     bool
}

type SendSchedulerOptions struct {
	LeaseDuration time.Duration
}

type SendScheduler struct {
	leaseDuration time.Duration
	items         map[string]ScheduledSendItem
	chatBlocks    map[string]time.Time
}

func NewSendScheduler(opts SendSchedulerOptions) *SendScheduler {
	lease := opts.LeaseDuration
	if lease <= 0 {
		lease = DefaultSendLeaseDuration
	}
	return &SendScheduler{
		leaseDuration: lease,
		items:         make(map[string]ScheduledSendItem),
		chatBlocks:    make(map[string]time.Time),
	}
}

func (s *SendScheduler) Enqueue(item ScheduledSendItem) (ScheduledSendItem, bool, error) {
	return s.EnqueueAt(time.Now(), item)
}

func (s *SendScheduler) EnqueueAt(now time.Time, item ScheduledSendItem) (ScheduledSendItem, bool, error) {
	s.ensure()
	item.ID = strings.TrimSpace(item.ID)
	item.ChatID = strings.TrimSpace(item.ChatID)
	if item.ID == "" {
		return ScheduledSendItem{}, false, fmt.Errorf("%w: id is required", ErrScheduledSendInvalid)
	}
	if item.ChatID == "" {
		return ScheduledSendItem{}, false, ErrScheduledSendChatIDMissing
	}
	if existing, ok := s.items[item.ID]; ok {
		return existing, false, nil
	}
	if item.Status == "" {
		item.Status = SendStatusQueued
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = item.CreatedAt
	}
	s.items[item.ID] = item
	return item, true, nil
}

func (s *SendScheduler) Next(now time.Time) (SendLease, bool) {
	s.ensure()
	candidates := s.nextCandidates(now)
	if len(candidates) == 0 {
		return SendLease{}, false
	}
	sort.Slice(candidates, func(i, j int) bool {
		return sendItemLess(candidates[i], candidates[j])
	})
	item := candidates[0]
	item.Status = SendStatusLeased
	item.Attempts++
	item.LeaseID = fmt.Sprintf("send-lease:%s:%d", item.ID, item.Attempts)
	item.LeaseUntil = now.Add(s.leaseDuration)
	item.UpdatedAt = now
	s.items[item.ID] = item
	return SendLease{
		LeaseID:   item.LeaseID,
		ItemID:    item.ID,
		ChatID:    item.ChatID,
		ExpiresAt: item.LeaseUntil,
		Item:      item,
	}, true
}

func (s *SendScheduler) Complete(lease SendLease, teamsMessageID string, now time.Time) error {
	item, err := s.currentLeaseItem(lease)
	if err != nil {
		return err
	}
	item.Status = SendStatusSent
	item.SentMessageID = strings.TrimSpace(teamsMessageID)
	if item.SentAt.IsZero() {
		item.SentAt = now
	}
	item.UpdatedAt = now
	item.LeaseID = ""
	item.LeaseUntil = time.Time{}
	s.items[item.ID] = item
	return nil
}

func (s *SendScheduler) Fail(lease SendLease, failure SendFailure, now time.Time) error {
	item, err := s.currentLeaseItem(lease)
	if err != nil {
		return err
	}
	if failure.Poison {
		item.Status = SendStatusPoisoned
		item.PoisonReason = strings.TrimSpace(failure.Reason)
		item.LastError = item.PoisonReason
		item.LeaseID = ""
		item.LeaseUntil = time.Time{}
		item.UpdatedAt = now
		s.items[item.ID] = item
		return nil
	}
	item.Status = SendStatusQueued
	item.LastError = strings.TrimSpace(failure.Reason)
	item.LeaseID = ""
	item.LeaseUntil = time.Time{}
	item.UpdatedAt = now
	s.items[item.ID] = item
	if until := failure.ChatBlockUntil(now); !until.IsZero() {
		s.BlockChatUntil(item.ChatID, until)
	}
	return nil
}

func (s *SendScheduler) PoisonItem(itemID string, reason string, now time.Time) error {
	s.ensure()
	itemID = strings.TrimSpace(itemID)
	item, ok := s.items[itemID]
	if !ok {
		return ErrScheduledSendItemNotFound
	}
	item.Status = SendStatusPoisoned
	item.PoisonReason = strings.TrimSpace(reason)
	item.LastError = item.PoisonReason
	item.LeaseID = ""
	item.LeaseUntil = time.Time{}
	item.UpdatedAt = now
	s.items[item.ID] = item
	return nil
}

func (s *SendScheduler) BlockChatUntil(chatID string, until time.Time) {
	s.ensure()
	chatID = strings.TrimSpace(chatID)
	if chatID == "" || until.IsZero() {
		return
	}
	if existing, ok := s.chatBlocks[chatID]; !ok || until.After(existing) {
		s.chatBlocks[chatID] = until
	}
}

func (s *SendScheduler) UnblockChat(chatID string) {
	s.ensure()
	delete(s.chatBlocks, strings.TrimSpace(chatID))
}

func (s *SendScheduler) ChatBlockedUntil(chatID string) (time.Time, bool) {
	s.ensure()
	until, ok := s.chatBlocks[strings.TrimSpace(chatID)]
	return until, ok
}

func (s *SendScheduler) Item(itemID string) (ScheduledSendItem, bool) {
	s.ensure()
	item, ok := s.items[strings.TrimSpace(itemID)]
	return item, ok
}

func (s *SendScheduler) EnqueuePersistedSendPlan(ctx context.Context, plan PersistedSendPlan) error {
	_ = ctx
	return s.EnqueuePersistedSendPlanAt(time.Now(), plan)
}

func (s *SendScheduler) EnqueuePersistedSendPlanAt(now time.Time, plan PersistedSendPlan) error {
	chatID := strings.TrimSpace(plan.ChatID)
	for _, item := range plan.Items {
		if item.ChatID == "" {
			item.ChatID = chatID
		}
		if item.SourceBatchID == "" {
			item.SourceBatchID = plan.BatchID
		}
		if _, _, err := s.EnqueueAt(now, item); err != nil {
			return err
		}
	}
	return nil
}

func (f SendFailure) ChatBlockUntil(now time.Time) time.Time {
	if !f.BlockUntil.IsZero() {
		return f.BlockUntil
	}
	if f.RetryAfter > 0 {
		return now.Add(f.RetryAfter)
	}
	return time.Time{}
}

func (s *SendScheduler) currentLeaseItem(lease SendLease) (ScheduledSendItem, error) {
	s.ensure()
	itemID := strings.TrimSpace(lease.ItemID)
	if itemID == "" {
		itemID = strings.TrimSpace(lease.Item.ID)
	}
	item, ok := s.items[itemID]
	if !ok {
		return ScheduledSendItem{}, ErrScheduledSendItemNotFound
	}
	if item.Status != SendStatusLeased || item.LeaseID == "" || item.LeaseID != lease.LeaseID {
		return ScheduledSendItem{}, ErrScheduledSendLeaseNotHeld
	}
	return item, nil
}

func (s *SendScheduler) nextCandidates(now time.Time) []ScheduledSendItem {
	byChat := make(map[string][]ScheduledSendItem)
	for _, item := range s.items {
		byChat[item.ChatID] = append(byChat[item.ChatID], item)
	}
	candidates := make([]ScheduledSendItem, 0, len(byChat))
	for chatID, items := range byChat {
		if s.chatBlocked(chatID, now) {
			continue
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].Sequence != items[j].Sequence {
				return items[i].Sequence < items[j].Sequence
			}
			return sendItemLess(items[i], items[j])
		})
	itemLoop:
		for _, item := range items {
			switch normalizedSendStatus(item.Status) {
			case SendStatusSent, SendStatusPoisoned:
				continue
			case SendStatusLeased:
				if sendLeaseActive(item, now) {
					break itemLoop
				}
				candidates = append(candidates, item)
				break itemLoop
			default:
				candidates = append(candidates, item)
				break itemLoop
			}
		}
	}
	return candidates
}

func (s *SendScheduler) chatBlocked(chatID string, now time.Time) bool {
	until, ok := s.chatBlocks[chatID]
	if !ok || until.IsZero() {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(s.chatBlocks, chatID)
	return false
}

func (s *SendScheduler) ensure() {
	if s.leaseDuration <= 0 {
		s.leaseDuration = DefaultSendLeaseDuration
	}
	if s.items == nil {
		s.items = make(map[string]ScheduledSendItem)
	}
	if s.chatBlocks == nil {
		s.chatBlocks = make(map[string]time.Time)
	}
}

func normalizedSendStatus(status SendScheduleStatus) SendScheduleStatus {
	if status == "" {
		return SendStatusQueued
	}
	return status
}

func sendLeaseActive(item ScheduledSendItem, now time.Time) bool {
	return item.Status == SendStatusLeased && !item.LeaseUntil.IsZero() && now.Before(item.LeaseUntil)
}

func sendItemLess(a, b ScheduledSendItem) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	if a.Sequence != b.Sequence {
		return a.Sequence < b.Sequence
	}
	return a.ID < b.ID
}

type RenderedSendPlan struct {
	BatchID    string
	ChatID     string
	SourceID   string
	SourceKind string
	RenderedAt time.Time
	Parts      []RenderedSendPart
	Checkpoint DurableCheckpoint
}

type RenderedSendPart struct {
	Kind         string
	Body         string
	ContentType  string
	SequenceHint int64
}

type PersistedSendPlan struct {
	BatchID     string
	ChatID      string
	PersistedAt time.Time
	Items       []ScheduledSendItem
	Checkpoint  DurableCheckpoint
}

type SendPlanPersister interface {
	PersistSendPlan(ctx context.Context, plan RenderedSendPlan) (PersistedSendPlan, error)
}

type PersistedSendPlanScheduler interface {
	EnqueuePersistedSendPlan(ctx context.Context, plan PersistedSendPlan) error
}
