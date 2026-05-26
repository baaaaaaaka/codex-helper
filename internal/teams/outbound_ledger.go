package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/flock"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	globalOutboundLockTimeout  = 500 * time.Millisecond
	maxGlobalOutboundLedgerIDs = 32768
)

type globalOutboundLedger struct {
	Version int                           `json:"version"`
	Items   map[string]globalOutboundItem `json:"items,omitempty"`
}

type globalOutboundItem struct {
	ChatID         string    `json:"chat_id"`
	MessageID      string    `json:"message_id"`
	ScopeID        string    `json:"scope_id,omitempty"`
	MachineID      string    `json:"machine_id,omitempty"`
	OutboxID       string    `json:"outbox_id,omitempty"`
	SessionID      string    `json:"session_id,omitempty"`
	TurnID         string    `json:"turn_id,omitempty"`
	Kind           string    `json:"kind,omitempty"`
	Origin         string    `json:"origin,omitempty"`
	RecordedAt     time.Time `json:"recorded_at,omitempty"`
	TeamsCreatedAt time.Time `json:"teams_created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

func globalOutboundLedgerPathForRegistry(registryPath string) (string, bool) {
	return globalTeamsLedgerPathForScopedFile(registryPath, "registry.json", "global-outbound-ledger.json", "teams-global-outbound-ledger.json")
}

func globalOutboundLedgerPathForStore(storePath string) (string, bool) {
	return globalTeamsLedgerPathForScopedFile(storePath, "state.json", "global-outbound-ledger.json", "teams-global-outbound-ledger.json")
}

func globalTeamsLedgerPathForScopedFile(path string, fileName string, scopedLedgerName string, fallbackLedgerName string) (string, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", false
	}
	clean := filepath.Clean(path)
	dir := filepath.Dir(clean)
	if filepath.Base(clean) == fileName && filepath.Base(filepath.Dir(dir)) == "scopes" {
		return filepath.Join(filepath.Dir(filepath.Dir(dir)), scopedLedgerName), true
	}
	return filepath.Join(dir, fallbackLedgerName), true
}

func (b *Bridge) globalOutboundLedgerPath() (string, bool) {
	if b == nil {
		return "", false
	}
	if path, ok := globalOutboundLedgerPathForRegistry(b.registryPath); ok {
		return path, true
	}
	if b.store != nil {
		return globalOutboundLedgerPathForStore(b.store.Path())
	}
	return "", false
}

func (b *Bridge) hasGlobalOutboundMessage(ctx context.Context, chatID string, messageID string) (bool, error) {
	chatID = strings.TrimSpace(chatID)
	messageID = strings.TrimSpace(messageID)
	if b == nil || chatID == "" || messageID == "" {
		return false, nil
	}
	path, ok := b.globalOutboundLedgerPath()
	if !ok {
		return false, nil
	}
	if err := b.ensureGlobalOutboundBackfilled(ctx, path); err != nil {
		return false, err
	}
	ledger, err := readGlobalOutboundLedger(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, exists := ledger.Items[globalOutboundKey(chatID, messageID)]
	return exists, nil
}

func (b *Bridge) recordGlobalOutboundMessage(ctx context.Context, outbox teamstore.OutboxMessage, msg ChatMessage) error {
	if b == nil {
		return nil
	}
	messageID := strings.TrimSpace(firstNonEmptyString(msg.ID, outbox.TeamsMessageID))
	if strings.TrimSpace(outbox.TeamsChatID) == "" || messageID == "" {
		return nil
	}
	path, ok := b.globalOutboundLedgerPath()
	if !ok {
		return nil
	}
	item := globalOutboundItem{
		ChatID:         outbox.TeamsChatID,
		MessageID:      messageID,
		ScopeID:        firstNonEmptyString(outbox.ScopeID, b.scope.ID),
		MachineID:      firstNonEmptyString(outbox.MachineID, b.machine.ID),
		OutboxID:       outbox.ID,
		SessionID:      outbox.SessionID,
		TurnID:         outbox.TurnID,
		Kind:           outbox.Kind,
		Origin:         teamstore.MessageOriginHelperOutbox,
		RecordedAt:     firstNonZeroTime(outbox.SentAt, outbox.UpdatedAt, outbox.CreatedAt),
		TeamsCreatedAt: parseGraphTime(msg.CreatedDateTime),
	}
	return recordGlobalOutbound(ctx, path, item, time.Now())
}

func (b *Bridge) recordGlobalOutboundSuppressionProvenance(ctx context.Context, chatID string, messageID string) {
	if b == nil || b.store == nil {
		return
	}
	_, err := b.store.RecordMessageProvenance(ctx, teamstore.MessageProvenanceRecord{
		TeamsChatID:    chatID,
		TeamsMessageID: messageID,
		Origin:         teamstore.MessageOriginHelperOutbox,
		Diagnostic:     "global_outbound_ledger",
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	})
	if err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams global outbound provenance record error: %v\n", err)
	}
}

func (b *Bridge) ensureGlobalOutboundBackfilled(ctx context.Context, path string) error {
	if b == nil || strings.TrimSpace(path) == "" {
		return nil
	}
	b.globalOutboundMu.Lock()
	defer b.globalOutboundMu.Unlock()
	if b.globalOutboundBackfilled {
		return nil
	}
	records, err := b.globalOutboundBackfillItems(ctx)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		b.globalOutboundBackfilled = true
		return nil
	}
	if err := updateGlobalOutboundLedger(ctx, path, func(ledger *globalOutboundLedger, now time.Time) {
		for _, item := range records {
			upsertGlobalOutboundItem(ledger, item, now)
		}
	}); err != nil {
		return err
	}
	b.globalOutboundBackfilled = true
	return nil
}

func (b *Bridge) globalOutboundBackfillItems(ctx context.Context) ([]globalOutboundItem, error) {
	if b == nil || b.store == nil {
		return nil, nil
	}
	paths, err := siblingScopeStorePaths(b.store.Path())
	if err != nil {
		return nil, err
	}
	var out []globalOutboundItem
	for _, path := range paths {
		st, err := teamstore.Open(path)
		if err != nil {
			return nil, err
		}
		state, loadErr := st.Load(ctx)
		closeErr := st.Close()
		if loadErr != nil {
			return nil, loadErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if !b.globalOutboundBackfillStateMatches(state) {
			continue
		}
		for _, msg := range state.OutboxMessages {
			switch msg.Status {
			case teamstore.OutboxStatusAccepted, teamstore.OutboxStatusSent:
			default:
				continue
			}
			if strings.TrimSpace(msg.TeamsChatID) == "" || strings.TrimSpace(msg.TeamsMessageID) == "" {
				continue
			}
			out = append(out, globalOutboundItem{
				ChatID:     msg.TeamsChatID,
				MessageID:  msg.TeamsMessageID,
				ScopeID:    firstNonEmptyString(msg.ScopeID, state.Scope.ID),
				MachineID:  firstNonEmptyString(msg.MachineID, state.MachineIdentity.ID),
				OutboxID:   msg.ID,
				SessionID:  msg.SessionID,
				TurnID:     msg.TurnID,
				Kind:       msg.Kind,
				Origin:     teamstore.MessageOriginHelperOutbox,
				RecordedAt: firstNonZeroTime(msg.SentAt, msg.UpdatedAt, msg.CreatedAt),
			})
		}
		for _, record := range state.MessageProvenance {
			if strings.TrimSpace(record.Origin) != teamstore.MessageOriginHelperOutbox {
				continue
			}
			if strings.TrimSpace(record.TeamsChatID) == "" || strings.TrimSpace(record.TeamsMessageID) == "" {
				continue
			}
			out = append(out, globalOutboundItem{
				ChatID:     record.TeamsChatID,
				MessageID:  record.TeamsMessageID,
				ScopeID:    state.Scope.ID,
				MachineID:  state.MachineIdentity.ID,
				OutboxID:   record.OutboxID,
				SessionID:  record.SessionID,
				TurnID:     record.TurnID,
				Kind:       record.Kind,
				Origin:     teamstore.MessageOriginHelperOutbox,
				RecordedAt: firstNonZeroTime(record.UpdatedAt, record.CreatedAt),
			})
		}
	}
	return out, nil
}

func (b *Bridge) globalOutboundBackfillStateMatches(state teamstore.State) bool {
	if b == nil {
		return false
	}
	if scopeStateMatches(b.scope, state) {
		return true
	}
	controlChatID := strings.TrimSpace(firstNonEmptyString(b.reg.ControlChatID, state.ControlChat.TeamsChatID))
	if controlChatID == "" || strings.TrimSpace(state.ControlChat.TeamsChatID) != controlChatID {
		return false
	}
	if strings.TrimSpace(b.scope.Profile) != "" && strings.TrimSpace(state.ControlChat.Profile) != "" && strings.TrimSpace(b.scope.Profile) != strings.TrimSpace(state.ControlChat.Profile) {
		return false
	}
	if strings.TrimSpace(b.user.ID) != "" && strings.TrimSpace(state.ControlChat.AccountID) != "" && strings.TrimSpace(b.user.ID) != strings.TrimSpace(state.ControlChat.AccountID) {
		return false
	}
	return strings.TrimSpace(state.ControlChat.ScopeID) != "" || strings.TrimSpace(state.ControlChat.AccountID) != ""
}

func siblingScopeStorePaths(currentPath string) ([]string, error) {
	currentPath = strings.TrimSpace(currentPath)
	if currentPath == "" {
		return nil, nil
	}
	seen := map[string]bool{}
	var paths []string
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		clean := filepath.Clean(path)
		if seen[clean] {
			return
		}
		seen[clean] = true
		paths = append(paths, clean)
	}
	clean := filepath.Clean(currentPath)
	add(clean)
	dir := filepath.Dir(clean)
	if filepath.Base(clean) == "state.json" && filepath.Base(filepath.Dir(dir)) == "scopes" {
		matches, err := filepath.Glob(filepath.Join(filepath.Dir(dir), "*", "state.json"))
		if err != nil {
			return nil, err
		}
		sort.Strings(matches)
		for _, path := range matches {
			add(path)
		}
	}
	return paths, nil
}

func recordGlobalOutbound(ctx context.Context, path string, item globalOutboundItem, now time.Time) error {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(item.ChatID) == "" || strings.TrimSpace(item.MessageID) == "" {
		return nil
	}
	return updateGlobalOutboundLedger(ctx, path, func(ledger *globalOutboundLedger, _ time.Time) {
		upsertGlobalOutboundItem(ledger, item, now)
	})
}

func updateGlobalOutboundLedger(ctx context.Context, path string, fn func(*globalOutboundLedger, time.Time)) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLockContext(ctx, globalOutboundLockTimeout)
	if err != nil {
		return err
	}
	if !ok {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("global Teams outbound ledger is locked: %s", path)
	}
	defer func() { _ = lock.Unlock() }()
	ledger, err := readGlobalOutboundLedger(path)
	if err != nil {
		return err
	}
	now := time.Now()
	pruneGlobalOutboundLedger(&ledger, now)
	fn(&ledger, now)
	pruneGlobalOutboundLedger(&ledger, now)
	return writeGlobalOutboundLedger(path, ledger)
}

func readGlobalOutboundLedger(path string) (globalOutboundLedger, error) {
	var ledger globalOutboundLedger
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		ledger.Version = 1
		ledger.Items = map[string]globalOutboundItem{}
		return ledger, nil
	}
	if err != nil {
		return ledger, err
	}
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := json.Unmarshal(data, &ledger); err != nil {
			return ledger, err
		}
	}
	if ledger.Version == 0 {
		ledger.Version = 1
	}
	if ledger.Items == nil {
		ledger.Items = map[string]globalOutboundItem{}
	}
	return ledger, nil
}

func writeGlobalOutboundLedger(path string, ledger globalOutboundLedger) error {
	if ledger.Version == 0 {
		ledger.Version = 1
	}
	if ledger.Items == nil {
		ledger.Items = map[string]globalOutboundItem{}
	}
	data, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return err
	}
	return durableWriteFile(path, data, 0o600)
}

func upsertGlobalOutboundItem(ledger *globalOutboundLedger, item globalOutboundItem, now time.Time) {
	if ledger == nil {
		return
	}
	if ledger.Items == nil {
		ledger.Items = map[string]globalOutboundItem{}
	}
	item.ChatID = strings.TrimSpace(item.ChatID)
	item.MessageID = strings.TrimSpace(item.MessageID)
	if item.ChatID == "" || item.MessageID == "" {
		return
	}
	if item.RecordedAt.IsZero() {
		item.RecordedAt = now
	}
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = now
	}
	key := globalOutboundKey(item.ChatID, item.MessageID)
	existing, ok := ledger.Items[key]
	if ok {
		item = mergeGlobalOutboundItem(existing, item)
		item.UpdatedAt = now
	}
	ledger.Items[key] = item
}

func mergeGlobalOutboundItem(existing globalOutboundItem, next globalOutboundItem) globalOutboundItem {
	if next.ChatID == "" {
		next.ChatID = existing.ChatID
	}
	if next.MessageID == "" {
		next.MessageID = existing.MessageID
	}
	if next.ScopeID == "" {
		next.ScopeID = existing.ScopeID
	}
	if next.MachineID == "" {
		next.MachineID = existing.MachineID
	}
	if next.OutboxID == "" {
		next.OutboxID = existing.OutboxID
	}
	if next.SessionID == "" {
		next.SessionID = existing.SessionID
	}
	if next.TurnID == "" {
		next.TurnID = existing.TurnID
	}
	if next.Kind == "" {
		next.Kind = existing.Kind
	}
	if next.Origin == "" {
		next.Origin = existing.Origin
	}
	if next.RecordedAt.IsZero() || (!existing.RecordedAt.IsZero() && existing.RecordedAt.Before(next.RecordedAt)) {
		next.RecordedAt = existing.RecordedAt
	}
	if next.TeamsCreatedAt.IsZero() {
		next.TeamsCreatedAt = existing.TeamsCreatedAt
	}
	return next
}

func pruneGlobalOutboundLedger(ledger *globalOutboundLedger, now time.Time) {
	if ledger == nil || len(ledger.Items) <= maxGlobalOutboundLedgerIDs {
		return
	}
	type entry struct {
		key string
		at  time.Time
	}
	entries := make([]entry, 0, len(ledger.Items))
	for key, item := range ledger.Items {
		at := item.UpdatedAt
		if at.IsZero() {
			at = item.RecordedAt
		}
		if at.IsZero() {
			at = item.TeamsCreatedAt
		}
		if at.IsZero() {
			at = now
		}
		entries = append(entries, entry{key: key, at: at})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].at.Before(entries[j].at) })
	for len(entries) > maxGlobalOutboundLedgerIDs {
		delete(ledger.Items, entries[0].key)
		entries = entries[1:]
	}
}

func globalOutboundKey(chatID string, messageID string) string {
	return strings.TrimSpace(chatID) + "\x00" + strings.TrimSpace(messageID)
}
