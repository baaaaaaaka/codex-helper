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
)

const (
	globalInboundClaimTTL     = 5 * time.Minute
	globalInboundLockTimeout  = 500 * time.Millisecond
	maxGlobalInboundLedgerIDs = 2000
)

type globalInboundLedger struct {
	Version int                          `json:"version"`
	Items   map[string]globalInboundItem `json:"items,omitempty"`
}

type globalInboundItem struct {
	ChatID    string    `json:"chat_id"`
	MessageID string    `json:"message_id"`
	Owner     string    `json:"owner,omitempty"`
	Status    string    `json:"status,omitempty"`
	ClaimedAt time.Time `json:"claimed_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type globalInboundClaim struct {
	Path      string
	Key       string
	ChatID    string
	MessageID string
	Owner     string
}

func globalInboundLedgerPathForRegistry(registryPath string) (string, bool) {
	registryPath = strings.TrimSpace(registryPath)
	if registryPath == "" {
		return "", false
	}
	clean := filepath.Clean(registryPath)
	dir := filepath.Dir(clean)
	if filepath.Base(clean) == "registry.json" && filepath.Base(filepath.Dir(dir)) == "scopes" {
		return filepath.Join(filepath.Dir(filepath.Dir(dir)), "global-inbound-ledger.json"), true
	}
	return filepath.Join(dir, "teams-global-inbound-ledger.json"), true
}

func (b *Bridge) tryClaimGlobalInbound(ctx context.Context, chatID string, messageID string) (globalInboundClaim, bool, error) {
	if b == nil || strings.TrimSpace(chatID) == "" || strings.TrimSpace(messageID) == "" {
		return globalInboundClaim{}, true, nil
	}
	path, ok := globalInboundLedgerPathForRegistry(b.registryPath)
	if !ok {
		return globalInboundClaim{}, true, nil
	}
	owner := strings.TrimSpace(b.machine.ID)
	if owner == "" {
		owner = strings.TrimSpace(b.scope.ID)
	}
	if owner == "" {
		owner = "unknown"
	}
	return claimGlobalInbound(ctx, path, chatID, messageID, owner, time.Now())
}

func completeGlobalInbound(ctx context.Context, claim globalInboundClaim) error {
	if strings.TrimSpace(claim.Path) == "" || strings.TrimSpace(claim.Key) == "" {
		return nil
	}
	return updateGlobalInboundLedger(ctx, claim.Path, func(ledger *globalInboundLedger, now time.Time) {
		item := ledger.Items[claim.Key]
		item.ChatID = claim.ChatID
		item.MessageID = claim.MessageID
		item.Owner = claim.Owner
		item.Status = "done"
		item.UpdatedAt = now
		ledger.Items[claim.Key] = item
	})
}

func releaseGlobalInbound(ctx context.Context, claim globalInboundClaim) {
	if strings.TrimSpace(claim.Path) == "" || strings.TrimSpace(claim.Key) == "" {
		return
	}
	_ = updateGlobalInboundLedger(ctx, claim.Path, func(ledger *globalInboundLedger, _ time.Time) {
		item, ok := ledger.Items[claim.Key]
		if !ok || item.Owner != claim.Owner || item.Status != "claimed" {
			return
		}
		delete(ledger.Items, claim.Key)
	})
}

func claimGlobalInbound(ctx context.Context, path string, chatID string, messageID string, owner string, now time.Time) (globalInboundClaim, bool, error) {
	claim := globalInboundClaim{
		Path:      path,
		Key:       globalInboundKey(chatID, messageID),
		ChatID:    chatID,
		MessageID: messageID,
		Owner:     owner,
	}
	claimed := false
	err := updateGlobalInboundLedger(ctx, path, func(ledger *globalInboundLedger, _ time.Time) {
		item, ok := ledger.Items[claim.Key]
		if ok {
			switch item.Status {
			case "done":
				return
			case "claimed":
				if !item.UpdatedAt.IsZero() && now.Sub(item.UpdatedAt) < globalInboundClaimTTL {
					return
				}
			}
		}
		ledger.Items[claim.Key] = globalInboundItem{
			ChatID:    chatID,
			MessageID: messageID,
			Owner:     owner,
			Status:    "claimed",
			ClaimedAt: now,
			UpdatedAt: now,
		}
		claimed = true
	})
	return claim, claimed, err
}

func updateGlobalInboundLedger(ctx context.Context, path string, fn func(*globalInboundLedger, time.Time)) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLockContext(ctx, globalInboundLockTimeout)
	if err != nil {
		return err
	}
	if !ok {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("global Teams inbound ledger is locked: %s", path)
	}
	defer func() { _ = lock.Unlock() }()
	ledger, err := readGlobalInboundLedger(path)
	if err != nil {
		return err
	}
	now := time.Now()
	pruneGlobalInboundLedger(&ledger, now)
	fn(&ledger, now)
	pruneGlobalInboundLedger(&ledger, now)
	return writeGlobalInboundLedger(path, ledger)
}

func readGlobalInboundLedger(path string) (globalInboundLedger, error) {
	var ledger globalInboundLedger
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		ledger.Version = 1
		ledger.Items = map[string]globalInboundItem{}
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
		ledger.Items = map[string]globalInboundItem{}
	}
	return ledger, nil
}

func writeGlobalInboundLedger(path string, ledger globalInboundLedger) error {
	if ledger.Version == 0 {
		ledger.Version = 1
	}
	if ledger.Items == nil {
		ledger.Items = map[string]globalInboundItem{}
	}
	data, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func pruneGlobalInboundLedger(ledger *globalInboundLedger, now time.Time) {
	if ledger == nil || len(ledger.Items) <= maxGlobalInboundLedgerIDs {
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
			at = item.ClaimedAt
		}
		if at.IsZero() {
			at = now
		}
		entries = append(entries, entry{key: key, at: at})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].at.Before(entries[j].at) })
	for len(entries) > maxGlobalInboundLedgerIDs {
		delete(ledger.Items, entries[0].key)
		entries = entries[1:]
	}
}

func globalInboundKey(chatID string, messageID string) string {
	return strings.TrimSpace(chatID) + "\x00" + strings.TrimSpace(messageID)
}
