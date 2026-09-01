package teams

// This file contains a deliberately small recovery oracle.  It does not call
// any of the production frontier reducers: its job is to make the durable
// invariants executable independently of the implementation that is being
// reviewed.  The listener/store vertical tests exercise the production path;
// this model catches a class of regressions where a refactor preserves a
// function-level result but changes ordering, restart, or ownership semantics.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type recoveryModelRecord struct {
	ID       string
	Bytes    int64
	Visible  bool
	Complete bool
}

type recoveryModelOutbox struct {
	Status   string
	Attempts int
	Posted   bool
}

type recoveryModelChat struct {
	PhysicalOffset   int64
	SemanticOffset   int64
	SourceGeneration string
	Pending          map[string]recoveryModelRecord
	Outbox           map[string]recoveryModelOutbox
	Disposition      map[string]string
	Owner            string
	LeaseGeneration  int64
	Held             bool
}

type recoveryModelState struct {
	Chats map[string]*recoveryModelChat
}

type recoveryModelOperation struct {
	Chat             string
	Kind             string
	Record           recoveryModelRecord
	SourceGeneration string
	Owner            string
	LeaseGeneration  int64
}

func newRecoveryModelState() *recoveryModelState {
	return &recoveryModelState{Chats: map[string]*recoveryModelChat{}}
}

func (s *recoveryModelState) chat(id string) *recoveryModelChat {
	if s.Chats[id] == nil {
		s.Chats[id] = &recoveryModelChat{
			SourceGeneration: "source-1",
			Pending:          map[string]recoveryModelRecord{},
			Outbox:           map[string]recoveryModelOutbox{},
			Disposition:      map[string]string{},
		}
	}
	return s.Chats[id]
}

// apply is intentionally policy-level rather than a copy of the production
// reducer.  In particular, an incomplete JSONL record never advances the
// physical cursor; an opaque complete record may advance it without creating
// an outbound message; and an unknown POST outcome is terminally held until
// an explicit reconciliation operation proves what happened.
func (s *recoveryModelState) apply(op recoveryModelOperation) {
	c := s.chat(op.Chat)
	switch op.Kind {
	case "append-partial":
		if op.Record.ID != "" {
			op.Record.Complete = false
			c.Pending[op.Record.ID] = op.Record
		}
	case "append-complete":
		if op.Record.ID == "" {
			return
		}
		delete(c.Pending, op.Record.ID)
		if op.SourceGeneration != "" && op.SourceGeneration != c.SourceGeneration {
			c.Held = true
			c.Disposition[op.Record.ID] = "held-source-generation"
			return
		}
		c.PhysicalOffset += op.Record.Bytes
		c.SemanticOffset = c.PhysicalOffset
		if op.Record.Visible {
			c.Outbox[op.Record.ID] = recoveryModelOutbox{Status: "queued"}
		} else {
			c.Disposition[op.Record.ID] = "opaque-complete"
		}
	case "complete-pending":
		record, ok := c.Pending[op.Record.ID]
		if !ok {
			return
		}
		delete(c.Pending, op.Record.ID)
		record.Complete = true
		record.Visible = op.Record.Visible
		sourceGeneration := op.SourceGeneration
		if sourceGeneration == "" {
			sourceGeneration = c.SourceGeneration
		}
		s.apply(recoveryModelOperation{Chat: op.Chat, Kind: "append-complete", Record: record, SourceGeneration: sourceGeneration})
	case "source-replace":
		if op.SourceGeneration == "" || op.SourceGeneration == c.SourceGeneration {
			return
		}
		c.SourceGeneration = op.SourceGeneration
		c.Held = true
		c.Disposition["source-rewrite"] = "held-source-rewrite"
	case "claim":
		if strings.TrimSpace(op.Owner) == "" || op.LeaseGeneration <= 0 {
			return
		}
		if op.LeaseGeneration >= c.LeaseGeneration {
			c.Owner = op.Owner
			c.LeaseGeneration = op.LeaseGeneration
		}
	case "stale-advance":
		if op.Owner != c.Owner || op.LeaseGeneration != c.LeaseGeneration {
			return
		}
		if op.Record.Bytes > 0 && op.Record.Bytes >= c.PhysicalOffset {
			c.PhysicalOffset = op.Record.Bytes
			if c.SemanticOffset < c.PhysicalOffset {
				c.SemanticOffset = c.PhysicalOffset
			}
		}
	case "post-unknown":
		if op.Owner != c.Owner || op.LeaseGeneration != c.LeaseGeneration {
			return
		}
		out, ok := c.Outbox[op.Record.ID]
		if !ok || out.Status != "queued" {
			return
		}
		out.Status = "ambiguous"
		out.Attempts++
		c.Outbox[op.Record.ID] = out
	case "retry-ambiguous":
		// An unknown external side effect is never retried by the automatic
		// scheduler.  A later explicit reconciliation may change disposition,
		// but it is not this operation.
	case "restart":
		// Durable cursors, held ranges and ambiguous outbox rows survive a
		// process restart.  In-memory ownership is reacquired explicitly.
	case "isolate":
		c.Held = true
		c.Disposition[op.Record.ID] = "held-chat-local"
	}
}

type recoveryModelSnapshot struct {
	PhysicalOffset   int64
	SemanticOffset   int64
	SourceGeneration string
	Owner            string
	LeaseGeneration  int64
	Held             bool
	Pending          []string
	Outbox           []string
	Disposition      []string
}

func (s *recoveryModelState) snapshot(chatID string) recoveryModelSnapshot {
	c := s.chat(chatID)
	out := recoveryModelSnapshot{
		PhysicalOffset:   c.PhysicalOffset,
		SemanticOffset:   c.SemanticOffset,
		SourceGeneration: c.SourceGeneration,
		Owner:            c.Owner,
		LeaseGeneration:  c.LeaseGeneration,
		Held:             c.Held,
	}
	for id := range c.Pending {
		out.Pending = append(out.Pending, id)
	}
	for id, item := range c.Outbox {
		out.Outbox = append(out.Outbox, fmt.Sprintf("%s:%s:%d:%t", id, item.Status, item.Attempts, item.Posted))
	}
	for id, disposition := range c.Disposition {
		out.Disposition = append(out.Disposition, id+":"+disposition)
	}
	sort.Strings(out.Pending)
	sort.Strings(out.Outbox)
	sort.Strings(out.Disposition)
	return out
}

func recoveryModelTrace() []recoveryModelOperation {
	return []recoveryModelOperation{
		{Chat: "healthy", Kind: "append-complete", SourceGeneration: "source-1", Record: recoveryModelRecord{ID: "h-1", Bytes: 64, Visible: true, Complete: true}},
		{Chat: "poison", Kind: "append-complete", SourceGeneration: "source-1", Record: recoveryModelRecord{ID: "p-opaque", Bytes: 8 << 20, Visible: false, Complete: true}},
		{Chat: "poison", Kind: "append-complete", SourceGeneration: "source-1", Record: recoveryModelRecord{ID: "p-final", Bytes: 128, Visible: true, Complete: true}},
		{Chat: "healthy", Kind: "append-partial", Record: recoveryModelRecord{ID: "h-partial", Bytes: 200, Visible: true}},
		{Chat: "healthy", Kind: "complete-pending", Record: recoveryModelRecord{ID: "h-partial", Visible: true}},
		{Chat: "healthy", Kind: "claim", Owner: "owner-a", LeaseGeneration: 1},
		{Chat: "healthy", Kind: "post-unknown", Owner: "owner-a", LeaseGeneration: 1, Record: recoveryModelRecord{ID: "h-1"}},
		{Chat: "healthy", Kind: "retry-ambiguous", Owner: "owner-a", LeaseGeneration: 1, Record: recoveryModelRecord{ID: "h-1"}},
		{Chat: "healthy", Kind: "claim", Owner: "owner-b", LeaseGeneration: 2},
		{Chat: "healthy", Kind: "stale-advance", Owner: "owner-a", LeaseGeneration: 1, Record: recoveryModelRecord{ID: "forged", Bytes: 999999}},
		{Chat: "healthy", Kind: "source-replace", SourceGeneration: "source-2"},
		{Chat: "healthy", Kind: "append-complete", SourceGeneration: "source-1", Record: recoveryModelRecord{ID: "h-old", Bytes: 32, Visible: true, Complete: true}},
		{Chat: "healthy", Kind: "restart"},
		{Chat: "poison", Kind: "isolate", Record: recoveryModelRecord{ID: "poison-local"}},
		{Chat: "healthy", Kind: "append-complete", SourceGeneration: "source-2", Record: recoveryModelRecord{ID: "h-new", Bytes: 16, Visible: true, Complete: true}},
	}
}

func chunkRecoveryModelTrace(base []recoveryModelOperation) []recoveryModelOperation {
	var out []recoveryModelOperation
	for _, op := range base {
		if op.Kind != "append-complete" || !op.Record.Complete {
			out = append(out, op)
			continue
		}
		partial := op
		partial.Kind = "append-partial"
		partial.Record.Complete = false
		complete := op
		complete.Kind = "complete-pending"
		out = append(out, partial, complete)
	}
	return out
}

func reverseUnrelatedRecoveryOperations(base []recoveryModelOperation) []recoveryModelOperation {
	byChat := map[string][]recoveryModelOperation{}
	var chats []string
	for _, op := range base {
		if len(byChat[op.Chat]) == 0 {
			chats = append(chats, op.Chat)
		}
		byChat[op.Chat] = append(byChat[op.Chat], op)
	}
	sort.Strings(chats)
	var out []recoveryModelOperation
	for remaining := len(base); remaining > 0; {
		for _, chat := range chats {
			if len(byChat[chat]) == 0 {
				continue
			}
			out = append(out, byChat[chat][0])
			byChat[chat] = byChat[chat][1:]
			remaining--
		}
	}
	return out
}

func runRecoveryModel(ops []recoveryModelOperation, check func(*recoveryModelState, *recoveryModelState, recoveryModelOperation) error) (*recoveryModelState, error) {
	state := newRecoveryModelState()
	for _, op := range ops {
		before := newRecoveryModelState()
		for id, chat := range state.Chats {
			copyChat := *chat
			copyChat.Pending = map[string]recoveryModelRecord{}
			for k, v := range chat.Pending {
				copyChat.Pending[k] = v
			}
			copyChat.Outbox = map[string]recoveryModelOutbox{}
			for k, v := range chat.Outbox {
				copyChat.Outbox[k] = v
			}
			copyChat.Disposition = map[string]string{}
			for k, v := range chat.Disposition {
				copyChat.Disposition[k] = v
			}
			before.Chats[id] = &copyChat
		}
		state.apply(op)
		if check != nil {
			if err := check(state, before, op); err != nil {
				return nil, err
			}
		}
	}
	return state, nil
}

func assertRecoveryModelInvariants(after, before *recoveryModelState, op recoveryModelOperation) error {
	for chatID, chat := range after.Chats {
		beforeChat := before.chat(chatID)
		if chat.PhysicalOffset < beforeChat.PhysicalOffset || chat.SemanticOffset < beforeChat.SemanticOffset {
			return fmt.Errorf("%s cursor regressed after %s: before=%#v after=%#v", chatID, op.Kind, beforeChat, chat)
		}
		if chat.SemanticOffset > chat.PhysicalOffset {
			return fmt.Errorf("%s semantic cursor passed physical cursor: %#v", chatID, chat)
		}
		for id, outbox := range chat.Outbox {
			if outbox.Attempts > 1 || (outbox.Status == "ambiguous" && outbox.Posted) {
				return fmt.Errorf("%s outbox %s violated unknown-side-effect fence: %#v", chatID, id, outbox)
			}
		}
	}
	if op.Kind == "stale-advance" && after.chat(op.Chat).PhysicalOffset != before.chat(op.Chat).PhysicalOffset {
		return fmt.Errorf("stale owner advanced %s", op.Chat)
	}
	return nil
}

func TestTeamsRecoveryIndependentModelMetamorphic(t *testing.T) {
	base := recoveryModelTrace()
	want, err := runRecoveryModel(base, assertRecoveryModelInvariants)
	if err != nil {
		t.Fatal(err)
	}

	variants := map[string][]recoveryModelOperation{
		"chunked":         chunkRecoveryModelTrace(base),
		"unrelated-order": reverseUnrelatedRecoveryOperations(base),
	}
	for name, ops := range variants {
		got, err := runRecoveryModel(ops, nil)
		if err != nil {
			t.Fatalf("variant %s: %v", name, err)
		}
		for _, chatID := range []string{"healthy", "poison"} {
			if !reflect.DeepEqual(got.snapshot(chatID), want.snapshot(chatID)) {
				t.Fatalf("variant %s changed durable result for %s: got=%#v want=%#v", name, chatID, got.snapshot(chatID), want.snapshot(chatID))
			}
		}
	}

	// Restart insertion is a metamorphic identity: it may discard an in-memory
	// capability, but it cannot move a durable cursor or re-post an ambiguous
	// side effect.
	withRestarts := make([]recoveryModelOperation, 0, len(base)*2)
	for _, op := range base {
		withRestarts = append(withRestarts, op, recoveryModelOperation{Chat: op.Chat, Kind: "restart"})
	}
	got, err := runRecoveryModel(withRestarts, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, chatID := range []string{"healthy", "poison"} {
		if !reflect.DeepEqual(got.snapshot(chatID), want.snapshot(chatID)) {
			t.Fatalf("restart metamorphic result changed for %s: got=%#v want=%#v", chatID, got.snapshot(chatID), want.snapshot(chatID))
		}
	}
}

func seededRecoveryModelTrace(seed int64) []recoveryModelOperation {
	rng := rand.New(rand.NewSource(seed))
	const chatCount = 7
	chatIDs := make([]string, chatCount)
	for i := range chatIDs {
		chatIDs[i] = fmt.Sprintf("chat-%d", i)
	}
	currentGeneration := make([]string, chatCount)
	owners := make([]string, chatCount)
	leaseGenerations := make([]int64, chatCount)
	for i := range currentGeneration {
		currentGeneration[i] = "source-1"
	}
	var out []recoveryModelOperation
	for step := 0; step < 96; step++ {
		chatIndex := rng.Intn(chatCount)
		chatID := chatIDs[chatIndex]
		recordID := fmt.Sprintf("r-%d-%d", seed, step)
		switch rng.Intn(10) {
		case 0, 1, 2:
			out = append(out, recoveryModelOperation{
				Chat: chatID, Kind: "append-complete", SourceGeneration: currentGeneration[chatIndex],
				Record: recoveryModelRecord{ID: recordID, Bytes: int64(rng.Intn(4096) + 1), Visible: rng.Intn(4) != 0, Complete: true},
			})
		case 3:
			out = append(out, recoveryModelOperation{
				Chat: chatID, Kind: "append-partial",
				Record: recoveryModelRecord{ID: recordID, Bytes: int64(rng.Intn(4096) + 1), Visible: true},
			})
			out = append(out, recoveryModelOperation{
				Chat: chatID, Kind: "complete-pending", SourceGeneration: currentGeneration[chatIndex],
				Record: recoveryModelRecord{ID: recordID, Visible: true},
			})
		case 4:
			leaseGenerations[chatIndex]++
			owners[chatIndex] = fmt.Sprintf("owner-%d-%d", seed, step)
			out = append(out, recoveryModelOperation{Chat: chatID, Kind: "claim", Owner: owners[chatIndex], LeaseGeneration: leaseGenerations[chatIndex]})
		case 5:
			// Use an owner and generation that cannot be the current capability.
			staleOwner := owners[chatIndex] + "-stale"
			if staleOwner == "-stale" {
				staleOwner = "unclaimed-stale"
			}
			out = append(out, recoveryModelOperation{
				Chat: chatID, Kind: "stale-advance", Owner: staleOwner, LeaseGeneration: leaseGenerations[chatIndex] + 100,
				Record: recoveryModelRecord{ID: recordID, Bytes: int64(rng.Intn(1<<20) + 1)},
			})
		case 6:
			currentGeneration[chatIndex] = fmt.Sprintf("source-%d", rng.Intn(4)+2)
			out = append(out, recoveryModelOperation{Chat: chatID, Kind: "source-replace", SourceGeneration: currentGeneration[chatIndex]})
		case 7:
			out = append(out, recoveryModelOperation{Chat: chatID, Kind: "post-unknown", Owner: owners[chatIndex], LeaseGeneration: leaseGenerations[chatIndex], Record: recoveryModelRecord{ID: recordID}})
		case 8:
			out = append(out, recoveryModelOperation{Chat: chatID, Kind: "restart"})
		case 9:
			out = append(out, recoveryModelOperation{Chat: chatID, Kind: "isolate", Record: recoveryModelRecord{ID: recordID}})
		}
	}
	return out
}

func TestTeamsRecoveryIndependentModelSeededSequences(t *testing.T) {
	for _, seed := range []int64{0x51_2501, 0x51_2502, 0x51_2503, 0x51_2504, 0x51_2505, 0x51_2506, 0x51_2507, 0x51_2508} {
		seed := seed
		t.Run(fmt.Sprintf("seed-%x", seed), func(t *testing.T) {
			if _, err := runRecoveryModel(seededRecoveryModelTrace(seed), assertRecoveryModelInvariants); err != nil {
				t.Fatalf("seed %x: %v", seed, err)
			}
		})
	}
}

// TestTeamsRecoveryModelProjectionRoundTripsBothStores makes the independent
// model observable through the actual JSON and SQLite serializers.  The
// projection is intentionally narrow: this is a persistence/parity test, not
// a second implementation of the production scanner.
func TestTeamsRecoveryModelProjectionRoundTripsBothStores(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.json")
			store, err := teamstore.Open(path)
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					_ = store.Close()
					t.Fatalf("migrate store: %v", err)
				}
			}
			t.Cleanup(func() { _ = store.Close() })
			model := newRecoveryModelState()
			for _, op := range recoveryModelTrace() {
				model.apply(op)
				if err := persistRecoveryModelProjection(ctx, store, model); err != nil {
					t.Fatalf("persist after %s: %v", op.Kind, err)
				}
				loaded, err := store.Load(ctx)
				if err != nil {
					t.Fatalf("load after %s: %v", op.Kind, err)
				}
				for chatID := range model.Chats {
					if got := recoveryModelStoreSnapshot(loaded, chatID); !reflect.DeepEqual(got, model.snapshot(chatID)) {
						t.Fatalf("store projection after %s for %s: got=%#v want=%#v", op.Kind, chatID, got, model.snapshot(chatID))
					}
				}
			}
		})
	}
}

func persistRecoveryModelProjection(ctx context.Context, store *teamstore.Store, model *recoveryModelState) error {
	return store.Update(ctx, func(state *teamstore.State) error {
		if state.Sessions == nil {
			state.Sessions = map[string]teamstore.SessionContext{}
		}
		if state.ImportCheckpoints == nil {
			state.ImportCheckpoints = map[string]teamstore.ImportCheckpoint{}
		}
		if state.OutboxMessages == nil {
			state.OutboxMessages = map[string]teamstore.OutboxMessage{}
		}
		if state.ChatPolls == nil {
			state.ChatPolls = map[string]teamstore.ChatPollState{}
		}
		for chatID, chat := range model.Chats {
			sessionID := "model-" + chatID
			state.Sessions[sessionID] = teamstore.SessionContext{ID: sessionID, Status: teamstore.SessionStatusActive, TeamsChatID: chatID}
			checkpointID := "transcript:" + sessionID
			checkpoint := state.ImportCheckpoints[checkpointID]
			checkpoint.ID = checkpointID
			checkpoint.SessionID = sessionID
			checkpoint.SourceGeneration = chat.SourceGeneration
			checkpoint.LastOffset = chat.PhysicalOffset
			checkpoint.LastOffsetKnown = true
			checkpoint.Status = "complete"
			checkpoint.SourceRewriteBlocked = chat.Held
			checkpoint.PartialSourceIdentity = ""
			checkpoint.PartialObservedSize = 0
			for recordID, record := range chat.Pending {
				checkpoint.PartialSourceIdentity = recordID
				checkpoint.PartialObservedSize = record.Bytes
				break
			}
			state.ImportCheckpoints[checkpointID] = checkpoint
			poll := state.ChatPolls[chatID]
			poll.ChatID = chatID
			poll.PollState = inboundPollStateHot
			poll.Attempt = nil
			if chat.Owner != "" {
				poll.Attempt = &teamstore.ChatPollAttempt{
					ID: "model-attempt:" + chatID, Owner: chat.Owner,
					LeaseGeneration: chat.LeaseGeneration, ExpiresAt: time.Now().Add(time.Hour),
				}
			}
			state.ChatPolls[chatID] = poll
			for recordID, out := range chat.Outbox {
				status := teamstore.OutboxStatusQueued
				if out.Status == "ambiguous" {
					status = teamstore.OutboxStatusSending
				}
				state.OutboxMessages["model-outbox:"+chatID+":"+recordID] = teamstore.OutboxMessage{
					ID: "model-outbox:" + chatID + ":" + recordID, SessionID: sessionID, TeamsChatID: chatID, Status: status, GraphRecoveryPageCount: out.Attempts,
				}
			}
			for recordID, disposition := range chat.Disposition {
				id := "model-disposition:" + chatID + ":" + recordID
				state.OutboxMessages[id] = teamstore.OutboxMessage{ID: id, SessionID: sessionID, TeamsChatID: chatID, Status: teamstore.OutboxStatusSkipped, Kind: "recovery-" + disposition}
			}
		}
		return nil
	})
}

func recoveryModelStoreSnapshot(state teamstore.State, chatID string) recoveryModelSnapshot {
	sessionID := "model-" + chatID
	checkpoint := state.ImportCheckpoints["transcript:"+sessionID]
	out := recoveryModelSnapshot{
		PhysicalOffset:   checkpoint.LastOffset,
		SemanticOffset:   checkpoint.LastOffset,
		SourceGeneration: checkpoint.SourceGeneration,
		Held:             checkpoint.SourceRewriteBlocked,
	}
	if pendingID := strings.TrimSpace(checkpoint.PartialSourceIdentity); pendingID != "" {
		out.Pending = append(out.Pending, pendingID)
	}
	poll := state.ChatPolls[chatID]
	if poll.Attempt != nil {
		out.Owner = poll.Attempt.Owner
		out.LeaseGeneration = poll.Attempt.LeaseGeneration
	}
	for id, msg := range state.OutboxMessages {
		if msg.TeamsChatID != chatID {
			continue
		}
		if msg.Status == teamstore.OutboxStatusSkipped && strings.HasPrefix(msg.Kind, "recovery-") {
			out.Disposition = append(out.Disposition, strings.TrimPrefix(id, "model-disposition:"+chatID+":")+":"+strings.TrimPrefix(msg.Kind, "recovery-"))
			continue
		}
		status := string(msg.Status)
		if status == string(teamstore.OutboxStatusSending) {
			status = "ambiguous"
		}
		out.Outbox = append(out.Outbox, fmt.Sprintf("%s:%s:%d:false", strings.TrimPrefix(id, "model-outbox:"+chatID+":"), status, msg.GraphRecoveryPageCount))
	}
	sort.Strings(out.Pending)
	sort.Strings(out.Outbox)
	sort.Strings(out.Disposition)
	return out
}

type sanitizedRecoveryFixture struct {
	Version string                          `json:"version"`
	Source  string                          `json:"source"`
	Entries []sanitizedRecoveryFixtureEntry `json:"entries"`
}

type sanitizedRecoveryFixtureEntry struct {
	ID                  string `json:"id"`
	ObservedShape       string `json:"observed_shape"`
	ExpectedDisposition string `json:"expected_disposition"`
	SafeToAutoAdvance   bool   `json:"safe_to_auto_advance"`
}

func TestTeamsSanitizedCurrentStateFixtureIsCompleteAndSecretFree(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "teams-current-state-recovery.json"))
	if err != nil {
		t.Fatalf("read sanitized current-state fixture: %v", err)
	}
	for _, secret := range []string{"access_token", "refresh_token", "Bearer ", "graph.microsoft.com", "/home/baka", "store.sqlite"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("sanitized fixture contains forbidden value %q", secret)
		}
	}
	var fixture sanitizedRecoveryFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("decode sanitized current-state fixture: %v", err)
	}
	if fixture.Version != "1" || fixture.Source != "sanitized-current-state-category-sample" {
		t.Fatalf("fixture identity = %#v", fixture)
	}
	wantShapes := map[string]bool{
		"healthy_backlog": true, "pending_continuation": true, "opaque_large_record": true,
		"ambiguous_outbox": true, "legacy_unverified_checkpoint": true, "source_rewrite": true,
	}
	seen := map[string]bool{}
	for _, entry := range fixture.Entries {
		if entry.ID == "" || seen[entry.ID] {
			t.Fatalf("fixture has duplicate/empty entry: %#v", entry)
		}
		seen[entry.ID] = true
		if !wantShapes[entry.ObservedShape] {
			t.Fatalf("fixture has unexpected shape: %#v", entry)
		}
		if entry.ExpectedDisposition == "" {
			t.Fatalf("fixture entry has no disposition: %#v", entry)
		}
	}
	for shape := range wantShapes {
		found := false
		for _, entry := range fixture.Entries {
			found = found || entry.ObservedShape == shape
		}
		if !found {
			t.Fatalf("fixture omitted observed shape %q", shape)
		}
	}
}
