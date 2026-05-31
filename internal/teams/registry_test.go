package teams

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

func TestSaveRegistryNoopDoesNotRewriteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	reg := Registry{
		Version:       1,
		UserID:        "user-1",
		ControlChatID: "control-chat",
		Chats: map[string]ChatState{
			"control-chat": {SeenMessageIDs: []string{"m1"}},
		},
	}
	if err := SaveRegistry(path, reg); err != nil {
		t.Fatalf("initial SaveRegistry error: %v", err)
	}
	oldTime := time.Date(2026, 5, 8, 1, 2, 3, 0, time.UTC)
	if err := os.Chtimes(path, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes registry: %v", err)
	}
	if err := SaveRegistry(path, reg); err != nil {
		t.Fatalf("noop SaveRegistry error: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat registry: %v", err)
	}
	if !info.ModTime().Equal(oldTime) {
		t.Fatalf("SaveRegistry rewrote unchanged file: modtime=%s want %s", info.ModTime(), oldTime)
	}
}

func TestSaveRegistryWritesChangedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	reg := Registry{
		Version:       1,
		UserID:        "user-1",
		ControlChatID: "control-chat",
		Chats: map[string]ChatState{
			"control-chat": {SeenMessageIDs: []string{"m1"}},
		},
	}
	if err := SaveRegistry(path, reg); err != nil {
		t.Fatalf("initial SaveRegistry error: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read registry before change: %v", err)
	}
	reg.Chats["control-chat"] = ChatState{SeenMessageIDs: []string{"m1", "m2"}}
	if err := SaveRegistry(path, reg); err != nil {
		t.Fatalf("changed SaveRegistry error: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read registry after change: %v", err)
	}
	if string(before) == string(after) {
		t.Fatal("SaveRegistry did not write changed registry contents")
	}
}

func TestSaveRegistryMergesProjectionState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	existing := Registry{
		Version:       1,
		UserID:        "user-1",
		ControlChatID: "control-chat",
		Sessions: []Session{{
			ID:            "s001",
			ChatID:        "chat-1",
			Status:        "active",
			CodexThreadID: "thread-old",
			ModelProfile:  modelprofile.Snapshot{Name: "mimo25", Provider: "mimo", APIKeyRef: "secret:model-profile/mimo25/api-key", Revision: 2},
		}},
		Chats: map[string]ChatState{
			"chat-1": {
				SeenMessageIDs: []string{"seen-old"},
				SentMessageIDs: []string{"sent-old"},
			},
		},
	}
	if err := SaveRegistry(path, existing); err != nil {
		t.Fatalf("initial SaveRegistry error: %v", err)
	}
	next := Registry{
		Version:       1,
		UserID:        "user-1",
		ControlChatID: "control-chat",
		Sessions: []Session{{
			ID:     "s001",
			ChatID: "chat-1",
			Status: "active",
		}},
		Chats: map[string]ChatState{
			"chat-1": {
				SeenMessageIDs: []string{"seen-new"},
				SentMessageIDs: []string{"sent-new"},
			},
		},
	}
	if err := SaveRegistry(path, next); err != nil {
		t.Fatalf("merged SaveRegistry error: %v", err)
	}
	merged, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("LoadRegistry error: %v", err)
	}
	if session := merged.SessionByID("s001"); session == nil || session.CodexThreadID != "thread-old" {
		t.Fatalf("merged session = %#v, want existing thread preserved", session)
	}
	if session := merged.SessionByID("s001"); session == nil || session.ModelProfile.Name != "mimo25" || session.ModelProfile.Revision != 2 {
		t.Fatalf("merged session = %#v, want existing model profile preserved", session)
	}
	if !merged.HasSeen("chat-1", "seen-old") || !merged.HasSeen("chat-1", "seen-new") {
		t.Fatalf("seen ids not merged: %#v", merged.Chats["chat-1"].SeenMessageIDs)
	}
	if !merged.HasSent("chat-1", "sent-old") || !merged.HasSent("chat-1", "sent-new") {
		t.Fatalf("sent ids not merged: %#v", merged.Chats["chat-1"].SentMessageIDs)
	}

	next.Sessions[0].CodexThreadID = "thread-new"
	if err := SaveRegistry(path, next); err != nil {
		t.Fatalf("thread update SaveRegistry error: %v", err)
	}
	updated, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("LoadRegistry updated error: %v", err)
	}
	if session := updated.SessionByID("s001"); session == nil || session.CodexThreadID != "thread-new" {
		t.Fatalf("updated session = %#v, want durable projection thread-new", session)
	}
}

func TestSaveRegistryConcurrentProjectionMergePreservesSeenSent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	if err := SaveRegistry(path, Registry{
		Version:       1,
		UserID:        "user-1",
		ControlChatID: "control-chat",
		Sessions: []Session{{
			ID:            "s001",
			ChatID:        "chat-1",
			Status:        "active",
			CodexThreadID: "thread-real",
		}},
		Chats: map[string]ChatState{"chat-1": {}},
	}); err != nil {
		t.Fatalf("initial SaveRegistry error: %v", err)
	}

	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- SaveRegistry(path, Registry{
				Version:       1,
				UserID:        "user-1",
				ControlChatID: "control-chat",
				Sessions: []Session{{
					ID:     "s001",
					ChatID: "chat-1",
					Status: "active",
				}},
				Chats: map[string]ChatState{
					"chat-1": {
						SeenMessageIDs: []string{fmt.Sprintf("seen-%02d", i)},
						SentMessageIDs: []string{fmt.Sprintf("sent-%02d", i)},
					},
				},
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent SaveRegistry error: %v", err)
		}
	}
	merged, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("LoadRegistry error: %v", err)
	}
	if session := merged.SessionByID("s001"); session == nil || session.CodexThreadID != "thread-real" {
		t.Fatalf("merged session = %#v, want preserved thread-real", session)
	}
	for i := 0; i < writers; i++ {
		seen := fmt.Sprintf("seen-%02d", i)
		sent := fmt.Sprintf("sent-%02d", i)
		if !merged.HasSeen("chat-1", seen) {
			t.Fatalf("missing seen id %s in %#v", seen, merged.Chats["chat-1"].SeenMessageIDs)
		}
		if !merged.HasSent("chat-1", sent) {
			t.Fatalf("missing sent id %s in %#v", sent, merged.Chats["chat-1"].SentMessageIDs)
		}
	}
}

func TestSaveRegistryDoesNotResurrectRemovedProjectionChats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	if err := SaveRegistry(path, Registry{
		Version:       1,
		UserID:        "user-1",
		ControlChatID: "control-chat",
		Sessions: []Session{{
			ID:            "s-old",
			ChatID:        "chat-old",
			Status:        "active",
			CodexThreadID: "thread-old",
		}},
		Chats: map[string]ChatState{
			"chat-old": {
				SeenMessageIDs: []string{"seen-old"},
				SentMessageIDs: []string{"sent-old"},
			},
		},
	}); err != nil {
		t.Fatalf("initial SaveRegistry error: %v", err)
	}

	if err := SaveRegistry(path, Registry{
		Version:       1,
		UserID:        "user-1",
		ControlChatID: "control-chat",
		Sessions: []Session{{
			ID:            "s-new",
			ChatID:        "chat-new",
			Status:        "active",
			CodexThreadID: "thread-new",
		}},
		Chats: map[string]ChatState{
			"chat-new": {
				SeenMessageIDs: []string{"seen-new"},
				SentMessageIDs: []string{"sent-new"},
			},
		},
	}); err != nil {
		t.Fatalf("replacement SaveRegistry error: %v", err)
	}

	merged, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("LoadRegistry error: %v", err)
	}
	if _, ok := merged.Chats["chat-old"]; ok {
		t.Fatalf("removed chat-old was resurrected: %#v", merged.Chats["chat-old"])
	}
	if session := merged.SessionByID("s-old"); session != nil {
		t.Fatalf("removed session s-old was resurrected: %#v", session)
	}
	if _, ok := merged.Chats["chat-new"]; !ok {
		t.Fatalf("chat-new missing from replacement projection: %#v", merged.Chats)
	}
	if session := merged.SessionByID("s-new"); session == nil || session.CodexThreadID != "thread-new" {
		t.Fatalf("new session = %#v, want thread-new", session)
	}
	if merged.HasSeen("chat-old", "seen-old") || merged.HasSent("chat-old", "sent-old") {
		t.Fatalf("removed chat-old retained seen/sent projection: %#v", merged.Chats["chat-old"])
	}
	if !merged.HasSeen("chat-new", "seen-new") || !merged.HasSent("chat-new", "sent-new") {
		t.Fatalf("new chat seen/sent missing: %#v", merged.Chats["chat-new"])
	}
}
