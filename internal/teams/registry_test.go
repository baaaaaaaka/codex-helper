package teams

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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
