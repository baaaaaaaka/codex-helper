//go:build linux

package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// TestTeamsRuntimeSafetySQLiteFullFilesystemDockerCI exercises the OS-level
// boundary that PRAGMA max_page_count cannot model: a real filesystem runs out
// of space while the durable Teams poll projection is being updated. It only
// runs from the isolated Docker boundary job, where root is a small tmpfs and
// no live helper state is mounted.
func TestTeamsRuntimeSafetySQLiteFullFilesystemDockerCI(t *testing.T) {
	if os.Getenv("CXP_TEAMS_BOUNDARY_DOCKER") != "1" {
		t.Skip("runs only in the isolated Docker boundary job")
	}
	root := strings.TrimSpace(os.Getenv("CXP_TEAMS_BOUNDARY_FS_ROOT"))
	if root == "" {
		t.Fatal("CXP_TEAMS_BOUNDARY_FS_ROOT is required")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("create boundary filesystem root: %v", err)
	}

	ctx := context.Background()
	statePath := filepath.Join(root, "teams-state.json")
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("open state on boundary filesystem: %v", err)
	}
	defer func() { _ = store.Close() }()
	if _, err := store.RecordChatPollSuccessWithContinuation(ctx, "chat-full-fs", time.Now(), true, true, 20, "/chats/chat-full-fs/messages?$skiptoken=old"); err != nil {
		t.Fatalf("seed poll projection: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate poll projection to SQLite: %v", err)
	}

	fillerPath := filepath.Join(root, "filler")
	filler, err := os.OpenFile(fillerPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open filesystem filler: %v", err)
	}
	chunk := make([]byte, 64*1024)
	for {
		free, err := boundaryFilesystemFreeBytes(root)
		if err != nil {
			_ = filler.Close()
			t.Fatalf("measure filesystem free space: %v", err)
		}
		if free < 512*1024 {
			break
		}
		if _, err := filler.Write(chunk); err != nil {
			if err != syscall.ENOSPC {
				_ = filler.Close()
				t.Fatalf("fill boundary filesystem: %v", err)
			}
			break
		}
	}
	if err := filler.Close(); err != nil {
		t.Fatalf("close filesystem filler: %v", err)
	}

	largePath := "/chats/chat-full-fs/messages?$skiptoken=" + strings.Repeat("x", 16*1024*1024)
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:                      "chat-full-fs",
		DeferredContinuationPath:    largePath,
		SetDeferredContinuationPath: true,
	}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "full") {
		t.Fatalf("filesystem-full deferred write error = %v, want a disk-full error", err)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-full-fs")
	if err != nil || !ok {
		t.Fatalf("read poll after filesystem-full write: ok=%v err=%v", ok, err)
	}
	if poll.DeferredContinuationPath != "" {
		t.Fatalf("filesystem-full write partially committed deferred state: %#v", poll)
	}

	if err := os.Remove(fillerPath); err != nil {
		t.Fatalf("release filesystem capacity: %v", err)
	}
	const recoveredPath = "/chats/chat-full-fs/messages?$skiptoken=recovered"
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:                      "chat-full-fs",
		DeferredContinuationPath:    recoveredPath,
		SetDeferredContinuationPath: true,
	}); err != nil {
		t.Fatalf("retry deferred write after capacity recovery: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close recovered boundary store: %v", err)
	}
	reopened, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("reopen recovered boundary store: %v", err)
	}
	defer reopened.Close()
	poll, ok, err = reopened.ChatPoll(ctx, "chat-full-fs")
	if err != nil || !ok || poll.DeferredContinuationPath != recoveredPath {
		t.Fatalf("reopened filesystem-full recovery = %#v ok=%v err=%v", poll, ok, err)
	}
}

func boundaryFilesystemFreeBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
