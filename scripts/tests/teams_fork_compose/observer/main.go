package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type codexState struct {
	ForkRequests int `json:"fork_requests"`
	Threads      []struct {
		ID           string `json:"id"`
		ForkedFromID string `json:"forkedFromId"`
		LatestTurnID string `json:"latestTurnId"`
	} `json:"threads"`
}

type graphState struct {
	Messages []struct {
		ID     string `json:"id"`
		ChatID string `json:"chatId"`
		Body   string `json:"body"`
	} `json:"messages"`
}

func main() {
	stateDir := firstNonEmpty(os.Getenv("STATE_DIR"), "/state")
	if err := waitMarker(stateDir, []string{"controller.done", "controller.failed"}, 10*time.Minute); err != nil {
		fatal(err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "controller.failed")); err == nil {
		fatal(fmt.Errorf("controller reported failure: %s", readText(filepath.Join(stateDir, "controller.failed"))))
	}
	store, err := teamstore.Open(filepath.Join(stateDir, "teams.json"))
	if err != nil {
		fatal(err)
	}
	defer func() { _ = store.Close() }()
	state, err := store.Load(context.Background())
	if err != nil {
		fatal(err)
	}
	op, ok := state.ForkOperations["fork:compose-operation"]
	if !ok {
		// forkOperationID is deterministic but deliberately opaque; locate the
		// single operation instead of duplicating its hash implementation here.
		if len(state.ForkOperations) != 1 {
			fatal(fmt.Errorf("expected one fork operation, got %d", len(state.ForkOperations)))
		}
		for _, candidate := range state.ForkOperations {
			op = candidate
		}
	}
	if op.Phase != teamstore.ForkPhaseLinkSent {
		fatal(fmt.Errorf("fork phase = %s, want link_sent", op.Phase))
	}
	if _, fenced, err := store.ParentFork(context.Background(), op.ParentSessionID); err != nil || fenced {
		fatal(fmt.Errorf("parent fence after compose recovery = %v err=%v", fenced, err))
	}
	child, ok := state.Sessions[op.ChildSessionID]
	if !ok || child.Status != teamstore.SessionStatusActive || child.CodexThreadID != "compose-child-codex" || child.TeamsChatID != "compose-child-chat" {
		fatal(fmt.Errorf("child session did not become active: %#v", child))
	}
	if err := checkCodex(filepath.Join(stateDir, "fake-codex.json"), op.ParentThreadID, op.CutoffCodexTurnID); err != nil {
		fatal(err)
	}
	if err := checkGraph(filepath.Join(stateDir, "fake-graph.json"), op.ParentChatID, op.ChildChatID); err != nil {
		fatal(err)
	}
	if op.OwnerMachineID == "compose-machine-b" && op.OwnerLeaseGeneration != 2 {
		fatal(fmt.Errorf("takeover owner = %s/%d, want compose-machine-b/2", op.OwnerMachineID, op.OwnerLeaseGeneration))
	}
	if err := os.WriteFile(filepath.Join(stateDir, "observer.pass"), []byte("durable fork invariants passed\n"), 0o600); err != nil {
		fatal(err)
	}
}

func checkCodex(path string, parentID string, cutoffID string) error {
	var state codexState
	if err := readJSON(path, &state); err != nil {
		return fmt.Errorf("read fake Codex state: %w", err)
	}
	if state.ForkRequests < 1 {
		return fmt.Errorf("fake Codex did not receive thread/fork")
	}
	children := 0
	for _, thread := range state.Threads {
		if thread.ID == "compose-child-codex" {
			children++
			if thread.ForkedFromID != parentID || thread.LatestTurnID != cutoffID {
				return fmt.Errorf("fake Codex child proof = %#v", thread)
			}
		}
	}
	if children != 1 {
		return fmt.Errorf("fake Codex child count = %d, want one", children)
	}
	return nil
}

func checkGraph(path string, parentChatID string, childChatID string) error {
	var state graphState
	if err := readJSON(path, &state); err != nil {
		return fmt.Errorf("read fake Graph state: %w", err)
	}
	seen := make(map[string]bool)
	historyLast := -1
	markerIndex := -1
	linkIndex := -1
	for index, message := range state.Messages {
		marker := outboxMarkerID(message.Body)
		if marker == "" {
			continue
		}
		if seen[marker] {
			return fmt.Errorf("duplicate visible Graph message for outbox %q", marker)
		}
		seen[marker] = true
		switch {
		case strings.HasPrefix(marker, "fork-marker:") && message.ChatID == childChatID && strings.Contains(message.Body, "History import complete"):
			markerIndex = index
		case strings.HasPrefix(marker, "fork-outbox:") && message.ChatID == childChatID:
			if markerIndex >= 0 {
				return fmt.Errorf("history chunk appeared after completion marker")
			}
			historyLast = index
		case strings.HasPrefix(marker, "fork-link:") && message.ChatID == parentChatID && strings.Contains(message.Body, "Fork complete"):
			linkIndex = index
		}
	}
	if historyLast < 0 || markerIndex < 0 || linkIndex < 0 || historyLast >= markerIndex || markerIndex >= linkIndex {
		return fmt.Errorf("Graph ordering history=%d marker=%d link=%d", historyLast, markerIndex, linkIndex)
	}
	return nil
}

func outboxMarkerID(body string) string {
	const prefix = "codex-helper-outbox:"
	start := strings.LastIndex(body, prefix)
	if start < 0 {
		return ""
	}
	start += len(prefix)
	end := strings.Index(body[start:], " -->")
	if end < 0 {
		return ""
	}
	return strings.TrimSpace(body[start : start+end])
}

func waitMarker(stateDir string, names []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, name := range names {
			if _, err := os.Stat(filepath.Join(stateDir, name)); err == nil {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %v", names)
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func readText(path string) string {
	data, _ := os.ReadFile(path)
	return strings.TrimSpace(string(data))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func fatal(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "observer: %v\n", err)
	os.Exit(1)
}
