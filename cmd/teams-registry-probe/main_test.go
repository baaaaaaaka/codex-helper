package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCardMessageRoundTrip(t *testing.T) {
	card := machineCard{
		Kind:                     "cxp.machine-card.v1",
		RegistryKey:              "registry-1",
		MachineID:                "machine-a",
		MachineLabel:             "Machine A",
		Capabilities:             []string{"docker"},
		Sequence:                 3,
		HeartbeatIntervalSeconds: 15,
		TTLSeconds:               45,
		PublishedAt:              time.Unix(100, 0).UTC().Format(time.RFC3339Nano),
		ExpiresAt:                time.Unix(145, 0).UTC().Format(time.RFC3339Nano),
	}
	parsed, ok := parseCardMessage(renderCardHTML(card))
	if !ok {
		t.Fatal("expected card message to parse")
	}
	if parsed.Kind != card.Kind ||
		parsed.RegistryKey != card.RegistryKey ||
		parsed.MachineID != card.MachineID ||
		parsed.Sequence != card.Sequence ||
		parsed.ExpiresAt != card.ExpiresAt {
		t.Fatalf("parsed card mismatch: %#v", parsed)
	}
}

func TestSplitStatusesSeparatesOnlineAndStale(t *testing.T) {
	statuses := []machineStatus{
		{MachineID: "machine-a", State: "online"},
		{MachineID: "machine-b", State: "stale"},
	}
	online, stale := splitStatuses(statuses)
	if len(online) != 1 || online[0].MachineID != "machine-a" {
		t.Fatalf("online statuses = %#v", online)
	}
	if len(stale) != 1 || stale[0].MachineID != "machine-b" {
		t.Fatalf("stale statuses = %#v", stale)
	}
}

func TestRegistryExternalIDIsStableAndOpaque(t *testing.T) {
	left := registryExternalID("tenant-user")
	right := registryExternalID("tenant-user")
	other := registryExternalID("tenant-user-2")
	if left == "" || left != right {
		t.Fatalf("external id should be stable: left=%q right=%q", left, right)
	}
	if left == other {
		t.Fatalf("external id should vary by key: %q", left)
	}
	if len(left) > 64 {
		t.Fatalf("external id too long: %d", len(left))
	}
}

func TestRegistryReadPageSizeSupportsTopAlias(t *testing.T) {
	size, err := registryReadPageSize(50, 0)
	if err != nil {
		t.Fatalf("default page size: %v", err)
	}
	if size != 50 {
		t.Fatalf("default page size = %d, want 50", size)
	}
	size, err = registryReadPageSize(50, 25)
	if err != nil {
		t.Fatalf("top alias page size: %v", err)
	}
	if size != 25 {
		t.Fatalf("top alias page size = %d, want 25", size)
	}
	if _, err := registryReadPageSize(0, 0); err == nil {
		t.Fatal("zero page size accepted")
	}
	if _, err := registryReadPageSize(51, 0); err == nil {
		t.Fatal("oversized page size accepted")
	}
	if _, err := registryReadPageSize(50, 51); err == nil {
		t.Fatal("oversized top alias accepted")
	}
}

func TestNormalizeProbeRoleRejectsInvalidValues(t *testing.T) {
	for _, role := range []string{"publisher", "watcher", "both", " BOTH "} {
		if _, err := normalizeProbeRole(role); err != nil {
			t.Fatalf("valid role %q rejected: %v", role, err)
		}
	}
	if _, err := normalizeProbeRole("idle"); err == nil {
		t.Fatal("invalid role accepted")
	}
}

func TestHeartbeatPublisherPatchesExistingSlot(t *testing.T) {
	graph := newFakeHeartbeatGraph()
	publisher := heartbeatPublisher{graph: graph, chatID: "chat-1"}
	ctx := context.Background()
	for seq := 1; seq <= 1000; seq++ {
		result, err := publisher.publish(ctx, testMachineCard("registry-1", "machine-a", seq, time.Unix(int64(seq), 0).UTC()))
		if err != nil {
			t.Fatalf("publish seq %d: %v", seq, err)
		}
		if seq == 1 && result.mode != "append-slot" {
			t.Fatalf("first publish mode = %q, want append-slot", result.mode)
		}
		if seq > 1 && result.mode != "patch" {
			t.Fatalf("publish seq %d mode = %q, want patch", seq, result.mode)
		}
	}
	if graph.sendCount != 1 {
		t.Fatalf("send count = %d, want 1", graph.sendCount)
	}
	if graph.patchCount != 999 {
		t.Fatalf("patch count = %d, want 999", graph.patchCount)
	}
	if len(graph.messages) != 1 {
		t.Fatalf("message slots = %d, want 1", len(graph.messages))
	}
	card, ok := parseCardMessage(graph.messages[publisher.slotMessageID].Body.Content)
	if !ok {
		t.Fatal("final slot message did not parse")
	}
	if card.Sequence != 1000 {
		t.Fatalf("final sequence = %d, want 1000", card.Sequence)
	}
}

func TestHeartbeatPublisherAppendsReplacementWhenPatchFails(t *testing.T) {
	graph := newFakeHeartbeatGraph()
	publisher := heartbeatPublisher{graph: graph, chatID: "chat-1"}
	ctx := context.Background()
	if _, err := publisher.publish(ctx, testMachineCard("registry-1", "machine-a", 1, time.Unix(1, 0).UTC())); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	firstSlot := publisher.slotMessageID
	graph.patchErr = errors.New("slot disappeared")
	result, err := publisher.publish(ctx, testMachineCard("registry-1", "machine-a", 2, time.Unix(2, 0).UTC()))
	if err != nil {
		t.Fatalf("fallback publish: %v", err)
	}
	if result.mode != "append-slot" {
		t.Fatalf("fallback mode = %q, want append-slot", result.mode)
	}
	if result.slotMessageID == "" || result.slotMessageID == firstSlot {
		t.Fatalf("replacement slot = %q, first slot = %q", result.slotMessageID, firstSlot)
	}
	if graph.sendCount != 2 || graph.patchCount != 1 || len(graph.messages) != 2 {
		t.Fatalf("counts send=%d patch=%d messages=%d, want 2/1/2", graph.sendCount, graph.patchCount, len(graph.messages))
	}
}

func TestHeartbeatPublisherDoesNotWriteLocalFiles(t *testing.T) {
	dir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}
	defer func() {
		if err := os.Chdir(oldWD); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}()

	graph := newFakeHeartbeatGraph()
	publisher := heartbeatPublisher{graph: graph, chatID: "chat-1"}
	ctx := context.Background()
	for seq := 1; seq <= 200; seq++ {
		if _, err := publisher.publish(ctx, testMachineCard("registry-1", "machine-a", seq, time.Unix(int64(seq), 0).UTC())); err != nil {
			t.Fatalf("publish seq %d: %v", seq, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("heartbeat publisher wrote local files: %#v", entries)
	}
}

func TestObserveStressLargeRegistryWindow(t *testing.T) {
	graph := newFakeHeartbeatGraph()
	now := time.Unix(5000, 0).UTC()
	for i := 0; i < 50; i++ {
		card := testMachineCard("registry-1", fmt.Sprintf("machine-%02d", i), i+1, now.Add(-time.Duration(i)*time.Second))
		graph.addMessage(fmt.Sprintf("msg-%02d", i), renderCardHTML(card))
	}
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		statuses, err := observe(ctx, graph, "chat-1", "registry-1", 50, now)
		if err != nil {
			t.Fatalf("observe iteration %d: %v", i, err)
		}
		if len(statuses) != 50 {
			t.Fatalf("observe iteration %d statuses = %d, want 50", i, len(statuses))
		}
	}
	if graph.listCount != 1000 {
		t.Fatalf("list count = %d, want 1000", graph.listCount)
	}
}

func BenchmarkHeartbeatSlotPublisher(b *testing.B) {
	graph := newFakeHeartbeatGraph()
	publisher := heartbeatPublisher{graph: graph, chatID: "chat-1"}
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := publisher.publish(ctx, testMachineCard("registry-1", "machine-a", i+1, now.Add(time.Duration(i)*time.Second))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkObserveRegistryWindow(b *testing.B) {
	graph := newFakeHeartbeatGraph()
	now := time.Unix(5000, 0).UTC()
	for i := 0; i < 50; i++ {
		card := testMachineCard("registry-1", fmt.Sprintf("machine-%02d", i), i+1, now.Add(-time.Duration(i)*time.Second))
		graph.addMessage(fmt.Sprintf("msg-%02d", i), renderCardHTML(card))
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := observe(ctx, graph, "chat-1", "registry-1", 50, now); err != nil {
			b.Fatal(err)
		}
	}
}

func testMachineCard(registryKey string, machineID string, seq int, published time.Time) machineCard {
	return machineCard{
		Kind:                     "cxp.machine-card.v1",
		RegistryKey:              registryKey,
		MachineID:                machineID,
		MachineLabel:             machineID,
		Capabilities:             []string{"docker", "teams-registry-probe"},
		Sequence:                 seq,
		HeartbeatIntervalSeconds: 300,
		TTLSeconds:               900,
		PublishedAt:              published.UTC().Format(time.RFC3339Nano),
		ExpiresAt:                published.Add(15 * time.Minute).UTC().Format(time.RFC3339Nano),
	}
}

type fakeHeartbeatGraph struct {
	messages   map[string]machineRegistryMessage
	order      []string
	nextID     int
	sendCount  int
	patchCount int
	listCount  int
	patchErr   error
}

func newFakeHeartbeatGraph() *fakeHeartbeatGraph {
	return &fakeHeartbeatGraph{messages: map[string]machineRegistryMessage{}}
}

func (g *fakeHeartbeatGraph) SendHTML(_ context.Context, _ string, content string) (machineRegistryMessage, error) {
	g.sendCount++
	g.nextID++
	id := fmt.Sprintf("message-%06d", g.nextID)
	return g.addMessage(id, content), nil
}

func (g *fakeHeartbeatGraph) UpdateChatMessageHTML(_ context.Context, _ string, messageID string, content string) error {
	g.patchCount++
	if g.patchErr != nil {
		err := g.patchErr
		g.patchErr = nil
		return err
	}
	msg, ok := g.messages[messageID]
	if !ok {
		return errors.New("message not found")
	}
	msg.Body.Content = content
	g.messages[messageID] = msg
	return nil
}

func (g *fakeHeartbeatGraph) ListMessages(_ context.Context, _ string, top int) ([]machineRegistryMessage, error) {
	g.listCount++
	if top <= 0 || top > len(g.order) {
		top = len(g.order)
	}
	out := make([]machineRegistryMessage, 0, top)
	for i := 0; i < top; i++ {
		out = append(out, g.messages[g.order[i]])
	}
	return out, nil
}

func (g *fakeHeartbeatGraph) addMessage(id string, content string) machineRegistryMessage {
	msg := machineRegistryMessage{ID: id}
	msg.Body.Content = content
	g.messages[id] = msg
	g.order = append([]string{id}, g.order...)
	return msg
}
