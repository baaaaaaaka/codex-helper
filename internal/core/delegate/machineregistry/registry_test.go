package machineregistry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCardRoundTripAndObserveLatest(t *testing.T) {
	now := time.Unix(5000, 0).UTC()
	oldCard := testCard("registry-1", "machine-a", 1, now.Add(-10*time.Minute), 15*time.Minute)
	newCard := testCard("registry-1", "machine-a", 2, now.Add(-time.Minute), 15*time.Minute)
	otherCard := testCard("registry-1", "machine-b", 1, now.Add(-20*time.Minute), 5*time.Minute)
	ignoredCard := testCard("other-registry", "machine-c", 1, now, 15*time.Minute)

	parsed, ok := ParseCardMessage(RenderCardHTML(newCard))
	if !ok {
		t.Fatal("expected card to parse")
	}
	if parsed.MachineID != newCard.MachineID || parsed.Sequence != newCard.Sequence {
		t.Fatalf("parsed card mismatch: %#v", parsed)
	}

	messages := []ChatMessage{
		messageWithHTML("m4", RenderCardHTML(ignoredCard)),
		messageWithHTML("m3", RenderCardHTML(oldCard)),
		messageWithHTML("m2", RenderCardHTML(otherCard)),
		messageWithHTML("m1", RenderCardHTML(newCard)),
	}
	statuses := ObserveMessages(messages, "registry-1", now)
	if len(statuses) != 2 {
		t.Fatalf("statuses = %#v, want 2 machines", statuses)
	}
	if statuses[0].MachineID != "machine-a" || statuses[0].Sequence != 2 || statuses[0].State != "online" {
		t.Fatalf("machine-a status = %#v, want latest online", statuses[0])
	}
	if !statuses[0].Accepting || statuses[0].Revision != 2 || len(statuses[0].Aliases) != 1 || statuses[0].Aliases[0] != "B" {
		t.Fatalf("machine-a capability status = %#v, want accepting alias and revision", statuses[0])
	}
	if statuses[1].MachineID != "machine-b" || statuses[1].State != "stale" {
		t.Fatalf("machine-b status = %#v, want stale", statuses[1])
	}
	online, stale := SplitStatuses(statuses)
	if len(online) != 1 || online[0].MachineID != "machine-a" || len(stale) != 1 || stale[0].MachineID != "machine-b" {
		t.Fatalf("split statuses online=%#v stale=%#v", online, stale)
	}
}

func TestRegistryExternalIDIsStableOpaqueAndUserScoped(t *testing.T) {
	key := RegistryKey("tenant-1", "user-1")
	if key == "" || key != RegistryKey("tenant-1", "user-1") {
		t.Fatalf("registry key should be stable: %q", key)
	}
	if key == RegistryKey("tenant-1", "user-2") {
		t.Fatalf("registry key should vary by signed-in user: %q", key)
	}
	external := ExternalID(key)
	if external == "" || external != ExternalID(key) || external == ExternalID(RegistryKey("tenant-1", "user-2")) {
		t.Fatalf("external id stability/scope mismatch: %q", external)
	}
	if len(external) > 64 {
		t.Fatalf("external id too long: %d", len(external))
	}
	inbox := InboxExternalID(key, "machine-a")
	if inbox == "" || inbox != InboxExternalID(key, "machine-a") || inbox == InboxExternalID(key, "machine-b") || inbox == external {
		t.Fatalf("inbox external id stability/scope mismatch: %q registry=%q", inbox, external)
	}
}

func TestObserveMessagesPrefersHigherRevisionOverNewerTimestamp(t *testing.T) {
	now := time.Unix(5000, 0).UTC()
	highRevision := testCard("registry-1", "machine-a", 10, now.Add(-10*time.Minute), 15*time.Minute)
	highRevision.Revision = 10
	lowRevision := testCard("registry-1", "machine-a", 11, now, 15*time.Minute)
	lowRevision.Revision = 9
	statuses := ObserveMessages([]ChatMessage{
		messageWithHTML("newer-low-revision", RenderCardHTML(lowRevision)),
		messageWithHTML("older-high-revision", RenderCardHTML(highRevision)),
	}, "registry-1", now)
	if len(statuses) != 1 || statuses[0].Revision != 10 || statuses[0].Sequence != 10 {
		t.Fatalf("statuses = %#v, want higher revision card", statuses)
	}
}

func TestSaveCacheNoopDoesNotRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	cache := Cache{SchemaVersion: 1, RegistryKey: "registry-1", RegistryChatID: "chat-1"}
	written, err := SaveCache(path, cache)
	if err != nil || !written {
		t.Fatalf("first SaveCache written=%v err=%v, want write", written, err)
	}
	oldTime := time.Unix(1000, 0)
	if err := os.Chtimes(path, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	written, err = SaveCache(path, cache)
	if err != nil {
		t.Fatalf("second SaveCache: %v", err)
	}
	if written {
		t.Fatal("second SaveCache wrote identical content")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat cache: %v", err)
	}
	if !info.ModTime().Equal(oldTime) {
		t.Fatalf("cache modtime changed on noop save: %v want %v", info.ModTime(), oldTime)
	}
	loaded, err := LoadCache(path)
	if err != nil {
		t.Fatalf("LoadCache: %v", err)
	}
	if loaded.RegistryKey != "registry-1" || loaded.RegistryChatID != "chat-1" {
		t.Fatalf("loaded cache mismatch: %#v", loaded)
	}
}

func TestCacheReplaceFileWithRetryRetriesRetryableErrors(t *testing.T) {
	retryErr := errors.New("access denied")
	attempts := 0
	err := cacheReplaceFileWithRetry("tmp", "registry.json", func(string, string) error {
		attempts++
		if attempts < 4 {
			return retryErr
		}
		return nil
	}, func(err error) bool {
		return errors.Is(err, retryErr)
	})
	if err != nil {
		t.Fatalf("cacheReplaceFileWithRetry error: %v", err)
	}
	if attempts != 4 {
		t.Fatalf("attempts = %d, want 4", attempts)
	}
}

func TestWriteCacheFileAtomicallyCleansFailedReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	replaceErr := errors.New("replace failed")
	prev := cacheReplaceFile
	t.Cleanup(func() { cacheReplaceFile = prev })
	var tempPath string
	cacheReplaceFile = func(src string, dst string) error {
		tempPath = src
		if dst != path {
			t.Fatalf("replace dst = %q, want %q", dst, path)
		}
		if filepath.Dir(src) != filepath.Dir(path) {
			t.Fatalf("replace src dir = %q, want %q", filepath.Dir(src), filepath.Dir(path))
		}
		data, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("read temp during replace: %v", err)
		}
		if string(data) != "new registry" {
			t.Fatalf("temp data = %q, want new registry", data)
		}
		return replaceErr
	}

	err := writeCacheFileAtomically(path, []byte("new registry"), 0o600)
	if !errors.Is(err, replaceErr) {
		t.Fatalf("writeCacheFileAtomically error = %v, want replace error", err)
	}
	if tempPath == "" {
		t.Fatal("cacheReplaceFile was not called")
	}
	if _, err := os.Stat(tempPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file still exists after replace failure: stat err=%v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target should not exist after failed replace: stat err=%v", err)
	}
}

func TestStoreEnsureCreatesLongWindowAndCaches(t *testing.T) {
	graph := newFakeRegistryGraph()
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "registry.json")
	store := Store{
		Graph:           graph,
		CachePath:       path,
		Subject:         "CXP Registry",
		WindowDuration:  45 * 24 * time.Hour,
		RefreshInterval: 7 * 24 * time.Hour,
		Now:             func() time.Time { return now },
	}

	result, err := store.Ensure(context.Background(), "tenant-1", "user-1")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if !result.CacheWritten || result.Cache.RegistryChatID != "chat-registry" || result.Cache.MeetingID != "meeting-registry" {
		t.Fatalf("unexpected ensure cache: written=%v cache=%#v", result.CacheWritten, result.Cache)
	}
	if graph.createCalls != 1 {
		t.Fatalf("create calls = %d, want 1", graph.createCalls)
	}
	if graph.createSubject != "CXP Registry" || graph.createExternalID != ExternalID(RegistryKey("tenant-1", "user-1")) {
		t.Fatalf("create input subject=%q external=%q", graph.createSubject, graph.createExternalID)
	}
	if !graph.createStart.Equal(now.Add(-5*time.Minute)) || !graph.createEnd.Equal(now.Add(45*24*time.Hour)) {
		t.Fatalf("create window = %v/%v", graph.createStart, graph.createEnd)
	}

	result, err = store.Ensure(context.Background(), "tenant-1", "user-1")
	if err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	if result.CacheWritten {
		t.Fatalf("second Ensure rewrote stable cache: %#v", result.Cache)
	}
	if graph.getCalls != 1 || graph.createCalls != 1 {
		t.Fatalf("second Ensure should validate cache without create: get=%d create=%d", graph.getCalls, graph.createCalls)
	}
}

func TestStoreEnsureInboxCachesLocatorAndObservePreservesIt(t *testing.T) {
	graph := newFakeRegistryGraph()
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "registry.json")
	store := Store{
		Graph:     graph,
		CachePath: path,
		Now:       func() time.Time { return now },
	}
	cache := Cache{SchemaVersion: 1, RegistryKey: "registry-1"}
	next, chat, _, written, err := store.EnsureInbox(context.Background(), cache, "machine-a")
	if err != nil {
		t.Fatalf("EnsureInbox: %v", err)
	}
	if !written || chat.ID != "chat-inbox" || next.InboxExternalID != InboxExternalID("registry-1", "machine-a") || next.InboxGeneration == "" {
		t.Fatalf("inbox result written=%v chat=%#v cache=%#v", written, chat, next)
	}
	oldTime := time.Unix(1000, 0)
	if err := os.Chtimes(path, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	again, _, _, written, err := store.EnsureInbox(context.Background(), next, "machine-a")
	if err != nil {
		t.Fatalf("second EnsureInbox: %v", err)
	}
	if written || again.InboxChatID != "chat-inbox" {
		t.Fatalf("second EnsureInbox wrote=%v cache=%#v", written, again)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat cache: %v", err)
	}
	if !info.ModTime().Equal(oldTime) {
		t.Fatalf("EnsureInbox rewrote stable cache: %v want %v", info.ModTime(), oldTime)
	}

	card := testCard("registry-1", "machine-a", 1, now, 15*time.Minute)
	card.InboxRef = next.InboxExternalID
	card.InboxGeneration = next.InboxGeneration
	statuses := ObserveMessages([]ChatMessage{messageWithHTML("m1", RenderCardHTML(card))}, "registry-1", now)
	if len(statuses) != 1 || statuses[0].InboxRef != next.InboxExternalID || statuses[0].InboxGeneration != next.InboxGeneration {
		t.Fatalf("statuses = %#v, want inbox locator", statuses)
	}
}

func TestObserveWindowedPaginatesRegistryMessages(t *testing.T) {
	graph := newFakeRegistryGraph()
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 75; i++ {
		card := testCard("registry-1", fmt.Sprintf("machine-%02d", i), i+1, now.Add(-time.Duration(i)*time.Second), 15*time.Minute)
		graph.addMessage(fmt.Sprintf("msg-%02d", i), RenderCardHTML(card))
	}
	store := Store{Graph: graph}
	statuses, err := store.Observe(context.Background(), Cache{RegistryChatID: "chat-registry", RegistryKey: "registry-1"}, 50, now)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if len(statuses) != 75 {
		t.Fatalf("statuses = %d, want paged 75", len(statuses))
	}
	if graph.windowListCount != 2 {
		t.Fatalf("window list count = %d, want 2 pages", graph.windowListCount)
	}
	if statuses[0].MachineID != "machine-00" || statuses[len(statuses)-1].MachineID != "machine-74" {
		t.Fatalf("status bounds = %#v / %#v, want full paged registry", statuses[0], statuses[len(statuses)-1])
	}
}

func TestObserveMessagesUsesGraphModifiedTimeForLiveness(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	card := testCard("registry-1", "machine-a", 1, now.Add(-time.Hour), 15*time.Minute)
	msg := messageWithHTML("m1", RenderCardHTML(card))
	msg.LastModifiedDateTime = now.Add(-time.Minute).Format(time.RFC3339Nano)
	statuses := ObserveMessages([]ChatMessage{msg}, "registry-1", now)
	if len(statuses) != 1 {
		t.Fatalf("statuses = %#v, want one machine", statuses)
	}
	if statuses[0].State != "online" {
		t.Fatalf("status = %#v, want graph-modified heartbeat to keep machine online", statuses[0])
	}
	if statuses[0].AgeSeconds < 59 || statuses[0].AgeSeconds > 61 {
		t.Fatalf("age = %d, want about 60 seconds", statuses[0].AgeSeconds)
	}
}

func TestStoreEnsureDoesNotRepairCacheOnTransientValidationError(t *testing.T) {
	graph := newFakeRegistryGraph()
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "registry.json")
	store := Store{
		Graph:     graph,
		CachePath: path,
		Now:       func() time.Time { return now },
	}
	cache := Cache{
		SchemaVersion:  1,
		TenantIDHash:   shortHash("tenant:tenant-1", 16),
		UserIDHash:     shortHash("user:user-1", 16),
		RegistryKey:    RegistryKey("tenant-1", "user-1"),
		ExternalID:     ExternalID(RegistryKey("tenant-1", "user-1")),
		RegistryChatID: "chat-registry",
		MeetingID:      "meeting-registry",
		ValidatedAt:    now,
		NextRefreshAt:  now.Add(time.Hour),
		SlotMessageID:  "slot-1",
		SlotMachineID:  "machine-a",
	}
	if _, err := SaveCache(path, cache); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	graph.getErr = &StatusError{StatusCode: http.StatusTooManyRequests}
	_, err := store.Ensure(context.Background(), "tenant-1", "user-1")
	if err == nil {
		t.Fatal("Ensure succeeded on transient cache validation failure")
	}
	if graph.createCalls != 0 {
		t.Fatalf("Ensure created a replacement registry on transient validation failure: create=%d", graph.createCalls)
	}
}

func TestRefreshMeetingWindowSkipsUntilDueAndPersistsOnRefresh(t *testing.T) {
	graph := newFakeRegistryGraph()
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "registry.json")
	store := Store{
		Graph:           graph,
		CachePath:       path,
		WindowDuration:  45 * 24 * time.Hour,
		RefreshInterval: 7 * 24 * time.Hour,
		Now:             func() time.Time { return now },
	}
	cache := Cache{
		SchemaVersion:  1,
		MeetingID:      "meeting-registry",
		RegistryChatID: "chat-registry",
		RegistryKey:    "registry-1",
		NextRefreshAt:  now.Add(time.Hour),
	}
	if _, err := SaveCache(path, cache); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	next, written, err := store.RefreshMeeting(context.Background(), cache)
	if err != nil {
		t.Fatalf("RefreshMeeting skip: %v", err)
	}
	if written || graph.refreshCalls != 0 || !next.NextRefreshAt.Equal(cache.NextRefreshAt) {
		t.Fatalf("refresh should have skipped: written=%v refreshCalls=%d next=%#v", written, graph.refreshCalls, next)
	}

	cache.NextRefreshAt = now.Add(-time.Second)
	next, written, err = store.RefreshMeeting(context.Background(), cache)
	if err != nil {
		t.Fatalf("RefreshMeeting due: %v", err)
	}
	if !written || graph.refreshCalls != 1 {
		t.Fatalf("refresh should have written once: written=%v refreshCalls=%d", written, graph.refreshCalls)
	}
	if !graph.refreshStart.Equal(now.Add(-5*time.Minute)) || !graph.refreshEnd.Equal(now.Add(45*24*time.Hour)) {
		t.Fatalf("refresh window = %v/%v", graph.refreshStart, graph.refreshEnd)
	}
	if !next.NextRefreshAt.Equal(now.Add(7 * 24 * time.Hour)) {
		t.Fatalf("next refresh = %v", next.NextRefreshAt)
	}
}

func TestStorePublishPatchesSlotWithoutCacheRewrite(t *testing.T) {
	graph := newFakeRegistryGraph()
	now := time.Unix(1000, 0).UTC()
	path := filepath.Join(t.TempDir(), "registry.json")
	store := Store{Graph: graph, CachePath: path}
	cache := Cache{SchemaVersion: 1, RegistryChatID: "chat-registry", RegistryKey: "registry-1"}

	var result PublishResult
	var written bool
	var err error
	cache, result, written, err = store.Publish(context.Background(), cache, testCard("registry-1", "machine-a", 1, now, 15*time.Minute))
	if err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	if result.Mode != "append-slot" || !written || cache.SlotMessageID == "" {
		t.Fatalf("initial publish result=%#v written=%v cache=%#v", result, written, cache)
	}
	oldTime := time.Unix(2000, 0)
	if err := os.Chtimes(path, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	for seq := 2; seq <= 1000; seq++ {
		cache, result, written, err = store.Publish(context.Background(), cache, testCard("registry-1", "machine-a", seq, now.Add(time.Duration(seq)*time.Second), 15*time.Minute))
		if err != nil {
			t.Fatalf("publish seq %d: %v", seq, err)
		}
		if result.Mode != "patch" || written {
			t.Fatalf("publish seq %d result=%#v written=%v, want patch/no-write", seq, result, written)
		}
	}
	if graph.sendCount != 1 || graph.patchCount != 999 || len(graph.messages) != 1 {
		t.Fatalf("graph counts send=%d patch=%d messages=%d, want 1/999/1", graph.sendCount, graph.patchCount, len(graph.messages))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat cache: %v", err)
	}
	if !info.ModTime().Equal(oldTime) {
		t.Fatalf("cache modtime changed during patch heartbeats: %v want %v", info.ModTime(), oldTime)
	}
}

func TestStorePublishFallbackPersistsReplacementSlot(t *testing.T) {
	graph := newFakeRegistryGraph()
	path := filepath.Join(t.TempDir(), "registry.json")
	store := Store{Graph: graph, CachePath: path}
	cache := Cache{SchemaVersion: 1, RegistryChatID: "chat-registry", RegistryKey: "registry-1"}
	now := time.Unix(1000, 0).UTC()

	cache, _, _, _ = store.Publish(context.Background(), cache, testCard("registry-1", "machine-a", 1, now, 15*time.Minute))
	firstSlot := cache.SlotMessageID
	graph.patchErr = errors.New("slot disappeared")
	cache, result, written, err := store.Publish(context.Background(), cache, testCard("registry-1", "machine-a", 2, now.Add(time.Minute), 15*time.Minute))
	if err != nil {
		t.Fatalf("fallback publish: %v", err)
	}
	if result.Mode != "append-slot" || !written || cache.SlotMessageID == "" || cache.SlotMessageID == firstSlot {
		t.Fatalf("fallback result=%#v written=%v cache=%#v first=%q", result, written, cache, firstSlot)
	}
	if graph.sendCount != 2 || graph.patchCount != 1 || len(graph.messages) != 2 {
		t.Fatalf("graph counts send=%d patch=%d messages=%d, want 2/1/2", graph.sendCount, graph.patchCount, len(graph.messages))
	}
}

func TestStorePublishDoesNotAppendOnTransientPatchFailure(t *testing.T) {
	graph := newFakeRegistryGraph()
	path := filepath.Join(t.TempDir(), "registry.json")
	store := Store{Graph: graph, CachePath: path}
	cache := Cache{SchemaVersion: 1, RegistryChatID: "chat-registry", RegistryKey: "registry-1"}
	now := time.Unix(1000, 0).UTC()

	cache, _, _, _ = store.Publish(context.Background(), cache, testCard("registry-1", "machine-a", 1, now, 15*time.Minute))
	graph.patchErr = &StatusError{StatusCode: http.StatusTooManyRequests}
	_, _, _, err := store.Publish(context.Background(), cache, testCard("registry-1", "machine-a", 2, now.Add(time.Minute), 15*time.Minute))
	if err == nil {
		t.Fatal("publish succeeded on transient patch failure")
	}
	if graph.sendCount != 1 || len(graph.messages) != 1 {
		t.Fatalf("transient patch failure appended a replacement: send=%d messages=%d", graph.sendCount, len(graph.messages))
	}
}

func TestStorePublishRotatesSlotWhenDue(t *testing.T) {
	graph := newFakeRegistryGraph()
	now := time.Unix(1000, 0).UTC()
	store := Store{
		Graph:        graph,
		SlotRotation: time.Hour,
		Now:          func() time.Time { return now.Add(2 * time.Hour) },
	}
	cache := Cache{
		SchemaVersion:      1,
		RegistryChatID:     "chat-registry",
		RegistryKey:        "registry-1",
		SlotMessageID:      "slot-old",
		SlotCreatedAt:      now,
		NextSlotRotationAt: now.Add(time.Hour),
	}
	graph.addMessage("slot-old", RenderCardHTML(testCard("registry-1", "machine-a", 1, now, 15*time.Minute)))
	next, result, written, err := store.Publish(context.Background(), cache, testCard("registry-1", "machine-a", 2, now.Add(2*time.Hour), 15*time.Minute))
	if err != nil {
		t.Fatalf("Publish rotate: %v", err)
	}
	if result.Mode != "append-slot" || written || next.SlotMessageID == "slot-old" {
		t.Fatalf("rotate result=%#v written=%v cache=%#v", result, written, next)
	}
}

func TestFakeGraphIntegrationTwoMachinesStaleBoundary(t *testing.T) {
	graph := newFakeRegistryGraph()
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	store := Store{Graph: graph}
	cacheA := Cache{RegistryChatID: "chat-registry", RegistryKey: "registry-1"}
	cacheB := Cache{RegistryChatID: "chat-registry", RegistryKey: "registry-1"}

	var err error
	cacheA, _, _, err = store.Publish(context.Background(), cacheA, testCard("registry-1", "machine-a", 1, now, 15*time.Minute))
	if err != nil {
		t.Fatalf("publish a: %v", err)
	}
	cacheB, _, _, err = store.Publish(context.Background(), cacheB, testCard("registry-1", "machine-b", 1, now, 15*time.Minute))
	if err != nil {
		t.Fatalf("publish b: %v", err)
	}
	statuses, err := store.Observe(context.Background(), cacheA, 50, now.Add(5*time.Minute))
	if err != nil {
		t.Fatalf("observe online: %v", err)
	}
	online, stale := SplitStatuses(statuses)
	if len(online) != 2 || len(stale) != 0 {
		t.Fatalf("initial statuses online=%#v stale=%#v", online, stale)
	}

	cacheA, _, _, err = store.Publish(context.Background(), cacheA, testCard("registry-1", "machine-a", 2, now.Add(20*time.Minute), 15*time.Minute))
	if err != nil {
		t.Fatalf("republish a: %v", err)
	}
	statuses, err = store.Observe(context.Background(), cacheA, 50, now.Add(25*time.Minute))
	if err != nil {
		t.Fatalf("observe stale: %v", err)
	}
	online, stale = SplitStatuses(statuses)
	if len(online) != 1 || online[0].MachineID != "machine-a" || len(stale) != 1 || stale[0].MachineID != "machine-b" {
		t.Fatalf("final statuses online=%#v stale=%#v", online, stale)
	}
}

func TestObserveStressLargeRegistryWindow(t *testing.T) {
	graph := newFakeRegistryGraph()
	now := time.Unix(5000, 0).UTC()
	for i := 0; i < 50; i++ {
		card := testCard("registry-1", fmt.Sprintf("machine-%02d", i), i+1, now.Add(-time.Duration(i)*time.Second), 15*time.Minute)
		graph.addMessage(fmt.Sprintf("msg-%02d", i), RenderCardHTML(card))
	}
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		statuses, err := Observe(ctx, graph, "chat-registry", "registry-1", 50, now)
		if err != nil {
			t.Fatalf("observe iteration %d: %v", i, err)
		}
		if len(statuses) != 50 {
			t.Fatalf("observe iteration %d statuses = %d, want 50", i, len(statuses))
		}
	}
	if graph.windowListCount != 1000 {
		t.Fatalf("window list count = %d, want 1000", graph.windowListCount)
	}
}

func BenchmarkHeartbeatSlotPublisher(b *testing.B) {
	graph := newFakeRegistryGraph()
	store := Store{Graph: graph}
	cache := Cache{RegistryChatID: "chat-registry", RegistryKey: "registry-1"}
	now := time.Unix(1000, 0).UTC()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		cache, _, _, err = store.Publish(ctx, cache, testCard("registry-1", "machine-a", i+1, now.Add(time.Duration(i)*time.Second), 15*time.Minute))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkObserveMessages50Cards(b *testing.B) {
	now := time.Unix(5000, 0).UTC()
	messages := make([]ChatMessage, 0, 50)
	for i := 0; i < 50; i++ {
		card := testCard("registry-1", fmt.Sprintf("machine-%02d", i), i+1, now.Add(-time.Duration(i)*time.Second), 15*time.Minute)
		messages = append(messages, messageWithHTML(fmt.Sprintf("msg-%02d", i), RenderCardHTML(card)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := ObserveMessages(messages, "registry-1", now); len(got) != 50 {
			b.Fatalf("statuses = %d, want 50", len(got))
		}
	}
}

func testCard(registryKey string, machineID string, seq int, published time.Time, ttl time.Duration) MachineCard {
	capabilities := []string{"docker", "teams-registry"}
	return MachineCard{
		Kind:                     CardKind,
		RegistryKey:              registryKey,
		MachineID:                machineID,
		InstanceID:               "instance-" + machineID,
		MachineLabel:             machineID,
		HostLabel:                machineID + ".local",
		Aliases:                  []string{"B"},
		HelperProfile:            "default",
		CXPVersion:               "v-test",
		Platform:                 MachinePlatform{OS: "linux", Arch: "amd64"},
		Capabilities:             capabilities,
		CapabilityFingerprint:    CapabilityFingerprint(capabilities),
		ProtocolVersions:         []string{"cxp-delegation-v1"},
		Workspaces:               []WorkspaceRef{{Kind: "git", Name: "codex-helper", RootHash: "root-hash"}},
		Skills:                   []string{"cxp"},
		ModelProfiles:            []string{"gpt-5.5-medium"},
		Load:                     MachineLoad{RunningTurns: 0, QueuedTurns: 0},
		Accepting:                true,
		Revision:                 seq,
		Sequence:                 seq,
		HeartbeatIntervalSeconds: int(DefaultHeartbeatInterval.Seconds()),
		TTLSeconds:               int(ttl.Seconds()),
		PublishedAt:              published.UTC().Format(time.RFC3339Nano),
		ExpiresAt:                published.Add(ttl).UTC().Format(time.RFC3339Nano),
	}
}

func messageWithHTML(id string, content string) ChatMessage {
	msg := ChatMessage{ID: id}
	msg.Body.Content = content
	return msg
}

type fakeRegistryGraph struct {
	messages map[string]ChatMessage
	order    []string
	nextID   int

	sendCount       int
	patchCount      int
	listCount       int
	windowListCount int
	patchErr        error

	createCalls      int
	createSubject    string
	createExternalID string
	createStart      time.Time
	createEnd        time.Time

	getCalls int
	getErr   error

	refreshCalls int
	refreshStart time.Time
	refreshEnd   time.Time
}

func newFakeRegistryGraph() *fakeRegistryGraph {
	return &fakeRegistryGraph{messages: map[string]ChatMessage{}}
}

func (g *fakeRegistryGraph) CreateOrGetMeetingChatWindow(_ context.Context, topic string, externalID string, start time.Time, end time.Time) (Chat, OnlineMeeting, error) {
	g.createCalls++
	g.createSubject = topic
	g.createExternalID = externalID
	g.createStart = start
	g.createEnd = end
	chatID := "chat-registry"
	meetingID := "meeting-registry"
	if strings.HasPrefix(externalID, "cxp-inbox-") {
		chatID = "chat-inbox"
		meetingID = "meeting-inbox"
	}
	meeting := OnlineMeeting{
		ID:            meetingID,
		Subject:       topic,
		JoinWebURL:    "https://teams.example/join",
		StartDateTime: start.UTC().Format(time.RFC3339),
		EndDateTime:   end.UTC().Format(time.RFC3339),
		ChatThreadID:  chatID,
	}
	return Chat{ID: chatID, Topic: topic, ChatType: "meeting", WebURL: "https://teams.example/" + chatID}, meeting, nil
}

func (g *fakeRegistryGraph) GetOnlineMeeting(_ context.Context, meetingID string) (OnlineMeeting, error) {
	g.getCalls++
	if g.getErr != nil {
		return OnlineMeeting{}, g.getErr
	}
	chatID := "chat-registry"
	if strings.Contains(meetingID, "inbox") {
		chatID = "chat-inbox"
	}
	return OnlineMeeting{ID: meetingID, Subject: "CXP Registry", ChatThreadID: chatID}, nil
}

func (g *fakeRegistryGraph) UpdateOnlineMeetingWindow(_ context.Context, meetingID string, start time.Time, end time.Time) (OnlineMeeting, error) {
	g.refreshCalls++
	g.refreshStart = start
	g.refreshEnd = end
	meeting := OnlineMeeting{
		ID:            meetingID,
		Subject:       "CXP Registry",
		StartDateTime: start.UTC().Format(time.RFC3339),
		EndDateTime:   end.UTC().Format(time.RFC3339),
		ChatThreadID:  "chat-registry",
	}
	return meeting, nil
}

func (g *fakeRegistryGraph) SendHTML(_ context.Context, _ string, content string) (ChatMessage, error) {
	g.sendCount++
	g.nextID++
	id := fmt.Sprintf("message-%06d", g.nextID)
	return g.addMessage(id, content), nil
}

func (g *fakeRegistryGraph) UpdateChatMessageHTML(_ context.Context, _ string, messageID string, content string) error {
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

func (g *fakeRegistryGraph) ListMessages(_ context.Context, _ string, top int) ([]ChatMessage, error) {
	g.listCount++
	if top <= 0 || top > len(g.order) {
		top = len(g.order)
	}
	out := make([]ChatMessage, 0, top)
	for i := 0; i < top; i++ {
		out = append(out, g.messages[g.order[i]])
	}
	return out, nil
}

func (g *fakeRegistryGraph) ListMessagesWindow(_ context.Context, _ string, top int) (MessageWindow, error) {
	return g.listMessagesWindowFromOffset(top, 0), nil
}

func (g *fakeRegistryGraph) ListMessagesWindowFromPath(_ context.Context, path string) (MessageWindow, error) {
	var top, offset int
	if _, err := fmt.Sscanf(path, "fake-window:%d:%d", &top, &offset); err != nil {
		return MessageWindow{}, err
	}
	return g.listMessagesWindowFromOffset(top, offset), nil
}

func (g *fakeRegistryGraph) listMessagesWindowFromOffset(top int, offset int) MessageWindow {
	g.windowListCount++
	if top <= 0 {
		top = len(g.order)
	}
	if offset < 0 {
		offset = 0
	}
	end := offset + top
	if end > len(g.order) {
		end = len(g.order)
	}
	out := make([]ChatMessage, 0, end-offset)
	for i := offset; i < end; i++ {
		out = append(out, g.messages[g.order[i]])
	}
	window := MessageWindow{Messages: out}
	if end < len(g.order) {
		window.Truncated = true
		window.NextPath = fmt.Sprintf("fake-window:%d:%d", top, end)
	}
	return window
}

func (g *fakeRegistryGraph) addMessage(id string, content string) ChatMessage {
	msg := messageWithHTML(id, content)
	g.messages[id] = msg
	g.order = append([]string{id}, g.order...)
	return msg
}
