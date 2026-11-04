package delegate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/teams/machineregistry"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// meetings is a MeetingChannel: meeting chats by key, messages by chat.
type meetings struct {
	mu       sync.Mutex
	byKey    map[string]chteams.Meeting
	msgs     map[string]map[string]string // chat -> id -> html
	seq      int
	sends    int
	edits    int
	creates  map[string]int // key -> createOrGet calls
	renews   int
	editErr  error
	sendErr  error
	chatOver map[string]string // key -> chat id returned instead
}

func newMeetings() *meetings {
	return &meetings{byKey: map[string]chteams.Meeting{}, msgs: map[string]map[string]string{}, creates: map[string]int{}, chatOver: map[string]string{}}
}

func (m *meetings) Owner(context.Context) (string, string, error) { return "owner-1", "Owner", nil }

func (m *meetings) CreateMeetingChat(_ context.Context, title, key string, start, end time.Time) (channel.ChatInfo, chteams.Meeting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creates[key]++
	mt, ok := m.byKey[key]
	if !ok {
		m.seq++
		mt = chteams.Meeting{ID: fmt.Sprintf("meeting-%d", m.seq), ChatID: fmt.Sprintf("19:chat-%d", m.seq), Subject: title, Start: start, End: end}
		if c := m.chatOver[key]; c != "" {
			mt.ChatID = c
		}
		m.byKey[key] = mt
	}
	return channel.ChatInfo{ID: mt.ChatID, Title: mt.Subject}, mt, nil
}

func (m *meetings) GetMeeting(_ context.Context, id string) (chteams.Meeting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, mt := range m.byKey {
		if mt.ID == id {
			return mt, nil
		}
	}
	return chteams.Meeting{}, &channel.Rejected{Permanent: true, Err: channel.ErrNotFound}
}

func (m *meetings) RenewMeeting(_ context.Context, id string, start, end time.Time) (chteams.Meeting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, mt := range m.byKey {
		if mt.ID == id {
			mt.Start, mt.End = start, end
			m.byKey[k] = mt
			m.renews++
			return mt, nil
		}
	}
	return chteams.Meeting{}, &channel.Rejected{Permanent: true, Err: channel.ErrNotFound}
}

func (m *meetings) Send(_ context.Context, chat string, p channel.Part, _ string) (channel.Sent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendErr != nil {
		return channel.Sent{}, m.sendErr
	}
	m.seq++
	id := fmt.Sprintf("msg-%d", m.seq)
	if m.msgs[chat] == nil {
		m.msgs[chat] = map[string]string{}
	}
	m.msgs[chat][id] = p.Body
	m.sends++
	return channel.Sent{ID: id}, nil
}

func (m *meetings) Edit(_ context.Context, chat, id string, p channel.Part, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.editErr != nil {
		return m.editErr
	}
	if _, ok := m.msgs[chat][id]; !ok {
		return &channel.Rejected{Permanent: true, Err: channel.ErrNotFound}
	}
	m.msgs[chat][id] = p.Body
	m.edits++
	return nil
}

// cards decodes every card message of the registry chat.
func (m *meetings) cards(t *testing.T, registryChat string) []machineregistry.MachineCard {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []machineregistry.MachineCard
	for _, body := range m.msgs[registryChat] {
		html := partHTML(t, body)
		c, ok := machineregistry.ParseCardMessage(html)
		if !ok {
			t.Fatalf("not a card: %s", html)
		}
		out = append(out, c)
	}
	return out
}

func partHTML(t *testing.T, body string) string {
	t.Helper()
	var v struct {
		Body struct{ Content string } `json:"body"`
	}
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatal(err)
	}
	return v.Body.Content
}

const (
	testTenant  = "tenant-1"
	testMachine = "machine:0011223344556677"
)

func rig(t *testing.T) (*meetings, *Registry, engine.RegistryCard, *time.Time) {
	t.Helper()
	ch := newMeetings()
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	reg := NewRegistry(RegistryConfig{Channel: ch, TenantID: testTenant, MachineID: testMachine, Label: "build-box",
		Host: "build-box", Version: "v1.2.3", CachePath: filepath.Join(t.TempDir(), "machine-registry.json"),
		Now: func() time.Time { return now }})
	// The engine created the inbox chat with the inbox key and a 24 h window.
	key := InboxKey(testTenant, "owner-1", testMachine)
	info, _, _ := ch.CreateMeetingChat(context.Background(), "CXP Machine Inbox build-box", key, now, now.Add(24*time.Hour))
	ch.creates[key] = 0
	return ch, reg, engine.RegistryCard{InboxChat: info.ID, InboxTitle: "CXP Machine Inbox build-box"}, &now
}

func registryChat(ch *meetings) string {
	return ch.byKey[machineregistry.ExternalID(machineregistry.RegistryKey(testTenant, "owner-1"))].ChatID
}

// The card carries what `cxp delegate resolve` reads: the machine id, the
// inbox key that opens the engine's inbox chat, and whether it accepts
// work. The next heartbeat edits the same message; the inbox meeting is
// kept open for the requesters.
func TestRegistryPublishesCard(t *testing.T) {
	ch, reg, card, now := rig(t)
	ctx := context.Background()
	if err := reg.Publish(ctx, card); err != nil {
		t.Fatal(err)
	}
	cards := ch.cards(t, registryChat(ch))
	if len(cards) != 1 {
		t.Fatalf("cards = %d", len(cards))
	}
	c := cards[0]
	key := InboxKey(testTenant, "owner-1", testMachine)
	if c.MachineID != testMachine || c.InboxRef != key || !c.Accepting || c.Draining || c.MachineLabel != "build-box" ||
		c.CXPVersion != "v1.2.3" || c.InboxGeneration == "" || c.RegistryKey != machineregistry.RegistryKey(testTenant, "owner-1") {
		t.Fatalf("card = %+v", c)
	}
	st := machineregistry.ObserveMessages([]machineregistry.ChatMessage{{ID: "x", Body: machineregistry.ChatMessageBody{Content: machineregistry.RenderCardHTML(c)}}}, c.RegistryKey, *now)
	if len(st) != 1 || st[0].State != "online" || !st[0].Accepting {
		t.Fatalf("observed = %+v", st)
	}
	if end := ch.byKey[key].End; end.Before(now.Add(30 * 24 * time.Hour)) {
		t.Errorf("inbox meeting window not renewed: ends %s", end)
	}

	*now = now.Add(5 * time.Minute)
	if err := reg.Publish(ctx, card); err != nil {
		t.Fatal(err)
	}
	if ch.sends != 1 || ch.edits != 1 {
		t.Errorf("second heartbeat: sends %d edits %d, want 1 and 1", ch.sends, ch.edits)
	}
	if ch.creates[key] != 1 {
		t.Errorf("inbox opened %d times, want once (then cached)", ch.creates[key])
	}

	card.Draining = true
	if err := reg.Publish(ctx, card); err != nil {
		t.Fatal(err)
	}
	c = ch.cards(t, registryChat(ch))[0]
	if c.Accepting || !c.Draining {
		t.Errorf("draining card = accepting %v draining %v", c.Accepting, c.Draining)
	}
}

// A card message that disappeared is posted again; a throttled write keeps
// its Retry-After for the engine.
func TestRegistryRepairsAndReportsThrottling(t *testing.T) {
	ch, reg, card, _ := rig(t)
	ctx := context.Background()
	if err := reg.Publish(ctx, card); err != nil {
		t.Fatal(err)
	}
	rc := registryChat(ch)
	for id := range ch.msgs[rc] {
		delete(ch.msgs[rc], id)
	}
	if err := reg.Publish(ctx, card); err != nil {
		t.Fatal(err)
	}
	if len(ch.cards(t, rc)) != 1 || ch.sends != 2 {
		t.Errorf("lost card not reposted: cards %d sends %d", len(ch.cards(t, rc)), ch.sends)
	}
	ch.editErr = &channel.Throttled{RetryAfter: 42 * time.Second, Err: errors.New("429")}
	err := reg.Publish(ctx, card)
	var th *channel.Throttled
	if !errors.As(err, &th) || th.RetryAfter != 42*time.Second {
		t.Fatalf("throttled publish = %v", err)
	}
}

// A card must never name an inbox other than the engine's.
func TestRegistryRefusesForeignInbox(t *testing.T) {
	ch, reg, card, _ := rig(t)
	card.InboxChat = "19:other"
	if err := reg.Publish(context.Background(), card); err == nil || !strings.Contains(err.Error(), "inbox") {
		t.Fatalf("publish = %v", err)
	}
	if ch.sends != 0 {
		t.Errorf("a card was posted")
	}
}

// The machine id is the old core's for a migrated store, is kept once
// derived, and the environment overrides it.
func TestResolveMachineID(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_MACHINE_ID", "")
	t.Setenv("CODEX_HELPER_TEAMS_MACHINE_KIND", "")
	open := func() *store.Store {
		st, err := store.Open(filepath.Join(t.TempDir(), "core.db"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Release(); st.Close() })
		return st
	}

	st := open()
	if err := st.Update(func(tx *store.Tx) error { return tx.SetMeta("scope_id", "scope:abc") }); err != nil {
		t.Fatal(err)
	}
	got, err := ResolveMachineID(st, testTenant, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	want := teams.MachineRecordForUser(teams.User{ID: "owner-1"}, teamstore.ScopeIdentity{ID: "scope:abc"}).ID
	if got != want {
		t.Fatalf("migrated id = %s, want the old core's %s", got, want)
	}
	t.Setenv("USER", "someone-else")
	if again, _ := ResolveMachineID(st, testTenant, "owner-1"); again != got {
		t.Errorf("kept id changed: %s -> %s", got, again)
	}
	t.Setenv("CODEX_HELPER_TEAMS_MACHINE_ID", "machine:override")
	if v, _ := ResolveMachineID(st, testTenant, "owner-1"); v != "machine:override" {
		t.Errorf("override = %s", v)
	}
	t.Setenv("CODEX_HELPER_TEAMS_MACHINE_ID", "")

	fresh := open()
	a, err := ResolveMachineID(fresh, testTenant, "owner-1")
	if err != nil || !strings.HasPrefix(a, "machine:") || a == got {
		t.Fatalf("fresh id = %q, %v", a, err)
	}
	if v, ok, _ := fresh.Meta(MetaMachineID); !ok || v != a {
		t.Errorf("fresh id not kept: %q", v)
	}
}
