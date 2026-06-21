package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/teams/delegation"
	"github.com/baaaaaaaka/codex-helper/internal/teams/machineregistry"
)

func TestDelegateResolveReturnsSingleStartCandidate(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	candidatesPath := writeJSONFile(t, "candidates.json", []delegation.Candidate{
		{MachineID: "machine-b", Label: "Windows GPU", Aliases: []string{"B", "win-gpu"}, State: "online", Accepting: true},
		{MachineID: "machine-c", Label: "Linux CPU", Aliases: []string{"C"}, State: "stale", Accepting: true},
	})
	result, err := runDelegateResolve(&delegateOptions{
		query:         "让 B 这台 Windows GPU 看一下",
		candidateFile: candidatesPath,
		now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("runDelegateResolve: %v", err)
	}
	if result.Action != delegation.ActionStart || result.CandidateToken == "" {
		t.Fatalf("resolve result = %#v, want start with token", result)
	}
	payload, err := delegation.DecodeCandidateToken(result.CandidateToken, now)
	if err != nil {
		t.Fatalf("candidate token invalid: %v", err)
	}
	if payload.MachineID != "machine-b" {
		t.Fatalf("payload = %#v, want machine-b", payload)
	}
}

func TestDelegateResolveAsksWhenMultipleMachinesMatch(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	candidatesPath := writeJSONFile(t, "candidates.json", []delegation.Candidate{
		{MachineID: "machine-b", Label: "GPU A", State: "online", Accepting: true, Confidence: 0.9},
		{MachineID: "machine-c", Label: "GPU B", State: "online", Accepting: true, Confidence: 0.9},
	})
	result, err := runDelegateResolve(&delegateOptions{
		query:         "找一台 GPU 机器",
		candidateFile: candidatesPath,
		now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("runDelegateResolve: %v", err)
	}
	if result.Action != delegation.ActionAskUser {
		t.Fatalf("resolve action = %q, want ask_user", result.Action)
	}
}

func TestDelegateResolveDefaultsToInjectedRegistryCandidateLoader(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	called := false
	result, err := runDelegateResolve(&delegateOptions{
		query: "ask B",
		now:   func() time.Time { return now },
		loadCandidates: func(opts *delegateOptions) ([]delegation.Candidate, error) {
			called = true
			if opts.query != "ask B" {
				t.Fatalf("query = %q", opts.query)
			}
			return []delegation.Candidate{{
				MachineID:       "machine-b",
				Label:           "B",
				Aliases:         []string{"B"},
				State:           "online",
				Accepting:       true,
				InboxRef:        "inbox-ref-b",
				InboxGeneration: "gen-b",
				Confidence:      0.95,
			}}, nil
		},
	})
	if err != nil {
		t.Fatalf("runDelegateResolve: %v", err)
	}
	if !called {
		t.Fatal("default candidate loader was not called")
	}
	if result.Action != delegation.ActionStart || result.CandidateToken == "" {
		t.Fatalf("result = %#v, want start with token", result)
	}
	payload, err := delegation.DecodeCandidateToken(result.CandidateToken, now)
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
	if payload.InboxRef != "inbox-ref-b" || payload.InboxGeneration != "gen-b" {
		t.Fatalf("payload = %#v, want inbox locator", payload)
	}
}

func TestDelegateResolveReturnsRemoteThreadReuseCandidates(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	routeStorePath := filepath.Join(t.TempDir(), "routes.json")
	store := delegation.Store{}
	store.UpsertRemoteThread(delegation.RemoteThread{
		ThreadID:             "rth-existing",
		MachineID:            "machine-b",
		Title:                "Remote log investigation",
		Summary:              "Previous turn inspected Windows logs.",
		LastResultSummary:    "Need to continue from error code 42.",
		WorkspaceFingerprint: "workspace-1",
		SourceSessionID:      "session-a",
		State:                delegation.RemoteThreadStateIdle,
		LastUsedAt:           now.Add(-time.Minute).Format(time.RFC3339Nano),
		UpdatedAt:            now.Add(-time.Minute).Format(time.RFC3339Nano),
		ExpiresAt:            now.Add(time.Hour).Format(time.RFC3339Nano),
		Generation:           "thread-gen",
	})
	if _, err := delegation.SaveStore(routeStorePath, store); err != nil {
		t.Fatalf("SaveStore: %v", err)
	}
	candidatesPath := writeJSONFile(t, "candidates.json", []delegation.Candidate{{
		MachineID:       "machine-b",
		Label:           "Windows GPU",
		State:           "online",
		Accepting:       true,
		Confidence:      0.95,
		InboxRef:        "inbox-ref-b",
		InboxGeneration: "gen-b",
	}})
	result, err := runDelegateResolve(&delegateOptions{
		query:                "让 Windows GPU 继续看之前的日志问题",
		candidateFile:        candidatesPath,
		sourceSession:        "session-a",
		workspaceFingerprint: "workspace-1",
		routeStorePath:       routeStorePath,
		now:                  func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("runDelegateResolve: %v", err)
	}
	if result.Action != delegation.ActionStart || result.NewThreadToken == "" || len(result.ThreadCandidates) != 1 {
		t.Fatalf("result = %#v, want start with new and reuse thread tokens", result)
	}
	newPayload, err := delegation.DecodeThreadToken(result.NewThreadToken, now)
	if err != nil {
		t.Fatalf("decode new thread token: %v", err)
	}
	if newPayload.Policy != delegation.ThreadPolicyNew || newPayload.InboxRef != "inbox-ref-b" || newPayload.InboxGeneration != "gen-b" {
		t.Fatalf("new payload = %#v", newPayload)
	}
	reuse := result.ThreadCandidates[0]
	if reuse.ThreadID != "rth-existing" || reuse.ThreadToken == "" || reuse.ReuseConfidence <= 0.8 {
		t.Fatalf("reuse candidate = %#v", reuse)
	}
	reusePayload, err := delegation.DecodeThreadToken(reuse.ThreadToken, now)
	if err != nil {
		t.Fatalf("decode reuse token: %v", err)
	}
	if reusePayload.Policy != delegation.ThreadPolicyReuse || reusePayload.ThreadID != "rth-existing" || reusePayload.ThreadGeneration != "thread-gen" {
		t.Fatalf("reuse payload = %#v", reusePayload)
	}
}

func TestDelegateResolveFiltersUnavailableRemoteThreadCandidates(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	routeStorePath := filepath.Join(t.TempDir(), "routes.json")
	store := delegation.Store{}
	for _, thread := range []delegation.RemoteThread{
		{
			ThreadID:             "rth-idle",
			MachineID:            "machine-b",
			Title:                "idle",
			WorkspaceFingerprint: "workspace-1",
			SourceSessionID:      "session-a",
			State:                delegation.RemoteThreadStateIdle,
			LastUsedAt:           now.Add(-time.Minute).Format(time.RFC3339Nano),
			ExpiresAt:            now.Add(time.Hour).Format(time.RFC3339Nano),
			Generation:           "gen-idle",
		},
		{
			ThreadID:           "rth-active",
			MachineID:          "machine-b",
			State:              delegation.RemoteThreadStateActive,
			ActiveDelegationID: "del-active",
			LastUsedAt:         now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
			ExpiresAt:          now.Add(time.Hour).Format(time.RFC3339Nano),
			Generation:         "gen-active",
		},
		{
			ThreadID:   "rth-stale",
			MachineID:  "machine-b",
			State:      delegation.RemoteThreadStateStale,
			LastUsedAt: now.Add(-3 * time.Minute).Format(time.RFC3339Nano),
			ExpiresAt:  now.Add(time.Hour).Format(time.RFC3339Nano),
			Generation: "gen-stale",
		},
		{
			ThreadID:   "rth-closed",
			MachineID:  "machine-b",
			State:      delegation.RemoteThreadStateClosed,
			LastUsedAt: now.Add(-4 * time.Minute).Format(time.RFC3339Nano),
			ExpiresAt:  now.Add(time.Hour).Format(time.RFC3339Nano),
			Generation: "gen-closed",
		},
	} {
		store.UpsertRemoteThread(thread)
	}
	if _, err := delegation.SaveStore(routeStorePath, store); err != nil {
		t.Fatalf("SaveStore: %v", err)
	}
	candidatesPath := writeJSONFile(t, "candidates.json", []delegation.Candidate{{
		MachineID:  "machine-b",
		Label:      "B",
		State:      "online",
		Accepting:  true,
		Confidence: 0.95,
	}})
	result, err := runDelegateResolve(&delegateOptions{
		query:                "ask B",
		candidateFile:        candidatesPath,
		sourceSession:        "session-a",
		workspaceFingerprint: "workspace-1",
		routeStorePath:       routeStorePath,
		now:                  func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("runDelegateResolve: %v", err)
	}
	if len(result.ThreadCandidates) != 1 || result.ThreadCandidates[0].ThreadID != "rth-idle" {
		t.Fatalf("thread candidates = %#v, want only idle reusable thread", result.ThreadCandidates)
	}
}

func TestCandidatesFromMachineStatusesPreservesAvailabilitySignals(t *testing.T) {
	candidates := candidatesFromMachineStatuses([]machineregistry.MachineStatus{
		{
			MachineID:             "machine-a",
			InstanceID:            "instance-a",
			MachineLabel:          "A",
			HostLabel:             "host-a",
			Aliases:               []string{"gpu-a"},
			State:                 "online",
			Accepting:             true,
			InboxRef:              "inbox-a",
			InboxGeneration:       "gen-a",
			Revision:              17,
			CapabilityFingerprint: "cap-fp",
			Capabilities:          []string{"gpu"},
			Skills:                []string{"cxp"},
			ProtocolVersions:      []string{"cxp-delegation-v1"},
		},
		{MachineID: "machine-b", MachineLabel: "B", State: "online", Accepting: true, Draining: true},
		{MachineID: "machine-c", MachineLabel: "C", State: "stale", Accepting: true},
	})
	if len(candidates) != 3 {
		t.Fatalf("candidates = %#v", candidates)
	}
	if !candidates[0].Accepting || len(candidates[0].NotStartableReasons) != 0 || candidates[0].Aliases[0] != "gpu-a" {
		t.Fatalf("online accepting candidate = %#v", candidates[0])
	}
	if candidates[0].InboxRef != "inbox-a" || candidates[0].InboxGeneration != "gen-a" {
		t.Fatalf("online accepting candidate lost inbox locator: %#v", candidates[0])
	}
	if candidates[0].InstanceID != "instance-a" || candidates[0].HostLabel != "host-a" || candidates[0].CardRevision != 17 ||
		candidates[0].CapabilityFingerprint != "cap-fp" || len(candidates[0].Capabilities) != 1 || candidates[0].Capabilities[0] != "gpu" ||
		len(candidates[0].Skills) != 1 || candidates[0].Skills[0] != "cxp" ||
		len(candidates[0].ProtocolVersions) != 1 || candidates[0].ProtocolVersions[0] != "cxp-delegation-v1" {
		t.Fatalf("online accepting candidate lost metadata: %#v", candidates[0])
	}
	if candidates[1].Accepting || !strings.Contains(strings.Join(candidates[1].NotStartableReasons, " "), "not accepting") {
		t.Fatalf("draining candidate = %#v, want not accepting", candidates[1])
	}
	if candidates[2].Accepting == false || !strings.Contains(strings.Join(candidates[2].NotStartableReasons, " "), "not online") {
		t.Fatalf("stale candidate = %#v, want not online reason while preserving accepting flag", candidates[2])
	}
}

func TestDelegateResolveMatchesCapabilitiesAndBindsCandidateMetadata(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	fingerprint := machineregistry.CapabilityFingerprint([]string{"windows", "gpu"})
	candidatesPath := writeJSONFile(t, "candidates.json", []delegation.Candidate{{
		MachineID:             "machine-b",
		Label:                 "Remote workstation",
		HostLabel:             "buildbox",
		State:                 "online",
		Accepting:             true,
		InboxRef:              "inbox-ref-b",
		InboxGeneration:       "gen-b",
		RegistryGeneration:    "registry-gen",
		CardRevision:          11,
		CapabilityFingerprint: fingerprint,
		Capabilities:          []string{"windows", "gpu"},
		Skills:                []string{"cxp"},
		ProtocolVersions:      []string{"cxp-delegation-v1"},
	}})
	result, err := runDelegateResolve(&delegateOptions{
		query:         "让有 gpu capability 的机器帮我看一下",
		candidateFile: candidatesPath,
		now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("runDelegateResolve: %v", err)
	}
	if result.Action != delegation.ActionStart || result.CandidateToken == "" {
		t.Fatalf("result = %#v, want capability match start", result)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].Confidence < 0.8 || !strings.Contains(strings.Join(result.Candidates[0].MatchedReasons, " "), "capability") {
		t.Fatalf("candidates = %#v, want capability-scored match", result.Candidates)
	}
	payload, err := delegation.DecodeCandidateToken(result.CandidateToken, now)
	if err != nil {
		t.Fatalf("DecodeCandidateToken: %v", err)
	}
	if payload.InboxGeneration != "gen-b" || payload.RegistryGeneration != "registry-gen" || payload.CardRevision != 11 ||
		payload.CapabilityFingerprint != fingerprint || len(payload.ProtocolVersions) != 1 || payload.ProtocolVersions[0] != "cxp-delegation-v1" {
		t.Fatalf("payload = %#v, want candidate metadata binding", payload)
	}
}

func TestDelegateStartIsIdempotentAndCancelIsReduced(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	storePath := filepath.Join(t.TempDir(), "delegation.json")
	taskPath := writeJSONFile(t, "task.json", delegation.TaskSpec{
		Title:          "Check remote logs",
		Objective:      "Inspect machine B logs and return final-or-blocked.",
		AllowedActions: []string{"read-only"},
	})
	token, _, err := delegation.NewCandidateToken("machine-b", now, time.Hour)
	if err != nil {
		t.Fatalf("NewCandidateToken: %v", err)
	}
	opts := &delegateOptions{
		storePath:      storePath,
		candidateToken: token,
		taskFile:       taskPath,
		sourceSession:  "session-a",
		sourceTurn:     "turn-1",
		path:           []string{"machine-a"},
		now:            func() time.Time { return now },
	}
	first, err := runDelegateStart(opts)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	second, err := runDelegateStart(opts)
	if err != nil {
		t.Fatalf("second start: %v", err)
	}
	if first.DelegationID == "" || first.DelegationID != second.DelegationID || second.Idempotent != true {
		t.Fatalf("start results first=%#v second=%#v", first, second)
	}
	store, err := delegation.LoadStore(storePath)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	if len(store.Records) != 1 {
		t.Fatalf("records = %#v, want one idempotent request", store.Records)
	}

	state, err := runDelegateCancel(&delegateOptions{
		storePath:    storePath,
		delegationID: first.DelegationID,
		reason:       "user changed plan",
		now:          func() time.Time { return now.Add(time.Minute) },
	})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if state.Status != delegation.StateCanceled {
		t.Fatalf("state = %#v, want canceled", state)
	}
}

func TestDelegateStartBindsNewThreadTokenAndTracksRemoteThread(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	storePath := filepath.Join(t.TempDir(), "delegation.json")
	taskPath := writeJSONFile(t, "task.json", delegation.TaskSpec{
		Title:     "Remote logs",
		Objective: "Inspect the remote logs.",
	})
	candidate := delegation.Candidate{MachineID: "machine-b", InboxRef: "inbox-ref-b", InboxGeneration: "gen-b"}
	candidateToken, _, err := delegation.NewCandidateTokenForCandidate(candidate, now, time.Hour)
	if err != nil {
		t.Fatalf("NewCandidateTokenForCandidate: %v", err)
	}
	threadToken, threadPayload, err := delegation.NewThreadTokenForCandidate(candidate, "session-a", "workspace-1", now, time.Hour)
	if err != nil {
		t.Fatalf("NewThreadTokenForCandidate: %v", err)
	}
	result, err := runDelegateStart(&delegateOptions{
		storePath:            storePath,
		candidateToken:       candidateToken,
		newThreadToken:       threadToken,
		taskFile:             taskPath,
		sourceSession:        "session-a",
		sourceTurn:           "turn-1",
		workspaceFingerprint: "workspace-1",
		path:                 []string{"machine-a"},
		now:                  func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("runDelegateStart: %v", err)
	}
	if result.RemoteThreadID != threadPayload.ThreadID || result.ThreadPolicy != delegation.ThreadPolicyNew {
		t.Fatalf("result = %#v, want bound new remote thread", result)
	}
	if result.State.Request == nil || result.State.Request.RemoteThreadID != threadPayload.ThreadID || result.State.Request.ThreadGeneration == "" {
		t.Fatalf("request = %#v, want thread fields", result.State.Request)
	}
	store, err := delegation.LoadStore(storePath)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	thread, ok := store.RemoteThreadForID(threadPayload.ThreadID)
	if !ok || thread.State != delegation.RemoteThreadStateActive || thread.ActiveDelegationID != result.DelegationID ||
		thread.SourceSessionID != "session-a" || thread.WorkspaceFingerprint != "workspace-1" {
		t.Fatalf("thread = %#v ok=%v, want active tracked thread", thread, ok)
	}
}

func TestDelegateStartRejectsInvalidThreadTokens(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	taskPath := writeJSONFile(t, "task.json", delegation.TaskSpec{Objective: "remote work"})
	candidateToken, _, err := delegation.NewCandidateTokenForCandidate(delegation.Candidate{MachineID: "machine-b"}, now, time.Hour)
	if err != nil {
		t.Fatalf("NewCandidateTokenForCandidate: %v", err)
	}
	wrongMachineToken, err := delegation.EncodeThreadToken(delegation.ThreadTokenPayload{
		Policy:     delegation.ThreadPolicyNew,
		ThreadID:   "rth-wrong",
		MachineID:  "machine-c",
		ObservedAt: now.Format(time.RFC3339Nano),
		ValidUntil: now.Add(time.Hour).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("EncodeThreadToken wrong machine: %v", err)
	}
	_, err = runDelegateStart(&delegateOptions{
		storePath:      filepath.Join(t.TempDir(), "delegation.json"),
		candidateToken: candidateToken,
		newThreadToken: wrongMachineToken,
		taskFile:       taskPath,
		now:            func() time.Time { return now },
	})
	if err == nil || !strings.Contains(err.Error(), "does not match candidate") {
		t.Fatalf("wrong machine err = %v, want mismatch", err)
	}

	reuseToken, err := delegation.EncodeThreadToken(delegation.ThreadTokenPayload{
		Policy:           delegation.ThreadPolicyReuse,
		ThreadID:         "rth-missing",
		MachineID:        "machine-b",
		ThreadGeneration: "gen-missing",
		ObservedAt:       now.Format(time.RFC3339Nano),
		ValidUntil:       now.Add(time.Hour).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("EncodeThreadToken reuse: %v", err)
	}
	_, err = runDelegateStart(&delegateOptions{
		storePath:      filepath.Join(t.TempDir(), "delegation.json"),
		candidateToken: candidateToken,
		threadToken:    reuseToken,
		taskFile:       taskPath,
		now:            func() time.Time { return now },
	})
	if err == nil || !strings.Contains(err.Error(), "not known locally") {
		t.Fatalf("missing reuse thread err = %v, want local validation failure", err)
	}

	activeStorePath := filepath.Join(t.TempDir(), "active-routes.json")
	activeStore := delegation.Store{}
	activeStore.UpsertRemoteThread(delegation.RemoteThread{
		ThreadID:           "rth-active",
		MachineID:          "machine-b",
		State:              delegation.RemoteThreadStateActive,
		ActiveDelegationID: "del-active",
		Generation:         "gen-active",
		UpdatedAt:          now.Format(time.RFC3339Nano),
		ExpiresAt:          now.Add(time.Hour).Format(time.RFC3339Nano),
	})
	if _, err := delegation.SaveStore(activeStorePath, activeStore); err != nil {
		t.Fatalf("SaveStore active: %v", err)
	}
	activeToken, err := delegation.EncodeThreadToken(delegation.ThreadTokenPayload{
		Policy:           delegation.ThreadPolicyReuse,
		ThreadID:         "rth-active",
		MachineID:        "machine-b",
		ThreadGeneration: "gen-active",
		ObservedAt:       now.Format(time.RFC3339Nano),
		ValidUntil:       now.Add(time.Hour).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("EncodeThreadToken active: %v", err)
	}
	_, err = runDelegateStart(&delegateOptions{
		storePath:      filepath.Join(t.TempDir(), "delegation.json"),
		routeStorePath: activeStorePath,
		candidateToken: candidateToken,
		threadToken:    activeToken,
		taskFile:       taskPath,
		now:            func() time.Time { return now },
	})
	if err == nil || !strings.Contains(err.Error(), "remote thread is active") {
		t.Fatalf("active reuse thread err = %v, want active rejection", err)
	}
}

func TestDelegateStartDefaultTransportRejectsTokenWithoutInboxRef(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	taskPath := writeJSONFile(t, "task.json", delegation.TaskSpec{Objective: "remote work"})
	token, _, err := delegation.NewCandidateToken("machine-b", now, time.Hour)
	if err != nil {
		t.Fatalf("NewCandidateToken: %v", err)
	}
	_, err = runDelegateStart(&delegateOptions{
		candidateToken: token,
		taskFile:       taskPath,
		now:            func() time.Time { return now },
	})
	if err == nil || !strings.Contains(err.Error(), "missing inbox_ref") {
		t.Fatalf("start err = %v, want missing inbox_ref", err)
	}
}

func TestDelegateInboxBackedLifecycle(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	graph := newFakeDelegateRegistryGraph()
	open := func(opts *delegateOptions) (*delegateRegistrySession, error) {
		return &delegateRegistrySession{
			graph:  graph,
			chatID: "chat-registry",
			close:  func(context.Context) error { return nil },
		}, nil
	}
	taskPath := writeJSONFile(t, "task.json", delegation.TaskSpec{
		Title:     "Remote check",
		Objective: "Run the remote check and return final-or-blocked.",
	})
	token, _, err := delegation.NewCandidateTokenForCandidate(delegation.Candidate{
		MachineID:       "machine-b",
		InboxRef:        "inbox-ref-b",
		InboxGeneration: "gen-b",
	}, now, time.Hour)
	if err != nil {
		t.Fatalf("NewCandidateToken: %v", err)
	}
	routeStorePath := filepath.Join(t.TempDir(), "routes.json")
	startOpts := &delegateOptions{
		candidateToken: token,
		taskFile:       taskPath,
		sourceSession:  "session-a",
		sourceTurn:     "turn-1",
		path:           []string{"machine-a"},
		now:            func() time.Time { return now },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	}
	first, err := runDelegateStart(startOpts)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.Transport != "inbox" || first.Status != delegation.StateOpen || graph.sendCountForChat("chat-inbox-ref-b") != 1 || graph.sendCountForChat("chat-registry") != 0 {
		t.Fatalf("first start = %#v send=%d", first, graph.sendCount)
	}
	second, err := runDelegateStart(startOpts)
	if err != nil {
		t.Fatalf("idempotent start: %v", err)
	}
	if !second.Idempotent || second.DelegationID != first.DelegationID || graph.sendCountForChat("chat-inbox-ref-b") != 1 {
		t.Fatalf("second start = %#v send=%d", second, graph.sendCount)
	}
	status, err := runDelegateStatus(&delegateOptions{
		delegationID:   first.DelegationID,
		now:            func() time.Time { return now },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Status != delegation.StateOpen || status.Request == nil {
		t.Fatalf("status = %#v, want open request", status)
	}

	claim, err := runDelegateClaim(&delegateOptions{
		delegationID:   first.DelegationID,
		machineID:      "machine-b",
		workerID:       "worker-b-1",
		claimEpoch:     1,
		now:            func() time.Time { return now.Add(time.Minute) },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !claim.Winning || !claim.ShouldExecute || claim.RecheckAfterSeconds != int(defaultDelegateClaimRecheckDelay.Seconds()) || claim.Status != delegation.StateClaimed || graph.sendCountForChat("chat-inbox-ref-b") != 2 {
		t.Fatalf("claim = %#v send=%d", claim, graph.sendCount)
	}
	result, err := runDelegateResult(&delegateOptions{
		delegationID:   first.DelegationID,
		machineID:      "machine-b",
		workerID:       "worker-b-1",
		claimID:        claim.Claim.ClaimID,
		claimEpoch:     claim.Claim.ClaimEpoch,
		resultStatus:   delegation.StateComplete,
		body:           "remote result",
		now:            func() time.Time { return now.Add(2 * time.Minute) },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	})
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	if result.Status != delegation.StateComplete || result.State.Terminal == nil || result.State.Terminal.Body != "remote result" {
		t.Fatalf("result = %#v, want complete terminal", result)
	}
}

func TestDelegateClaimLosingRaceDoesNotAdviseExecution(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	graph := newFakeDelegateRegistryGraph()
	open := func(opts *delegateOptions) (*delegateRegistrySession, error) {
		return &delegateRegistrySession{graph: graph, chatID: "chat-registry", close: func(context.Context) error { return nil }}, nil
	}
	req := mustDelegateRequestForCLITest(t, now)
	req.InboxRef = "inbox-ref-b"
	req.InboxGeneration = "gen-b"
	winningClaim, err := delegation.NewClaimRecord(req.DelegationID, "machine-b", "worker-a", 1, now.Add(time.Second))
	if err != nil {
		t.Fatalf("winning claim: %v", err)
	}
	for _, record := range []delegation.Record{req, winningClaim} {
		if _, err := graph.SendHTML(context.Background(), "chat-inbox-ref-b", delegation.RenderRecordHTML(record)); err != nil {
			t.Fatalf("seed %s: %v", record.Kind, err)
		}
	}
	routeStorePath := filepath.Join(t.TempDir(), "routes.json")
	store := delegation.Store{SchemaVersion: 1}
	store.UpsertRoute(delegation.Route{
		DelegationID:    req.DelegationID,
		SourceKey:       req.SourceKey,
		MachineID:       req.MachineID,
		InboxRef:        "inbox-ref-b",
		InboxGeneration: "gen-b",
		UpdatedAt:       now.Format(time.RFC3339Nano),
	})
	if _, err := delegation.SaveStore(routeStorePath, store); err != nil {
		t.Fatalf("save route: %v", err)
	}

	claim, err := runDelegateClaim(&delegateOptions{
		delegationID:   req.DelegationID,
		machineID:      "machine-b",
		workerID:       "worker-b",
		claimEpoch:     1,
		now:            func() time.Time { return now.Add(2 * time.Second) },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claim.Winning || claim.ShouldExecute || claim.RecheckAfterSeconds != 0 || claim.Status != delegation.StateClaimed {
		t.Fatalf("claim = %#v, want losing claimed state without execution advice", claim)
	}
	if !containsString(claim.State.ConflictRecordIDs, claim.Claim.RecordID) {
		t.Fatalf("conflicts = %#v, want losing claim record", claim.State.ConflictRecordIDs)
	}
}

func TestDelegateQuestionPublishesIntermediateStatusRecord(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	graph := newFakeDelegateRegistryGraph()
	open := func(opts *delegateOptions) (*delegateRegistrySession, error) {
		return &delegateRegistrySession{graph: graph, chatID: "chat-registry", close: func(context.Context) error { return nil }}, nil
	}
	req := mustDelegateRequestForCLITest(t, now)
	req.InboxRef = "inbox-ref-b"
	req.InboxGeneration = "gen-b"
	claim, err := delegation.NewClaimRecord(req.DelegationID, "machine-b", "worker-b-1", 1, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := graph.SendHTML(context.Background(), "chat-inbox-ref-b", delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	if _, err := graph.SendHTML(context.Background(), "chat-inbox-ref-b", delegation.RenderRecordHTML(claim)); err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	routeStorePath := filepath.Join(t.TempDir(), "routes.json")
	store := delegation.Store{SchemaVersion: 1}
	store.UpsertRoute(delegation.Route{
		DelegationID:    req.DelegationID,
		SourceKey:       req.SourceKey,
		MachineID:       req.MachineID,
		InboxRef:        "inbox-ref-b",
		InboxGeneration: "gen-b",
		UpdatedAt:       now.Format(time.RFC3339Nano),
	})
	if _, err := delegation.SaveStore(routeStorePath, store); err != nil {
		t.Fatalf("save route: %v", err)
	}
	result, err := runDelegateStatusRecord(&delegateOptions{
		delegationID:   req.DelegationID,
		machineID:      "machine-b",
		workerID:       "worker-b-1",
		claimID:        claim.ClaimID,
		claimEpoch:     claim.ClaimEpoch,
		body:           "Need the log path.",
		now:            func() time.Time { return now.Add(2 * time.Minute) },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	}, delegation.StateQuestion)
	if err != nil {
		t.Fatalf("question: %v", err)
	}
	if result.Status != delegation.StateQuestion || result.Record.Status != delegation.StateQuestion {
		t.Fatalf("result = %#v, want question state", result)
	}
	if len(result.State.StatusRecords) != 1 || result.State.StatusRecords[0].Body != "Need the log path." {
		t.Fatalf("status records = %#v, want question", result.State.StatusRecords)
	}
}

func TestDelegateWaitDefaultReturnsOnQuestion(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	storePath := filepath.Join(t.TempDir(), "delegation.json")
	req := mustDelegateRequestForCLITest(t, now)
	claim, err := delegation.NewClaimRecord(req.DelegationID, "machine-b", "worker-b-1", 1, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	question, err := delegation.NewQuestionRecord(req.DelegationID, claim, "Need the log path.", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("question: %v", err)
	}
	if _, err := delegation.SaveStore(storePath, delegation.Store{Records: []delegation.Record{req, claim, question}}); err != nil {
		t.Fatalf("SaveStore: %v", err)
	}
	state, err := runDelegateWait(&delegateOptions{
		storePath:    storePath,
		delegationID: req.DelegationID,
		waitUntil:    delegateWaitUntilTerminalOrQuestion,
		timeout:      time.Hour,
		now:          func() time.Time { return now.Add(3 * time.Minute) },
	})
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if state.Status != delegation.StateQuestion {
		t.Fatalf("state = %#v, want question", state)
	}
}

func TestDelegateStatusSyncsRemoteThreadFromTerminalUpdate(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	graph := newFakeDelegateRegistryGraph()
	open := func(opts *delegateOptions) (*delegateRegistrySession, error) {
		return &delegateRegistrySession{graph: graph, chatID: "chat-registry", close: func(context.Context) error { return nil }}, nil
	}
	req := mustDelegateRequestForCLITest(t, now)
	req.InboxRef = "inbox-ref-b"
	req.InboxGeneration = "gen-b"
	req.RemoteThreadID = "rth-existing"
	req.ThreadPolicy = delegation.ThreadPolicyReuse
	req.ThreadGeneration = "gen-existing"
	claim, err := delegation.NewClaimRecord(req.DelegationID, "machine-b", "worker-b-1", 1, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	result, err := delegation.NewResultRecord(req.DelegationID, claim, delegation.StateComplete, "fresh result", 1, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	result.ThreadUpdate = &delegation.ThreadUpdate{
		Title:             "Updated remote thread",
		SummaryDelta:      "Looked at the new logs.",
		LastResultSummary: "fresh result",
	}
	for _, record := range []delegation.Record{req, claim, result} {
		if _, err := graph.SendHTML(context.Background(), "chat-inbox-ref-b", delegation.RenderRecordHTML(record)); err != nil {
			t.Fatalf("seed %s: %v", record.Kind, err)
		}
	}
	routeStorePath := filepath.Join(t.TempDir(), "routes.json")
	store := delegation.Store{}
	store.UpsertRoute(delegation.Route{
		DelegationID:     req.DelegationID,
		SourceKey:        req.SourceKey,
		MachineID:        req.MachineID,
		InboxRef:         "inbox-ref-b",
		InboxGeneration:  "gen-b",
		RemoteThreadID:   req.RemoteThreadID,
		ThreadPolicy:     req.ThreadPolicy,
		ThreadGeneration: req.ThreadGeneration,
		UpdatedAt:        now.Format(time.RFC3339Nano),
	})
	store.UpsertRemoteThread(delegation.RemoteThread{
		ThreadID:           req.RemoteThreadID,
		MachineID:          req.MachineID,
		Title:              "Old title",
		Summary:            "Old summary.",
		State:              delegation.RemoteThreadStateActive,
		ActiveDelegationID: req.DelegationID,
		Generation:         req.ThreadGeneration,
		UpdatedAt:          now.Format(time.RFC3339Nano),
		ExpiresAt:          now.Add(time.Hour).Format(time.RFC3339Nano),
	})
	if _, err := delegation.SaveStore(routeStorePath, store); err != nil {
		t.Fatalf("save route store: %v", err)
	}
	state, err := runDelegateStatus(&delegateOptions{
		delegationID:   req.DelegationID,
		now:            func() time.Time { return now.Add(3 * time.Minute) },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if state.Status != delegation.StateComplete {
		t.Fatalf("state = %#v, want complete", state)
	}
	loaded, err := delegation.LoadStore(routeStorePath)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	thread, ok := loaded.RemoteThreadForID(req.RemoteThreadID)
	if !ok || thread.State != delegation.RemoteThreadStateIdle || thread.ActiveDelegationID != "" ||
		thread.Title != "Updated remote thread" || !strings.Contains(thread.Summary, "Looked at the new logs.") ||
		thread.LastResultSummary != "fresh result" {
		t.Fatalf("thread = %#v ok=%v, want synced idle thread summary", thread, ok)
	}
	firstUpdatedAt := thread.UpdatedAt
	firstLastUsedAt := thread.LastUsedAt
	firstTerminalID := thread.LastTerminalRecordID
	if firstTerminalID != result.RecordID {
		t.Fatalf("last terminal id = %q, want %q", firstTerminalID, result.RecordID)
	}
	if _, err := runDelegateStatus(&delegateOptions{
		delegationID:   req.DelegationID,
		now:            func() time.Time { return now.Add(10 * time.Minute) },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	}); err != nil {
		t.Fatalf("second status: %v", err)
	}
	loaded, err = delegation.LoadStore(routeStorePath)
	if err != nil {
		t.Fatalf("LoadStore after second status: %v", err)
	}
	thread, ok = loaded.RemoteThreadForID(req.RemoteThreadID)
	if !ok || thread.UpdatedAt != firstUpdatedAt || thread.LastUsedAt != firstLastUsedAt || thread.LastTerminalRecordID != firstTerminalID {
		t.Fatalf("thread after second status = %#v ok=%v, want unchanged terminal sync", thread, ok)
	}
}

func TestDelegateRouteSQLiteLoadMaterializesLegacyJSON(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	sqlitePath := filepath.Join(dir, "routes.sqlite")
	legacyPath := filepath.Join(dir, "routes.json")
	req := mustDelegateRequestForCLITest(t, now)
	store := delegation.Store{}
	store.UpsertRoute(delegation.Route{
		DelegationID:    req.DelegationID,
		SourceKey:       req.SourceKey,
		MachineID:       req.MachineID,
		InboxRef:        "legacy-inbox",
		InboxGeneration: "legacy-generation",
		CreatedAt:       now.Add(-time.Minute).Format(time.RFC3339Nano),
		UpdatedAt:       now.Format(time.RFC3339Nano),
	})
	if _, err := delegation.SaveStore(legacyPath, store); err != nil {
		t.Fatalf("SaveStore legacy: %v", err)
	}
	route, err := loadDelegateRoute(&delegateOptions{
		delegationID:   req.DelegationID,
		routeStorePath: sqlitePath,
		now:            func() time.Time { return now },
	}, req.DelegationID)
	if err != nil {
		t.Fatalf("loadDelegateRoute: %v", err)
	}
	if route.InboxRef != "legacy-inbox" || route.InboxGeneration != "legacy-generation" {
		t.Fatalf("route = %#v, want legacy route", route)
	}
	if _, err := os.Stat(sqlitePath); err != nil {
		t.Fatalf("sqlite route store was not materialized: %v", err)
	}
}

func TestDelegateInboxStatusReadsPagedResult(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	graph := newFakeDelegateRegistryGraph()
	open := func(opts *delegateOptions) (*delegateRegistrySession, error) {
		return &delegateRegistrySession{graph: graph, chatID: "chat-registry", close: func(context.Context) error { return nil }}, nil
	}
	req := mustDelegateRequestForCLITest(t, now)
	req.InboxRef = "inbox-ref-b"
	req.InboxGeneration = "gen-b"
	claim, err := delegation.NewClaimRecord(req.DelegationID, "machine-b", "worker-b-1", 1, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	result, err := delegation.NewResultRecord(req.DelegationID, claim, delegation.StateComplete, "paged answer", 1, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	for _, record := range []delegation.Record{req, claim, result} {
		if _, err := graph.SendHTML(context.Background(), "chat-inbox-ref-b", delegation.RenderRecordHTML(record)); err != nil {
			t.Fatalf("seed %s: %v", record.Kind, err)
		}
	}
	for i := 0; i < 55; i++ {
		if _, err := graph.SendHTML(context.Background(), "chat-inbox-ref-b", fmt.Sprintf("<p>noise %02d</p>", i)); err != nil {
			t.Fatalf("seed noise %d: %v", i, err)
		}
	}
	routeStorePath := filepath.Join(t.TempDir(), "routes.json")
	store := delegation.Store{SchemaVersion: 1}
	store.UpsertRoute(delegation.Route{
		DelegationID:    req.DelegationID,
		SourceKey:       req.SourceKey,
		MachineID:       req.MachineID,
		InboxRef:        "inbox-ref-b",
		InboxGeneration: "gen-b",
		UpdatedAt:       now.Format(time.RFC3339Nano),
	})
	if _, err := delegation.SaveStore(routeStorePath, store); err != nil {
		t.Fatalf("save route: %v", err)
	}
	state, err := runDelegateStatus(&delegateOptions{
		delegationID:   req.DelegationID,
		now:            func() time.Time { return now.Add(3 * time.Minute) },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if state.Status != delegation.StateComplete || state.Terminal == nil || state.Terminal.Body != "paged answer" {
		t.Fatalf("state = %#v, want complete result from second page", state)
	}
	if graph.windowListCount < 2 {
		t.Fatalf("window list count = %d, want paged read", graph.windowListCount)
	}
}

func TestDelegateRegistryRecordsReadsPagedMessages(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	graph := newFakeDelegateRegistryGraph()
	req := mustDelegateRequestForCLITest(t, now)
	if _, err := graph.SendHTML(context.Background(), "chat-registry", delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	for i := 0; i < 55; i++ {
		if _, err := graph.SendHTML(context.Background(), "chat-registry", fmt.Sprintf("<p>registry noise %02d</p>", i)); err != nil {
			t.Fatalf("seed noise %d: %v", i, err)
		}
	}
	records, err := readDelegateRegistryRecords(&delegateOptions{
		top: 50,
		now: func() time.Time { return now },
	}, &delegateRegistrySession{
		graph:  graph,
		chatID: "chat-registry",
		close:  func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("readDelegateRegistryRecords: %v", err)
	}
	if !containsDelegateRecordID(records, req.RecordID) {
		t.Fatalf("records = %#v, want request from second page", records)
	}
	if graph.windowListCount < 2 {
		t.Fatalf("window list count = %d, want paged registry read", graph.windowListCount)
	}
}

func TestDelegateInboxStartTreatsCommitThenTimeoutAsVisible(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	graph := newFakeDelegateRegistryGraph()
	graph.sendErrAfterCommit = errors.New("timeout after commit")
	open := func(opts *delegateOptions) (*delegateRegistrySession, error) {
		return &delegateRegistrySession{graph: graph, chatID: "chat-registry", close: func(context.Context) error { return nil }}, nil
	}
	taskPath := writeJSONFile(t, "task.json", delegation.TaskSpec{Objective: "remote work"})
	token, _, err := delegation.NewCandidateTokenForCandidate(delegation.Candidate{
		MachineID:       "machine-b",
		InboxRef:        "inbox-ref-b",
		InboxGeneration: "gen-b",
	}, now, time.Hour)
	if err != nil {
		t.Fatalf("NewCandidateTokenForCandidate: %v", err)
	}
	routeStorePath := filepath.Join(t.TempDir(), "routes.json")
	opts := &delegateOptions{
		candidateToken: token,
		taskFile:       taskPath,
		sourceSession:  "session-a",
		sourceTurn:     "turn-1",
		now:            func() time.Time { return now },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	}
	result, err := runDelegateStart(opts)
	if err != nil {
		t.Fatalf("start with commit-then-timeout: %v", err)
	}
	if result.Status != delegation.StateOpen || result.Idempotent {
		t.Fatalf("result = %#v, want visible open non-idempotent start", result)
	}
	store, err := delegation.LoadStore(routeStorePath)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	outbox, ok := store.OutboxForRecordID(result.State.Request.RecordID)
	if !ok || outbox.Status != delegation.OutboxVisible || outbox.Attempts != 1 {
		t.Fatalf("outbox = %#v ok=%v, want visible after one attempted send", outbox, ok)
	}
	second, err := runDelegateStart(opts)
	if err != nil {
		t.Fatalf("idempotent start after visible timeout: %v", err)
	}
	if !second.Idempotent || graph.sendCountForChat("chat-inbox-ref-b") != 1 {
		t.Fatalf("second = %#v send=%d, want no duplicate send", second, graph.sendCountForChat("chat-inbox-ref-b"))
	}
}

func TestDelegateStartRejectsStaleCandidateTokenGeneration(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	machineGraph := newFakeMachineRegistryGraph()
	fingerprint := machineregistry.CapabilityFingerprint([]string{"gpu"})
	card := machineregistry.MachineCard{
		Kind:                  machineregistry.CardKind,
		RegistryKey:           "registry-1",
		MachineID:             "machine-b",
		MachineLabel:          "B",
		Capabilities:          []string{"gpu"},
		CapabilityFingerprint: fingerprint,
		Accepting:             true,
		InboxRef:              "inbox-ref-b",
		InboxGeneration:       "new-gen",
		Revision:              3,
		Sequence:              3,
		TTLSeconds:            int((15 * time.Minute).Seconds()),
		PublishedAt:           now.Format(time.RFC3339Nano),
		ExpiresAt:             now.Add(15 * time.Minute).Format(time.RFC3339Nano),
	}
	if _, err := machineGraph.SendHTML(context.Background(), "chat-registry", machineregistry.RenderCardHTML(card)); err != nil {
		t.Fatalf("seed card: %v", err)
	}
	open := func(opts *delegateOptions) (*delegateRegistrySession, error) {
		return &delegateRegistrySession{
			graph: newFakeDelegateRegistryGraph(),
			store: machineregistry.Store{
				Graph: machineGraph,
				Now:   func() time.Time { return now },
			},
			cache:  machineregistry.Cache{RegistryKey: "registry-1", RegistryChatID: "chat-registry", RegistryGeneration: "registry-gen"},
			chatID: "chat-registry",
			close:  func(context.Context) error { return nil },
		}, nil
	}
	token, _, err := delegation.NewCandidateTokenForCandidate(delegation.Candidate{
		MachineID:             "machine-b",
		InboxRef:              "inbox-ref-b",
		InboxGeneration:       "old-gen",
		RegistryGeneration:    "registry-gen",
		CardRevision:          3,
		CapabilityFingerprint: fingerprint,
	}, now, time.Hour)
	if err != nil {
		t.Fatalf("NewCandidateTokenForCandidate: %v", err)
	}
	taskPath := writeJSONFile(t, "task.json", delegation.TaskSpec{Objective: "remote work"})
	_, err = runDelegateStart(&delegateOptions{
		candidateToken: token,
		taskFile:       taskPath,
		sourceSession:  "session-a",
		sourceTurn:     "turn-1",
		now:            func() time.Time { return now },
		openRegistry:   open,
		routeStorePath: filepath.Join(t.TempDir(), "routes.json"),
	})
	if err == nil || !strings.Contains(err.Error(), "inbox generation changed") {
		t.Fatalf("start err = %v, want stale generation rejection", err)
	}
}

func TestDelegateInboxCancelBeatsLateResult(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	graph := newFakeDelegateRegistryGraph()
	open := func(opts *delegateOptions) (*delegateRegistrySession, error) {
		return &delegateRegistrySession{graph: graph, chatID: "chat-registry", close: func(context.Context) error { return nil }}, nil
	}
	req := mustDelegateRequestForCLITest(t, now)
	req.InboxRef = "inbox-ref-b"
	req.InboxGeneration = "gen-b"
	if _, err := graph.SendHTML(context.Background(), "chat-inbox-ref-b", delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	routeStorePath := filepath.Join(t.TempDir(), "routes.json")
	store := delegation.Store{SchemaVersion: 1}
	store.UpsertRoute(delegation.Route{
		DelegationID:    req.DelegationID,
		SourceKey:       req.SourceKey,
		MachineID:       req.MachineID,
		InboxRef:        "inbox-ref-b",
		InboxGeneration: "gen-b",
		UpdatedAt:       now.Format(time.RFC3339Nano),
	})
	if _, err := delegation.SaveStore(routeStorePath, store); err != nil {
		t.Fatalf("save route: %v", err)
	}
	state, err := runDelegateCancel(&delegateOptions{
		delegationID:   req.DelegationID,
		reason:         "no longer needed",
		now:            func() time.Time { return now.Add(time.Minute) },
		openRegistry:   open,
		routeStorePath: routeStorePath,
	})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if state.Status != delegation.StateCanceled {
		t.Fatalf("state = %#v, want canceled", state)
	}
}

func TestDelegateCommandWiresLifecycleSubcommands(t *testing.T) {
	cmd := newRootCmd()
	delegateCmd, _, err := cmd.Find([]string{"delegate"})
	if err != nil {
		t.Fatalf("find delegate: %v", err)
	}
	var names []string
	for _, sub := range delegateCmd.Commands() {
		names = append(names, sub.Name())
	}
	sort.Strings(names)
	want := []string{"cancel", "claim", "machine", "progress", "question", "resolve", "result", "start", "status", "wait"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("delegate subcommands\n got: %#v\nwant: %#v", names, want)
	}
}

func TestDelegateMachinePublishOncePublishesCapabilityCard(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	recordGraph := newFakeDelegateRegistryGraph()
	machineGraph := newFakeMachineRegistryGraph()
	open := func(opts *delegateOptions) (*delegateRegistrySession, error) {
		cache := machineregistry.Cache{RegistryKey: "registry-1", RegistryChatID: "chat-registry"}
		return &delegateRegistrySession{
			graph:  recordGraph,
			store:  machineregistry.Store{Graph: machineGraph, Now: func() time.Time { return now }},
			cache:  cache,
			chatID: "chat-registry",
			close:  func(context.Context) error { return nil },
		}, nil
	}
	result, err := runDelegateMachinePublishOnce(&delegateOptions{
		machineID:    "machine-b",
		machineLabel: "Windows GPU",
		aliases:      []string{"B", "win-gpu"},
		capabilities: []string{"windows", "gpu"},
		accepting:    true,
		heartbeat:    5 * time.Minute,
		ttl:          15 * time.Minute,
		now:          func() time.Time { return now },
		openRegistry: open,
	})
	if err != nil {
		t.Fatalf("publish-once: %v", err)
	}
	if result.Transport != "registry" || result.Mode != "append-slot" || result.MachineID != "machine-b" {
		t.Fatalf("result = %#v", result)
	}
	if len(machineGraph.messages) != 1 {
		t.Fatalf("messages = %#v, want one card", machineGraph.messages)
	}
	card, ok := machineregistry.ParseCardMessage(machineGraph.messages[0].Body.Content)
	if !ok {
		t.Fatalf("published message did not contain machine card: %s", machineGraph.messages[0].Body.Content)
	}
	if card.MachineID != "machine-b" || !card.Accepting || card.RegistryKey != "registry-1" || len(card.Capabilities) != 2 {
		t.Fatalf("card = %#v", card)
	}
	if card.InstanceID != "manual_machine-b" || card.HostLabel != "Windows GPU" ||
		card.CapabilityFingerprint != machineregistry.CapabilityFingerprint([]string{"windows", "gpu"}) ||
		len(card.ProtocolVersions) != 1 || card.ProtocolVersions[0] != "cxp-delegation-v1" {
		t.Fatalf("card metadata = %#v", card)
	}
}

func TestDelegateCommandPrintsJSON(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	candidatesPath := writeJSONFile(t, "candidates.json", []delegation.Candidate{
		{MachineID: "machine-b", Label: "B", State: "online", Accepting: true, Confidence: 0.95},
	})
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"delegate", "resolve", "--query", "ask B", "--candidate-file", candidatesPath, "--json"})
	delegateCmd, _, err := cmd.Find([]string{"delegate"})
	if err != nil {
		t.Fatalf("find delegate: %v", err)
	}
	if delegateCmd == nil {
		t.Fatal("delegate command missing")
	}
	// Direct execution uses real time; token validity is still parseable.
	_ = now
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute: %v\n%s", err, out.String())
	}
	var result delegation.ResolveResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out.String())
	}
	if result.Action != delegation.ActionStart || result.CandidateToken == "" {
		t.Fatalf("output = %#v", result)
	}
}

func TestDelegateStartRejectsLoopingPath(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	taskPath := writeJSONFile(t, "task.json", delegation.TaskSpec{Objective: "remote work"})
	token, _, err := delegation.NewCandidateToken("machine-b", now, time.Hour)
	if err != nil {
		t.Fatalf("NewCandidateToken: %v", err)
	}
	_, err = runDelegateStart(&delegateOptions{
		storePath:      filepath.Join(t.TempDir(), "delegation.json"),
		candidateToken: token,
		taskFile:       taskPath,
		path:           []string{"machine-a", "machine-b"},
		now:            func() time.Time { return now },
	})
	if err == nil || !strings.Contains(err.Error(), "loop") {
		t.Fatalf("start err = %v, want loop rejection", err)
	}
}

func BenchmarkDelegateResolveWithManyRemoteThreads(b *testing.B) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	dir := b.TempDir()
	routeStorePath := filepath.Join(dir, "routes.json")
	store := delegation.Store{}
	for i := 0; i < 1000; i++ {
		store.UpsertRemoteThread(delegation.RemoteThread{
			ThreadID:             fmt.Sprintf("rth-%04d", i),
			MachineID:            "machine-b",
			Title:                fmt.Sprintf("thread %04d", i),
			Summary:              "Prior remote work summary.",
			LastResultSummary:    "Last useful result.",
			WorkspaceFingerprint: "workspace-1",
			SourceSessionID:      "session-a",
			State:                delegation.RemoteThreadStateIdle,
			LastUsedAt:           now.Add(-time.Duration(i) * time.Minute).Format(time.RFC3339Nano),
			ExpiresAt:            now.Add(time.Hour).Format(time.RFC3339Nano),
			Generation:           fmt.Sprintf("gen-%04d", i),
		})
	}
	if _, err := delegation.SaveStore(routeStorePath, store); err != nil {
		b.Fatalf("SaveStore: %v", err)
	}
	candidatesPath := filepath.Join(dir, "candidates.json")
	raw, err := json.Marshal([]delegation.Candidate{{
		MachineID:  "machine-b",
		Label:      "B",
		State:      "online",
		Accepting:  true,
		Confidence: 0.95,
	}})
	if err != nil {
		b.Fatalf("marshal candidates: %v", err)
	}
	if err := os.WriteFile(candidatesPath, raw, 0o600); err != nil {
		b.Fatalf("write candidates: %v", err)
	}
	opts := &delegateOptions{
		query:                "ask B to continue",
		candidateFile:        candidatesPath,
		sourceSession:        "session-a",
		workspaceFingerprint: "workspace-1",
		routeStorePath:       routeStorePath,
		now:                  func() time.Time { return now },
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := runDelegateResolve(opts)
		if err != nil {
			b.Fatalf("runDelegateResolve: %v", err)
		}
		if len(result.ThreadCandidates) != 3 {
			b.Fatalf("thread candidates = %d, want 3", len(result.ThreadCandidates))
		}
	}
}

func BenchmarkDelegateOutboxSQLiteWriteAmplification(b *testing.B) {
	base := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	for _, threadCount := range []int{0, 1000, 10000} {
		b.Run(fmt.Sprintf("remote-threads-%d", threadCount), func(b *testing.B) {
			now := base
			dir := b.TempDir()
			routeStorePath := filepath.Join(dir, "routes.sqlite")
			if threadCount > 0 {
				store := delegation.Store{}
				for i := 0; i < threadCount; i++ {
					updatedAt := base.Add(-time.Duration(i%3600) * time.Second)
					store.UpsertRemoteThread(delegation.RemoteThread{
						ThreadID:             fmt.Sprintf("rth-%05d", i),
						MachineID:            "machine-b",
						Title:                fmt.Sprintf("thread %05d", i),
						Summary:              "Prior remote work summary.",
						LastResultSummary:    "Prior useful result.",
						WorkspaceFingerprint: "workspace-1",
						SourceSessionID:      "session-a",
						State:                delegation.RemoteThreadStateIdle,
						Generation:           fmt.Sprintf("gen-%05d", i),
						CreatedAt:            base.Add(-time.Hour).Format(time.RFC3339Nano),
						UpdatedAt:            updatedAt.Format(time.RFC3339Nano),
						LastUsedAt:           updatedAt.Format(time.RFC3339Nano),
						ExpiresAt:            base.Add(time.Hour).Format(time.RFC3339Nano),
					})
				}
				if _, err := delegation.SaveStore(routeStorePath, store); err != nil {
					b.Fatalf("seed route store: %v", err)
				}
			}
			opts := &delegateOptions{
				routeStorePath: routeStorePath,
				now:            func() time.Time { return now },
			}
			beforeIO, beforeOK := cliPerfReadProcSelfIO()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				now = base.Add(time.Duration(i) * time.Second)
				req, err := delegation.NewRequestRecord(
					"session-a",
					fmt.Sprintf("turn-%05d", i),
					"",
					[]string{"machine-a"},
					"machine-b",
					delegation.TaskSpec{Objective: "benchmark source outbox writes"},
					now,
				)
				if err != nil {
					b.Fatalf("request: %v", err)
				}
				if err := saveDelegateOutbox(opts, req, delegation.OutboxPending, "chat-inbox", "inbox-ref", "", ""); err != nil {
					b.Fatalf("pending outbox: %v", err)
				}
				if err := saveDelegateOutbox(opts, req, delegation.OutboxVisible, "chat-inbox", "inbox-ref", fmt.Sprintf("msg-%05d", i), ""); err != nil {
					b.Fatalf("visible outbox: %v", err)
				}
			}
			b.StopTimer()
			cliPerfReportProcIODelta(b, beforeIO, beforeOK, b.N)
			if info, err := os.Stat(routeStorePath); err == nil {
				b.ReportMetric(float64(info.Size()), "store_file_B")
			}
		})
	}
}

type cliPerfProcIO struct {
	writeBytes uint64
}

func cliPerfReadProcSelfIO() (cliPerfProcIO, bool) {
	raw, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return cliPerfProcIO{}, false
	}
	var out cliPerfProcIO
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(key) != "write_bytes" {
			continue
		}
		parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return cliPerfProcIO{}, false
		}
		out.writeBytes = parsed
		return out, true
	}
	return cliPerfProcIO{}, false
}

func cliPerfReportProcIODelta(b *testing.B, before cliPerfProcIO, beforeOK bool, n int) {
	b.Helper()
	if !beforeOK || n <= 0 {
		return
	}
	after, afterOK := cliPerfReadProcSelfIO()
	if !afterOK || after.writeBytes < before.writeBytes {
		return
	}
	b.ReportMetric(float64(after.writeBytes-before.writeBytes)/float64(n), "disk_write_B/op")
}

func writeJSONFile(t *testing.T, name string, value any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func mustDelegateRequestForCLITest(t *testing.T, now time.Time) delegation.Record {
	t.Helper()
	req, err := delegation.NewRequestRecord(
		"session-a",
		"turn-1",
		"",
		[]string{"machine-a"},
		"machine-b",
		delegation.TaskSpec{Objective: "remote work"},
		now,
	)
	if err != nil {
		t.Fatalf("NewRequestRecord: %v", err)
	}
	return req
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

type fakeDelegateRegistryGraph struct {
	messagesByChat     map[string][]teams.ChatMessage
	chatsByRef         map[string]string
	sendCount          int
	windowListCount    int
	sendErrAfterCommit error
}

func newFakeDelegateRegistryGraph() *fakeDelegateRegistryGraph {
	return &fakeDelegateRegistryGraph{
		messagesByChat: map[string][]teams.ChatMessage{},
		chatsByRef:     map[string]string{},
	}
}

func (g *fakeDelegateRegistryGraph) SendHTML(_ context.Context, chatID string, content string) (teams.ChatMessage, error) {
	g.sendCount++
	id := fmt.Sprintf("message-%06d", g.sendCount)
	msg := teams.ChatMessage{ID: id}
	msg.Body.Content = content
	chatID = strings.TrimSpace(chatID)
	g.messagesByChat[chatID] = append([]teams.ChatMessage{msg}, g.messagesByChat[chatID]...)
	if g.sendErrAfterCommit != nil {
		err := g.sendErrAfterCommit
		g.sendErrAfterCommit = nil
		return msg, err
	}
	return msg, nil
}

func (g *fakeDelegateRegistryGraph) ListMessages(_ context.Context, chatID string, top int) ([]teams.ChatMessage, error) {
	messages := g.messagesByChat[strings.TrimSpace(chatID)]
	if top <= 0 || top > len(messages) {
		top = len(messages)
	}
	return append([]teams.ChatMessage(nil), messages[:top]...), nil
}

func (g *fakeDelegateRegistryGraph) ListMessagesExactTopWithoutRateLimitRetry(ctx context.Context, chatID string, top int) ([]teams.ChatMessage, error) {
	return g.ListMessages(ctx, chatID, top)
}

func (g *fakeDelegateRegistryGraph) ListMessagesWindow(_ context.Context, chatID string, top int, _ time.Time) (teams.MessageWindow, error) {
	return g.listMessagesWindowFromOffset(chatID, top, 0), nil
}

func (g *fakeDelegateRegistryGraph) ListMessagesWindowFromPath(_ context.Context, path string) (teams.MessageWindow, error) {
	var chatID string
	var top, offset int
	if _, err := fmt.Sscanf(path, "fake-window:%s %d %d", &chatID, &top, &offset); err != nil {
		return teams.MessageWindow{}, err
	}
	return g.listMessagesWindowFromOffset(chatID, top, offset), nil
}

func (g *fakeDelegateRegistryGraph) listMessagesWindowFromOffset(chatID string, top int, offset int) teams.MessageWindow {
	g.windowListCount++
	chatID = strings.TrimSpace(chatID)
	messages := g.messagesByChat[chatID]
	if top <= 0 {
		top = len(messages)
	}
	if offset < 0 {
		offset = 0
	}
	end := offset + top
	if end > len(messages) {
		end = len(messages)
	}
	window := teams.MessageWindow{Messages: append([]teams.ChatMessage(nil), messages[offset:end]...)}
	if end < len(messages) {
		window.Truncated = true
		window.NextPath = fmt.Sprintf("fake-window:%s %d %d", chatID, top, end)
	}
	return window
}

func (g *fakeDelegateRegistryGraph) CreateOrGetMeetingChatWindow(_ context.Context, topic string, externalID string, _ time.Time, _ time.Time) (teams.Chat, teams.OnlineMeeting, error) {
	externalID = strings.TrimSpace(externalID)
	chatID := g.chatsByRef[externalID]
	if chatID == "" {
		chatID = "chat-" + externalID
		g.chatsByRef[externalID] = chatID
	}
	return teams.Chat{ID: chatID, Topic: topic, ChatType: "meeting"}, teams.OnlineMeeting{ID: "meeting-" + externalID}, nil
}

func (g *fakeDelegateRegistryGraph) sendCountForChat(chatID string) int {
	return len(g.messagesByChat[strings.TrimSpace(chatID)])
}

type fakeMachineRegistryGraph struct {
	messages  []machineregistry.ChatMessage
	sendCount int
}

func newFakeMachineRegistryGraph() *fakeMachineRegistryGraph {
	return &fakeMachineRegistryGraph{}
}

func (g *fakeMachineRegistryGraph) SendHTML(_ context.Context, _ string, content string) (machineregistry.ChatMessage, error) {
	g.sendCount++
	id := fmt.Sprintf("machine-message-%06d", g.sendCount)
	msg := machineregistry.ChatMessage{ID: id}
	msg.Body.Content = content
	g.messages = append([]machineregistry.ChatMessage{msg}, g.messages...)
	return msg, nil
}

func (g *fakeMachineRegistryGraph) UpdateChatMessageHTML(context.Context, string, string, string) error {
	return fmt.Errorf("fake machine registry has no existing slot")
}

func (g *fakeMachineRegistryGraph) ListMessages(_ context.Context, _ string, top int) ([]machineregistry.ChatMessage, error) {
	if top <= 0 || top > len(g.messages) {
		top = len(g.messages)
	}
	return append([]machineregistry.ChatMessage(nil), g.messages[:top]...), nil
}

func (g *fakeMachineRegistryGraph) CreateOrGetMeetingChatWindow(_ context.Context, _ string, externalID string, _ time.Time, _ time.Time) (machineregistry.Chat, machineregistry.OnlineMeeting, error) {
	chatID := "chat-registry"
	meetingID := "meeting-registry"
	if strings.Contains(externalID, "inbox") {
		chatID = "chat-inbox"
		meetingID = "meeting-inbox"
	}
	return machineregistry.Chat{ID: chatID}, machineregistry.OnlineMeeting{ID: meetingID, ChatThreadID: chatID}, nil
}

func (g *fakeMachineRegistryGraph) GetOnlineMeeting(_ context.Context, meetingID string) (machineregistry.OnlineMeeting, error) {
	threadID := "chat-registry"
	if strings.Contains(meetingID, "inbox") {
		threadID = "chat-inbox"
	}
	return machineregistry.OnlineMeeting{ID: meetingID, ChatThreadID: threadID}, nil
}

func (g *fakeMachineRegistryGraph) UpdateOnlineMeetingWindow(context.Context, string, time.Time, time.Time) (machineregistry.OnlineMeeting, error) {
	return machineregistry.OnlineMeeting{ID: "meeting-registry", ChatThreadID: "chat-registry"}, nil
}
