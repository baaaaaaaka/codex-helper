package beacon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type profileDoctorSmokeAdapter struct {
	calls []string
}

func (a *profileDoctorSmokeAdapter) SubmitAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error) {
	a.calls = append(a.calls, "submit")
	return SchedulerQueryResult{ProviderJobID: "slurm-42", RawState: "PD", Reason: "submitted"}, nil
}

func (a *profileDoctorSmokeAdapter) QueryAllocation(_ context.Context, req AllocationRequest) (SchedulerQueryResult, error) {
	a.calls = append(a.calls, "query:"+req.ProviderIdentity.ProviderJobID)
	return SchedulerQueryResult{ProviderJobID: req.ProviderIdentity.ProviderJobID, RawState: "R", Reason: "node-a"}, nil
}

func (a *profileDoctorSmokeAdapter) CancelAllocation(_ context.Context, req AllocationRequest) (SchedulerQueryResult, error) {
	a.calls = append(a.calls, "cancel:"+req.ProviderIdentity.ProviderJobID)
	return SchedulerQueryResult{ProviderJobID: req.ProviderIdentity.ProviderJobID, RawState: "CA", Reason: "cancel_requested"}, nil
}

func TestProfileLifecycleDraftDoctorConfirmAndProxy(t *testing.T) {
	var st State
	proxyExists := func(name string) bool { return name == "jump-a" }
	p, err := CreateProfile(&st, CreateProfileInput{
		Name:         "gpu",
		Provider:     ProviderSlurm,
		ProxyMode:    ProxySSHProfile,
		ProxyProfile: "jump-a",
		Slurm:        SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4},
		Now:          time.Unix(1, 0),
	})
	if err != nil {
		t.Fatalf("CreateProfile: %v", err)
	}
	if p.Ready(proxyExists) {
		t.Fatal("created profile should remain draft until doctor and confirm")
	}
	if reasons := strings.Join(p.DraftReasons(proxyExists), ","); !strings.Contains(reasons, "doctor failed") || !strings.Contains(reasons, "needs confirm") {
		t.Fatalf("draft reasons should mention doctor and confirm, got %q", reasons)
	}
	p, err = DoctorProfile(&st, "gpu", time.Unix(2, 0), proxyExists)
	if err != nil {
		t.Fatalf("DoctorProfile: %v", err)
	}
	p, err = ConfirmProfile(&st, "gpu", time.Unix(3, 0), proxyExists)
	if err != nil {
		t.Fatalf("ConfirmProfile: %v", err)
	}
	if !p.Ready(proxyExists) {
		t.Fatalf("profile should be ready, reasons=%v", p.DraftReasons(proxyExists))
	}
}

func TestLSFProfileQueueOnlyDraftNeedsOnlyDoctorAndConfirm(t *testing.T) {
	var st State
	p, err := CreateProfile(&st, CreateProfileInput{
		Name:      "lsf",
		Provider:  ProviderLSF,
		ProxyMode: ProxyNone,
		LSF:       LSFProfile{QueueName: "o_pri_interactive"},
	})
	if err != nil {
		t.Fatalf("CreateProfile: %v", err)
	}
	if got := strings.Join(p.DraftReasons(nil), ","); strings.Contains(got, "lsf resource shape cannot be derived") || !strings.Contains(got, "doctor failed") || !strings.Contains(got, "needs confirm") {
		t.Fatalf("queue-only LSF profile should need only doctor/confirm, got %q", got)
	}
	p, err = DoctorProfile(&st, "lsf", time.Unix(2, 0), nil)
	if err != nil {
		t.Fatalf("DoctorProfile: %v", err)
	}
	p, err = ConfirmProfile(&st, "lsf", time.Unix(3, 0), nil)
	if err != nil {
		t.Fatalf("ConfirmProfile: %v", err)
	}
	if !p.Ready(nil) {
		t.Fatalf("queue-only LSF profile should be ready after doctor/confirm, got %v", p.DraftReasons(nil))
	}
}

func TestProfileDoctorReportChecksProviderAdapters(t *testing.T) {
	var st State
	if _, err := CreateProfile(&st, CreateProfileInput{
		Name:      "gpu",
		Provider:  ProviderSlurm,
		ProxyMode: ProxyNone,
		Slurm:     SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4},
	}); err != nil {
		t.Fatalf("CreateProfile: %v", err)
	}
	p, report, err := DoctorProfileWithInput(&st, "gpu", DoctorProfileInput{Now: time.Unix(2, 0)})
	if err != nil {
		t.Fatalf("DoctorProfileWithInput: %v", err)
	}
	if p.DoctorOK || report.Passed || len(report.Operations) != 4 || !strings.Contains(strings.Join(report.Issues, ","), "query adapter") {
		t.Fatalf("missing adapters should fail doctor: profile=%#v report=%#v", p, report)
	}
	adapterConfig := ProviderCommandConfigForProvider(ProviderSlurm, "/ok/query", "/ok/submit", "/ok/cancel", "/ok/renew")
	p, report, err = DoctorProfileWithInput(&st, "gpu", DoctorProfileInput{
		Now:                 time.Unix(3, 0),
		EnvProviderCommands: adapterConfig,
		CheckExecutable:     func(string) error { return nil },
	})
	if err != nil {
		t.Fatalf("DoctorProfileWithInput env adapters: %v", err)
	}
	if !p.DoctorOK || !report.Passed {
		t.Fatalf("configured adapters should pass doctor: profile=%#v report=%#v", p, report)
	}
}

func TestProfileDoctorSmokeSubmitQueryCancelAndPersistsReport(t *testing.T) {
	var st State
	p, err := CreateProfile(&st, CreateProfileInput{
		Name:      "gpu",
		Provider:  ProviderSlurm,
		ProxyMode: ProxyNone,
		Slurm:     SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4},
		Adapter:   ProviderCommandConfigForProvider(ProviderSlurm, "/query", "/submit", "/cancel", "/renew"),
	})
	if err != nil {
		t.Fatalf("CreateProfile: %v", err)
	}
	adapter := &profileDoctorSmokeAdapter{}
	smoke := RunProfileDoctorSmoke(context.Background(), p, ProfileDoctorSmokeInput{Adapter: adapter})
	if got, want := strings.Join(adapter.calls, ","), "submit,query:slurm-42,cancel:slurm-42"; got != want {
		t.Fatalf("smoke adapter calls = %q, want %q", got, want)
	}
	if len(smoke) != 3 || smoke[0].ProviderJobID != "slurm-42" || smoke[1].RawState != "R" || smoke[2].RawState != "CA" {
		t.Fatalf("unexpected smoke report: %#v", smoke)
	}
	p, err = ApplyProfileDoctorSmokeReport(&st, "gpu", p.Revision, smoke, time.Unix(2, 0))
	if err != nil {
		t.Fatalf("ApplyProfileDoctorSmokeReport: %v", err)
	}
	if !p.DoctorReport.Passed || len(p.DoctorReport.Smoke) != 3 {
		t.Fatalf("profile smoke report not persisted cleanly: %#v", p.DoctorReport)
	}
}

func TestNewTargetDefaultsLocalAndExplicitBeaconNeverFallsBack(t *testing.T) {
	var st State
	got := ResolveNewTarget(st, NewTargetInput{LegacyProxyRoute: "jump-a"}, nil)
	if got.Target != TargetLocal || got.ProxyRoute != "jump-a" || got.Error != "" {
		t.Fatalf("default new should stay local with proxy route preserved, got %#v", got)
	}
	got = ResolveNewTarget(st, NewTargetInput{ExplicitBeaconProfile: "missing", LegacyProxyRoute: "jump-a"}, nil)
	if got.Target != TargetBeacon || got.Error == "" {
		t.Fatalf("explicit beacon missing profile should error without local fallback, got %#v", got)
	}
}

func TestPlacementDecidesLocalReuseAllocateAndExclusive(t *testing.T) {
	st := State{
		Profiles: map[string]Profile{
			"gpu": readyProfile("gpu"),
		},
		Machines: map[string]Machine{
			"gpu-shared": {
				ID:        "gpu-shared",
				LeaseID:   "lease-shared",
				Profile:   "gpu",
				State:     "accepting",
				Isolation: IsolationShared,
				Jobs:      []string{"job-existing"},
			},
			"gpu-exclusive-busy": {
				ID:        "gpu-exclusive-busy",
				LeaseID:   "lease-exclusive-busy",
				Profile:   "gpu",
				State:     "accepting",
				Isolation: IsolationExclusive,
				Chats:     []string{"other-conv"},
			},
			"gpu-lost": {
				ID:        "gpu-lost",
				LeaseID:   "lease-lost",
				Profile:   "gpu",
				State:     "lost",
				Isolation: IsolationShared,
			},
		},
	}
	local, err := DecidePlacement(st, PlacementInput{LegacyProxyRoute: "jump-a"}, nil)
	if err != nil {
		t.Fatalf("local placement: %v", err)
	}
	if local.Action != "run_local" || local.Target.Target != TargetLocal || local.Target.ProxyRoute != "jump-a" {
		t.Fatalf("default placement should be local and keep proxy route separate, got %#v", local)
	}
	shared, err := DecidePlacement(st, PlacementInput{ExplicitBeaconProfile: "gpu", ConversationID: "conv"}, nil)
	if err != nil {
		t.Fatalf("shared placement: %v", err)
	}
	if shared.Action != "reuse_machine" || shared.MachineID != "gpu-shared" {
		t.Fatalf("shared placement should reuse accepting shared machine, got %#v", shared)
	}
	exclusive, err := DecidePlacement(st, PlacementInput{ExplicitBeaconProfile: "gpu", ConversationID: "conv", Isolation: IsolationExclusive}, nil)
	if err != nil {
		t.Fatalf("exclusive placement: %v", err)
	}
	if exclusive.Action != "allocate_machine" {
		t.Fatalf("exclusive placement should not share busy exclusive machine, got %#v", exclusive)
	}
	st.Machines["gpu-exclusive-idle"] = Machine{ID: "gpu-exclusive-idle", LeaseID: "lease-exclusive-idle", Profile: "gpu", State: "accepting", Isolation: IsolationExclusive}
	exclusive, err = DecidePlacement(st, PlacementInput{ExplicitBeaconProfile: "gpu", ConversationID: "conv", Isolation: IsolationExclusive}, nil)
	if err != nil {
		t.Fatalf("exclusive idle placement: %v", err)
	}
	if exclusive.Action != "reuse_machine" || exclusive.MachineID != "gpu-exclusive-idle" {
		t.Fatalf("exclusive placement should reuse idle exclusive machine only, got %#v", exclusive)
	}
}

func TestPlacementDoesNotReuseExternalOrIncompatibleMachines(t *testing.T) {
	st := State{
		Profiles: map[string]Profile{
			"gpu": readyProfile("gpu"),
		},
		Machines: map[string]Machine{
			"external": {
				ID:            "external",
				LeaseID:       "lease-external",
				Profile:       "gpu",
				State:         "accepting",
				Isolation:     IsolationShared,
				ExternalOwned: true,
			},
			"wrong-profile": {
				ID:        "wrong-profile",
				LeaseID:   "lease-cpu",
				Profile:   "cpu",
				State:     "accepting",
				Isolation: IsolationShared,
			},
			"incompatible-state": {
				ID:        "incompatible-state",
				LeaseID:   "lease-incompatible",
				Profile:   "gpu",
				State:     "protocol_mismatch",
				Isolation: IsolationShared,
			},
			"exclusive-busy": {
				ID:        "exclusive-busy",
				LeaseID:   "lease-exclusive",
				Profile:   "gpu",
				State:     "accepting",
				Isolation: IsolationExclusive,
				Chats:     []string{"other-conv"},
			},
		},
	}
	decision, err := DecidePlacement(st, PlacementInput{ExplicitBeaconProfile: "gpu", ConversationID: "conv"}, nil)
	if err != nil {
		t.Fatalf("DecidePlacement: %v", err)
	}
	if decision.Action != "allocate_machine" {
		t.Fatalf("placement should allocate instead of reusing external/incompatible machines, got %#v", decision)
	}
	st.Machines["exclusive-same-conv"] = Machine{
		ID:        "exclusive-same-conv",
		LeaseID:   "lease-same-conv",
		Profile:   "gpu",
		State:     "accepting",
		Isolation: IsolationExclusive,
		Chats:     []string{"conv"},
	}
	decision, err = DecidePlacement(st, PlacementInput{ExplicitBeaconProfile: "gpu", ConversationID: "conv", Isolation: IsolationExclusive}, nil)
	if err != nil {
		t.Fatalf("exclusive DecidePlacement: %v", err)
	}
	if decision.Action != "reuse_machine" || decision.MachineID != "exclusive-same-conv" {
		t.Fatalf("exclusive same-conversation idle machine should be reusable, got %#v", decision)
	}
}

func TestPlacementRejectsDraftBeaconProfileWithoutLocalFallback(t *testing.T) {
	st := State{
		Profiles: map[string]Profile{
			"draft": {Name: "draft", Provider: ProviderSlurm, ProxyMode: ProxyNone},
		},
	}
	decision, err := DecidePlacement(st, PlacementInput{ExplicitBeaconProfile: "draft", LegacyProxyRoute: "jump-a"}, nil)
	if err == nil || decision.Action != "reject" || decision.Target.Target != TargetBeacon {
		t.Fatalf("draft explicit beacon profile should reject without local fallback, decision=%#v err=%v", decision, err)
	}
}

func TestSwitchProfileSnapshotsFutureQueuedTurns(t *testing.T) {
	var st State
	st.Profiles = map[string]Profile{
		"gpu": readyProfile("gpu"),
		"cpu": readyProfile("cpu"),
	}
	st.Conversations = map[string]Conversation{
		"conv": {ID: "conv", Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu", Signature: "sig-gpu"}},
	}
	first, err := QueueTurn(&st, "conv", "turn-1", time.Unix(1, 0))
	if err != nil {
		t.Fatalf("QueueTurn first: %v", err)
	}
	if first.Snapshot.Profile != "gpu" {
		t.Fatalf("first turn should snapshot gpu, got %#v", first)
	}
	res, err := SwitchProfile(&st, SwitchInput{ConversationID: "conv", ProfileName: "cpu", Signature: "sig-cpu", HasQueuedOrRunning: true, SignatureCompatible: true}, nil)
	if err != nil {
		t.Fatalf("SwitchProfile: %v", err)
	}
	if res.Action != "pending" {
		t.Fatalf("switch with queued work should be pending, got %#v", res)
	}
	second, err := QueueTurn(&st, "conv", "turn-2", time.Unix(2, 0))
	if err != nil {
		t.Fatalf("QueueTurn second: %v", err)
	}
	if second.Snapshot.Profile != "cpu" || st.Conversations["conv"].Queued[0].Snapshot.Profile != "gpu" {
		t.Fatalf("queued snapshots should preserve old and use pending for future, conv=%#v", st.Conversations["conv"])
	}
}

func TestDeleteProfileArchivesProfilesInUse(t *testing.T) {
	st := State{
		Profiles: map[string]Profile{
			"gpu": readyProfile("gpu"),
		},
		Machines: map[string]Machine{
			"gpu-a": {ID: "gpu-a", Profile: "gpu", State: "accepting"},
		},
	}
	if err := DeleteProfile(&st, "gpu"); err != nil {
		t.Fatalf("DeleteProfile active machine: %v", err)
	}
	if !st.Profiles["gpu"].Archived || st.Profiles["gpu"].Ready(nil) {
		t.Fatalf("in-use profile should be archived and no longer ready: %#v", st.Profiles["gpu"])
	}
	st.Profiles["gpu"] = readyProfile("gpu")
	delete(st.Machines, "gpu-a")
	st.Conversations = map[string]Conversation{
		"conv": {
			ID:      "conv",
			Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu"},
			Queued:  []QueuedTurn{{ID: "turn-1", Snapshot: TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}}},
		},
	}
	if err := DeleteProfile(&st, "gpu"); err != nil {
		t.Fatalf("DeleteProfile conversation: %v", err)
	}
	if !st.Profiles["gpu"].Archived || st.Conversations["conv"].Current.ProfileRevision != 1 || st.Conversations["conv"].Queued[0].Snapshot.ProfileRevision != 1 {
		t.Fatalf("conversation archive should pin old references: profile=%#v conv=%#v", st.Profiles["gpu"], st.Conversations["conv"])
	}
	historyKey := profileHistoryKey("gpu", 1)
	if st.ProfileHistory[historyKey].Archived {
		t.Fatalf("archiving should keep a live historical snapshot for pinned work: %#v", st.ProfileHistory[historyKey])
	}
	if err := DeleteProfile(&st, "gpu"); err != nil {
		t.Fatalf("repeated DeleteProfile should be idempotent: %v", err)
	}
	if st.ProfileHistory[historyKey].Archived {
		t.Fatalf("repeated archive must not overwrite the live historical snapshot: %#v", st.ProfileHistory[historyKey])
	}
	if revisions := ListProfileRevisions(st, "gpu"); len(revisions) != 1 || !revisions[0].Archived {
		t.Fatalf("profile revision list should show the latest profile as archived while history keeps the live snapshot: %#v", revisions)
	}
	st.Profiles["gpu"] = readyProfile("gpu")
	st.Conversations = nil
	if err := DeleteProfile(&st, "gpu"); err != nil {
		t.Fatalf("unused DeleteProfile: %v", err)
	}
	if !st.Profiles["gpu"].Archived {
		t.Fatalf("unused DeleteProfile should archive profile instead of removing it")
	}
	restored, err := RollbackProfileRevision(&st, "gpu", 1, time.Unix(10, 0))
	if err != nil {
		t.Fatalf("RollbackProfileRevision from archived profile: %v", err)
	}
	if restored.Archived || !restored.Ready(nil) || restored.Revision != 2 {
		t.Fatalf("rollback should publish a live replacement revision, got %#v", restored)
	}
}

func TestUpdateProfileCreatesRevisionAndPinsExistingTargets(t *testing.T) {
	now := time.Unix(1, 0)
	st := State{}
	p, err := CreateProfile(&st, CreateProfileInput{
		Name:             "gpu",
		Provider:         ProviderSlurm,
		IsolationDefault: IsolationShared,
		Slurm:            SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "old", Image: "old.sqsh", Duration: 2},
		Adapter:          ProviderCommandConfigForProvider(ProviderSlurm, "/old/query", "/old/submit", "", ""),
		Now:              now,
	})
	if err != nil {
		t.Fatalf("CreateProfile: %v", err)
	}
	if p.Revision != 1 {
		t.Fatalf("initial revision = %d, want 1", p.Revision)
	}
	if _, err := DoctorProfile(&st, "gpu", now.Add(time.Second), nil); err != nil {
		t.Fatalf("DoctorProfile old: %v", err)
	}
	if _, err := ConfirmProfile(&st, "gpu", now.Add(2*time.Second), nil); err != nil {
		t.Fatalf("ConfirmProfile old: %v", err)
	}
	st.Conversations = map[string]Conversation{
		"conv": {ID: "conv", Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}},
	}
	updated, err := UpdateProfileConfig(&st, UpdateProfileInput{
		Name:             "gpu",
		Provider:         ProviderSlurm,
		IsolationDefault: IsolationShared,
		Slurm:            SlurmProfile{Nodes: 2, GPUCount: 4, Partition: "new", Image: "new.sqsh", Duration: 4},
		Now:              now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("UpdateProfileConfig: %v", err)
	}
	if updated.Revision != 2 || updated.Slurm.Image != "new.sqsh" || updated.Adapter.SlurmQueryCommand != "/old/query" || updated.Confirmed || updated.DoctorOK {
		t.Fatalf("updated profile = %#v, want new draft revision", updated)
	}
	if got := st.Conversations["conv"].Current.ProfileRevision; got != 1 {
		t.Fatalf("existing conversation revision = %d, want pinned old revision", got)
	}
	req, _, err := EnsureAllocationRequest(&st, "conv", "turn-1", st.Conversations["conv"].Current, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("EnsureAllocationRequest old revision: %v", err)
	}
	if req.ProfileSnapshot.Revision != 1 || req.ProfileSnapshot.Slurm.Image != "old.sqsh" {
		t.Fatalf("allocation should use old snapshot, got revision=%d image=%q", req.ProfileSnapshot.Revision, req.ProfileSnapshot.Slurm.Image)
	}
}

func TestRollbackProfileRevisionPublishesNewRevisionAndPruneKeepsPinnedHistory(t *testing.T) {
	now := time.Unix(1, 0)
	st := State{}
	if _, err := CreateProfile(&st, CreateProfileInput{
		Name:             "gpu",
		Provider:         ProviderSlurm,
		IsolationDefault: IsolationShared,
		Slurm:            SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "old", Image: "old.sqsh", Duration: 2},
		Adapter:          ProviderCommandConfigForProvider(ProviderSlurm, "/old/query", "/old/submit", "", ""),
		Now:              now,
	}); err != nil {
		t.Fatalf("CreateProfile: %v", err)
	}
	if _, err := DoctorProfile(&st, "gpu", now.Add(time.Second), nil); err != nil {
		t.Fatalf("DoctorProfile old: %v", err)
	}
	if _, err := ConfirmProfile(&st, "gpu", now.Add(2*time.Second), nil); err != nil {
		t.Fatalf("ConfirmProfile old: %v", err)
	}
	st.Conversations["conv-old"] = Conversation{ID: "conv-old", Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}}
	if _, err := UpdateProfileConfig(&st, UpdateProfileInput{
		Name:             "gpu",
		Provider:         ProviderSlurm,
		IsolationDefault: IsolationShared,
		Slurm:            SlurmProfile{Nodes: 2, GPUCount: 2, Partition: "new", Image: "new.sqsh", Duration: 4},
		Adapter:          ProviderCommandConfigForProvider(ProviderSlurm, "/new/query", "/new/submit", "", ""),
		Now:              now.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("UpdateProfileConfig: %v", err)
	}
	st.Conversations["conv-new"] = Conversation{ID: "conv-new", Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}}
	rolledBack, err := RollbackProfileRevision(&st, "gpu", 1, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("RollbackProfileRevision: %v", err)
	}
	if rolledBack.Revision != 3 || rolledBack.Slurm.Image != "old.sqsh" || rolledBack.Adapter.SlurmQueryCommand != "/old/query" || !rolledBack.Ready(nil) {
		t.Fatalf("rolled back profile = %#v", rolledBack)
	}
	if got := st.Conversations["conv-old"].Current.ProfileRevision; got != 1 {
		t.Fatalf("old conversation revision = %d, want 1", got)
	}
	if got := st.Conversations["conv-new"].Current.ProfileRevision; got != 2 {
		t.Fatalf("new conversation revision = %d, want 2", got)
	}
	revisions := ListProfileRevisions(st, "gpu")
	if len(revisions) != 3 || revisions[0].Revision != 1 || revisions[1].Revision != 2 || revisions[2].Revision != 3 {
		t.Fatalf("profile revisions = %#v", revisions)
	}
	removed, err := PruneProfileHistory(&st, "gpu")
	if err != nil {
		t.Fatalf("PruneProfileHistory: %v", err)
	}
	if removed != 0 {
		t.Fatalf("pruned referenced revisions = %d, want 0", removed)
	}
	delete(st.Conversations, "conv-old")
	delete(st.Conversations, "conv-new")
	removed, err = PruneProfileHistory(&st, "gpu")
	if err != nil {
		t.Fatalf("PruneProfileHistory after unpin: %v", err)
	}
	if removed != 2 || len(st.ProfileHistory) != 0 {
		t.Fatalf("prune after unpin removed=%d history=%#v, want both historical revisions removed", removed, st.ProfileHistory)
	}
}

func TestIdempotencySurvivesStoreReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "beacon.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	err = store.Update(func(st *State) error {
		_, _, err := ApplyIdempotent(st, "msg-1", "new --beacon-profile gpu", "", time.Unix(1, 0), func() (string, error) {
			return "allocation-1", nil
		})
		return err
	})
	if err != nil {
		t.Fatalf("first update: %v", err)
	}
	store2, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore reload: %v", err)
	}
	err = store2.Update(func(st *State) error {
		result, created, err := ApplyIdempotent(st, "msg-1", "new --beacon-profile gpu", "", time.Unix(2, 0), func() (string, error) {
			return "allocation-2", nil
		})
		if err != nil {
			return err
		}
		if created || result != "allocation-1" {
			t.Fatalf("duplicate after reload should reuse original result, created=%v result=%q", created, result)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
}

func TestStoreConcurrentUpdatesFromDistinctHandlesDoNotLoseWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "beacon.json")
	const writers = 40
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store, err := NewStore(path)
			if err != nil {
				errs <- err
				return
			}
			errs <- store.Update(func(st *State) error {
				st.Idempotency[fmt.Sprintf("writer-%02d", i)] = IdempotencyRecord{Key: fmt.Sprintf("writer-%02d", i), Result: "ok"}
				return nil
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent update: %v", err)
		}
	}
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore final: %v", err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("Load final: %v", err)
	}
	if len(st.Idempotency) != writers {
		t.Fatalf("idempotency records = %d, want %d", len(st.Idempotency), writers)
	}
}

func TestStoreLoadWaitsForProcessLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "beacon.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(State{Version: StateVersion}); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	processLock := beaconStoreProcessLock(path)
	processLock.Lock()
	locked := true
	defer func() {
		if locked {
			processLock.Unlock()
		}
	}()

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := store.Load()
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("Load returned while beacon state process lock was held: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	processLock.Unlock()
	locked = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Load after unlock: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Load did not resume after beacon state process lock was released")
	}
}

func TestReleasePreviewHardKillAndExternalProtection(t *testing.T) {
	m := Machine{ID: "gpu-a", LeaseID: "lease-1", ProviderJobID: "slurm-1", Chats: []string{"chat-a"}, Jobs: []string{"job-1"}}
	res, err := DecideRelease(m, ReleaseInput{})
	if err != nil {
		t.Fatalf("DecideRelease: %v", err)
	}
	if res.Action != "drain" || res.Preview.Confirmation != "KILL-lease-1" {
		t.Fatalf("running release should drain and show confirmation preview, got %#v", res)
	}
	res, err = DecideRelease(m, ReleaseInput{HardKill: true, ExactID: "lease-1", ConfirmToken: "KILL-lease-1", ProvidedToken: "KILL-lease-1"})
	if err != nil {
		t.Fatalf("DecideRelease kill: %v", err)
	}
	if res.Action != "kill_quarantine" {
		t.Fatalf("confirmed hard kill should quarantine, got %#v", res)
	}
	m.ExternalOwned = true
	res, err = DecideRelease(m, ReleaseInput{HardKill: true, ExactID: "lease-1", ConfirmToken: "KILL-lease-1", ProvidedToken: "KILL-lease-1"})
	if err != nil {
		t.Fatalf("DecideRelease external: %v", err)
	}
	if res.Action != "reject_external" {
		t.Fatalf("external BYO allocation should reject provider kill, got %#v", res)
	}
}

func TestAllocationReleasePreviewRequiresConfirmationAcrossMultipleChats(t *testing.T) {
	st := State{
		Allocations: map[string]AllocationRequest{
			"req-a": {ID: "req-a", ConversationID: "chat-a", Profile: "gpu", Provider: ProviderLocal, State: AllocationRunning},
			"req-b": {ID: "req-b", ConversationID: "chat-b", Profile: "gpu", Provider: ProviderLocal, State: AllocationRunning},
		},
	}
	preview := PreviewAllocationRelease(st, "profile", "gpu", []AllocationRequest{st.Allocations["req-a"], st.Allocations["req-b"]}, false)
	if !preview.RequiresConfirmation || !strings.HasPrefix(preview.Confirmation, "RELEASE-") {
		t.Fatalf("multi-chat profile release should require confirmation: %#v", preview)
	}
}

func TestDetachConversationDemandPreservesSharedMachineForOtherChats(t *testing.T) {
	st := State{
		Allocations: map[string]AllocationRequest{
			"req-a": {ID: "req-a", ConversationID: "chat-a", Profile: "gpu", Provider: ProviderSlurm, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}, State: AllocationRunning},
			"req-b": {ID: "req-b", ConversationID: "chat-b", Profile: "gpu", Provider: ProviderSlurm, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}, State: AllocationRunning},
		},
		Machines: map[string]Machine{
			"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", Profile: "gpu", State: string(LeaseAccepting), Chats: []string{"chat-a", "chat-b"}},
		},
	}
	result := DetachConversationDemand(&st, "chat-a", "user released work chat", time.Unix(3, 0))
	if got := strings.Join(result.Detached, ","); got != "req-a" {
		t.Fatalf("detached = %q, want req-a result=%#v", got, result)
	}
	if st.Allocations["req-a"].State != AllocationCanceled || st.Allocations["req-b"].State != AllocationRunning {
		t.Fatalf("detach should cancel only chat-a allocation: %#v", st.Allocations)
	}
	if got := strings.Join(st.Machines["machine-1"].Chats, ","); got != "chat-b" {
		t.Fatalf("machine chats = %q, want chat-b", got)
	}
	if st.Machines["machine-1"].State != string(LeaseAccepting) {
		t.Fatalf("shared machine should remain accepting: %#v", st.Machines["machine-1"])
	}
}

func TestDetachConversationDemandDrainsStartedJobWithoutProviderCancelIntent(t *testing.T) {
	now := time.Unix(3, 0)
	st := State{
		Allocations: map[string]AllocationRequest{
			"req-a": {ID: "req-a", ConversationID: "chat-a", Profile: "gpu", Provider: ProviderSlurm, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}, State: AllocationRunning},
			"req-b": {ID: "req-b", ConversationID: "chat-b", Profile: "gpu", Provider: ProviderSlurm, ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"}, State: AllocationRunning},
		},
		Machines: map[string]Machine{
			"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", Profile: "gpu", State: string(LeaseAccepting), Chats: []string{"chat-a", "chat-b"}, Jobs: []string{"job-a"}},
		},
		JobAttempts: map[string]JobAttempt{
			"job-a": {ID: "job-a", RequestID: "req-a", TurnID: "turn-a", Phase: JobStarted},
		},
	}
	result := DetachConversationDemand(&st, "chat-a", "user released work chat", now)
	if got := strings.Join(result.Draining, ","); got != "req-a" {
		t.Fatalf("draining = %q, want req-a result=%#v", got, result)
	}
	req := st.Allocations["req-a"]
	if req.DetachRequestedAt.IsZero() || !req.CancelRequestedAt.IsZero() {
		t.Fatalf("detach should not become provider cancel intent while job is active: %#v", req)
	}
	finalized := FinalizeConversationDetachIntents(&st, now.Add(time.Minute))
	if got := strings.Join(finalized.Draining, ","); got != "req-a" {
		t.Fatalf("active detach finalization = %#v, want draining req-a", finalized)
	}
	req = st.Allocations["req-a"]
	if req.State != AllocationRunning || !req.CancelRequestedAt.IsZero() || strings.Join(st.Machines["machine-1"].Chats, ",") != "chat-a,chat-b" {
		t.Fatalf("active detach should leave provider and chat membership untouched: req=%#v machine=%#v", req, st.Machines["machine-1"])
	}

	job := st.JobAttempts["job-a"]
	job.Phase = JobTerminal
	st.JobAttempts[job.ID] = job
	finalized = FinalizeConversationDetachIntents(&st, now.Add(2*time.Minute))
	if got := strings.Join(finalized.Detached, ","); got != "req-a" {
		t.Fatalf("final detached = %q, want req-a result=%#v", got, finalized)
	}
	req = st.Allocations["req-a"]
	if req.State != AllocationCanceled || !req.CancelRequestedAt.IsZero() || !req.DetachRequestedAt.IsZero() {
		t.Fatalf("drained shared detach should only cancel this allocation: %#v", req)
	}
	if got := strings.Join(st.Machines["machine-1"].Chats, ","); got != "chat-b" {
		t.Fatalf("machine chats = %q, want chat-b", got)
	}
	if st.Allocations["req-b"].State != AllocationRunning {
		t.Fatalf("other chat allocation should keep running: %#v", st.Allocations["req-b"])
	}
}

func TestFinalizeConversationDetachEscalatesWhenNoOtherChatRemains(t *testing.T) {
	now := time.Unix(3, 0)
	st := State{
		Allocations: map[string]AllocationRequest{
			"req-a": {
				ID:                "req-a",
				ConversationID:    "chat-a",
				Profile:           "gpu",
				Provider:          ProviderSlurm,
				ProviderIdentity:  ProviderIdentity{ProviderJobID: "slurm-1"},
				State:             AllocationRunning,
				DetachRequestedAt: now,
				DetachReason:      "user released work chat",
			},
		},
		Machines: map[string]Machine{
			"machine-1": {ID: "machine-1", LeaseID: "lease-1", ProviderJobID: "slurm-1", Profile: "gpu", State: string(LeaseAccepting), Chats: []string{"chat-a"}},
		},
		JobAttempts: map[string]JobAttempt{
			"job-a": {ID: "job-a", RequestID: "req-a", TurnID: "turn-a", Phase: JobTerminal},
		},
	}
	finalized := FinalizeConversationDetachIntents(&st, now.Add(time.Minute))
	if got := strings.Join(finalized.Escalated, ","); got != "req-a" {
		t.Fatalf("escalated = %q, want req-a result=%#v", got, finalized)
	}
	req := st.Allocations["req-a"]
	if req.State != AllocationRunning || req.CancelRequestedAt.IsZero() || !req.DetachRequestedAt.IsZero() {
		t.Fatalf("last-chat detach should become normal provider cancel intent: %#v", req)
	}
}

func TestDrainIdleWorkerMachinesOnlyDrainsNoDemandMachines(t *testing.T) {
	now := time.Unix(1000, 0)
	st := State{Machines: map[string]Machine{
		"idle":      {ID: "idle", State: string(LeaseAccepting), UpdatedAt: now.Add(-time.Hour)},
		"chat":      {ID: "chat", State: string(LeaseAccepting), UpdatedAt: now.Add(-time.Hour), Chats: []string{"s001"}},
		"job":       {ID: "job", State: string(LeaseAccepting), UpdatedAt: now.Add(-time.Hour), Jobs: []string{"job-1"}},
		"fresh":     {ID: "fresh", State: string(LeaseAccepting), UpdatedAt: now.Add(-time.Minute)},
		"draining":  {ID: "draining", State: string(LeaseDraining), UpdatedAt: now.Add(-time.Hour)},
		"exclusive": {ID: "exclusive", State: string(LeaseAccepting), Isolation: IsolationExclusive, UpdatedAt: now.Add(-time.Hour)},
		"external":  {ID: "external", State: string(LeaseAccepting), ExternalOwned: true, UpdatedAt: now.Add(-time.Hour)},
	}}
	drained := DrainIdleWorkerMachines(&st, 30*time.Minute, now)
	if got := len(drained); got != 2 {
		t.Fatalf("DrainIdleWorkerMachines drained %d, want 2: %#v", got, drained)
	}
	if st.Machines["idle"].State != string(LeaseDraining) || st.Machines["exclusive"].State != string(LeaseDraining) {
		t.Fatalf("idle shared/exclusive machines should drain: %#v", st.Machines)
	}
	for _, id := range []string{"chat", "job", "fresh", "external"} {
		if st.Machines[id].State != string(LeaseAccepting) {
			t.Fatalf("machine %s should stay accepting: %#v", id, st.Machines[id])
		}
	}
}

func TestUpgradeBlockersAreOperationSpecific(t *testing.T) {
	if blockers := UpgradeBlockers(UpgradeInput{Operation: UpgradeHelperRestart, QueuedTeamsTurns: 1}); len(blockers) != 0 {
		t.Fatalf("helper restart may preserve queued turns, got %#v", blockers)
	}
	if blockers := UpgradeBlockers(UpgradeInput{Operation: UpgradeHelperRestart, RunningTeamsTurns: 1, Force: true}); !contains(blockers, "running_teams_turn") {
		t.Fatalf("force must not bypass running Teams turns, got %#v", blockers)
	}
	if blockers := UpgradeBlockers(UpgradeInput{Operation: UpgradePrelistenCodex, QueuedTeamsTurns: 1}); !contains(blockers, "queued_teams_turn") {
		t.Fatalf("Codex upgrade must block queued turns, got %#v", blockers)
	}
	if blockers := UpgradeBlockers(UpgradeInput{Operation: UpgradeBeaconCodexTarget, ActiveBeaconOtherTarget: 1}); len(blockers) != 0 {
		t.Fatalf("per-target Codex upgrade should ignore other target, got %#v", blockers)
	}
}

func TestUpgradeBlockersForStateClassifiesBeaconWork(t *testing.T) {
	st := State{
		Machines: map[string]Machine{
			"idle": {
				ID:      "idle",
				LeaseID: "lease-idle",
				State:   "accepting",
			},
			"active": {
				ID:            "active",
				LeaseID:       "lease-active",
				ProviderJobID: "slurm-1",
				Profile:       "gpu",
				State:         "accepting",
				Jobs:          []string{"job-1"},
			},
			"ambiguous": {
				ID:      "ambiguous",
				LeaseID: "lease-ambiguous",
				State:   "ambiguous",
			},
		},
		Conversations: map[string]Conversation{
			"conv": {
				ID: "conv",
				Queued: []QueuedTurn{
					{ID: "turn-gpu", Snapshot: TargetSnapshot{Target: TargetBeacon, Profile: "gpu", Signature: "sig-gpu"}},
					{ID: "turn-cpu", Snapshot: TargetSnapshot{Target: TargetBeacon, Profile: "cpu", Signature: "sig-cpu"}},
					{ID: "turn-local", Snapshot: TargetSnapshot{Target: TargetLocal}},
				},
			},
		},
	}
	helper := UpgradeBlockersForState(st, UpgradePendingReplacement, "")
	if len(helper) != 2 || helper[0].Kind != "beacon_job" || helper[1].Kind != "beacon_marker" {
		t.Fatalf("helper blockers should include active job and ambiguous marker only, got %#v", helper)
	}
	target := UpgradeBlockersForState(st, UpgradeBeaconCodexTarget, "sig-gpu")
	if !hasUpgradeBlocker(target, "beacon_queued_turn", "turn-gpu") {
		t.Fatalf("target upgrade should block matching queued turn, got %#v", target)
	}
	if hasUpgradeBlocker(target, "beacon_queued_turn", "turn-cpu") {
		t.Fatalf("target upgrade should ignore other signature, got %#v", target)
	}
}

func TestUpgradeBlockersForStateIncludesActiveAllocations(t *testing.T) {
	st := State{
		Allocations: map[string]AllocationRequest{
			"req-active": {
				ID:             "req-active",
				ConversationID: "conv",
				TurnID:         "turn",
				Profile:        "gpu",
				State:          AllocationSubmitted,
				Execution:      ExecutionSignature{Hash: "sig-gpu"},
				RenewEpoch:     2,
				ReplacementID:  "req-replacement",
				ProviderIdentity: ProviderIdentity{
					ProviderJobID: "slurm-1",
				},
			},
			"req-persisted": {
				ID:        "req-persisted",
				State:     AllocationRequestPersisted,
				Profile:   "gpu",
				Execution: ExecutionSignature{Hash: "sig-gpu"},
			},
			"req-canceled": {ID: "req-canceled", State: AllocationCanceled, Execution: ExecutionSignature{Hash: "sig-gpu"}},
			"req-other":    {ID: "req-other", State: AllocationSubmitted, Execution: ExecutionSignature{Hash: "sig-other"}},
		},
	}
	blockers := UpgradeBlockersForState(st, UpgradeBeaconCodexTarget, "sig-gpu")
	if !hasUpgradeBlocker(blockers, "beacon_allocation", "req-active") {
		t.Fatalf("target upgrade should block matching active allocation, got %#v", blockers)
	}
	if hasUpgradeBlocker(blockers, "beacon_allocation", "req-canceled") ||
		hasUpgradeBlocker(blockers, "beacon_allocation", "req-other") ||
		hasUpgradeBlocker(blockers, "beacon_allocation", "req-persisted") {
		t.Fatalf("target upgrade should ignore canceled, other-target, and no-resource allocations, got %#v", blockers)
	}
	if !strings.Contains(blockers[0].Detail, "renew_epoch=2") || !strings.Contains(blockers[0].Detail, "replacement=req-replacement") {
		t.Fatalf("allocation blocker should expose renewal/replacement detail, got %#v", blockers)
	}
}

func TestArtifactTerminalAndAuditSafety(t *testing.T) {
	if err := ValidateArtifact(ArtifactRef{
		Root:                 "/shared/job/artifacts",
		Path:                 "/shared/job/artifacts/report.txt",
		DeclaredHash:         "hash",
		ActualHash:           "hash",
		Size:                 10,
		Limit:                100,
		OpenedNoFollow:       true,
		FstatStable:          true,
		HashFromOpenedFile:   true,
		StagedFromOpenedFile: true,
		HardlinkCount:        1,
		UploadOK:             true,
	}); err != nil {
		t.Fatalf("valid artifact: %v", err)
	}
	if err := ValidateArtifact(ArtifactRef{Root: "/shared/job/artifacts", Path: "/shared/job/artifacts/missing", Missing: true}); err == nil {
		t.Fatal("missing artifact should fail")
	}
	var st State
	terminal, err := AcceptTerminal(&st, "job-1", []byte(`{"ok":true}`), time.Unix(1, 0))
	if err != nil || terminal.Action != "complete" || !terminal.OutboxQueued {
		t.Fatalf("first terminal = %#v err=%v", terminal, err)
	}
	terminal, err = AcceptTerminal(&st, "job-1", []byte(`{"ok":true}`), time.Unix(2, 0))
	if err != nil || terminal.Action != "complete" || terminal.OutboxQueued {
		t.Fatalf("duplicate terminal should be no-op, got %#v err=%v", terminal, err)
	}
	terminal, err = AcceptTerminal(&st, "job-1", []byte(`{"ok":false}`), time.Unix(3, 0))
	if err != nil || terminal.Action != "quarantine" {
		t.Fatalf("conflicting terminal should quarantine, got %#v err=%v", terminal, err)
	}
	if _, err := AppendAudit(&st, "terminal_accept", "job-1", time.Unix(4, 0)); err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}
	if err := ValidateAudit(st); err != nil {
		t.Fatalf("ValidateAudit: %v", err)
	}
	st.Audit = st.Audit[:0]
	if err := ValidateAudit(st); err == nil {
		t.Fatal("audit truncation should be detected")
	}
}

func TestArtifactValidationRejectsUnsafeVariants(t *testing.T) {
	base := ArtifactRef{
		Root:                 "/shared/job/artifacts",
		Path:                 "/shared/job/artifacts/report.txt",
		DeclaredHash:         "hash",
		ActualHash:           "hash",
		Size:                 10,
		Limit:                100,
		OpenedNoFollow:       true,
		FstatStable:          true,
		HashFromOpenedFile:   true,
		StagedFromOpenedFile: true,
		HardlinkCount:        1,
		UploadOK:             true,
	}
	cases := map[string]ArtifactRef{
		"symlink race":           copyArtifact(base, func(ref *ArtifactRef) { ref.OpenedNoFollow = false }),
		"unstable fstat":         copyArtifact(base, func(ref *ArtifactRef) { ref.FstatStable = false }),
		"hash not opened file":   copyArtifact(base, func(ref *ArtifactRef) { ref.HashFromOpenedFile = false }),
		"staged not opened file": copyArtifact(base, func(ref *ArtifactRef) { ref.StagedFromOpenedFile = false }),
		"hardlink":               copyArtifact(base, func(ref *ArtifactRef) { ref.HardlinkCount = 2 }),
		"zero size":              copyArtifact(base, func(ref *ArtifactRef) { ref.Size = 0 }),
		"too large":              copyArtifact(base, func(ref *ArtifactRef) { ref.Size = 101 }),
		"hash mismatch":          copyArtifact(base, func(ref *ArtifactRef) { ref.ActualHash = "other" }),
		"outside root":           copyArtifact(base, func(ref *ArtifactRef) { ref.Path = "/shared/job/report.txt" }),
		"upload failed":          copyArtifact(base, func(ref *ArtifactRef) { ref.UploadOK = false }),
		"worker delivery field":  copyArtifact(base, func(ref *ArtifactRef) { ref.WorkerDeliveryField = true }),
	}
	for name, ref := range cases {
		if err := ValidateArtifact(ref); err == nil {
			t.Fatalf("%s should be rejected", name)
		}
	}
}

func TestTerminalRejectsEmptyJobAndRepairsMissingOutboxFlag(t *testing.T) {
	var st State
	if _, err := AcceptTerminal(&st, "", []byte(`{"ok":true}`), time.Unix(1, 0)); err == nil {
		t.Fatal("empty terminal job id should fail")
	}
	first, err := AcceptTerminal(&st, "job-1", []byte(`{"ok":true}`), time.Unix(2, 0))
	if err != nil || !first.OutboxQueued {
		t.Fatalf("first terminal = %#v err=%v", first, err)
	}
	rec := st.Terminals["job-1"]
	rec.OutboxQueued = false
	st.Terminals["job-1"] = rec
	second, err := AcceptTerminal(&st, "job-1", []byte(`{"ok":true}`), time.Unix(3, 0))
	if err != nil || second.Action != "complete" || !second.OutboxQueued {
		t.Fatalf("duplicate terminal should repair missing outbox flag, got %#v err=%v", second, err)
	}
}

func TestAuditValidationRejectsTamperAndSecretLeak(t *testing.T) {
	var st State
	if _, err := AppendAudit(&st, "machine_release", "lease-1", time.Unix(1, 0)); err != nil {
		t.Fatalf("AppendAudit first: %v", err)
	}
	if _, err := AppendAudit(&st, "machine_kill", "lease-2", time.Unix(2, 0)); err != nil {
		t.Fatalf("AppendAudit second: %v", err)
	}
	for name, mutate := range map[string]func(State) State{
		"seq gap": func(in State) State {
			in.Audit[1].Seq = 3
			return in
		},
		"bad prev": func(in State) State {
			in.Audit[1].PrevHash = "wrong"
			return in
		},
		"secret leak": func(in State) State {
			in.Audit[0].Secret = "token"
			return in
		},
		"hash tamper": func(in State) State {
			in.Audit[0].Target = "lease-other"
			return in
		},
		"head mismatch": func(in State) State {
			in.AuditHead.Hash = "wrong"
			return in
		},
	} {
		copied := cloneAuditState(st)
		if err := ValidateAudit(mutate(copied)); err == nil {
			t.Fatalf("%s should be rejected", name)
		}
	}
}

func TestStoreRejectsCorruptOversizedAndUnsupportedState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.Load(); err == nil {
		t.Fatal("corrupt JSON should fail")
	}
	big := strings.Repeat(" ", 16<<20+1)
	if err := os.WriteFile(path, []byte(big), 0o600); err != nil {
		t.Fatalf("write big: %v", err)
	}
	if _, err := store.Load(); err == nil {
		t.Fatal("oversized state should fail")
	}
	if err := os.WriteFile(path, []byte(`{"version":999}`), 0o600); err != nil {
		t.Fatalf("write unsupported version: %v", err)
	}
	if _, err := store.Load(); err == nil || !strings.Contains(err.Error(), "unsupported beacon state version") {
		t.Fatalf("unsupported version error = %v", err)
	}
	if err := store.Save(State{Version: 999}); err == nil {
		t.Fatal("Save should refuse unsupported version")
	}
	parentFile := filepath.Join(dir, "parent-file")
	if err := os.WriteFile(parentFile, []byte("not a dir"), 0o600); err != nil {
		t.Fatalf("write parent file: %v", err)
	}
	if _, err := NewStore(filepath.Join(parentFile, "state.json")); err == nil {
		t.Fatal("NewStore should reject a non-directory parent")
	}
}

func copyArtifact(in ArtifactRef, fn func(*ArtifactRef)) ArtifactRef {
	fn(&in)
	return in
}

func cloneAuditState(in State) State {
	out := in
	out.Audit = append([]AuditRecord(nil), in.Audit...)
	return out
}

func hasUpgradeBlocker(values []UpgradeBlocker, kind string, id string) bool {
	for _, value := range values {
		if value.Kind == kind && value.ID == id {
			return true
		}
	}
	return false
}

func readyProfile(name string) Profile {
	return Profile{
		Name:              name,
		Provider:          ProviderLocal,
		ProxyMode:         ProxyNone,
		IsolationDefault:  IsolationShared,
		Confirmed:         true,
		ProviderPreviewOK: true,
		DoctorOK:          true,
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
