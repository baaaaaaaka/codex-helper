package sim

import (
	"path/filepath"
	"strings"
	"testing"
)

type slurmProfileFields struct {
	nodes     int
	gpuCount  int
	partition string
	image     string
	duration  int
}

func slurmProfileValid(in slurmProfileFields) bool {
	return in.nodes > 0 &&
		in.gpuCount >= 0 &&
		strings.TrimSpace(in.partition) != "" &&
		strings.TrimSpace(in.image) != "" &&
		in.duration > 0
}

func TestSlurmProfileValidationRequiresSimpleWizardFields(t *testing.T) {
	base := slurmProfileFields{nodes: 1, gpuCount: 1, partition: "interactive", image: "image.sqsh", duration: 4}
	if !slurmProfileValid(base) {
		t.Fatal("complete Slurm wizard fields should validate")
	}
	for name, in := range map[string]slurmProfileFields{
		"nodes":     copySlurmProfile(base, func(p *slurmProfileFields) { p.nodes = 0 }),
		"gpu count": copySlurmProfile(base, func(p *slurmProfileFields) { p.gpuCount = -1 }),
		"partition": copySlurmProfile(base, func(p *slurmProfileFields) { p.partition = "" }),
		"image":     copySlurmProfile(base, func(p *slurmProfileFields) { p.image = "" }),
		"duration":  copySlurmProfile(base, func(p *slurmProfileFields) { p.duration = 0 }),
	} {
		if slurmProfileValid(in) {
			t.Fatalf("missing/invalid %s should keep Slurm profile draft: %#v", name, in)
		}
	}
}

func copySlurmProfile(in slurmProfileFields, fn func(*slurmProfileFields)) slurmProfileFields {
	fn(&in)
	return in
}

type membershipSource string

const (
	sourceSchedulerWorker membershipSource = "scheduler_worker"
	sourceLoginNode       membershipSource = "login_node"
	sourceSystemSSHD      membershipSource = "system_sshd"
	sourceDropbearInJob   membershipSource = "dropbear_in_job"
)

type membershipProofInput struct {
	source           membershipSource
	schedulerEnv     bool
	cgroupMatches    bool
	hostInAllocation bool
}

func membershipProofValid(in membershipProofInput) bool {
	if in.source == sourceLoginNode || in.source == sourceSystemSSHD {
		return false
	}
	return in.schedulerEnv && in.cgroupMatches && in.hostInAllocation
}

func TestMembershipProofRejectsLoginNodeAndSystemSSHDExecution(t *testing.T) {
	if !membershipProofValid(membershipProofInput{source: sourceSchedulerWorker, schedulerEnv: true, cgroupMatches: true, hostInAllocation: true}) {
		t.Fatal("scheduler-launched worker with matching env/cgroup/host should pass")
	}
	if !membershipProofValid(membershipProofInput{source: sourceDropbearInJob, schedulerEnv: true, cgroupMatches: true, hostInAllocation: true}) {
		t.Fatal("job-launched dropbear should pass when membership proof matches")
	}
	for _, in := range []membershipProofInput{
		{source: sourceLoginNode, schedulerEnv: true, cgroupMatches: true, hostInAllocation: true},
		{source: sourceSystemSSHD, schedulerEnv: true, cgroupMatches: true, hostInAllocation: true},
		{source: sourceSchedulerWorker, schedulerEnv: false, cgroupMatches: true, hostInAllocation: true},
		{source: sourceSchedulerWorker, schedulerEnv: true, cgroupMatches: false, hostInAllocation: true},
		{source: sourceSchedulerWorker, schedulerEnv: true, cgroupMatches: true, hostInAllocation: false},
	} {
		if membershipProofValid(in) {
			t.Fatalf("invalid membership proof should reject execution: %#v", in)
		}
	}
}

type pathReuseStore struct {
	tombstones map[string]bool
	tempFiles  map[string]bool
}

func newPathReuseStore() *pathReuseStore {
	return &pathReuseStore{tombstones: map[string]bool{}, tempFiles: map[string]bool{}}
}

func (s *pathReuseStore) tombstone(path string) {
	s.tombstones[filepath.Clean(path)] = true
}

func (s *pathReuseStore) canCreateJobPath(path string) bool {
	return !s.tombstones[filepath.Clean(path)]
}

func (s *pathReuseStore) reapTemp(path string, leaseAlive bool) bool {
	clean := filepath.Clean(path)
	if leaseAlive || !s.tempFiles[clean] {
		return false
	}
	delete(s.tempFiles, clean)
	return true
}

func TestCleanupRejectsTombstonePathReuseAndReapsZombieTemps(t *testing.T) {
	store := newPathReuseStore()
	store.tombstone("/shared/beacon/jobs/job-1")
	if store.canCreateJobPath("/shared/beacon/jobs/job-1") {
		t.Fatal("job path must never be reused after tombstone")
	}
	store.tempFiles["/shared/beacon/jobs/job-2/result.tmp"] = true
	if store.reapTemp("/shared/beacon/jobs/job-2/result.tmp", true) {
		t.Fatal("temp file for live lease must not be reaped")
	}
	if !store.reapTemp("/shared/beacon/jobs/job-2/result.tmp", false) {
		t.Fatal("zombie temp file should be reaped after lease is not live")
	}
}

type artifactIngestResult string

const (
	artifactAccepted       artifactIngestResult = "accepted"
	artifactNeedsAttention artifactIngestResult = "needs_attention"
)

func artifactIngestResultFor(ref artifactRef, uploadOK bool, missing bool) artifactIngestResult {
	if missing || !uploadOK || !artifactIngestAllowed(ref) {
		return artifactNeedsAttention
	}
	return artifactAccepted
}

func TestArtifactMissingAndUploadFailureBecomeProtectedNeedsAttention(t *testing.T) {
	base := artifactRef{
		sharedRoot:           "/shared/beacon/jobs/job-1/artifacts",
		path:                 "/shared/beacon/jobs/job-1/artifacts/report.txt",
		declaredHash:         "hash",
		actualHash:           "hash",
		size:                 10,
		limit:                100,
		openedNoFollow:       true,
		fstatStable:          true,
		hashFromOpenedFile:   true,
		stagedFromOpenedFile: true,
		hardlinkCount:        1,
	}
	if got := artifactIngestResultFor(base, true, false); got != artifactAccepted {
		t.Fatalf("valid uploaded artifact should be accepted, got %s", got)
	}
	if got := artifactIngestResultFor(base, false, false); got != artifactNeedsAttention {
		t.Fatalf("upload failure should become protected needs-attention, got %s", got)
	}
	if got := artifactIngestResultFor(base, true, true); got != artifactNeedsAttention {
		t.Fatalf("missing artifact should become protected needs-attention, got %s", got)
	}
}

func TestHelperLifecycleOperationsShareBeaconBlockerRules(t *testing.T) {
	for _, op := range []upgradeOperation{upgradeHelperReload, upgradeHelperRestart, upgradePendingReplacement} {
		if operationUpgradeBlocked(operationUpgradeInput{op: op, queuedTeamsTurns: 1}) {
			t.Fatalf("%s should preserve queued Teams turns", op)
		}
		if !operationUpgradeBlocked(operationUpgradeInput{op: op, runningTeamsTurns: 1}) {
			t.Fatalf("%s should block running Teams turns", op)
		}
		if !operationUpgradeBlocked(operationUpgradeInput{op: op, activeBeaconSameTarget: 1}) {
			t.Fatalf("%s should block active beacon work", op)
		}
		if !operationUpgradeBlocked(operationUpgradeInput{op: op, protectedOutbox: 1}) {
			t.Fatalf("%s should block protected outbox", op)
		}
	}
}
