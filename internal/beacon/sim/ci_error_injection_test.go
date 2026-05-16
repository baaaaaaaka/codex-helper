package sim

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type ciFault string
type ciPhase string
type ciOutcome string

const (
	ciSubmitRateLimited      ciFault = "submit_rate_limited"
	ciSubmitDenied           ciFault = "submit_denied"
	ciImageMissing           ciFault = "image_missing"
	ciProxyAuthFailed        ciFault = "proxy_auth_failed"
	ciSharedRootUnavailable  ciFault = "shared_root_unavailable"
	ciWorkerBinaryMissing    ciFault = "worker_binary_missing"
	ciProtocolMismatch       ciFault = "protocol_mismatch"
	ciWorkerStartTimeout     ciFault = "worker_start_timeout"
	ciHeartbeatStalled       ciFault = "heartbeat_stalled"
	ciLeaseEndedEarly        ciFault = "lease_ended_early"
	ciNodeOOMKilled          ciFault = "node_oom_killed"
	ciGPUOOMResult           ciFault = "gpu_oom_result"
	ciDiskFull               ciFault = "disk_full"
	ciTerminalPartialWrite   ciFault = "terminal_partial_write"
	ciArtifactUploadTimeout  ciFault = "artifact_upload_timeout"
	ciProviderCompletedEarly ciFault = "provider_completed_early"
	ciStoreLockContention    ciFault = "store_lock_contention"

	ciBeforeSubmit ciPhase = "before_submit"
	ciBeforeClaim  ciPhase = "before_claim"
	ciAfterClaim   ciPhase = "after_claim"
	ciAfterStart   ciPhase = "after_start"
	ciFinalizing   ciPhase = "finalizing"

	ciRetryRequest      ciOutcome = "retry_request"
	ciProfileDraft      ciOutcome = "profile_draft"
	ciNoClaim           ciOutcome = "no_claim"
	ciNeedsAttention    ciOutcome = "needs_attention"
	ciAmbiguous         ciOutcome = "ambiguous"
	ciFailedResult      ciOutcome = "failed_result"
	ciReconcileProvider ciOutcome = "reconcile_provider"
)

func classifyCIFault(fault ciFault, phase ciPhase) ciOutcome {
	switch fault {
	case ciSubmitRateLimited:
		return ciRetryRequest
	case ciSubmitDenied, ciImageMissing, ciProxyAuthFailed:
		return ciProfileDraft
	case ciSharedRootUnavailable, ciWorkerBinaryMissing, ciProtocolMismatch:
		return ciNeedsAttention
	case ciWorkerStartTimeout:
		if phase == ciBeforeClaim {
			return ciNoClaim
		}
		return ciAmbiguous
	case ciHeartbeatStalled, ciLeaseEndedEarly, ciProviderCompletedEarly:
		return ciReconcileProvider
	case ciNodeOOMKilled:
		if phase == ciAfterStart {
			return ciAmbiguous
		}
		return ciNoClaim
	case ciGPUOOMResult:
		return ciFailedResult
	case ciDiskFull:
		if phase == ciBeforeClaim {
			return ciNoClaim
		}
		return ciNeedsAttention
	case ciTerminalPartialWrite, ciArtifactUploadTimeout:
		return ciNeedsAttention
	case ciStoreLockContention:
		return ciRetryRequest
	default:
		return ciNeedsAttention
	}
}

func TestCIFaultInjectionMatrixCoversClusterOperationalFailures(t *testing.T) {
	tests := []struct {
		name  string
		fault ciFault
		phase ciPhase
		want  ciOutcome
	}{
		{name: "scheduler rate limit retries idempotent request", fault: ciSubmitRateLimited, phase: ciBeforeSubmit, want: ciRetryRequest},
		{name: "permission denied keeps profile draft", fault: ciSubmitDenied, phase: ciBeforeSubmit, want: ciProfileDraft},
		{name: "missing image keeps profile draft", fault: ciImageMissing, phase: ciBeforeSubmit, want: ciProfileDraft},
		{name: "proxy auth failure keeps profile draft", fault: ciProxyAuthFailed, phase: ciBeforeSubmit, want: ciProfileDraft},
		{name: "shared root missing quarantines machine before accepting", fault: ciSharedRootUnavailable, phase: ciBeforeClaim, want: ciNeedsAttention},
		{name: "worker binary missing quarantines machine before accepting", fault: ciWorkerBinaryMissing, phase: ciBeforeClaim, want: ciNeedsAttention},
		{name: "protocol mismatch quarantines machine before accepting", fault: ciProtocolMismatch, phase: ciBeforeClaim, want: ciNeedsAttention},
		{name: "start timeout before claim is safe no-claim", fault: ciWorkerStartTimeout, phase: ciBeforeClaim, want: ciNoClaim},
		{name: "start timeout after claim is ambiguous", fault: ciWorkerStartTimeout, phase: ciAfterClaim, want: ciAmbiguous},
		{name: "heartbeat stall reconciles with provider", fault: ciHeartbeatStalled, phase: ciAfterStart, want: ciReconcileProvider},
		{name: "lease ended early reconciles with provider", fault: ciLeaseEndedEarly, phase: ciAfterStart, want: ciReconcileProvider},
		{name: "node oom after start is ambiguous", fault: ciNodeOOMKilled, phase: ciAfterStart, want: ciAmbiguous},
		{name: "node oom before claim does not replay claimed work", fault: ciNodeOOMKilled, phase: ciBeforeClaim, want: ciNoClaim},
		{name: "gpu oom envelope is failed result", fault: ciGPUOOMResult, phase: ciAfterStart, want: ciFailedResult},
		{name: "disk full before claim leaves no claim", fault: ciDiskFull, phase: ciBeforeClaim, want: ciNoClaim},
		{name: "disk full after claim needs attention", fault: ciDiskFull, phase: ciAfterClaim, want: ciNeedsAttention},
		{name: "partial terminal write needs attention", fault: ciTerminalPartialWrite, phase: ciFinalizing, want: ciNeedsAttention},
		{name: "artifact upload timeout needs attention", fault: ciArtifactUploadTimeout, phase: ciFinalizing, want: ciNeedsAttention},
		{name: "provider completed before terminal is reconciled", fault: ciProviderCompletedEarly, phase: ciAfterStart, want: ciReconcileProvider},
		{name: "store lock contention retries", fault: ciStoreLockContention, phase: ciBeforeClaim, want: ciRetryRequest},
	}
	for _, tt := range tests {
		if got := classifyCIFault(tt.fault, tt.phase); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}

func claimCIJob(root, jobID, workerID string) bool {
	path := filepath.Join(root, jobID+".claim")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return false
	}
	defer f.Close()
	_, _ = f.WriteString(workerID)
	return true
}

func TestCIAtomicQueueClaimsDoNotCollideUnderParallelWorkers(t *testing.T) {
	root := t.TempDir()
	const workers = 48
	var wg sync.WaitGroup
	results := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results <- claimCIJob(root, "job-shared", string(rune('a'+i%26)))
		}(i)
	}
	wg.Wait()
	close(results)
	success := 0
	for ok := range results {
		if ok {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("parallel workers claimed same job %d times, want exactly 1", success)
	}

	for i := 0; i < workers; i++ {
		if !claimCIJob(root, "job-unique-"+string(rune('a'+i%26))+string(rune('A'+i/26)), "worker") {
			t.Fatalf("unique job %d should be claimable", i)
		}
	}
}

type ciLeaseFile struct {
	leaseAlive        bool
	heartbeatFresh    bool
	providerReachable bool
	fileAgeSeconds    int
	ttlSeconds        int
	tombstone         bool
	jobStarted        bool
}

type ciCleanupAction string

const (
	ciKeepFile       ciCleanupAction = "keep"
	ciDeleteZombie   ciCleanupAction = "delete_zombie"
	ciQuarantineFile ciCleanupAction = "quarantine"
	ciKeepTombstone  ciCleanupAction = "keep_tombstone"
	ciReconcileFirst ciCleanupAction = "reconcile_first"
)

func cleanupCILeaseFile(in ciLeaseFile) ciCleanupAction {
	if in.tombstone {
		return ciKeepTombstone
	}
	if !in.providerReachable {
		return ciReconcileFirst
	}
	if in.leaseAlive && in.heartbeatFresh {
		return ciKeepFile
	}
	if in.jobStarted {
		return ciQuarantineFile
	}
	if in.fileAgeSeconds > in.ttlSeconds {
		return ciDeleteZombie
	}
	return ciKeepFile
}

func TestCIZombieCleanupKeepsLiveAndAmbiguousFiles(t *testing.T) {
	tests := []struct {
		name string
		in   ciLeaseFile
		want ciCleanupAction
	}{
		{name: "live lease keeps file", in: ciLeaseFile{leaseAlive: true, heartbeatFresh: true, providerReachable: true, fileAgeSeconds: 999, ttlSeconds: 60}, want: ciKeepFile},
		{name: "tombstone is retained forever", in: ciLeaseFile{tombstone: true, providerReachable: true, fileAgeSeconds: 999, ttlSeconds: 60}, want: ciKeepTombstone},
		{name: "provider outage reconciles before deleting", in: ciLeaseFile{providerReachable: false, fileAgeSeconds: 999, ttlSeconds: 60}, want: ciReconcileFirst},
		{name: "started job is quarantined not deleted", in: ciLeaseFile{providerReachable: true, jobStarted: true, fileAgeSeconds: 999, ttlSeconds: 60}, want: ciQuarantineFile},
		{name: "expired unstarted temp is deleted", in: ciLeaseFile{providerReachable: true, fileAgeSeconds: 999, ttlSeconds: 60}, want: ciDeleteZombie},
		{name: "young unstarted temp is kept", in: ciLeaseFile{providerReachable: true, fileAgeSeconds: 10, ttlSeconds: 60}, want: ciKeepFile},
	}
	for _, tt := range tests {
		if got := cleanupCILeaseFile(tt.in); got != tt.want {
			t.Fatalf("%s: got %s want %s", tt.name, got, tt.want)
		}
	}
}
