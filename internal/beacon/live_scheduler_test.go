package beacon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLiveSchedulerProfileAdapterSubmitCancel(t *testing.T) {
	if os.Getenv("CODEX_HELPER_BEACON_LIVE") != "1" {
		t.Skip("set CODEX_HELPER_BEACON_LIVE=1 on a scheduler-capable CI runner to exercise real provider adapters")
	}
	provider := Provider(strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_BEACON_LIVE_PROVIDER"))))
	if provider == "" {
		provider = ProviderSlurm
	}
	envConfig := ProviderCommandConfigFromEnv(os.Getenv)
	profile := liveSchedulerProfileFromEnv(t, provider, envConfig)
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	var req AllocationRequest
	now := time.Now()
	if err := store.Update(func(st *State) error {
		st.Profiles[profile.Name] = profile
		var err error
		req, _, err = EnsureAllocationRequest(st, "live-conv", "live-turn", TargetSnapshot{Target: TargetBeacon, Profile: profile.Name}, now)
		return err
	}); err != nil {
		t.Fatalf("seed live allocation: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	adapter := CommandProviderAdapter{Runner: ExecProviderCommandRunner{}}
	updated, action, err := ReconcileAllocationSubmitOutsideLock(ctx, store, req.ID, adapter, time.Now())
	if err != nil {
		t.Fatalf("live reconcile submit action=%s req=%#v err=%v", action, updated, err)
	}
	if action != AllocationSubmitNow && action != AllocationSubmitAdopt && action != AllocationSubmitAlreadyKnown {
		t.Fatalf("unexpected live submit action=%s req=%#v", action, updated)
	}
	if strings.TrimSpace(updated.ProviderIdentity.ProviderJobID) == "" {
		t.Fatalf("live provider did not return a provider job id: %#v", updated)
	}
	cancelResult, cancelErr := CancelAllocationOutsideLock(ctx, store, updated.ID, adapter, "live scheduler test cleanup", true, time.Now())
	if cancelErr != nil {
		t.Fatalf("live provider cancel failed; allocation may need manual cleanup provider_job=%s: %v", updated.ProviderIdentity.ProviderJobID, cancelErr)
	}
	if cancelResult.Request.State != AllocationCanceled {
		t.Fatalf("live cancel result = %#v", cancelResult)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := ValidateAudit(st); err != nil {
		t.Fatalf("ValidateAudit: %v", err)
	}
}

func liveSchedulerProfileFromEnv(t *testing.T, provider Provider, envConfig ProviderCommandConfig) Profile {
	t.Helper()
	profile := Profile{
		Name:              firstNonEmpty(os.Getenv("CODEX_HELPER_BEACON_LIVE_PROFILE"), "live"),
		Provider:          provider,
		ProxyMode:         ProxyNone,
		IsolationDefault:  IsolationExclusive,
		Confirmed:         true,
		ProviderPreviewOK: true,
		DoctorOK:          true,
		Adapter:           envConfig,
	}
	switch provider {
	case ProviderSlurm:
		if strings.TrimSpace(envConfig.SlurmQueryCommand) == "" || strings.TrimSpace(envConfig.SlurmSubmitCommand) == "" || strings.TrimSpace(envConfig.SlurmCancelCommand) == "" {
			t.Fatalf("CODEX_HELPER_BEACON_LIVE=1 requires %s, %s, and %s", BeaconSlurmQueryCommandEnv, BeaconSlurmSubmitCommandEnv, BeaconSlurmCancelCommandEnv)
		}
		profile.Slurm = SlurmProfile{
			Nodes:     liveSchedulerIntEnv("CODEX_HELPER_BEACON_LIVE_NODES", 1),
			GPUCount:  liveSchedulerIntEnv("CODEX_HELPER_BEACON_LIVE_GPU", 0),
			Partition: strings.TrimSpace(os.Getenv("CODEX_HELPER_BEACON_LIVE_PARTITION")),
			Image:     strings.TrimSpace(os.Getenv("CODEX_HELPER_BEACON_LIVE_IMAGE")),
			Duration:  liveSchedulerIntEnv("CODEX_HELPER_BEACON_LIVE_DURATION", 1),
		}
	case ProviderLSF:
		if strings.TrimSpace(envConfig.LSFQueryCommand) == "" || strings.TrimSpace(envConfig.LSFSubmitCommand) == "" || strings.TrimSpace(envConfig.LSFCancelCommand) == "" {
			t.Fatalf("CODEX_HELPER_BEACON_LIVE=1 requires %s, %s, and %s", BeaconLSFQueryCommandEnv, BeaconLSFSubmitCommandEnv, BeaconLSFCancelCommandEnv)
		}
		profile.LSF = LSFProfile{QueueName: strings.TrimSpace(os.Getenv("CODEX_HELPER_BEACON_LIVE_QUEUE"))}
	default:
		t.Fatalf("unsupported CODEX_HELPER_BEACON_LIVE_PROVIDER %q; expected slurm or lsf", provider)
	}
	return profile
}

func liveSchedulerIntEnv(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	var out int
	for _, r := range value {
		if r < '0' || r > '9' {
			return fallback
		}
		out = out*10 + int(r-'0')
	}
	if out <= 0 {
		return fallback
	}
	return out
}
