package lease

import (
	"os"
	"strings"
)

// Ident identifies the process that wants the lease.
type Ident struct {
	Host  string // hostname
	Boot  string // kernel boot id ("" = unknown)
	PIDNS string // pid namespace ("" = unknown)
	PID   int
	Start string // the process's start time ("" = unknown)
	Batch bool   // a batch-job host (defers to interactive standbys)
}

// MachineKindEnv overrides batch detection: "batch" or "interactive".
const MachineKindEnv = "CODEX_HELPER_TEAMS_MACHINE_KIND"

// batchEnv are variables set inside batch jobs (Slurm, PBS/Torque, LSF,
// Grid Engine) and Kubernetes pods.
var batchEnv = []string{"SLURM_JOB_ID", "PBS_JOBID", "LSB_JOBID", "JOB_ID", "KUBERNETES_SERVICE_HOST"}

// Local describes this process.
func Local(host, boot string) Ident {
	pid := os.Getpid()
	return Ident{
		Host:  host,
		Boot:  boot,
		PIDNS: pidNamespace(),
		PID:   pid,
		Start: startTime(pid),
		Batch: IsBatch(os.Getenv),
	}
}

// IsBatch reports whether the environment is a batch job.
func IsBatch(getenv func(string) string) bool {
	switch strings.ToLower(strings.TrimSpace(getenv(MachineKindEnv))) {
	case "batch":
		return true
	case "interactive":
		return false
	}
	for _, k := range batchEnv {
		if strings.TrimSpace(getenv(k)) != "" {
			return true
		}
	}
	return false
}

// sameMachine reports whether r was written by a process of this machine
// and kernel instance, so its pid can be checked here. A matching hostname
// with another boot id is a machine that rebooted since (see rebooted).
func (id Ident) sameMachine(r Record) bool {
	return strings.EqualFold(r.Host, id.Host) && r.Boot != "" && r.Boot == id.Boot && r.PIDNS == id.PIDNS
}

// rebooted reports whether r was written on this host before its last
// boot: its process is gone. This assumes hostnames are unique among the
// hosts that share the state directory.
func (id Ident) rebooted(r Record) bool {
	return strings.EqualFold(r.Host, id.Host) && r.Boot != "" && id.Boot != "" && r.Boot != id.Boot
}
