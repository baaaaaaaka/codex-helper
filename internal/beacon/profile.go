package beacon

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type CreateProfileInput struct {
	Name                         string
	Provider                     Provider
	ProxyMode                    ProxyMode
	ProxyProfile                 string
	IsolationDefault             Isolation
	Slurm                        SlurmProfile
	LSF                          LSFProfile
	Now                          time.Time
	ExistingProxyProfileResolver func(string) bool
}

func CreateProfile(st *State, in CreateProfileInput) (Profile, error) {
	if st == nil {
		return Profile{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Profile{}, fmt.Errorf("profile name is required")
	}
	if _, exists := st.Profiles[name]; exists {
		return Profile{}, fmt.Errorf("beacon profile %q already exists", name)
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	proxyMode := in.ProxyMode
	if proxyMode == "" {
		proxyMode = ProxyNone
	}
	isolation := in.IsolationDefault
	if isolation == "" {
		isolation = IsolationShared
	}
	p := Profile{
		Name:             name,
		Provider:         in.Provider,
		ProxyMode:        proxyMode,
		ProxyProfile:     strings.TrimSpace(in.ProxyProfile),
		IsolationDefault: isolation,
		Slurm:            in.Slurm,
		LSF:              in.LSF,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	p.ProviderPreviewOK = providerPreviewOK(p)
	if reasons := p.DraftReasons(in.ExistingProxyProfileResolver); hasFatalProfileReason(reasons) {
		st.Profiles[name] = p
		return p, nil
	}
	st.Profiles[name] = p
	return p, nil
}

func ConfirmProfile(st *State, name string, now time.Time, proxyExists func(string) bool) (Profile, error) {
	return updateProfile(st, name, now, func(p *Profile) {
		p.Confirmed = true
		p.ProviderPreviewOK = providerPreviewOK(*p)
	}, proxyExists)
}

func DoctorProfile(st *State, name string, now time.Time, proxyExists func(string) bool) (Profile, error) {
	return updateProfile(st, name, now, func(p *Profile) {
		p.ProviderPreviewOK = providerPreviewOK(*p)
		p.DoctorOK = true
	}, proxyExists)
}

func DeleteProfile(st *State, name string) error {
	if st == nil {
		return fmt.Errorf("nil beacon state")
	}
	st.normalize()
	name = strings.TrimSpace(name)
	if _, ok := st.Profiles[name]; !ok {
		return fmt.Errorf("beacon profile %q not found", name)
	}
	if profileInUse(*st, name) {
		return fmt.Errorf("beacon profile %q is in use", name)
	}
	delete(st.Profiles, name)
	return nil
}

func profileInUse(st State, name string) bool {
	for _, m := range st.Machines {
		if strings.TrimSpace(m.Profile) == name {
			return true
		}
	}
	for _, conv := range st.Conversations {
		if strings.TrimSpace(conv.Current.Profile) == name {
			return true
		}
		if conv.Pending != nil && strings.TrimSpace(conv.Pending.Profile) == name {
			return true
		}
		for _, queued := range conv.Queued {
			if strings.TrimSpace(queued.Snapshot.Profile) == name {
				return true
			}
		}
	}
	for _, snap := range st.TurnTargets {
		if strings.TrimSpace(snap.Profile) == name {
			return true
		}
	}
	for _, req := range st.Allocations {
		if strings.TrimSpace(req.Profile) == name || strings.TrimSpace(req.Target.Profile) == name {
			return true
		}
	}
	return false
}

func updateProfile(st *State, name string, now time.Time, fn func(*Profile), proxyExists func(string) bool) (Profile, error) {
	if st == nil {
		return Profile{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	name = strings.TrimSpace(name)
	p, ok := st.Profiles[name]
	if !ok {
		return Profile{}, fmt.Errorf("beacon profile %q not found", name)
	}
	fn(&p)
	if now.IsZero() {
		now = time.Now()
	}
	p.UpdatedAt = now
	st.Profiles[name] = p
	return p, nil
}

func ListProfiles(st State) []Profile {
	st.normalize()
	out := make([]Profile, 0, len(st.Profiles))
	for _, p := range st.Profiles {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (p Profile) Ready(proxyExists func(string) bool) bool {
	return len(p.DraftReasons(proxyExists)) == 0
}

func (p Profile) DraftReasons(proxyExists func(string) bool) []string {
	var reasons []string
	if strings.TrimSpace(p.Name) == "" {
		reasons = append(reasons, "profile name is required")
	}
	switch p.Provider {
	case ProviderSlurm:
		if p.Slurm.Nodes <= 0 {
			reasons = append(reasons, "slurm nodes must be > 0")
		}
		if p.Slurm.GPUCount < 0 {
			reasons = append(reasons, "slurm gpu count must be >= 0")
		}
		if strings.TrimSpace(p.Slurm.Partition) == "" {
			reasons = append(reasons, "slurm partition is required")
		}
		if strings.TrimSpace(p.Slurm.Image) == "" {
			reasons = append(reasons, "slurm image is required")
		}
		if p.Slurm.Duration <= 0 {
			reasons = append(reasons, "slurm duration must be > 0")
		}
	case ProviderLSF:
		if strings.TrimSpace(p.LSF.QueueName) == "" {
			reasons = append(reasons, "lsf queue name is required")
		}
	case ProviderLocal:
	default:
		reasons = append(reasons, "provider must be slurm, lsf, or local")
	}
	switch p.ProxyMode {
	case ProxyNone:
	case ProxySSHProfile:
		if strings.TrimSpace(p.ProxyProfile) == "" {
			reasons = append(reasons, "proxy profile is required")
		} else if proxyExists != nil && !proxyExists(p.ProxyProfile) {
			reasons = append(reasons, "proxy profile not found")
		}
	default:
		reasons = append(reasons, "proxy mode must be none or ssh_profile")
	}
	if p.IsolationDefault != "" && p.IsolationDefault != IsolationShared && p.IsolationDefault != IsolationExclusive {
		reasons = append(reasons, "isolation must be shared or exclusive")
	}
	if !p.ProviderPreviewOK {
		reasons = append(reasons, "provider preview missing")
	}
	if !p.DoctorOK {
		reasons = append(reasons, "doctor failed")
	}
	if !p.Confirmed {
		reasons = append(reasons, "needs confirm")
	}
	return reasons
}

func providerPreviewOK(p Profile) bool {
	switch p.Provider {
	case ProviderSlurm:
		return p.Slurm.Nodes > 0 &&
			p.Slurm.GPUCount >= 0 &&
			strings.TrimSpace(p.Slurm.Partition) != "" &&
			strings.TrimSpace(p.Slurm.Image) != "" &&
			p.Slurm.Duration > 0
	case ProviderLSF:
		return strings.TrimSpace(p.LSF.QueueName) != ""
	case ProviderLocal:
		return true
	default:
		return false
	}
}

func hasFatalProfileReason(reasons []string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, "provider must") || strings.Contains(reason, "proxy mode must") {
			return true
		}
	}
	return false
}
