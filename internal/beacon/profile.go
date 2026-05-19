package beacon

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
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
	Adapter                      ProviderCommandConfig
	Now                          time.Time
	ExistingProxyProfileResolver func(string) bool
}

type UpdateProfileInput = CreateProfileInput

type DoctorProfileInput struct {
	Now                      time.Time
	ProxyExists              func(string) bool
	EnvProviderCommands      ProviderCommandConfig
	SkipProviderAdapterCheck bool
	CheckExecutable          func(string) error
}

type ProfileDoctorSmokeInput struct {
	Now     time.Time
	Adapter interface {
		SubmitAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error)
		QueryAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error)
		CancelAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error)
	}
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
		Revision:         1,
		Provider:         in.Provider,
		ProxyMode:        proxyMode,
		ProxyProfile:     strings.TrimSpace(in.ProxyProfile),
		IsolationDefault: isolation,
		Slurm:            in.Slurm,
		LSF:              in.LSF,
		Adapter:          in.Adapter,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	p.ProviderPreviewOK = providerPreviewOK(p)
	if reasons := p.DraftReasons(in.ExistingProxyProfileResolver); hasFatalProfileReason(reasons) {
		st.Profiles[name] = p
		_, _ = AppendAudit(st, "profile_create", name, now)
		return p, nil
	}
	st.Profiles[name] = p
	_, _ = AppendAudit(st, "profile_create", name, now)
	return p, nil
}

func UpdateProfileConfig(st *State, in UpdateProfileInput) (Profile, error) {
	if st == nil {
		return Profile{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Profile{}, fmt.Errorf("profile name is required")
	}
	old, ok := st.Profiles[name]
	if !ok {
		return Profile{}, fmt.Errorf("beacon profile %q not found", name)
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	old = normalizeProfileRevision(old)
	st.ProfileHistory[profileHistoryKey(old.Name, old.Revision)] = old
	pinProfileReferences(st, old.Name, old.Revision)
	proxyMode := in.ProxyMode
	if proxyMode == "" {
		proxyMode = ProxyNone
	}
	isolation := in.IsolationDefault
	if isolation == "" {
		isolation = IsolationShared
	}
	adapter := in.Adapter
	if old.Provider == in.Provider {
		adapter = MergeProviderCommandConfig(old.Adapter, in.Adapter)
	}
	p := Profile{
		Name:             name,
		Revision:         old.Revision + 1,
		Provider:         in.Provider,
		ProxyMode:        proxyMode,
		ProxyProfile:     strings.TrimSpace(in.ProxyProfile),
		IsolationDefault: isolation,
		Slurm:            in.Slurm,
		LSF:              in.LSF,
		Adapter:          adapter,
		CreatedAt:        old.CreatedAt,
		UpdatedAt:        now,
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = now
	}
	p.ProviderPreviewOK = providerPreviewOK(p)
	st.Profiles[name] = p
	_, _ = AppendAudit(st, "profile_update", profileHistoryKey(name, p.Revision), now)
	return p, nil
}

func ListProfileRevisions(st State, name string) []Profile {
	st.normalize()
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	byRevision := map[int]Profile{}
	for key, p := range st.ProfileHistory {
		p = normalizeProfileRevision(p)
		if p.Name != name && !strings.HasPrefix(key, name+"@") {
			continue
		}
		byRevision[p.Revision] = p
	}
	if p, ok := st.Profiles[name]; ok {
		p = normalizeProfileRevision(p)
		byRevision[p.Revision] = p
	}
	revisions := make([]int, 0, len(byRevision))
	for revision := range byRevision {
		revisions = append(revisions, revision)
	}
	sort.Ints(revisions)
	out := make([]Profile, 0, len(revisions))
	for _, revision := range revisions {
		out = append(out, byRevision[revision])
	}
	return out
}

func RollbackProfileRevision(st *State, name string, revision int, now time.Time) (Profile, error) {
	if st == nil {
		return Profile{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	name = strings.TrimSpace(name)
	if name == "" {
		return Profile{}, fmt.Errorf("profile name is required")
	}
	if revision <= 0 {
		return Profile{}, fmt.Errorf("profile revision must be positive")
	}
	current, ok := st.Profiles[name]
	if !ok {
		return Profile{}, fmt.Errorf("beacon profile %q not found", name)
	}
	current = normalizeProfileRevision(current)
	target, ok := profileForRevision(*st, name, revision)
	if !ok {
		return Profile{}, fmt.Errorf("beacon profile %q revision %d not found", name, revision)
	}
	if now.IsZero() {
		now = time.Now()
	}
	st.ProfileHistory[profileHistoryKey(current.Name, current.Revision)] = current
	pinProfileReferences(st, current.Name, current.Revision)
	target = normalizeProfileRevision(target)
	target.Revision = current.Revision + 1
	target.CreatedAt = current.CreatedAt
	if target.CreatedAt.IsZero() {
		target.CreatedAt = now
	}
	target.UpdatedAt = now
	target.Archived = false
	target.ArchivedAt = time.Time{}
	target.ProviderPreviewOK = providerPreviewOK(target)
	st.Profiles[name] = target
	_, _ = AppendAudit(st, "profile_rollback", fmt.Sprintf("%s@%d->%d", name, revision, target.Revision), now)
	return target, nil
}

func PruneProfileHistory(st *State, name string) (int, error) {
	if st == nil {
		return 0, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, fmt.Errorf("profile name is required")
	}
	removed := 0
	for key, p := range st.ProfileHistory {
		p = normalizeProfileRevision(p)
		if p.Name != name && !strings.HasPrefix(key, name+"@") {
			continue
		}
		if profileRevisionInUse(*st, name, p.Revision) {
			continue
		}
		delete(st.ProfileHistory, key)
		removed++
	}
	if removed > 0 {
		_, _ = AppendAudit(st, "profile_history_prune", name, time.Time{})
	}
	return removed, nil
}

func ConfirmProfile(st *State, name string, now time.Time, proxyExists func(string) bool) (Profile, error) {
	p, err := updateProfile(st, name, now, func(p *Profile) {
		*p = normalizeProfileRevision(*p)
		p.Confirmed = true
		p.ProviderPreviewOK = providerPreviewOK(*p)
	}, proxyExists)
	if err == nil {
		_, _ = AppendAudit(st, "profile_confirm", name, now)
	}
	return p, err
}

func DoctorProfile(st *State, name string, now time.Time, proxyExists func(string) bool) (Profile, error) {
	p, _, err := DoctorProfileWithInput(st, name, DoctorProfileInput{
		Now:                      now,
		ProxyExists:              proxyExists,
		SkipProviderAdapterCheck: true,
	})
	return p, err
}

func DoctorProfileWithInput(st *State, name string, in DoctorProfileInput) (Profile, ProfileDoctorReport, error) {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	in.Now = now
	var report ProfileDoctorReport
	p, err := updateProfile(st, name, now, func(p *Profile) {
		*p = normalizeProfileRevision(*p)
		p.ProviderPreviewOK = providerPreviewOK(*p)
		report = BuildProfileDoctorReport(*p, in)
		p.DoctorOK = report.Passed
		p.DoctorReport = report
	}, in.ProxyExists)
	if err == nil {
		_, _ = AppendAudit(st, "profile_doctor", name, now)
	}
	return p, report, err
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
	if normalizeProfileRevision(st.Profiles[name]).Archived {
		return nil
	}
	now := time.Now()
	if profileInUse(*st, name) {
		p := normalizeProfileRevision(st.Profiles[name])
		st.ProfileHistory[profileHistoryKey(p.Name, p.Revision)] = p
		pinProfileReferences(st, p.Name, p.Revision)
		p.Archived = true
		p.ArchivedAt = now
		p.UpdatedAt = now
		st.Profiles[name] = p
		_, _ = AppendAudit(st, "profile_archive", name, now)
		return nil
	}
	p := normalizeProfileRevision(st.Profiles[name])
	p.Archived = true
	p.ArchivedAt = now
	p.UpdatedAt = now
	st.Profiles[name] = p
	_, _ = AppendAudit(st, "profile_archive", name, now)
	return nil
}

func BuildProfileDoctorReport(p Profile, in DoctorProfileInput) ProfileDoctorReport {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	p = normalizeProfileRevision(p)
	p.ProviderPreviewOK = providerPreviewOK(p)
	report := ProfileDoctorReport{
		CheckedAt: now,
		Provider:  p.Provider,
	}
	report.Issues = append(report.Issues, profileDoctorConfigurationIssues(p, in.ProxyExists)...)
	if !in.SkipProviderAdapterCheck {
		report.Operations = profileDoctorAdapterOperations(p, in)
		for _, op := range report.Operations {
			if op.Status != "ok" {
				report.Issues = append(report.Issues, fmt.Sprintf("%s adapter %s", op.Operation, firstNonEmpty(op.Error, op.Status)))
			}
		}
	}
	report.Passed = len(report.Issues) == 0
	return report
}

func RunProfileDoctorSmoke(ctx context.Context, p Profile, in ProfileDoctorSmokeInput) []ProfileDoctorOperation {
	p = normalizeProfileRevision(p)
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	if in.Adapter == nil {
		return []ProfileDoctorOperation{{Operation: "smoke", Status: "failed", Error: "provider adapter is not configured"}}
	}
	switch p.Provider {
	case ProviderSlurm, ProviderLSF:
	default:
		return []ProfileDoctorOperation{{Operation: "smoke", Status: "skipped", Reason: "local provider does not use external scheduler adapters"}}
	}
	req := AllocationRequest{
		ID:                "doctor-" + sanitizeJobName(firstNonEmpty(p.Name, "profile")),
		ConversationID:    "doctor",
		TurnID:            "doctor",
		Profile:           p.Name,
		ProfileSnapshot:   p,
		Provider:          p.Provider,
		Isolation:         p.IsolationDefault,
		Target:            TargetSnapshot{Target: TargetBeacon, Profile: p.Name, ProfileRevision: p.Revision},
		DeterministicName: "cxp-doctor-" + sanitizeJobName(firstNonEmpty(p.Name, "profile")),
		State:             AllocationRequestPersisted,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if req.Isolation == "" {
		req.Isolation = IsolationExclusive
	}
	var out []ProfileDoctorOperation
	submit, err := in.Adapter.SubmitAllocation(ctx, req)
	out = append(out, profileDoctorSmokeOperation("submit", submit, err))
	if err != nil {
		return out
	}
	if strings.TrimSpace(submit.ProviderJobID) != "" {
		req.ProviderIdentity.ProviderJobID = strings.TrimSpace(submit.ProviderJobID)
		req.Target.ProviderJobID = strings.TrimSpace(submit.ProviderJobID)
	}
	query, queryErr := in.Adapter.QueryAllocation(ctx, req)
	out = append(out, profileDoctorSmokeOperation("query", query, queryErr))
	cancel, cancelErr := in.Adapter.CancelAllocation(ctx, req)
	out = append(out, profileDoctorSmokeOperation("cancel", cancel, cancelErr))
	return out
}

func ApplyProfileDoctorSmokeReport(st *State, name string, revision int, smoke []ProfileDoctorOperation, now time.Time) (Profile, error) {
	if st == nil {
		return Profile{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	name = strings.TrimSpace(name)
	if name == "" {
		return Profile{}, fmt.Errorf("profile name is required")
	}
	p, ok := st.Profiles[name]
	if !ok {
		return Profile{}, fmt.Errorf("beacon profile %q not found", name)
	}
	p = normalizeProfileRevision(p)
	if revision > 0 && p.Revision != revision {
		return Profile{}, fmt.Errorf("beacon profile %q changed from revision %d to %d during doctor smoke", name, revision, p.Revision)
	}
	if now.IsZero() {
		now = time.Now()
	}
	p.DoctorReport.Smoke = append([]ProfileDoctorOperation(nil), smoke...)
	p.DoctorReport.Issues = profileDoctorIssuesWithSmoke(p.DoctorReport.Issues, smoke)
	p.DoctorReport.Passed = len(p.DoctorReport.Issues) == 0
	p.DoctorOK = p.DoctorReport.Passed
	p.UpdatedAt = now
	st.Profiles[name] = p
	_, _ = AppendAudit(st, "profile_doctor_smoke", profileHistoryKey(name, p.Revision), now)
	return p, nil
}

func profileDoctorSmokeOperation(operation string, result SchedulerQueryResult, err error) ProfileDoctorOperation {
	op := ProfileDoctorOperation{
		Operation:     "smoke_" + strings.TrimSpace(operation),
		Status:        "ok",
		ProviderJobID: strings.TrimSpace(result.ProviderJobID),
		RawState:      strings.TrimSpace(result.RawState),
		Reason:        strings.TrimSpace(result.Reason),
	}
	if result.QueryError {
		op.Status = "failed"
		op.Error = firstNonEmpty(op.Error, "provider returned query_error=true")
	}
	if err != nil {
		op.Status = "failed"
		op.Error = err.Error()
	}
	if result.DurableNegative && op.Reason == "" {
		op.Reason = "durable_negative"
	}
	return op
}

func profileDoctorIssuesWithSmoke(base []string, smoke []ProfileDoctorOperation) []string {
	var issues []string
	for _, issue := range base {
		if strings.Contains(strings.ToLower(issue), "smoke") {
			continue
		}
		issues = append(issues, issue)
	}
	for _, op := range smoke {
		switch strings.TrimSpace(op.Status) {
		case "", "ok", "skipped":
			continue
		default:
			issues = append(issues, fmt.Sprintf("%s smoke %s", op.Operation, firstNonEmpty(op.Error, op.Status)))
		}
	}
	return issues
}

func profileDoctorConfigurationIssues(p Profile, proxyExists func(string) bool) []string {
	var issues []string
	if strings.TrimSpace(p.Name) == "" {
		issues = append(issues, "profile name is required")
	}
	switch p.Provider {
	case ProviderSlurm:
		if p.Slurm.Nodes <= 0 {
			issues = append(issues, "slurm nodes must be > 0")
		}
		if p.Slurm.GPUCount < 0 {
			issues = append(issues, "slurm gpu count must be >= 0")
		}
		if strings.TrimSpace(p.Slurm.Partition) == "" {
			issues = append(issues, "slurm partition is required")
		}
		if strings.TrimSpace(p.Slurm.Image) == "" {
			issues = append(issues, "slurm image is required")
		}
		if p.Slurm.Duration <= 0 {
			issues = append(issues, "slurm duration must be > 0")
		}
	case ProviderLSF:
		if strings.TrimSpace(p.LSF.QueueName) == "" {
			issues = append(issues, "lsf queue name is required")
		}
	case ProviderLocal:
	default:
		issues = append(issues, "provider must be slurm, lsf, or local")
	}
	switch p.ProxyMode {
	case ProxyNone:
	case ProxySSHProfile:
		if strings.TrimSpace(p.ProxyProfile) == "" {
			issues = append(issues, "proxy profile is required")
		} else if proxyExists != nil && !proxyExists(p.ProxyProfile) {
			issues = append(issues, "proxy profile not found")
		}
	default:
		issues = append(issues, "proxy mode must be none or ssh_profile")
	}
	if p.IsolationDefault != "" && p.IsolationDefault != IsolationShared && p.IsolationDefault != IsolationExclusive {
		issues = append(issues, "isolation must be shared or exclusive")
	}
	if !p.ProviderPreviewOK {
		issues = append(issues, "provider preview missing")
	}
	if p.Archived {
		issues = append(issues, "profile is archived")
	}
	return issues
}

func profileDoctorAdapterOperations(p Profile, in DoctorProfileInput) []ProfileDoctorOperation {
	switch p.Provider {
	case ProviderSlurm, ProviderLSF:
	default:
		return nil
	}
	check := in.CheckExecutable
	if check == nil {
		check = defaultProviderCommandExecutableCheck
	}
	var out []ProfileDoctorOperation
	for _, operation := range []string{"query", "submit", "cancel", "renew"} {
		command, _, flag := providerCommandFromConfig(p.Adapter, p.Provider, operation)
		envName := ""
		source := "profile"
		if strings.TrimSpace(command) == "" {
			command, envName, flag = providerCommandFromConfig(in.EnvProviderCommands, p.Provider, operation)
			source = "helper environment"
		}
		op := ProfileDoctorOperation{
			Operation:   operation,
			Source:      source,
			Command:     strings.TrimSpace(command),
			EnvName:     envName,
			ProfileFlag: flag,
		}
		if strings.TrimSpace(op.Command) == "" {
			op.Status = "missing"
			if strings.TrimSpace(envName) != "" {
				op.Error = fmt.Sprintf("set %s or %s", flag, envName)
			} else {
				op.Error = fmt.Sprintf("set %s", flag)
			}
			out = append(out, op)
			continue
		}
		if err := check(op.Command); err != nil {
			op.Status = "failed"
			op.Error = err.Error()
			out = append(out, op)
			continue
		}
		op.Status = "ok"
		out = append(out, op)
	}
	return out
}

func defaultProviderCommandExecutableCheck(command string) error {
	command = strings.TrimSpace(command)
	if command == "" {
		return fmt.Errorf("command is empty")
	}
	if strings.Contains(command, string(os.PathSeparator)) {
		info, err := os.Stat(command)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return fmt.Errorf("%s is a directory", command)
		}
		if runtime.GOOS != "windows" && info.Mode()&0111 == 0 {
			return fmt.Errorf("%s is not executable", command)
		}
		return nil
	}
	_, err := exec.LookPath(command)
	return err
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

func profileForSnapshot(st State, snap TargetSnapshot) (Profile, bool) {
	st.normalize()
	name := strings.TrimSpace(snap.Profile)
	if name == "" {
		return Profile{}, false
	}
	if snap.ProfileRevision > 0 {
		if p, ok := st.ProfileHistory[profileHistoryKey(name, snap.ProfileRevision)]; ok {
			return normalizeProfileRevision(p), true
		}
		if p, ok := st.Profiles[name]; ok {
			p = normalizeProfileRevision(p)
			if p.Revision == snap.ProfileRevision {
				return p, true
			}
		}
	}
	p, ok := st.Profiles[name]
	if !ok {
		return Profile{}, false
	}
	return normalizeProfileRevision(p), true
}

func profileForRevision(st State, name string, revision int) (Profile, bool) {
	st.normalize()
	name = strings.TrimSpace(name)
	if name == "" || revision <= 0 {
		return Profile{}, false
	}
	if p, ok := st.ProfileHistory[profileHistoryKey(name, revision)]; ok {
		return normalizeProfileRevision(p), true
	}
	if p, ok := st.Profiles[name]; ok {
		p = normalizeProfileRevision(p)
		if p.Revision == revision {
			return p, true
		}
	}
	return Profile{}, false
}

func normalizeProfileRevision(p Profile) Profile {
	if p.Revision <= 0 {
		p.Revision = 1
	}
	return p
}

func profileHistoryKey(name string, revision int) string {
	return strings.TrimSpace(name) + "@" + strconv.Itoa(revision)
}

func profileRevisionInUse(st State, name string, revision int) bool {
	if strings.TrimSpace(name) == "" || revision <= 0 {
		return false
	}
	check := func(snap TargetSnapshot) bool {
		return strings.TrimSpace(snap.Profile) == name && snap.ProfileRevision == revision
	}
	for _, conv := range st.Conversations {
		if check(conv.Current) {
			return true
		}
		if conv.Pending != nil && check(*conv.Pending) {
			return true
		}
		for _, queued := range conv.Queued {
			if check(queued.Snapshot) {
				return true
			}
		}
	}
	for _, snap := range st.TurnTargets {
		if check(snap) {
			return true
		}
	}
	for _, req := range st.Allocations {
		if check(req.Target) {
			return true
		}
		if strings.TrimSpace(req.Profile) == name && normalizeProfileRevision(req.ProfileSnapshot).Revision == revision {
			return true
		}
	}
	return false
}

func pinProfileReferences(st *State, name string, revision int) {
	if st == nil || strings.TrimSpace(name) == "" || revision <= 0 {
		return
	}
	for id, conv := range st.Conversations {
		conv.Current = pinTargetProfileRevision(conv.Current, name, revision)
		if conv.Pending != nil {
			pending := pinTargetProfileRevision(*conv.Pending, name, revision)
			conv.Pending = &pending
		}
		for i := range conv.Queued {
			conv.Queued[i].Snapshot = pinTargetProfileRevision(conv.Queued[i].Snapshot, name, revision)
		}
		st.Conversations[id] = conv
	}
	for id, snap := range st.TurnTargets {
		st.TurnTargets[id] = pinTargetProfileRevision(snap, name, revision)
	}
	for id, req := range st.Allocations {
		req.Target = pinTargetProfileRevision(req.Target, name, revision)
		st.Allocations[id] = req
	}
}

func pinTargetProfileRevision(snap TargetSnapshot, name string, revision int) TargetSnapshot {
	if strings.TrimSpace(snap.Profile) == strings.TrimSpace(name) && snap.ProfileRevision == 0 {
		snap.ProfileRevision = revision
	}
	return snap
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
	if p.Archived {
		reasons = append(reasons, "profile is archived")
	}
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
