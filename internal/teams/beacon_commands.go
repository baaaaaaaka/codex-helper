package teams

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func (b *Bridge) handleBeaconControlCommand(ctx context.Context, msg ChatMessage, arg string) error {
	out, err := b.runBeaconControlCommand(ctx, msg, arg)
	if err != nil {
		out = controlCommandErrorMessage(err)
	}
	return b.sendControl(ctx, out)
}

func (b *Bridge) handleBeaconWorkCommand(ctx context.Context, session *Session, msg ChatMessage, arg string) error {
	if session == nil {
		return nil
	}
	out, err := b.runBeaconWorkCommand(ctx, session, msg, arg)
	if err != nil {
		out = controlCommandErrorMessage(err)
	}
	return b.sendToChat(ctx, session.ChatID, out)
}

func (b *Bridge) runBeaconControlCommand(ctx context.Context, msg ChatMessage, arg string) (string, error) {
	words := strings.Fields(strings.TrimSpace(arg))
	if len(words) == 0 {
		return b.formatBeaconList()
	}
	switch strings.ToLower(words[0]) {
	case "help", "h", "?":
		return beaconControlHelpText(), nil
	case "list", "ls":
		return b.formatBeaconList()
	case "profiles":
		if len(words) == 1 {
			return b.formatBeaconProfiles()
		}
		return b.handleBeaconProfileCommand(ctx, msg, b.reg.ControlChatID, words[1:])
	case "profile":
		return b.handleBeaconProfileCommand(ctx, msg, b.reg.ControlChatID, words[1:])
	case "machines":
		if len(words) == 1 {
			return b.formatBeaconMachines()
		}
		return b.handleBeaconMachineCommand(ctx, msg, b.reg.ControlChatID, words[1:])
	case "machine":
		return b.handleBeaconMachineCommand(ctx, msg, b.reg.ControlChatID, words[1:])
	case "status":
		sessionID := beaconStatusSessionArg(words[1:])
		if sessionID == "" {
			return b.formatBeaconList()
		}
		return b.formatBeaconSessionStatus(sessionID)
	case "switch", "switch-profile", "local", "fork":
		return "Wrong chat.\n\n`beacon switch ...` changes one Work chat. Open the target Work chat and send `beacon switch <profile>` there.", nil
	default:
		return "", fmt.Errorf("unknown beacon command %q; send `beacon help`", words[0])
	}
}

func (b *Bridge) runBeaconWorkCommand(ctx context.Context, session *Session, msg ChatMessage, arg string) (string, error) {
	words := strings.Fields(strings.TrimSpace(arg))
	if len(words) == 0 {
		return b.formatBeaconSessionStatus(session.ID)
	}
	switch strings.ToLower(words[0]) {
	case "help", "h", "?":
		return beaconWorkHelpText(), nil
	case "list", "ls":
		return b.formatBeaconList()
	case "status", "st":
		return b.formatBeaconSessionStatus(session.ID)
	case "switch", "switch-profile":
		return b.handleBeaconWorkSwitch(ctx, msg, session, words[1:], false)
	case "fork":
		return b.handleBeaconWorkSwitch(ctx, msg, session, words[1:], true)
	case "local":
		return b.handleBeaconWorkSwitchLocal(ctx, msg, session)
	case "profile", "profiles", "machine", "machines":
		return "Wrong chat.\n\nBeacon profile and machine administration belongs in the control chat. Send `beacon list` there for the global view.", nil
	default:
		return "", fmt.Errorf("unknown beacon work command %q; send `beacon help`", words[0])
	}
}

func (b *Bridge) handleBeaconProfileCommand(ctx context.Context, msg ChatMessage, idempotencyScope string, words []string) (string, error) {
	if len(words) == 0 {
		return b.formatBeaconProfiles()
	}
	switch strings.ToLower(words[0]) {
	case "list", "ls":
		return b.formatBeaconProfiles()
	case "create":
		in, err := parseBeaconProfileCreateInput(words[1:])
		if err != nil {
			return "", err
		}
		normalized := normalizedBeaconCommand("profile create " + strings.Join(words[1:], " "))
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, "", func(st *beacon.State) (string, error) {
			proxyExists := b.beaconProxyResolver()
			p, err := beacon.CreateProfile(st, in.withProxyResolver(proxyExists))
			if err != nil {
				return "", err
			}
			return formatBeaconProfileMutation("Created", p, proxyExists), nil
		})
	case "doctor":
		name, err := singleBeaconNameArg("beacon profile doctor", words[1:])
		if err != nil {
			return "", err
		}
		normalized := normalizedBeaconCommand("profile doctor " + name)
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, "", func(st *beacon.State) (string, error) {
			proxyExists := b.beaconProxyResolver()
			p, err := beacon.DoctorProfile(st, name, time.Now(), proxyExists)
			if err != nil {
				return "", err
			}
			return formatBeaconProfileMutation("Doctor passed for", p, proxyExists), nil
		})
	case "confirm":
		name, err := singleBeaconNameArg("beacon profile confirm", words[1:])
		if err != nil {
			return "", err
		}
		normalized := normalizedBeaconCommand("profile confirm " + name)
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, "", func(st *beacon.State) (string, error) {
			proxyExists := b.beaconProxyResolver()
			p, err := beacon.ConfirmProfile(st, name, time.Now(), proxyExists)
			if err != nil {
				return "", err
			}
			return formatBeaconProfileMutation("Confirmed", p, proxyExists), nil
		})
	case "status":
		name, err := singleBeaconNameArg("beacon profile status", words[1:])
		if err != nil {
			return "", err
		}
		store, err := beacon.NewStore("")
		if err != nil {
			return "", err
		}
		st, err := store.Load()
		if err != nil {
			return "", err
		}
		p, ok := st.Profiles[name]
		if !ok {
			return "", fmt.Errorf("beacon profile %q not found", name)
		}
		return formatBeaconProfileStatus(p, b.beaconProxyResolver()), nil
	case "delete", "remove", "rm":
		name, err := singleBeaconNameArg("beacon profile delete", words[1:])
		if err != nil {
			return "", err
		}
		normalized := normalizedBeaconCommand("profile delete " + name)
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, "", func(st *beacon.State) (string, error) {
			if err := beacon.DeleteProfile(st, name); err != nil {
				return "", err
			}
			return fmt.Sprintf("Deleted beacon profile %q.", name), nil
		})
	default:
		return "", fmt.Errorf("unknown beacon profile command %q; use list, create, status, doctor, confirm, or delete", words[0])
	}
}

func (b *Bridge) handleBeaconMachineCommand(ctx context.Context, msg ChatMessage, idempotencyScope string, words []string) (string, error) {
	if len(words) == 0 {
		return b.formatBeaconMachines()
	}
	switch strings.ToLower(words[0]) {
	case "list", "ls":
		return b.formatBeaconMachines()
	case "status":
		ref, err := singleBeaconNameArg("beacon machine status", words[1:])
		if err != nil {
			return "", err
		}
		store, err := beacon.NewStore("")
		if err != nil {
			return "", err
		}
		st, err := store.Load()
		if err != nil {
			return "", err
		}
		m, ok := findBeaconMachine(st, ref)
		if !ok {
			return "", fmt.Errorf("beacon machine or lease %q not found", ref)
		}
		return formatBeaconMachineStatus(m), nil
	case "release":
		ref, err := singleBeaconNameArg("beacon machine release", words[1:])
		if err != nil {
			return "", err
		}
		normalized := normalizedBeaconCommand("machine release " + ref)
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, "", func(st *beacon.State) (string, error) {
			key, m, ok := findBeaconMachineEntry(*st, ref)
			if !ok {
				return "", fmt.Errorf("beacon machine or lease %q not found", ref)
			}
			res, err := beacon.DecideRelease(m, beacon.ReleaseInput{})
			if err != nil {
				return "", err
			}
			applyBeaconMachineRelease(st, key, m, res.Action)
			return formatBeaconReleaseResult(res), nil
		})
	case "kill":
		ref, confirm, err := parseBeaconMachineKillArgs(words[1:])
		if err != nil {
			return "", err
		}
		normalized := normalizedBeaconCommand("machine kill " + ref)
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, confirm, func(st *beacon.State) (string, error) {
			key, m, ok := findBeaconMachineEntry(*st, ref)
			if !ok {
				return "", fmt.Errorf("beacon machine or lease %q not found", ref)
			}
			preview := beacon.PreviewRelease(m)
			res, err := beacon.DecideRelease(m, beacon.ReleaseInput{
				HardKill:      true,
				ExactID:       ref,
				JobID:         ref,
				ConfirmToken:  preview.Confirmation,
				ProvidedToken: confirm,
			})
			if err != nil {
				return "", err
			}
			applyBeaconMachineRelease(st, key, m, res.Action)
			return formatBeaconReleaseResult(res), nil
		})
	default:
		return "", fmt.Errorf("unknown beacon machine command %q; use list, status, release, or kill", words[0])
	}
}

func (b *Bridge) handleBeaconWorkSwitch(ctx context.Context, msg ChatMessage, session *Session, words []string, fork bool) (string, error) {
	if len(words) == 0 {
		return "", fmt.Errorf("usage: `beacon switch <profile>`")
	}
	name := strings.TrimSpace(words[0])
	if strings.EqualFold(name, "local") {
		return b.handleBeaconWorkSwitchLocal(ctx, msg, session)
	}
	queued, err := b.sessionHasQueuedOrRunning(ctx, session.ID)
	if err != nil {
		return "", err
	}
	normalized := normalizedBeaconCommand("switch " + name)
	if fork {
		normalized = normalizedBeaconCommand("fork " + name)
	}
	return b.updateBeaconStateFromTeams(msg, session.ChatID, normalized, "", func(st *beacon.State) (string, error) {
		res, err := beacon.SwitchProfile(st, beacon.SwitchInput{
			ConversationID:      session.ID,
			ProfileName:         name,
			Fork:                fork,
			HasQueuedOrRunning:  queued,
			SignatureCompatible: true,
		}, b.beaconProxyResolver())
		if err != nil {
			return "", err
		}
		return formatBeaconSwitchResult(res, queued, name), nil
	})
}

func (b *Bridge) handleBeaconWorkSwitchLocal(ctx context.Context, msg ChatMessage, session *Session) (string, error) {
	queued, err := b.sessionHasQueuedOrRunning(ctx, session.ID)
	if err != nil {
		return "", err
	}
	return b.updateBeaconStateFromTeams(msg, session.ChatID, normalizedBeaconCommand("switch local"), "", func(st *beacon.State) (string, error) {
		if st.Conversations == nil {
			st.Conversations = map[string]beacon.Conversation{}
		}
		conv := st.Conversations[session.ID]
		conv.ID = session.ID
		local := beacon.TargetSnapshot{Target: beacon.TargetLocal}
		if queued {
			conv.Pending = &local
		} else {
			conv.Current = local
			conv.Pending = nil
		}
		conv.UpdatedAt = time.Now()
		st.Conversations[session.ID] = conv
		if queued {
			return "Scheduled target switch.\n\nCurrent queued or running work keeps its original target.\nFuture turns will use local.", nil
		}
		return "Switched this Work chat to local execution.\n\nFuture turns will use local.", nil
	})
}

func (b *Bridge) updateBeaconStateFromTeams(msg ChatMessage, idempotencyScope string, normalized string, confirm string, fn func(*beacon.State) (string, error)) (string, error) {
	store, err := beacon.NewStore("")
	if err != nil {
		return "", err
	}
	var out string
	err = store.Update(func(st *beacon.State) error {
		messageID := scopedBeaconTeamsMessageID(idempotencyScope, msg.ID)
		if messageID == "" {
			result, err := fn(st)
			out = result
			return err
		}
		result, _, err := beacon.ApplyIdempotent(st, messageID, normalized, confirm, time.Now(), func() (string, error) {
			return fn(st)
		})
		out = result
		return err
	})
	return out, err
}

func scopedBeaconTeamsMessageID(scope string, messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return ""
	}
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return messageID
	}
	return scope + ":" + messageID
}

func (b *Bridge) formatBeaconList() (string, error) {
	store, err := beacon.NewStore("")
	if err != nil {
		return "", err
	}
	st, err := store.Load()
	if err != nil {
		return "", err
	}
	proxyExists := b.beaconProxyResolver()
	lines := []string{"Beacon list", "", "Profiles:"}
	lines = append(lines, formatBeaconProfileListLines(st, proxyExists)...)
	lines = append(lines, "", "Machines:")
	lines = append(lines, formatBeaconMachineListLines(st)...)
	return strings.Join(lines, "\n"), nil
}

func (b *Bridge) formatBeaconProfiles() (string, error) {
	store, err := beacon.NewStore("")
	if err != nil {
		return "", err
	}
	st, err := store.Load()
	if err != nil {
		return "", err
	}
	lines := []string{"Beacon profiles"}
	lines = append(lines, formatBeaconProfileListLines(st, b.beaconProxyResolver())...)
	return strings.Join(lines, "\n"), nil
}

func (b *Bridge) formatBeaconMachines() (string, error) {
	store, err := beacon.NewStore("")
	if err != nil {
		return "", err
	}
	st, err := store.Load()
	if err != nil {
		return "", err
	}
	lines := []string{"Beacon machines"}
	lines = append(lines, formatBeaconMachineListLines(st)...)
	return strings.Join(lines, "\n"), nil
}

func (b *Bridge) formatBeaconSessionStatus(sessionID string) (string, error) {
	store, err := beacon.NewStore("")
	if err != nil {
		return "", err
	}
	st, err := store.Load()
	if err != nil {
		return "", err
	}
	status := beacon.RenderStatus(st, sessionID)
	return "Beacon status\n\nSession: " + strings.TrimSpace(sessionID) + "\n" + status, nil
}

func (b *Bridge) beaconTargetSummary(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ""
	}
	store, err := beacon.NewStore("")
	if err != nil {
		return "Execution target: unavailable (" + err.Error() + ")"
	}
	st, err := store.Load()
	if err != nil {
		return "Execution target: unavailable (" + err.Error() + ")"
	}
	conv := st.Conversations[sessionID]
	current := targetSnapshotLabelForTeams(conv.Current)
	if current == "" {
		current = beacon.TargetLocal
	}
	if conv.Pending == nil {
		return "Execution target: " + current
	}
	return "Execution target: " + current + " (pending: " + targetSnapshotLabelForTeams(*conv.Pending) + ")"
}

func (b *Bridge) beaconProxyResolver() func(string) bool {
	store, err := config.NewStore(strings.TrimSpace(b.scope.ConfigPath))
	if err != nil {
		return nil
	}
	cfg, err := store.Load()
	if err != nil {
		return nil
	}
	return func(name string) bool {
		_, ok := cfg.FindProfile(name)
		return ok
	}
}

func (b *Bridge) sessionHasQueuedOrRunning(ctx context.Context, sessionID string) (bool, error) {
	if b == nil || b.store == nil {
		return false, nil
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return false, err
	}
	return sessionHasQueuedOrRunningTurnState(state, sessionID), nil
}

func parseBeaconOptionsFromNewSession(raw string) (string, string, string, error) {
	if !strings.Contains(raw, "--beacon") {
		return raw, "", "", nil
	}
	words := strings.Fields(raw)
	var keep []string
	var profile string
	var isolation string
	for i := 0; i < len(words); i++ {
		switch strings.ToLower(strings.TrimSpace(words[i])) {
		case "--beacon", "--beacon-profile":
			if i+1 >= len(words) {
				return "", "", "", fmt.Errorf("%s requires a profile name", words[i])
			}
			i++
			profile = strings.TrimSpace(words[i])
		case "--beacon-isolation":
			if i+1 >= len(words) {
				return "", "", "", fmt.Errorf("--beacon-isolation requires shared or exclusive")
			}
			i++
			isolation = strings.TrimSpace(words[i])
		default:
			keep = append(keep, words[i])
		}
	}
	return strings.Join(keep, " "), profile, isolation, nil
}

func (b *Bridge) validateBeaconNewSession(req newSessionRequest) error {
	profile := strings.TrimSpace(req.BeaconProfile)
	if profile == "" && strings.TrimSpace(req.BeaconIsolation) != "" {
		return fmt.Errorf("cannot create beacon work chat: --beacon-isolation requires --beacon <profile>")
	}
	if profile == "" {
		return nil
	}
	if iso := strings.TrimSpace(req.BeaconIsolation); iso != "" && iso != string(beacon.IsolationShared) && iso != string(beacon.IsolationExclusive) {
		return fmt.Errorf("cannot create beacon work chat: isolation must be shared or exclusive")
	}
	store, err := beacon.NewStore("")
	if err != nil {
		return err
	}
	st, err := store.Load()
	if err != nil {
		return err
	}
	resolved := beacon.ResolveNewTarget(st, beacon.NewTargetInput{ExplicitBeaconProfile: profile}, b.beaconProxyResolver())
	if strings.TrimSpace(resolved.Error) != "" {
		return fmt.Errorf("cannot create beacon work chat for profile %q: %s", profile, resolved.Error)
	}
	return nil
}

func (b *Bridge) activateBeaconNewSession(sessionID string, req newSessionRequest) error {
	profile := strings.TrimSpace(req.BeaconProfile)
	if profile == "" {
		return nil
	}
	store, err := beacon.NewStore("")
	if err != nil {
		return err
	}
	return store.Update(func(st *beacon.State) error {
		if st.Conversations == nil {
			st.Conversations = map[string]beacon.Conversation{}
		}
		p, ok := st.Profiles[profile]
		if !ok {
			return fmt.Errorf("beacon profile %q not found", profile)
		}
		isolation := beacon.Isolation(strings.TrimSpace(req.BeaconIsolation))
		if isolation == "" {
			isolation = p.IsolationDefault
		}
		if isolation == "" {
			isolation = beacon.IsolationShared
		}
		conv := st.Conversations[sessionID]
		conv.ID = sessionID
		conv.Current = beacon.TargetSnapshot{
			Target:     beacon.TargetBeacon,
			Profile:    profile,
			ProxyRoute: beaconProfileProxyRoute(p),
			Isolation:  isolation,
		}
		conv.Pending = nil
		conv.UpdatedAt = time.Now()
		st.Conversations[sessionID] = conv
		return nil
	})
}

type teamsBeaconProfileCreateInput struct {
	beacon.CreateProfileInput
}

func (in teamsBeaconProfileCreateInput) withProxyResolver(proxyExists func(string) bool) beacon.CreateProfileInput {
	out := in.CreateProfileInput
	out.ExistingProxyProfileResolver = proxyExists
	out.Now = time.Now()
	return out
}

func parseBeaconProfileCreateInput(words []string) (teamsBeaconProfileCreateInput, error) {
	if len(words) == 0 {
		return teamsBeaconProfileCreateInput{}, fmt.Errorf("usage: `beacon profile create <name> [--provider slurm|lsf|local ...]`")
	}
	in := teamsBeaconProfileCreateInput{CreateProfileInput: beacon.CreateProfileInput{
		Name:             words[0],
		Provider:         beacon.ProviderSlurm,
		ProxyMode:        beacon.ProxyNone,
		IsolationDefault: beacon.IsolationShared,
	}}
	for i := 1; i < len(words); i++ {
		key := strings.ToLower(strings.TrimSpace(words[i]))
		value := func() (string, error) {
			if i+1 >= len(words) {
				return "", fmt.Errorf("%s requires a value", words[i])
			}
			i++
			return words[i], nil
		}
		switch key {
		case "--provider":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.Provider = beacon.Provider(v)
		case "--partition":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.Slurm.Partition = v
		case "--image":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.Slurm.Image = v
		case "--nodes":
			v, err := parseBeaconIntFlag(words[i], value)
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.Slurm.Nodes = v
		case "--gpu", "--gpus":
			v, err := parseBeaconIntFlag(words[i], value)
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.Slurm.GPUCount = v
		case "--duration":
			v, err := parseBeaconIntFlag(words[i], value)
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.Slurm.Duration = v
		case "--queue":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.LSF.QueueName = v
		case "--proxy":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.ProxyMode = beacon.ProxyMode(v)
		case "--proxy-profile":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.ProxyProfile = v
		case "--isolation":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			in.IsolationDefault = beacon.Isolation(v)
		case "--lsf-site-policy":
			in.LSF.SitePolicyDerivesResources = true
		case "--lsf-advanced-approved":
			in.LSF.AdvancedApproved = true
		default:
			return teamsBeaconProfileCreateInput{}, fmt.Errorf("unknown beacon profile create flag %q", words[i])
		}
	}
	return in, nil
}

func parseBeaconIntFlag(flag string, value func() (string, error)) (int, error) {
	raw, err := value()
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", flag)
	}
	return n, nil
}

func singleBeaconNameArg(usage string, words []string) (string, error) {
	if len(words) != 1 || strings.TrimSpace(words[0]) == "" {
		return "", fmt.Errorf("usage: `%s <name>`", usage)
	}
	return strings.TrimSpace(words[0]), nil
}

func parseBeaconMachineKillArgs(words []string) (string, string, error) {
	if len(words) == 0 || strings.TrimSpace(words[0]) == "" {
		return "", "", fmt.Errorf("usage: `beacon machine kill <machine-or-lease-or-job> --confirm <token>`")
	}
	ref := strings.TrimSpace(words[0])
	var confirm string
	for i := 1; i < len(words); i++ {
		switch strings.ToLower(strings.TrimSpace(words[i])) {
		case "--confirm":
			if i+1 >= len(words) {
				return "", "", fmt.Errorf("--confirm requires a token")
			}
			i++
			confirm = strings.TrimSpace(words[i])
		default:
			return "", "", fmt.Errorf("unknown beacon machine kill flag %q", words[i])
		}
	}
	return ref, confirm, nil
}

func beaconStatusSessionArg(words []string) string {
	if len(words) == 0 {
		return ""
	}
	if len(words) >= 2 && strings.EqualFold(words[0], "--session") {
		return strings.TrimSpace(words[1])
	}
	return strings.TrimSpace(words[0])
}

func normalizedBeaconCommand(text string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(text))), " ")
}

func formatBeaconProfileListLines(st beacon.State, proxyExists func(string) bool) []string {
	profiles := beacon.ListProfiles(st)
	if len(profiles) == 0 {
		return []string{"- none"}
	}
	lines := make([]string, 0, len(profiles))
	for _, p := range profiles {
		state := "draft"
		if p.Ready(proxyExists) {
			state = "ready"
		}
		lines = append(lines, fmt.Sprintf("- %s: %s, provider=%s, isolation=%s, proxy=%s", p.Name, state, p.Provider, p.IsolationDefault, profileProxyLabel(p)))
	}
	return lines
}

func formatBeaconMachineListLines(st beacon.State) []string {
	machines := sortedBeaconMachines(st)
	if len(machines) == 0 {
		return []string{"- none"}
	}
	lines := make([]string, 0, len(machines))
	for _, m := range machines {
		lines = append(lines, fmt.Sprintf("- %s: state=%s, profile=%s, lease=%s, jobs=%d, chats=%d", firstNonEmptyString(m.ID, m.LeaseID), firstNonEmptyString(m.State, "unknown"), m.Profile, m.LeaseID, len(m.Jobs), len(m.Chats)))
	}
	return lines
}

func formatBeaconProfileMutation(action string, p beacon.Profile, proxyExists func(string) bool) string {
	state := "draft"
	if p.Ready(proxyExists) {
		state = "ready"
	}
	lines := []string{
		fmt.Sprintf("%s beacon profile %q.", action, p.Name),
		"",
		"Profile:",
		fmt.Sprintf("- state: %s", state),
		fmt.Sprintf("- provider: %s", p.Provider),
		fmt.Sprintf("- isolation: %s", p.IsolationDefault),
		fmt.Sprintf("- proxy route: %s", profileProxyLabel(p)),
	}
	if reasons := p.DraftReasons(proxyExists); len(reasons) > 0 {
		lines = append(lines, "", "Missing or pending:")
		for _, reason := range reasons {
			lines = append(lines, "- "+reason)
		}
		lines = append(lines, "", "Next: run `beacon profile doctor "+p.Name+"`, then `beacon profile confirm "+p.Name+"` after reviewing the profile.")
	}
	return strings.Join(lines, "\n")
}

func formatBeaconProfileStatus(p beacon.Profile, proxyExists func(string) bool) string {
	lines := []string{
		"Beacon profile",
		"- name: " + p.Name,
		fmt.Sprintf("- provider: %s", p.Provider),
		fmt.Sprintf("- ready: %t", p.Ready(proxyExists)),
		fmt.Sprintf("- confirmed: %t", p.Confirmed),
		fmt.Sprintf("- doctor: %t", p.DoctorOK),
		fmt.Sprintf("- isolation: %s", p.IsolationDefault),
		"- proxy route: " + profileProxyLabel(p),
	}
	switch p.Provider {
	case beacon.ProviderSlurm:
		lines = append(lines, fmt.Sprintf("- slurm: partition=%s image=%s nodes=%d gpu=%d duration=%d", p.Slurm.Partition, p.Slurm.Image, p.Slurm.Nodes, p.Slurm.GPUCount, p.Slurm.Duration))
	case beacon.ProviderLSF:
		lines = append(lines, fmt.Sprintf("- lsf: queue=%s", p.LSF.QueueName))
	}
	if reasons := p.DraftReasons(proxyExists); len(reasons) > 0 {
		lines = append(lines, "Draft reasons:")
		for _, reason := range reasons {
			lines = append(lines, "- "+reason)
		}
	}
	return strings.Join(lines, "\n")
}

func formatBeaconMachineStatus(m beacon.Machine) string {
	p := beacon.PreviewRelease(m)
	return strings.Join([]string{
		"Beacon machine",
		"- machine: " + firstNonEmptyString(p.MachineID, m.ID),
		"- lease: " + p.LeaseID,
		"- provider job: " + p.ProviderJobID,
		"- profile: " + m.Profile,
		"- state: " + firstNonEmptyString(m.State, "unknown"),
		"- host: " + m.Host,
		"- chats: " + strings.Join(p.Chats, ","),
		"- jobs: " + strings.Join(p.Jobs, ","),
		"- kill confirmation: " + p.Confirmation,
	}, "\n")
}

func formatBeaconReleaseResult(res beacon.ReleaseResult) string {
	return strings.Join([]string{
		"Beacon machine action",
		"- action: " + res.Action,
		"- machine: " + res.Preview.MachineID,
		"- lease: " + res.Preview.LeaseID,
		"- chats: " + strings.Join(res.Preview.Chats, ","),
		"- jobs: " + strings.Join(res.Preview.Jobs, ","),
		"- kill confirmation: " + res.Preview.Confirmation,
	}, "\n")
}

func formatBeaconSwitchResult(res beacon.SwitchResult, queued bool, profile string) string {
	target := "beacon:" + strings.TrimSpace(profile)
	if res.Action == "require_fork" {
		return res.Message
	}
	if queued || res.Action == "pending" {
		return "Scheduled switch to " + target + ".\n\nCurrent queued or running work keeps its original target. Future turns will use the pending target."
	}
	return "Switched this Work chat to " + target + ".\n\nFuture turns will use " + target + "."
}

func profileProxyLabel(p beacon.Profile) string {
	if p.ProxyMode == beacon.ProxySSHProfile {
		return "ssh_profile:" + strings.TrimSpace(p.ProxyProfile)
	}
	return firstNonEmptyString(string(p.ProxyMode), string(beacon.ProxyNone))
}

func beaconProfileProxyRoute(p beacon.Profile) string {
	if p.ProxyMode == beacon.ProxySSHProfile && strings.TrimSpace(p.ProxyProfile) != "" {
		return "ssh_profile:" + strings.TrimSpace(p.ProxyProfile)
	}
	return ""
}

func targetSnapshotLabelForTeams(snapshot beacon.TargetSnapshot) string {
	switch strings.TrimSpace(snapshot.Target) {
	case "", beacon.TargetLocal:
		return beacon.TargetLocal
	case beacon.TargetBeacon:
		if strings.TrimSpace(snapshot.Profile) != "" {
			return beacon.TargetBeacon + ":" + strings.TrimSpace(snapshot.Profile)
		}
		return beacon.TargetBeacon
	default:
		return strings.TrimSpace(snapshot.Target)
	}
}

func sortedBeaconMachines(st beacon.State) []beacon.Machine {
	out := make([]beacon.Machine, 0, len(st.Machines))
	for _, m := range st.Machines {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		left := firstNonEmptyString(out[i].ID, out[i].LeaseID, out[i].ProviderJobID)
		right := firstNonEmptyString(out[j].ID, out[j].LeaseID, out[j].ProviderJobID)
		return left < right
	})
	return out
}

func findBeaconMachine(st beacon.State, ref string) (beacon.Machine, bool) {
	_, m, ok := findBeaconMachineEntry(st, ref)
	return m, ok
}

func findBeaconMachineEntry(st beacon.State, ref string) (string, beacon.Machine, bool) {
	ref = strings.TrimSpace(ref)
	for key, m := range st.Machines {
		if m.ID == ref || m.LeaseID == ref || m.ProviderJobID == ref {
			return key, m, true
		}
		for _, job := range m.Jobs {
			if job == ref {
				return key, m, true
			}
		}
	}
	return "", beacon.Machine{}, false
}

func applyBeaconMachineRelease(st *beacon.State, key string, m beacon.Machine, action string) {
	switch action {
	case "drain":
		m.State = "draining"
		st.Machines[key] = m
	case "release":
		delete(st.Machines, key)
	case "kill_quarantine":
		m.State = "kill_quarantine"
		st.Machines[key] = m
	}
}

func beaconControlHelpText() string {
	return strings.Join([]string{
		"Beacon commands",
		"",
		"Control chat:",
		"- `beacon list` - list all profiles and machines",
		"- `beacon profile create <name> --provider slurm --partition <partition> --image <image> --nodes <n> --gpu <n> --duration <hours>`",
		"- `beacon profile create <name> --provider lsf --queue <queue>`",
		"- `beacon profile create <name> --provider local`",
		"- `beacon profile doctor <name>` then `beacon profile confirm <name>`",
		"- `beacon profile status <name>`",
		"- `beacon machine list|status|release|kill`",
		"- `new <directory> --beacon <profile>` - create a Work chat on a ready beacon profile",
		"",
		"Work chat:",
		"- `beacon status`",
		"- `beacon switch <profile>`",
		"- `beacon switch local`",
	}, "\n")
}

func beaconWorkHelpText() string {
	return strings.Join([]string{
		"Beacon work chat commands",
		"",
		"- `beacon status` - show this Work chat target",
		"- `beacon list` - list all profiles and machines",
		"- `beacon switch <profile>` - switch future turns to a ready profile",
		"- `beacon switch local` - switch future turns back to local execution",
		"- `beacon fork <profile>` - fork when the execution signature is incompatible",
		"",
		"Profile and machine administration belongs in the control chat.",
	}, "\n")
}
