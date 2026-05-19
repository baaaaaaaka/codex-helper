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
	case "allocations":
		if len(words) == 1 {
			return b.formatBeaconAllocations()
		}
		return b.handleBeaconAllocationCommand(ctx, words[1:])
	case "allocation":
		return b.handleBeaconAllocationCommand(ctx, words[1:])
	case "status":
		sessionID := beaconStatusSessionArg(words[1:])
		if sessionID == "" {
			return b.formatBeaconList()
		}
		return b.formatBeaconSessionStatus(sessionID)
	case "release":
		return b.handleBeaconControlRelease(ctx, words[1:])
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
	case "release":
		return b.handleBeaconWorkRelease(ctx, session, words[1:])
	case "profile", "profiles", "machine", "machines", "allocation", "allocations":
		return "Wrong chat.\n\nBeacon profile, allocation, and machine administration belongs in the control chat. Send `beacon list` there for the global view.", nil
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
	case "update", "edit":
		in, err := parseBeaconProfileCreateInput(words[1:])
		if err != nil {
			return "", fmt.Errorf("%s", strings.NewReplacer("create", "update").Replace(err.Error()))
		}
		normalized := normalizedBeaconCommand("profile update " + strings.Join(words[1:], " "))
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, "", func(st *beacon.State) (string, error) {
			proxyExists := b.beaconProxyResolver()
			p, err := beacon.UpdateProfileConfig(st, in.withProxyResolver(proxyExists))
			if err != nil {
				return "", err
			}
			return formatBeaconProfileMutation("Updated", p, proxyExists), nil
		})
	case "doctor":
		name, smoke, err := parseBeaconProfileDoctorArgs(words[1:])
		if err != nil {
			return "", err
		}
		normalized := normalizedBeaconCommand("profile doctor " + name)
		if smoke {
			normalized = normalizedBeaconCommand("profile doctor " + name + " --smoke")
			return b.handleBeaconProfileDoctorSmoke(ctx, msg, idempotencyScope, normalized, name)
		}
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, "", func(st *beacon.State) (string, error) {
			proxyExists := b.beaconProxyResolver()
			p, report, err := beacon.DoctorProfileWithInput(st, name, beacon.DoctorProfileInput{
				Now:                 time.Now(),
				ProxyExists:         proxyExists,
				EnvProviderCommands: beacon.ProviderCommandConfigFromEnv(nil),
			})
			if err != nil {
				return "", err
			}
			return formatBeaconProfileDoctorResult(p, report, proxyExists), nil
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
	case "history":
		name, err := singleBeaconNameArg("beacon profile history", words[1:])
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
		return formatBeaconProfileHistory(st, name, b.beaconProxyResolver())
	case "rollback":
		if len(words) != 3 {
			return "", fmt.Errorf("usage: `beacon profile rollback <name> <revision>`")
		}
		name := strings.TrimSpace(words[1])
		revision, err := strconv.Atoi(strings.TrimSpace(words[2]))
		if err != nil || revision <= 0 {
			return "", fmt.Errorf("profile revision must be a positive integer")
		}
		normalized := normalizedBeaconCommand("profile rollback " + name + " " + strconv.Itoa(revision))
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, "", func(st *beacon.State) (string, error) {
			proxyExists := b.beaconProxyResolver()
			p, err := beacon.RollbackProfileRevision(st, name, revision, time.Now())
			if err != nil {
				return "", err
			}
			return formatBeaconProfileMutation("Rolled back", p, proxyExists), nil
		})
	case "gc", "prune-history":
		name, err := singleBeaconNameArg("beacon profile gc", words[1:])
		if err != nil {
			return "", err
		}
		normalized := normalizedBeaconCommand("profile gc " + name)
		return b.updateBeaconStateFromTeams(msg, idempotencyScope, normalized, "", func(st *beacon.State) (string, error) {
			removed, err := beacon.PruneProfileHistory(st, name)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("Pruned %d unreferenced revisions for beacon profile %q.", removed, name), nil
		})
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
			return fmt.Sprintf("Archived beacon profile %q. Existing pinned turns can drain, but new turns cannot select it.", name), nil
		})
	default:
		return "", fmt.Errorf("unknown beacon profile command %q; use list, create, update, history, rollback, gc, status, doctor, confirm, or delete", words[0])
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

func (b *Bridge) handleBeaconAllocationCommand(ctx context.Context, words []string) (string, error) {
	if len(words) == 0 {
		return b.formatBeaconAllocations()
	}
	switch strings.ToLower(words[0]) {
	case "list", "ls":
		return b.formatBeaconAllocations()
	case "status":
		ref, err := singleBeaconNameArg("beacon allocation status", words[1:])
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
		req, ok := findBeaconAllocation(st, ref)
		if !ok {
			return "", fmt.Errorf("beacon allocation %q not found", ref)
		}
		return formatBeaconAllocationStatus(req), nil
	case "cancel", "release":
		ref, force, err := parseBeaconAllocationCancelArgs(words[1:])
		if err != nil {
			return "", err
		}
		store, err := beacon.NewStore("")
		if err != nil {
			return "", err
		}
		res, err := beacon.CancelAllocationOutsideLock(ctx, store, ref, beacon.NewCommandProviderAdapterFromEnv(nil), "canceled from Teams control chat", force, time.Now())
		if err != nil && strings.TrimSpace(res.Request.ID) == "" {
			return "", err
		}
		return formatBeaconAllocationCancelResultWithError(res, err), nil
	default:
		return "", fmt.Errorf("unknown beacon allocation command %q; use list, status, or cancel", words[0])
	}
}

func (b *Bridge) handleBeaconControlRelease(ctx context.Context, words []string) (string, error) {
	ref, force, confirm, err := parseBeaconControlReleaseArgs(words)
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
	if _, ok := st.Profiles[ref]; ok {
		allocations := beaconAllocationsForProfile(st, ref)
		if len(allocations) == 0 {
			return "Beacon release\n\nNo active allocations are using profile `" + ref + "`.", nil
		}
		preview := beacon.PreviewAllocationRelease(st, "profile", ref, allocations, force)
		if releaseConfirmationRequired(preview, confirm) {
			return formatBeaconAllocationReleasePreview(preview), nil
		}
		var lines []string
		lines = append(lines, formatBeaconAllocationReleasePreview(preview))
		for _, req := range allocations {
			res, cancelErr := beacon.CancelAllocationOutsideLock(ctx, store, req.ID, beacon.NewCommandProviderAdapterFromEnv(nil), "released profile "+ref+" from Teams control chat", force, time.Now())
			lines = append(lines, formatBeaconAllocationCancelResult(res))
			if cancelErr != nil {
				lines = append(lines, "Error: "+cancelErr.Error())
			}
		}
		return strings.Join(lines, "\n\n"), nil
	}
	if req, ok := beacon.FindAllocationByRef(st, ref); ok {
		preview := beacon.PreviewAllocationRelease(st, "allocation", ref, []beacon.AllocationRequest{req}, force)
		if releaseConfirmationRequired(preview, confirm) {
			return formatBeaconAllocationReleasePreview(preview), nil
		}
		res, err := beacon.CancelAllocationOutsideLock(ctx, store, req.ID, beacon.NewCommandProviderAdapterFromEnv(nil), "released from Teams control chat", force, time.Now())
		if err != nil && strings.TrimSpace(res.Request.ID) == "" {
			return "", err
		}
		return formatBeaconAllocationReleasePreview(preview) + "\n\n" + formatBeaconAllocationCancelResultWithError(res, err), nil
	}
	var res beacon.ReleaseResult
	if err := store.Update(func(st *beacon.State) error {
		key, m, ok := findBeaconMachineEntry(*st, ref)
		if !ok {
			return fmt.Errorf("beacon resource %q not found", ref)
		}
		var err error
		res, err = beacon.DecideRelease(m, beacon.ReleaseInput{})
		if err != nil {
			return err
		}
		applyBeaconMachineRelease(st, key, m, res.Action)
		return nil
	}); err != nil {
		return "", err
	}
	return formatBeaconReleaseResult(res), nil
}

func (b *Bridge) handleBeaconWorkRelease(ctx context.Context, session *Session, words []string) (string, error) {
	if len(words) > 0 {
		return "", fmt.Errorf("usage: `beacon release`")
	}
	return releaseBeaconConversationAllocations(ctx, session.ID, "released from Teams Work chat")
}

func releaseBeaconConversationAllocations(ctx context.Context, sessionID string, reason string) (string, error) {
	store, err := beacon.NewStore("")
	if err != nil {
		return "", err
	}
	st, err := store.Load()
	if err != nil {
		return "", err
	}
	allocations := beacon.AllocationsForConversation(st, sessionID)
	if len(allocations) == 0 {
		return "Beacon release\n\nNo active beacon allocation is attached to this Work chat. The profile binding is unchanged.", nil
	}
	preview := beacon.PreviewAllocationRelease(st, "work-chat", sessionID, allocations, false)
	if releaseConfirmationRequired(preview, "") {
		var detach beacon.ConversationDetachResult
		if err := store.Update(func(st *beacon.State) error {
			detach = beacon.DetachConversationDemand(st, sessionID, reason, time.Now())
			return nil
		}); err != nil {
			return "", err
		}
		if len(detach.Detached) > 0 || len(detach.Draining) > 0 {
			return formatBeaconConversationDetachResult(detach), nil
		}
		return formatBeaconAllocationReleasePreview(preview) + "\n\nThis shared resource is used by more than one chat. Use the control chat with the confirmation token if you want to release it for everyone.", nil
	}
	var lines []string
	lines = append(lines, formatBeaconAllocationReleasePreview(preview))
	for _, req := range allocations {
		res, cancelErr := beacon.CancelAllocationOutsideLock(ctx, store, req.ID, beacon.NewCommandProviderAdapterFromEnv(nil), reason, false, time.Now())
		lines = append(lines, formatBeaconAllocationCancelResult(res))
		if cancelErr != nil {
			lines = append(lines, "Error: "+cancelErr.Error())
		}
	}
	lines = append(lines, "Profile binding is unchanged. Future turns may acquire a new worker for the same profile.")
	return strings.Join(lines, "\n\n"), nil
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
	out, err := b.updateBeaconStateFromTeams(msg, session.ChatID, normalizedBeaconCommand("switch local"), "", func(st *beacon.State) (string, error) {
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
	if err != nil {
		return "", err
	}
	if queued {
		return out, nil
	}
	releaseOut, releaseErr := releaseBeaconConversationAllocations(ctx, session.ID, "released automatically after Work chat switched to local")
	if releaseErr != nil {
		if releaseOut != "" {
			return out + "\n\n" + releaseOut + "\n\nRelease warning: " + releaseErr.Error(), nil
		}
		return out + "\n\nRelease warning: " + releaseErr.Error(), nil
	}
	if releaseOut != "" {
		return out + "\n\n" + releaseOut, nil
	}
	return out, nil
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

func (b *Bridge) handleBeaconProfileDoctorSmoke(ctx context.Context, msg ChatMessage, idempotencyScope string, normalized string, name string) (string, error) {
	store, err := beacon.NewStore("")
	if err != nil {
		return "", err
	}
	messageID := scopedBeaconTeamsMessageID(idempotencyScope, msg.ID)
	key := beacon.IdempotencyKey(messageID, normalized, "")
	proxyExists := b.beaconProxyResolver()
	var p beacon.Profile
	var cached string
	var staticFailed bool
	if err := store.Update(func(st *beacon.State) error {
		if messageID != "" {
			if rec, ok := st.Idempotency[key]; ok {
				cached = rec.Result
				return nil
			}
		}
		var report beacon.ProfileDoctorReport
		var err error
		p, report, err = beacon.DoctorProfileWithInput(st, name, beacon.DoctorProfileInput{
			Now:                 time.Now(),
			ProxyExists:         proxyExists,
			EnvProviderCommands: beacon.ProviderCommandConfigFromEnv(nil),
		})
		if err != nil {
			return err
		}
		staticFailed = !report.Passed
		return nil
	}); err != nil {
		return "", err
	}
	if cached != "" {
		return cached, nil
	}
	if staticFailed {
		out := formatBeaconProfileDoctorResult(p, p.DoctorReport, proxyExists)
		if messageID != "" {
			if err := store.Update(func(st *beacon.State) error {
				st.Idempotency[key] = beacon.IdempotencyRecord{Key: key, Result: out, CreatedAt: time.Now()}
				return nil
			}); err != nil {
				return "", err
			}
		}
		return out, nil
	}
	smokeOps := beacon.RunProfileDoctorSmoke(ctx, p, beacon.ProfileDoctorSmokeInput{
		Adapter: beacon.NewCommandProviderAdapterFromEnv(nil),
	})
	var out string
	if err := store.Update(func(st *beacon.State) error {
		updated, err := beacon.ApplyProfileDoctorSmokeReport(st, p.Name, p.Revision, smokeOps, time.Now())
		if err != nil {
			return err
		}
		out = formatBeaconProfileDoctorResult(updated, updated.DoctorReport, proxyExists)
		if messageID != "" {
			st.Idempotency[key] = beacon.IdempotencyRecord{Key: key, Result: out, CreatedAt: time.Now()}
		}
		return nil
	}); err != nil {
		return "", err
	}
	return out, nil
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
	lines = append(lines, "", "Allocations:")
	lines = append(lines, formatBeaconAllocationListLines(st)...)
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

func (b *Bridge) formatBeaconAllocations() (string, error) {
	store, err := beacon.NewStore("")
	if err != nil {
		return "", err
	}
	st, err := store.Load()
	if err != nil {
		return "", err
	}
	lines := []string{"Beacon allocations"}
	lines = append(lines, formatBeaconAllocationListLines(st)...)
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
	return beacon.ConversationStatusNotice(st, sessionID).Render(), nil
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
		revision := p.Revision
		if revision <= 0 {
			revision = 1
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
			Target:          beacon.TargetBeacon,
			Profile:         profile,
			ProfileRevision: revision,
			ProxyRoute:      beaconProfileProxyRoute(p),
			Isolation:       isolation,
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
	var queryCommand string
	var submitCommand string
	var cancelCommand string
	var renewCommand string
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
		case "--query-command":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			queryCommand = v
		case "--submit-command":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			submitCommand = v
		case "--cancel-command":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			cancelCommand = v
		case "--renew-command":
			v, err := value()
			if err != nil {
				return teamsBeaconProfileCreateInput{}, err
			}
			renewCommand = v
		case "--lsf-site-policy":
			in.LSF.SitePolicyDerivesResources = true
		case "--lsf-advanced-approved":
			in.LSF.AdvancedApproved = true
		default:
			return teamsBeaconProfileCreateInput{}, fmt.Errorf("unknown beacon profile create flag %q", words[i])
		}
	}
	in.Adapter = beacon.ProviderCommandConfigForProvider(in.Provider, queryCommand, submitCommand, cancelCommand, renewCommand)
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

func parseBeaconProfileDoctorArgs(words []string) (string, bool, error) {
	var name string
	var smoke bool
	for _, word := range words {
		word = strings.TrimSpace(word)
		if word == "" {
			continue
		}
		switch strings.ToLower(word) {
		case "--smoke":
			smoke = true
		default:
			if strings.HasPrefix(word, "-") {
				return "", false, fmt.Errorf("unknown beacon profile doctor flag %q", word)
			}
			if name != "" {
				return "", false, fmt.Errorf("usage: `beacon profile doctor <name> [--smoke]`")
			}
			name = word
		}
	}
	if name == "" {
		return "", false, fmt.Errorf("usage: `beacon profile doctor <name> [--smoke]`")
	}
	return name, smoke, nil
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

func parseBeaconAllocationCancelArgs(words []string) (string, bool, error) {
	if len(words) == 0 || strings.TrimSpace(words[0]) == "" {
		return "", false, fmt.Errorf("usage: `beacon allocation cancel <allocation-or-provider-job> [--force]`")
	}
	ref := strings.TrimSpace(words[0])
	force := false
	for _, word := range words[1:] {
		switch strings.ToLower(strings.TrimSpace(word)) {
		case "--force":
			force = true
		default:
			return "", false, fmt.Errorf("unknown beacon allocation cancel flag %q", word)
		}
	}
	return ref, force, nil
}

func parseBeaconControlReleaseArgs(words []string) (string, bool, string, error) {
	if len(words) == 0 || strings.TrimSpace(words[0]) == "" {
		return "", false, "", fmt.Errorf("usage: `beacon release <profile|allocation|provider-job|machine> [--force] [--confirm <token>]`")
	}
	ref := strings.TrimSpace(words[0])
	force := false
	var confirm string
	for i := 1; i < len(words); i++ {
		switch strings.ToLower(strings.TrimSpace(words[i])) {
		case "--force":
			force = true
		case "--confirm":
			if i+1 >= len(words) {
				return "", false, "", fmt.Errorf("--confirm requires a token")
			}
			i++
			confirm = strings.TrimSpace(words[i])
		default:
			return "", false, "", fmt.Errorf("unknown beacon release flag %q", words[i])
		}
	}
	return ref, force, confirm, nil
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
		if p.Archived {
			state = "archived"
		} else if p.Ready(proxyExists) {
			state = "ready"
		}
		revision := p.Revision
		if revision <= 0 {
			revision = 1
		}
		lines = append(lines, fmt.Sprintf("- %s: %s, rev=%d, provider=%s, isolation=%s, proxy=%s, adapter=%s", p.Name, state, revision, p.Provider, p.IsolationDefault, profileProxyLabel(p), beaconProfileAdapterLabel(p)))
	}
	return lines
}

func formatBeaconMachineListLines(st beacon.State) []string {
	return beacon.MachineSummaryLines(st)
}

func formatBeaconAllocationListLines(st beacon.State) []string {
	return beacon.AllocationSummaryLines(st)
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
		fmt.Sprintf("- revision: %d", maxBeaconProfileRevision(p.Revision)),
		fmt.Sprintf("- provider: %s", p.Provider),
		fmt.Sprintf("- isolation: %s", p.IsolationDefault),
		fmt.Sprintf("- proxy route: %s", profileProxyLabel(p)),
		fmt.Sprintf("- adapter: %s", beaconProfileAdapterLabel(p)),
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

func formatBeaconProfileDoctorResult(p beacon.Profile, report beacon.ProfileDoctorReport, proxyExists func(string) bool) string {
	state := "failed"
	if report.Passed {
		state = "passed"
	}
	lines := []string{
		"Beacon profile doctor",
		"",
		"Summary:",
		fmt.Sprintf("Profile `%s` doctor %s.", p.Name, state),
		"",
		"State:",
		fmt.Sprintf("- profile: `%s`", p.Name),
		fmt.Sprintf("- revision: `%d`", maxBeaconProfileRevision(p.Revision)),
		fmt.Sprintf("- provider: `%s`", p.Provider),
		fmt.Sprintf("- ready: `%t`", p.Ready(proxyExists)),
		fmt.Sprintf("- adapter: `%s`", beaconProfileAdapterLabel(p)),
	}
	if len(report.Operations) > 0 {
		lines = append(lines, "", "Adapter checks:")
		for _, op := range report.Operations {
			source := strings.TrimSpace(op.Source)
			if source == "" {
				source = "unknown"
			}
			line := fmt.Sprintf("- %s: `%s` via %s", op.Operation, firstNonEmptyString(op.Status, "unknown"), source)
			if strings.TrimSpace(op.ProfileFlag) != "" {
				line += " " + op.ProfileFlag
			}
			if strings.TrimSpace(op.EnvName) != "" {
				line += " / `" + op.EnvName + "`"
			}
			if strings.TrimSpace(op.Error) != "" {
				line += " - " + op.Error
			}
			lines = append(lines, line)
		}
	}
	if len(report.Smoke) > 0 {
		lines = append(lines, "", "Smoke test:")
		for _, op := range report.Smoke {
			line := fmt.Sprintf("- %s: `%s`", op.Operation, firstNonEmptyString(op.Status, "unknown"))
			if strings.TrimSpace(op.ProviderJobID) != "" {
				line += " provider_job=`" + op.ProviderJobID + "`"
			}
			if strings.TrimSpace(op.RawState) != "" {
				line += " state=`" + op.RawState + "`"
			}
			if strings.TrimSpace(op.Reason) != "" {
				line += " reason=`" + op.Reason + "`"
			}
			if strings.TrimSpace(op.Error) != "" {
				line += " - " + op.Error
			}
			lines = append(lines, line)
		}
	}
	if len(report.Issues) > 0 {
		lines = append(lines, "", "Action needed:")
		for _, issue := range report.Issues {
			lines = append(lines, "- "+issue)
		}
		lines = append(lines, "", "After fixing these items, run `beacon profile doctor "+p.Name+"` again, then `beacon profile confirm "+p.Name+"`.")
	} else {
		lines = append(lines, "", "Action needed:", "- Review the profile, then run `beacon profile confirm "+p.Name+"` if it is not confirmed yet.")
	}
	return strings.Join(lines, "\n")
}

func formatBeaconProfileStatus(p beacon.Profile, proxyExists func(string) bool) string {
	lines := []string{
		"Beacon profile",
		"- name: " + p.Name,
		fmt.Sprintf("- revision: %d", maxBeaconProfileRevision(p.Revision)),
		fmt.Sprintf("- provider: %s", p.Provider),
		fmt.Sprintf("- ready: %t", p.Ready(proxyExists)),
		fmt.Sprintf("- confirmed: %t", p.Confirmed),
		fmt.Sprintf("- doctor: %t", p.DoctorOK),
		fmt.Sprintf("- archived: %t", p.Archived),
		fmt.Sprintf("- isolation: %s", p.IsolationDefault),
		"- proxy route: " + profileProxyLabel(p),
		"- adapter: " + beaconProfileAdapterLabel(p),
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
	if len(p.DoctorReport.Operations) > 0 || len(p.DoctorReport.Smoke) > 0 || len(p.DoctorReport.Issues) > 0 {
		lines = append(lines, "Doctor report:")
		for _, op := range p.DoctorReport.Operations {
			line := fmt.Sprintf("- %s: %s", op.Operation, firstNonEmptyString(op.Status, "unknown"))
			if strings.TrimSpace(op.Source) != "" {
				line += " via " + op.Source
			}
			if strings.TrimSpace(op.Error) != "" {
				line += " - " + op.Error
			}
			lines = append(lines, line)
		}
		for _, op := range p.DoctorReport.Smoke {
			line := fmt.Sprintf("- %s: %s", op.Operation, firstNonEmptyString(op.Status, "unknown"))
			if strings.TrimSpace(op.ProviderJobID) != "" {
				line += " provider_job=" + op.ProviderJobID
			}
			if strings.TrimSpace(op.RawState) != "" {
				line += " state=" + op.RawState
			}
			if strings.TrimSpace(op.Error) != "" {
				line += " - " + op.Error
			}
			lines = append(lines, line)
		}
		for _, issue := range p.DoctorReport.Issues {
			lines = append(lines, "- issue: "+issue)
		}
	}
	return strings.Join(lines, "\n")
}

func formatBeaconProfileHistory(st beacon.State, name string, proxyExists func(string) bool) (string, error) {
	revisions := beacon.ListProfileRevisions(st, name)
	if len(revisions) == 0 {
		return "", fmt.Errorf("beacon profile %q not found", name)
	}
	currentRevision := 0
	if current, ok := st.Profiles[strings.TrimSpace(name)]; ok {
		currentRevision = maxBeaconProfileRevision(current.Revision)
	}
	lines := []string{"Beacon profile history", ""}
	for _, p := range revisions {
		state := "draft"
		if p.Archived {
			state = "archived"
		} else if p.Ready(proxyExists) {
			state = "ready"
		}
		kind := "history"
		if maxBeaconProfileRevision(p.Revision) == currentRevision {
			kind = "current"
		}
		lines = append(lines, fmt.Sprintf("- %s rev=%d: %s, provider=%s, isolation=%s, adapter=%s, state=%s", p.Name, maxBeaconProfileRevision(p.Revision), kind, p.Provider, p.IsolationDefault, beaconProfileAdapterLabel(p), state))
	}
	return strings.Join(lines, "\n"), nil
}

func maxBeaconProfileRevision(revision int) int {
	if revision <= 0 {
		return 1
	}
	return revision
}

func formatBeaconMachineStatus(m beacon.Machine) string {
	return beacon.MachineStatusNotice(m).Render()
}

func formatBeaconAllocationStatus(req beacon.AllocationRequest) string {
	return beacon.AllocationStatusNotice(req).Render()
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

func formatBeaconAllocationCancelResult(res beacon.AllocationCancelResult) string {
	req := res.Request
	lines := []string{
		"Beacon release",
		"",
		"Summary:",
		fmt.Sprintf("Allocation `%s` is now `%s`.", req.ID, req.State),
		"",
		"State:",
		"- action: `" + string(res.Action) + "`",
		"- profile: `" + req.Profile + "`",
	}
	if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" {
		lines = append(lines, "- provider job: `"+req.ProviderIdentity.ProviderJobID+"`")
	}
	if strings.TrimSpace(req.RawProviderState) != "" {
		lines = append(lines, "- provider state: `"+req.RawProviderState+"`")
	}
	if strings.TrimSpace(req.ProviderReason) != "" {
		lines = append(lines, "", "Details:", req.ProviderReason)
	}
	return strings.Join(lines, "\n")
}

func formatBeaconConversationDetachResult(res beacon.ConversationDetachResult) string {
	lines := []string{
		"Beacon release",
		"",
		"Summary:",
		"Detached this Work chat from shared beacon resources without releasing them for other chats.",
		"",
		"State:",
		"- chat: `" + res.ConversationID + "`",
		"- detached allocations: `" + strings.Join(res.Detached, ",") + "`",
		"- draining allocations: `" + strings.Join(res.Draining, ",") + "`",
		"- escalated releases: `" + strings.Join(res.Escalated, ",") + "`",
		"",
		"Action needed:",
		"- None. The profile binding is unchanged; a future turn can acquire a fresh worker if needed.",
	}
	if len(res.Skipped) > 0 {
		lines = append(lines, "", "Details:", "Skipped non-shared allocations: `"+strings.Join(res.Skipped, ",")+"`.")
	}
	return strings.Join(lines, "\n")
}

func formatBeaconAllocationReleasePreview(preview beacon.AllocationReleasePreview) string {
	lines := []string{
		"Beacon release preview",
		"",
		"Summary:",
		fmt.Sprintf("cxp will %s %d allocation(s) for `%s`.", releasePreviewVerb(preview), len(preview.Allocations), preview.Ref),
		"",
		"State:",
		"- scope: `" + preview.Scope + "`",
		"- ref: `" + preview.Ref + "`",
		"- affected chats: `" + strings.Join(preview.AffectedChats, ",") + "`",
		"- queued turns: `" + strings.Join(preview.QueuedTurns, ",") + "`",
		"- running turns: `" + strings.Join(preview.RunningTurns, ",") + "`",
	}
	for _, item := range preview.Allocations {
		lines = append(lines,
			fmt.Sprintf("- allocation `%s`: action=`%s` profile=`%s` state=`%s` provider_job=`%s` machines=`%s`", item.AllocationID, item.Action, item.Profile, item.State, item.ProviderJob, strings.Join(item.Machines, ",")),
		)
	}
	if preview.RequiresConfirmation {
		lines = append(lines,
			"",
			"Action needed:",
			"- This release can affect a shared or forced resource.",
			"- Re-send the command with `--confirm "+preview.Confirmation+"` if this is intended.",
		)
	} else {
		lines = append(lines,
			"",
			"Action needed:",
			"- None. cxp can apply this release safely.",
		)
	}
	return strings.Join(lines, "\n")
}

func releasePreviewVerb(preview beacon.AllocationReleasePreview) string {
	if preview.Force {
		return "force release"
	}
	actions := map[string]struct{}{}
	for _, item := range preview.Allocations {
		actions[item.Action] = struct{}{}
	}
	if _, ok := actions["drain"]; ok {
		return "drain"
	}
	if _, ok := actions["cancel_provider_job"]; ok {
		return "cancel"
	}
	return "release"
}

func releaseConfirmationRequired(preview beacon.AllocationReleasePreview, provided string) bool {
	if !preview.RequiresConfirmation {
		return false
	}
	return strings.TrimSpace(provided) == "" || strings.TrimSpace(provided) != strings.TrimSpace(preview.Confirmation)
}

func formatBeaconAllocationCancelResultWithError(res beacon.AllocationCancelResult, err error) string {
	out := formatBeaconAllocationCancelResult(res)
	if err != nil {
		out += "\n\nError:\n" + err.Error()
	}
	return out
}

func beaconAllocationsForProfile(st beacon.State, profile string) []beacon.AllocationRequest {
	profile = strings.TrimSpace(profile)
	var out []beacon.AllocationRequest
	for _, req := range st.Allocations {
		if strings.TrimSpace(req.Profile) != profile && strings.TrimSpace(req.Target.Profile) != profile {
			continue
		}
		switch req.State {
		case beacon.AllocationCanceled, beacon.AllocationExpired, beacon.AllocationFailed:
			continue
		}
		out = append(out, req)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
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

func beaconProfileAdapterLabel(p beacon.Profile) string {
	ops := beacon.ConfiguredProviderCommandOperations(p.Adapter, p.Provider)
	if len(ops) == 0 {
		return "helper environment"
	}
	return "profile:" + strings.Join(ops, ",")
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

func sortedBeaconAllocations(st beacon.State) []beacon.AllocationRequest {
	out := make([]beacon.AllocationRequest, 0, len(st.Allocations))
	for _, req := range st.Allocations {
		out = append(out, req)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func findBeaconAllocation(st beacon.State, ref string) (beacon.AllocationRequest, bool) {
	ref = strings.TrimSpace(ref)
	for _, req := range st.Allocations {
		if req.ID == ref || req.DeterministicName == ref || req.ProviderIdentity.ProviderJobID == ref {
			return req, true
		}
	}
	return beacon.AllocationRequest{}, false
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
		"- add `--query-command <script> --submit-command <script> --cancel-command <script> --renew-command <script>` to store provider adapters on the profile",
		"- `beacon profile update <name> ...` - create a new profile revision without breaking bound Work chats",
		"- `beacon profile history <name>` / `beacon profile rollback <name> <revision>` / `beacon profile gc <name>`",
		"- `beacon profile create <name> --provider lsf --queue <queue>`",
		"- `beacon profile create <name> --provider local`",
		"- `beacon profile doctor <name>` - validate profile fields and provider adapters without touching the scheduler",
		"- `beacon profile doctor <name> --smoke` - submit, query, and cancel one real scheduler allocation",
		"- `beacon profile confirm <name>` - confirm a profile after reviewing doctor output",
		"- `beacon profile status <name>`",
		"- `beacon release <profile|allocation|provider-job|machine> [--force] [--confirm <token>]` - preview and release a resource without internal object knowledge",
		"- advanced: `beacon allocation list|status|cancel`, `beacon machine list|status|release|kill`",
		"- provider adapter: `cxp beacon provider template slurm|lsf`",
		"- worker process: `cxp beacon worker serve --allocation <request-id>`",
		"- `new <directory> --beacon <profile>` - create a Work chat on a ready beacon profile",
		"",
		"Work chat:",
		"- `beacon status`",
		"- `beacon switch <profile>`",
		"- `beacon switch local` - future turns run locally and cxp drains/releases this chat's old beacon resource when safe",
		"- `beacon release` - release this Work chat's current beacon resource; shared workers detach only this chat unless control chat confirms a global release",
	}, "\n")
}

func beaconWorkHelpText() string {
	return strings.Join([]string{
		"Beacon work chat commands",
		"",
		"- `beacon status` - show this Work chat target",
		"- `beacon list` - list all profiles and machines",
		"- `beacon switch <profile>` - switch future turns to a ready profile",
		"- `beacon switch local` - switch future turns back to local execution and release old beacon resources when safe",
		"- `beacon release` - release this chat's current beacon resource; shared workers detach only this chat",
		"- `beacon fork <profile>` - fork when the execution signature is incompatible",
		"",
		"Profile and machine administration belongs in the control chat.",
	}, "\n")
}
