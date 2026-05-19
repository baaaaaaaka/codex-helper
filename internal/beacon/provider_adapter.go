package beacon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	BeaconSlurmQueryCommandEnv  = "CODEX_HELPER_BEACON_SLURM_QUERY"
	BeaconSlurmSubmitCommandEnv = "CODEX_HELPER_BEACON_SLURM_SUBMIT"
	BeaconSlurmCancelCommandEnv = "CODEX_HELPER_BEACON_SLURM_CANCEL"
	BeaconSlurmRenewCommandEnv  = "CODEX_HELPER_BEACON_SLURM_RENEW"
	BeaconLSFQueryCommandEnv    = "CODEX_HELPER_BEACON_LSF_QUERY"
	BeaconLSFSubmitCommandEnv   = "CODEX_HELPER_BEACON_LSF_SUBMIT"
	BeaconLSFCancelCommandEnv   = "CODEX_HELPER_BEACON_LSF_CANCEL"
	BeaconLSFRenewCommandEnv    = "CODEX_HELPER_BEACON_LSF_RENEW"
)

type ProviderCommandRunner interface {
	RunProviderCommand(ctx context.Context, name string, args []string) (string, error)
}

type ProviderCommandRunnerFunc func(context.Context, string, []string) (string, error)

func (f ProviderCommandRunnerFunc) RunProviderCommand(ctx context.Context, name string, args []string) (string, error) {
	return f(ctx, name, args)
}

type ProviderCommandConfig struct {
	SlurmQueryCommand  string `json:"slurm_query_command,omitempty"`
	SlurmSubmitCommand string `json:"slurm_submit_command,omitempty"`
	SlurmCancelCommand string `json:"slurm_cancel_command,omitempty"`
	SlurmRenewCommand  string `json:"slurm_renew_command,omitempty"`
	LSFQueryCommand    string `json:"lsf_query_command,omitempty"`
	LSFSubmitCommand   string `json:"lsf_submit_command,omitempty"`
	LSFCancelCommand   string `json:"lsf_cancel_command,omitempty"`
	LSFRenewCommand    string `json:"lsf_renew_command,omitempty"`
}

func ProviderCommandConfigFromEnv(getenv func(string) string) ProviderCommandConfig {
	if getenv == nil {
		getenv = os.Getenv
	}
	return ProviderCommandConfig{
		SlurmQueryCommand:  strings.TrimSpace(getenv(BeaconSlurmQueryCommandEnv)),
		SlurmSubmitCommand: strings.TrimSpace(getenv(BeaconSlurmSubmitCommandEnv)),
		SlurmCancelCommand: strings.TrimSpace(getenv(BeaconSlurmCancelCommandEnv)),
		SlurmRenewCommand:  strings.TrimSpace(getenv(BeaconSlurmRenewCommandEnv)),
		LSFQueryCommand:    strings.TrimSpace(getenv(BeaconLSFQueryCommandEnv)),
		LSFSubmitCommand:   strings.TrimSpace(getenv(BeaconLSFSubmitCommandEnv)),
		LSFCancelCommand:   strings.TrimSpace(getenv(BeaconLSFCancelCommandEnv)),
		LSFRenewCommand:    strings.TrimSpace(getenv(BeaconLSFRenewCommandEnv)),
	}
}

func ProviderCommandConfigForProvider(provider Provider, query, submit, cancel, renew string) ProviderCommandConfig {
	config := ProviderCommandConfig{}
	switch provider {
	case ProviderSlurm:
		config.SlurmQueryCommand = strings.TrimSpace(query)
		config.SlurmSubmitCommand = strings.TrimSpace(submit)
		config.SlurmCancelCommand = strings.TrimSpace(cancel)
		config.SlurmRenewCommand = strings.TrimSpace(renew)
	case ProviderLSF:
		config.LSFQueryCommand = strings.TrimSpace(query)
		config.LSFSubmitCommand = strings.TrimSpace(submit)
		config.LSFCancelCommand = strings.TrimSpace(cancel)
		config.LSFRenewCommand = strings.TrimSpace(renew)
	}
	return config
}

func MergeProviderCommandConfig(base ProviderCommandConfig, override ProviderCommandConfig) ProviderCommandConfig {
	out := base
	if strings.TrimSpace(override.SlurmQueryCommand) != "" {
		out.SlurmQueryCommand = strings.TrimSpace(override.SlurmQueryCommand)
	}
	if strings.TrimSpace(override.SlurmSubmitCommand) != "" {
		out.SlurmSubmitCommand = strings.TrimSpace(override.SlurmSubmitCommand)
	}
	if strings.TrimSpace(override.SlurmCancelCommand) != "" {
		out.SlurmCancelCommand = strings.TrimSpace(override.SlurmCancelCommand)
	}
	if strings.TrimSpace(override.SlurmRenewCommand) != "" {
		out.SlurmRenewCommand = strings.TrimSpace(override.SlurmRenewCommand)
	}
	if strings.TrimSpace(override.LSFQueryCommand) != "" {
		out.LSFQueryCommand = strings.TrimSpace(override.LSFQueryCommand)
	}
	if strings.TrimSpace(override.LSFSubmitCommand) != "" {
		out.LSFSubmitCommand = strings.TrimSpace(override.LSFSubmitCommand)
	}
	if strings.TrimSpace(override.LSFCancelCommand) != "" {
		out.LSFCancelCommand = strings.TrimSpace(override.LSFCancelCommand)
	}
	if strings.TrimSpace(override.LSFRenewCommand) != "" {
		out.LSFRenewCommand = strings.TrimSpace(override.LSFRenewCommand)
	}
	return out
}

func ConfiguredProviderCommandOperations(config ProviderCommandConfig, provider Provider) []string {
	var out []string
	for _, operation := range []string{"query", "submit", "cancel", "renew"} {
		if command, _, _ := providerCommandFromConfig(config, provider, operation); strings.TrimSpace(command) != "" {
			out = append(out, operation)
		}
	}
	return out
}

type CommandProviderAdapter struct {
	Config ProviderCommandConfig
	Runner ProviderCommandRunner
}

func NewCommandProviderAdapterFromEnv(getenv func(string) string) CommandProviderAdapter {
	return CommandProviderAdapter{
		Config: ProviderCommandConfigFromEnv(getenv),
		Runner: ExecProviderCommandRunner{},
	}
}

func (a CommandProviderAdapter) QueryAllocation(ctx context.Context, req AllocationRequest) (SchedulerQueryResult, error) {
	command, source, err := a.commandFor(req, "query")
	if err != nil {
		return SchedulerQueryResult{}, err
	}
	out, err := a.run(ctx, command, providerCommandArgs(req, "query"))
	if err != nil {
		return SchedulerQueryResult{}, fmt.Errorf("query beacon provider allocation via %s: %w", source, err)
	}
	result, err := ParseProviderCommandResult(out)
	if err != nil {
		return SchedulerQueryResult{}, fmt.Errorf("parse beacon provider query result from %s: %w", source, err)
	}
	return result, nil
}

func (a CommandProviderAdapter) SubmitAllocation(ctx context.Context, req AllocationRequest) (SchedulerQueryResult, error) {
	command, source, err := a.commandFor(req, "submit")
	if err != nil {
		return SchedulerQueryResult{}, err
	}
	out, err := a.run(ctx, command, providerCommandArgs(req, "submit"))
	if err != nil {
		return SchedulerQueryResult{}, fmt.Errorf("submit beacon provider allocation via %s: %w", source, err)
	}
	result, err := ParseProviderCommandResult(out)
	if err != nil {
		return SchedulerQueryResult{}, fmt.Errorf("parse beacon provider submit result from %s: %w", source, err)
	}
	return result, nil
}

func (a CommandProviderAdapter) CancelAllocation(ctx context.Context, req AllocationRequest) (SchedulerQueryResult, error) {
	command, source, err := a.commandFor(req, "cancel")
	if err != nil {
		return SchedulerQueryResult{}, err
	}
	out, err := a.run(ctx, command, providerCommandArgs(req, "cancel"))
	if err != nil {
		return SchedulerQueryResult{}, fmt.Errorf("cancel beacon provider allocation via %s: %w", source, err)
	}
	result, err := ParseProviderCommandResult(out)
	if err != nil {
		return SchedulerQueryResult{}, fmt.Errorf("parse beacon provider cancel result from %s: %w", source, err)
	}
	return result, nil
}

func (a CommandProviderAdapter) RenewAllocation(ctx context.Context, req AllocationRequest) (SchedulerQueryResult, error) {
	command, source, err := a.commandFor(req, "renew")
	if err != nil {
		return SchedulerQueryResult{}, err
	}
	out, err := a.run(ctx, command, providerCommandArgs(req, "renew"))
	if err != nil {
		return SchedulerQueryResult{}, fmt.Errorf("renew beacon provider allocation via %s: %w", source, err)
	}
	result, err := ParseProviderCommandResult(out)
	if err != nil {
		return SchedulerQueryResult{}, fmt.Errorf("parse beacon provider renew result from %s: %w", source, err)
	}
	return result, nil
}

type ProviderCommandNotConfiguredError struct {
	Provider    Provider
	Operation   string
	EnvName     string
	ProfileName string
	ProfileFlag string
}

func (e ProviderCommandNotConfiguredError) Error() string {
	provider := strings.TrimSpace(string(e.Provider))
	if provider == "" {
		provider = "provider"
	}
	operation := strings.TrimSpace(e.Operation)
	if operation != "" {
		operation = " " + operation
	}
	hints := []string{}
	if strings.TrimSpace(e.ProfileFlag) != "" {
		hints = append(hints, "profile adapter "+e.ProfileFlag)
	}
	if strings.TrimSpace(e.EnvName) != "" {
		hints = append(hints, e.EnvName)
	}
	if len(hints) == 0 {
		hints = append(hints, "a provider adapter command")
	}
	return fmt.Sprintf("beacon provider adapter for %s%s is not configured: set %s to an executable that accepts beacon allocation flags and prints JSON or key=value status", provider, operation, strings.Join(hints, " or "))
}

func IsProviderCommandNotConfigured(err error) bool {
	var target ProviderCommandNotConfiguredError
	return errors.As(err, &target)
}

func (a CommandProviderAdapter) commandFor(req AllocationRequest, operation string) (string, string, error) {
	provider := req.Provider
	profile := req.ProfileSnapshot
	if strings.TrimSpace(profile.Name) == "" {
		profile.Name = req.Profile
	}
	if profile.Provider == "" {
		profile.Provider = provider
	}
	if command, _, flag := providerCommandFromConfig(profile.Adapter, provider, operation); strings.TrimSpace(command) != "" {
		source := "profile adapter " + flag
		if strings.TrimSpace(profile.Name) != "" {
			source += " for " + profile.Name
		}
		return command, source, nil
	}
	command, envName, flag := providerCommandFromConfig(a.Config, provider, operation)
	if strings.TrimSpace(command) == "" {
		if envName == "" {
			return "", "", providerCommandError(provider, operation)
		}
		return "", envName, ProviderCommandNotConfiguredError{
			Provider:    provider,
			Operation:   operation,
			EnvName:     envName,
			ProfileName: profile.Name,
			ProfileFlag: flag,
		}
	}
	return command, envName, nil
}

func providerCommandFromConfig(config ProviderCommandConfig, provider Provider, operation string) (string, string, string) {
	switch provider {
	case ProviderSlurm:
		switch operation {
		case "query":
			return strings.TrimSpace(config.SlurmQueryCommand), BeaconSlurmQueryCommandEnv, "--query-command"
		case "submit":
			return strings.TrimSpace(config.SlurmSubmitCommand), BeaconSlurmSubmitCommandEnv, "--submit-command"
		case "cancel":
			return strings.TrimSpace(config.SlurmCancelCommand), BeaconSlurmCancelCommandEnv, "--cancel-command"
		case "renew":
			return strings.TrimSpace(config.SlurmRenewCommand), BeaconSlurmRenewCommandEnv, "--renew-command"
		default:
			return "", "", ""
		}
	case ProviderLSF:
		switch operation {
		case "query":
			return strings.TrimSpace(config.LSFQueryCommand), BeaconLSFQueryCommandEnv, "--query-command"
		case "submit":
			return strings.TrimSpace(config.LSFSubmitCommand), BeaconLSFSubmitCommandEnv, "--submit-command"
		case "cancel":
			return strings.TrimSpace(config.LSFCancelCommand), BeaconLSFCancelCommandEnv, "--cancel-command"
		case "renew":
			return strings.TrimSpace(config.LSFRenewCommand), BeaconLSFRenewCommandEnv, "--renew-command"
		default:
			return "", "", ""
		}
	default:
		return "", "", ""
	}
}

func providerCommandError(provider Provider, operation string) error {
	switch provider {
	case ProviderSlurm, ProviderLSF:
		return fmt.Errorf("unknown beacon provider operation %q", operation)
	default:
		return fmt.Errorf("beacon provider %q does not support external allocation commands", provider)
	}
}

func (a CommandProviderAdapter) run(ctx context.Context, command string, args []string) (string, error) {
	runner := a.Runner
	if runner == nil {
		runner = ExecProviderCommandRunner{}
	}
	return runner.RunProviderCommand(ctx, command, args)
}

type ExecProviderCommandRunner struct{}

func (ExecProviderCommandRunner) RunProviderCommand(ctx context.Context, name string, args []string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("empty provider command")
	}
	cmd := exec.CommandContext(ctx, name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		text := strings.TrimSpace(out.String())
		if text != "" {
			return out.String(), fmt.Errorf("%w: %s", err, truncateProviderOutput(text))
		}
		return out.String(), err
	}
	return out.String(), nil
}

func providerCommandArgs(req AllocationRequest, operation string) []string {
	profile := req.ProfileSnapshot
	if strings.TrimSpace(profile.Name) == "" {
		profile.Name = req.Profile
		profile.Provider = req.Provider
	}
	args := []string{
		"--request-id", strings.TrimSpace(req.ID),
		"--name", strings.TrimSpace(req.DeterministicName),
		"--conversation", strings.TrimSpace(req.ConversationID),
		"--turn", strings.TrimSpace(req.TurnID),
		"--profile", strings.TrimSpace(req.Profile),
		"--provider", string(req.Provider),
		"--isolation", string(req.Isolation),
	}
	args = append(args, "--operation", strings.TrimSpace(operation))
	if strings.TrimSpace(req.Execution.Hash) != "" {
		args = append(args, "--execution-hash", strings.TrimSpace(req.Execution.Hash))
	}
	if strings.TrimSpace(req.Target.ProxyRoute) != "" {
		args = append(args, "--proxy-route", strings.TrimSpace(req.Target.ProxyRoute))
	}
	if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" {
		args = append(args, "--provider-job-id", strings.TrimSpace(req.ProviderIdentity.ProviderJobID))
	}
	if req.RenewEpoch > 0 {
		args = append(args, "--renew-epoch", strconv.Itoa(req.RenewEpoch))
	}
	if req.ReplacementEpoch > 0 {
		args = append(args, "--replacement-epoch", strconv.Itoa(req.ReplacementEpoch))
	}
	if strings.TrimSpace(req.ReplacementID) != "" {
		args = append(args, "--replacement-of", strings.TrimSpace(req.ReplacementID))
	}
	switch req.Provider {
	case ProviderSlurm:
		if profile.Slurm.Partition != "" {
			args = append(args, "--partition", profile.Slurm.Partition)
		}
		if profile.Slurm.Image != "" {
			args = append(args, "--image", profile.Slurm.Image)
		}
		if profile.Slurm.Nodes > 0 {
			args = append(args, "--nodes", strconv.Itoa(profile.Slurm.Nodes))
		}
		if profile.Slurm.GPUCount > 0 {
			args = append(args, "--gpu", strconv.Itoa(profile.Slurm.GPUCount))
		}
		if profile.Slurm.Duration > 0 {
			args = append(args, "--duration", strconv.Itoa(profile.Slurm.Duration))
		}
	case ProviderLSF:
		if profile.LSF.QueueName != "" {
			args = append(args, "--queue", profile.LSF.QueueName)
		}
		if profile.LSF.SitePolicyDerivesResources {
			args = append(args, "--lsf-site-policy")
		}
		if profile.LSF.AdvancedApproved {
			args = append(args, "--lsf-advanced-approved")
		}
	}
	return args
}

func ParseProviderCommandResult(out string) (SchedulerQueryResult, error) {
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return SchedulerQueryResult{}, nil
	}
	var wire struct {
		ProviderJobID    string `json:"provider_job_id"`
		RawState         string `json:"raw_state"`
		Reason           string `json:"reason"`
		DurableNegative  bool   `json:"durable_negative"`
		QueryError       bool   `json:"query_error"`
		MultipleMatches  bool   `json:"multiple_matches"`
		ProviderDeadline string `json:"provider_deadline"`
	}
	if err := json.Unmarshal([]byte(trimmed), &wire); err == nil {
		deadline, _ := parseProviderDeadline(wire.ProviderDeadline)
		return SchedulerQueryResult{
			ProviderJobID:    strings.TrimSpace(wire.ProviderJobID),
			RawState:         strings.TrimSpace(wire.RawState),
			Reason:           strings.TrimSpace(wire.Reason),
			DurableNegative:  wire.DurableNegative,
			QueryError:       wire.QueryError,
			MultipleMatches:  wire.MultipleMatches,
			ProviderDeadline: deadline,
		}, nil
	}
	values := parseProviderKeyValues(trimmed)
	if len(values) == 0 {
		return SchedulerQueryResult{}, fmt.Errorf("provider command output must be JSON or key=value pairs")
	}
	return SchedulerQueryResult{
		ProviderJobID:    firstNonEmpty(values["provider_job_id"], values["job_id"]),
		RawState:         firstNonEmpty(values["raw_state"], values["state"]),
		Reason:           values["reason"],
		DurableNegative:  parseProviderBool(values["durable_negative"]),
		QueryError:       parseProviderBool(values["query_error"]),
		MultipleMatches:  parseProviderBool(values["multiple_matches"]),
		ProviderDeadline: mustParseProviderDeadline(firstNonEmpty(values["provider_deadline"], values["deadline"])),
	}, nil
}

func parseProviderKeyValues(out string) map[string]string {
	values := map[string]string{}
	for _, field := range strings.Fields(out) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			continue
		}
		values[key] = strings.TrimSpace(value)
	}
	return values
}

func parseProviderBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y":
		return true
	default:
		return false
	}
}

func parseProviderDeadline(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	if ts, err := time.Parse(time.RFC3339, value); err == nil {
		return ts, nil
	}
	if unix, err := strconv.ParseInt(value, 10, 64); err == nil && unix > 0 {
		return time.Unix(unix, 0), nil
	}
	return time.Time{}, fmt.Errorf("invalid provider deadline %q", value)
}

func mustParseProviderDeadline(value string) time.Time {
	deadline, _ := parseProviderDeadline(value)
	return deadline
}

func truncateProviderOutput(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 512 {
		return value
	}
	return value[:512] + "...(truncated)"
}
