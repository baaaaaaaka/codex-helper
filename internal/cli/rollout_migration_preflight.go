package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
)

const maxCodexRolloutMigrationOutputBytes = 1 << 20

type codexRolloutMigrationReport struct {
	Outcomes []codexRolloutMigrationOutcome `json:"outcomes"`
}

type codexRolloutMigrationOutcome struct {
	ThreadID string `json:"thread_id"`
	Status   string `json:"status"`
	Message  string `json:"message"`
}

// migrateCodexRolloutBeforeTUI asks the official Codex CLI to publish one
// legacy rollout before the remote broker can start app-server. The broker
// must never see a legacy full-history resume: its stdout transport is a
// single-line JSONL protocol with a finite line limit.
func migrateCodexRolloutBeforeTUI(
	ctx context.Context,
	launch codexrunner.AppServerLaunchContext,
	identity *execIdentity,
	threadID string,
) error {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return fmt.Errorf("cannot migrate Codex rollout before TUI: missing thread id")
	}
	if strings.TrimSpace(launch.Command) == "" {
		return fmt.Errorf("cannot migrate Codex rollout %q before TUI: missing Codex command", threadID)
	}

	stdout := &limitedBuffer{max: maxCodexRolloutMigrationOutputBytes}
	stderr := &limitedBuffer{max: maxCodexRolloutMigrationOutputBytes}
	extraEnv := clearEnvironmentKeys(launch.ExtraEnv, codexrunner.RemoteBrokerAuthTokenEnv, envCodexSQLiteHome)
	err := runTargetOnceWithOptions(
		ctx,
		[]string{launch.Command, "migrate-rollouts", "--apply", "--thread", threadID, "--json"},
		"",
		nil,
		nil,
		stdout,
		stderr,
		runTargetOptions{
			Cwd:          launch.WorkingDir,
			ExtraEnv:     extraEnv,
			ExecIdentity: identity,
			Stdin:        strings.NewReader(""),
			Stdout:       io.Discard,
			Stderr:       io.Discard,
		},
	)
	if err != nil {
		return fmt.Errorf("migrate legacy Codex rollout %q before TUI: %w%s%s", threadID, err, migrationOutputDetail("stdout", stdout), migrationOutputDetail("stderr", stderr))
	}

	var report codexRolloutMigrationReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		return fmt.Errorf("migrate legacy Codex rollout %q before TUI: parse JSON report: %w%s", threadID, err, migrationOutputDetail("stdout", stdout))
	}
	if err := validateCodexRolloutMigrationReport(report, threadID); err != nil {
		return fmt.Errorf("migrate legacy Codex rollout %q before TUI: %w", threadID, err)
	}
	return nil
}

func clearEnvironmentKeys(environment []string, keys ...string) []string {
	if len(keys) == 0 {
		return append([]string(nil), environment...)
	}
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			filtered = append(filtered, entry)
			continue
		}
		remove := false
		for _, candidate := range keys {
			if envKeyEqual(key, candidate) {
				remove = true
				break
			}
		}
		if !remove {
			filtered = append(filtered, entry)
		}
	}
	for _, key := range keys {
		filtered = append(filtered, key+"=")
	}
	return filtered
}

func validateCodexRolloutMigrationReport(report codexRolloutMigrationReport, threadID string) error {
	threadID = strings.TrimSpace(threadID)
	var matched *codexRolloutMigrationOutcome
	for index := range report.Outcomes {
		outcome := &report.Outcomes[index]
		if !strings.EqualFold(strings.TrimSpace(outcome.ThreadID), threadID) {
			continue
		}
		if matched != nil {
			return fmt.Errorf("migration report contains multiple outcomes for the target thread")
		}
		matched = outcome
	}
	if matched == nil {
		return fmt.Errorf("migration report contains no outcome for the target thread")
	}
	switch strings.TrimSpace(matched.Status) {
	case "migrated", "already_paginated", "skipped_empty":
		return nil
	case "skipped_busy":
		return fmt.Errorf("migration skipped the target because it is busy")
	case "failed":
		if message := strings.TrimSpace(matched.Message); message != "" {
			return fmt.Errorf("migration failed: %s", message)
		}
		return fmt.Errorf("migration failed")
	default:
		return fmt.Errorf("migration returned unsupported status %q", matched.Status)
	}
}

func migrationOutputDetail(label string, output *limitedBuffer) string {
	if output == nil {
		return ""
	}
	value := strings.TrimSpace(output.String())
	if value == "" {
		return ""
	}
	if output.Truncated() {
		value += " [truncated]"
	}
	return fmt.Sprintf("; %s: %s", label, value)
}

// explicitCodexResumeThreadID intentionally accepts only the unambiguous
// `resume <uuid>` form. Name lookup, --last and picker-based resume do not
// identify the rollout early enough to safely run this preflight.
func explicitCodexResumeThreadID(args []string) string {
	if len(args) != 1 {
		return ""
	}
	threadID := strings.TrimSpace(args[0])
	if !isCanonicalCodexThreadID(threadID) {
		return ""
	}
	return threadID
}

func isCanonicalCodexThreadID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, char := range value {
		switch index {
		case 8, 13, 18, 23:
			if char != '-' {
				return false
			}
		default:
			if !isHexDigit(char) {
				return false
			}
		}
	}
	return true
}

func isHexDigit(char rune) bool {
	return (char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')
}
