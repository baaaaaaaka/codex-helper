package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
)

func TestValidateCodexRolloutMigrationReport(t *testing.T) {
	threadID := "11111111-2222-3333-4444-555555555555"
	tests := []struct {
		name   string
		status string
		msg    string
		want   string
	}{
		{name: "migrated", status: "migrated"},
		{name: "already paginated", status: "already_paginated"},
		{name: "empty rollout", status: "skipped_empty"},
		{name: "busy", status: "skipped_busy", want: "busy"},
		{name: "failed", status: "failed", msg: "invalid session metadata", want: "invalid session metadata"},
		{name: "unknown", status: "future_status", want: "unsupported status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateCodexRolloutMigrationReport(codexRolloutMigrationReport{
				Outcomes: []codexRolloutMigrationOutcome{{ThreadID: threadID, Status: test.status, Message: test.msg}},
			}, threadID)
			if test.want == "" {
				if err != nil {
					t.Fatalf("validate report: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate report error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidateCodexRolloutMigrationReportRequiresOneTargetOutcome(t *testing.T) {
	threadID := "11111111-2222-3333-4444-555555555555"
	tests := []struct {
		name     string
		outcomes []codexRolloutMigrationOutcome
	}{
		{name: "empty", outcomes: nil},
		{name: "other thread", outcomes: []codexRolloutMigrationOutcome{{ThreadID: "22222222-3333-4444-5555-666666666666", Status: "migrated"}}},
		{name: "duplicate target", outcomes: []codexRolloutMigrationOutcome{{ThreadID: threadID, Status: "migrated"}, {ThreadID: threadID, Status: "already_paginated"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateCodexRolloutMigrationReport(codexRolloutMigrationReport{Outcomes: test.outcomes}, threadID); err == nil {
				t.Fatal("validate report unexpectedly succeeded")
			}
		})
	}
}

func TestExplicitCodexResumeThreadIDOnlyAcceptsCanonicalID(t *testing.T) {
	valid := "11111111-2222-3333-4444-555555555555"
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "canonical id", args: []string{valid}, want: valid},
		{name: "last", args: []string{"--last"}},
		{name: "name", args: []string{"named-session"}},
		{name: "extra argument", args: []string{valid, "prompt"}},
		{name: "wrong uuid shape", args: []string{"11111111222233334444555555555555"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := explicitCodexResumeThreadID(test.args); got != test.want {
				t.Fatalf("explicitCodexResumeThreadID(%v) = %q, want %q", test.args, got, test.want)
			}
		})
	}
}

func TestClearEnvironmentKeysRemovesInheritedBrokerState(t *testing.T) {
	environment := []string{
		"PATH=/bin",
		codexrunner.RemoteBrokerAuthTokenEnv + "=inherited-token",
		envCodexSQLiteHome + "=/tmp/remote-sqlite",
		"CODEX_HOME=/tmp/codex",
	}
	filtered := clearEnvironmentKeys(environment, codexrunner.RemoteBrokerAuthTokenEnv, envCodexSQLiteHome)
	joined := strings.Join(filtered, "\n")
	for _, forbidden := range []string{"inherited-token", "/tmp/remote-sqlite"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("filtered environment retained %q: %v", forbidden, filtered)
		}
	}
	if !strings.Contains(joined, "CODEX_HOME=/tmp/codex") {
		t.Fatalf("filtered environment lost Codex home: %v", filtered)
	}
	if got := clearEnvironmentKeys(nil, codexrunner.RemoteBrokerAuthTokenEnv, envCodexSQLiteHome); !strings.Contains(strings.Join(got, "\n"), codexrunner.RemoteBrokerAuthTokenEnv+"=") || !strings.Contains(strings.Join(got, "\n"), envCodexSQLiteHome+"=") {
		t.Fatalf("empty environment did not clear inherited keys: %v", got)
	}
}

func TestMigrateCodexRolloutBeforeTUIRejectsMissingTarget(t *testing.T) {
	err := migrateCodexRolloutBeforeTUI(context.Background(), codexrunner.AppServerLaunchContext{Command: "codex"}, nil, " ")
	if err == nil || !strings.Contains(err.Error(), "missing thread id") {
		t.Fatalf("missing target error = %v", err)
	}
}
