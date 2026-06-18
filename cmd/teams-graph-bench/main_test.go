package main

import (
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

func TestValidateMembersRequiresExactOwnerAndTestUser(t *testing.T) {
	members := []teams.ChatMember{
		{UserID: "owner-user", DisplayName: "Owner"},
		{UserID: "test-user", DisplayName: "Test"},
	}
	if err := validateMembers(members, "owner-user", "test-user"); err != nil {
		t.Fatalf("validateMembers returned error: %v", err)
	}

	cases := []struct {
		name       string
		members    []teams.ChatMember
		wantErrSub string
	}{
		{
			name: "missing member",
			members: []teams.ChatMember{
				{UserID: "owner-user", DisplayName: "Owner"},
			},
			wantErrSub: "want exactly 2",
		},
		{
			name: "unexpected member",
			members: []teams.ChatMember{
				{UserID: "owner-user", DisplayName: "Owner"},
				{UserID: "other-user", DisplayName: "Other"},
			},
			wantErrSub: "unexpected member",
		},
		{
			name: "duplicate owner",
			members: []teams.ChatMember{
				{UserID: "owner-user", DisplayName: "Owner 1"},
				{UserID: "owner-user", DisplayName: "Owner 2"},
			},
			wantErrSub: "missing expected member",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMembers(tc.members, "owner-user", "test-user")
			if err == nil {
				t.Fatalf("validateMembers returned nil error")
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("validateMembers error = %q, want substring %q", err.Error(), tc.wantErrSub)
			}
		})
	}
}

func TestBenchSummaryPredicates(t *testing.T) {
	msg := teams.ChatMessage{
		Attachments: []teams.MessageAttachment{{ContentType: "messageReference"}},
	}
	msg.Body.Content = "<p>Codex graph bench accepted nonce-123</p>"
	if !hasMessageReferenceAttachment(msg) {
		t.Fatalf("hasMessageReferenceAttachment returned false")
	}
	if !recentContains([]teams.ChatMessage{msg}, "accepted nonce-123") {
		t.Fatalf("recentContains did not find expected text")
	}
	if recentContains([]teams.ChatMessage{msg}, "missing text") {
		t.Fatalf("recentContains found unexpected text")
	}
}

func TestSanitizeMarkerKeepsGraphBenchNonceStable(t *testing.T) {
	got := sanitizeMarker(" group guard / 2026-06-18 ! ")
	if got != "groupguard2026-06-18" {
		t.Fatalf("sanitizeMarker = %q", got)
	}

	long := sanitizeMarker(strings.Repeat("a", 80))
	if len(long) != 64 {
		t.Fatalf("sanitizeMarker long len = %d, want 64", len(long))
	}
}
