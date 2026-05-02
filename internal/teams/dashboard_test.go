package teams

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestControlDashboardStableNumbersAcrossRefresh(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	first := BuildControlDashboard(ControlDashboard{}, ControlDashboardInput{
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: "workspace-a",
		Workspaces: []DashboardWorkspaceInput{
			{
				ID:   "workspace-a",
				Path: "/home/baka/projects/a",
				Sessions: []DashboardSessionInput{
					{ID: "session-a1", Topic: "first"},
					{ID: "session-a2", Topic: "second"},
				},
			},
			{ID: "workspace-b", Path: "/home/baka/projects/b"},
		},
	}, now)

	workspaceANumber := dashboardWorkspaceNumber(t, first, "workspace-a")
	workspaceBNumber := dashboardWorkspaceNumber(t, first, "workspace-b")
	sessionA1Number := dashboardSessionNumber(t, first, "workspace-a", "session-a1")
	sessionA2Number := dashboardSessionNumber(t, first, "workspace-a", "session-a2")

	second := BuildControlDashboard(first, ControlDashboardInput{
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: "workspace-a",
		Workspaces: []DashboardWorkspaceInput{
			{ID: "workspace-b", Path: "/home/baka/projects/b"},
			{
				ID:   "workspace-a",
				Path: "/home/baka/projects/a",
				Sessions: []DashboardSessionInput{
					{ID: "session-a2", Topic: "second renamed order"},
					{ID: "session-a3", Topic: "third"},
					{ID: "session-a1", Topic: "first renamed order"},
				},
			},
		},
	}, now.Add(time.Minute))

	if got := dashboardWorkspaceNumber(t, second, "workspace-a"); got != workspaceANumber {
		t.Fatalf("workspace-a number = %d, want stable %d", got, workspaceANumber)
	}
	if got := dashboardWorkspaceNumber(t, second, "workspace-b"); got != workspaceBNumber {
		t.Fatalf("workspace-b number = %d, want stable %d", got, workspaceBNumber)
	}
	if got := dashboardSessionNumber(t, second, "workspace-a", "session-a1"); got != sessionA1Number {
		t.Fatalf("session-a1 number = %d, want stable %d", got, sessionA1Number)
	}
	if got := dashboardSessionNumber(t, second, "workspace-a", "session-a2"); got != sessionA2Number {
		t.Fatalf("session-a2 number = %d, want stable %d", got, sessionA2Number)
	}
	newNumber := dashboardSessionNumber(t, second, "workspace-a", "session-a3")
	if newNumber == sessionA1Number || newNumber == sessionA2Number {
		t.Fatalf("new session number = %d, reused existing number", newNumber)
	}
}

func TestChatTitlesUseDistinctLeadingEmoji(t *testing.T) {
	control := ControlChatTitle(ChatTitleOptions{MachineLabel: "devbox"})
	if !strings.HasPrefix(control, "🏠 ") || !strings.Contains(control, "Codex Control") || !strings.Contains(control, "devbox") {
		t.Fatalf("control title = %q, want leading main-chat emoji, marker, and machine label", control)
	}
	work := WorkChatTitle(ChatTitleOptions{
		MachineLabel: "devbox",
		SessionID:    "s001",
		Topic:        "inspect logs",
	})
	if !strings.HasPrefix(work, "💬 ") || !strings.Contains(work, "Codex Work") || !strings.Contains(work, "s001") {
		t.Fatalf("work title = %q, want leading work-chat emoji, marker, and session id", work)
	}
	if control == work {
		t.Fatalf("control and work titles should be visually distinct: %q", control)
	}
}

func TestBareNumberResolvesCurrentControlViewOnly(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	input := ControlDashboardInput{
		SelectedWorkspaceID: "workspace-a",
		Workspaces: []DashboardWorkspaceInput{
			{
				ID:   "workspace-a",
				Path: "/home/baka/projects/a",
				Sessions: []DashboardSessionInput{
					{ID: "session-a1", Topic: "first"},
				},
			},
		},
	}
	workspacesView := BuildControlDashboard(ControlDashboard{}, input, now)
	selection, err := ResolveBareDashboardNumber(ChatScopeControl, workspacesView.CurrentView, "1", now)
	if err != nil {
		t.Fatalf("ResolveBareDashboardNumber workspaces error: %v", err)
	}
	if selection.Kind != DashboardSelectionWorkspace || selection.WorkspaceID != "workspace-a" || selection.SessionID != "" {
		t.Fatalf("workspace selection = %#v", selection)
	}

	input.ViewKind = DashboardViewSessions
	sessionsView := BuildControlDashboard(workspacesView, input, now.Add(time.Second))
	selection, err = ResolveBareDashboardNumber(ChatScopeControl, sessionsView.CurrentView, "1", now.Add(time.Second))
	if err != nil {
		t.Fatalf("ResolveBareDashboardNumber sessions error: %v", err)
	}
	if selection.Kind != DashboardSelectionSession || selection.WorkspaceID != "workspace-a" || selection.SessionID != "session-a1" {
		t.Fatalf("session selection = %#v", selection)
	}

	if _, err := ResolveBareDashboardNumber(ChatScopeWork, sessionsView.CurrentView, "1", now.Add(time.Second)); !errors.Is(err, ErrDashboardWrongScope) {
		t.Fatalf("work chat bare number error = %v, want ErrDashboardWrongScope", err)
	}
}

func TestExpiredDashboardViewDoesNotGuess(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	dashboard := BuildControlDashboard(ControlDashboard{}, ControlDashboardInput{
		ViewTTL:  time.Minute,
		ViewKind: DashboardViewWorkspaces,
		Workspaces: []DashboardWorkspaceInput{{
			ID:   "workspace-a",
			Path: "/home/baka/projects/a",
		}},
	}, now)

	selection, err := ResolveBareDashboardNumber(ChatScopeControl, dashboard.CurrentView, "1", now.Add(2*time.Minute))
	if !errors.Is(err, ErrDashboardViewExpired) {
		t.Fatalf("ResolveBareDashboardNumber error = %v, want ErrDashboardViewExpired", err)
	}
	if selection != (DashboardSelection{}) {
		t.Fatalf("expired view returned selection: %#v", selection)
	}
}

func TestDashboardTitlesSanitizeAndHidePathDetails(t *testing.T) {
	longPrompt := "please inspect /home/baka/private/client-alpha/secret-repo and then " + strings.Repeat("summarize ", 20)
	displayTitle := DashboardDisplayTitle("", longPrompt, "")
	if strings.Contains(displayTitle, "/home/baka") || strings.Contains(displayTitle, "private/client-alpha") {
		t.Fatalf("display title leaked full path: %q", displayTitle)
	}
	if !strings.Contains(displayTitle, "secret-repo") {
		t.Fatalf("display title should keep basename clue: %q", displayTitle)
	}
	if got := len([]rune(displayTitle)); got > maxDashboardTitleRunes {
		t.Fatalf("display title length = %d, want <= %d: %q", got, maxDashboardTitleRunes, displayTitle)
	}

	workspaceTitle := WorkspaceDisplayTitle("", "/home/baka/private/client-alpha/secret-repo")
	if workspaceTitle != "secret-repo" {
		t.Fatalf("workspace title = %q, want basename only", workspaceTitle)
	}

	workTitle := WorkChatTitle(ChatTitleOptions{
		MachineLabel: "devbox",
		SessionID:    "session-1",
		Topic:        longPrompt,
	})
	if strings.Contains(workTitle, "/home/baka") || strings.Contains(workTitle, "private/client-alpha") {
		t.Fatalf("work title leaked path: %q", workTitle)
	}

	renamed := WorkChatTitle(ChatTitleOptions{
		MachineLabel: "devbox",
		SessionID:    "session-1",
		UserTitle:    "Release Room",
		Topic:        "old prompt title",
	})
	if !strings.Contains(renamed, "Release Room") || strings.Contains(renamed, "old prompt title") {
		t.Fatalf("renamed work title = %q, want user title only", renamed)
	}
}

func TestControlDashboardLargeCorpusStressKeepsStablePrivateNumbers(t *testing.T) {
	now := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	workspaces := makeDashboardStressInputs(120, 12, now)
	first := BuildControlDashboard(ControlDashboard{}, ControlDashboardInput{
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: "workspace-042",
		Workspaces:          workspaces,
	}, now)
	if got := len(first.Workspaces); got != 120 {
		t.Fatalf("workspace count = %d, want 120", got)
	}
	if got := len(first.Sessions); got != 120*12 {
		t.Fatalf("session count = %d, want %d", got, 120*12)
	}
	assertDashboardViewNumbersUnique(t, first.CurrentView)
	assertDashboardTitlesDoNotLeakPrivatePaths(t, first)
	workspaceNumber := dashboardWorkspaceNumber(t, first, "workspace-042")
	sessionNumber := dashboardSessionNumber(t, first, "workspace-042", "session-042-007")

	for i, j := 0, len(workspaces)-1; i < j; i, j = i+1, j-1 {
		workspaces[i], workspaces[j] = workspaces[j], workspaces[i]
	}
	for i, j := 0, len(workspaces[77].Sessions)-1; i < j; i, j = i+1, j-1 {
		workspaces[77].Sessions[i], workspaces[77].Sessions[j] = workspaces[77].Sessions[j], workspaces[77].Sessions[i]
	}
	for i := range workspaces {
		if workspaces[i].ID == "workspace-042" {
			workspaces[i].Sessions = append(workspaces[i].Sessions, DashboardSessionInput{
				ID:        "session-042-new",
				Topic:     "new session after rediscovery",
				Cwd:       "/home/baka/private/customer-042/repo-042",
				Status:    "active",
				CreatedAt: now.Add(time.Minute),
				UpdatedAt: now.Add(time.Minute),
			})
			break
		}
	}
	second := BuildControlDashboard(first, ControlDashboardInput{
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: "workspace-042",
		Workspaces:          workspaces,
	}, now.Add(time.Minute))
	if got := dashboardWorkspaceNumber(t, second, "workspace-042"); got != workspaceNumber {
		t.Fatalf("workspace-042 number = %d, want stable %d", got, workspaceNumber)
	}
	if got := dashboardSessionNumber(t, second, "workspace-042", "session-042-007"); got != sessionNumber {
		t.Fatalf("session-042-007 number = %d, want stable %d", got, sessionNumber)
	}
	newNumber := dashboardSessionNumber(t, second, "workspace-042", "session-042-new")
	if newNumber == sessionNumber {
		t.Fatalf("new session reused existing number %d", newNumber)
	}
	assertDashboardViewNumbersUnique(t, second.CurrentView)
	assertDashboardTitlesDoNotLeakPrivatePaths(t, second)
	if _, err := ResolveDashboardNumber(ChatScopeControl, second.CurrentView, sessionNumber, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("stable session number should resolve before expiry: %v", err)
	}
	if _, err := ResolveDashboardNumber(ChatScopeControl, second.CurrentView, sessionNumber, now.Add(20*time.Minute)); !errors.Is(err, ErrDashboardViewExpired) {
		t.Fatalf("expired large dashboard view error = %v, want ErrDashboardViewExpired", err)
	}
}

func dashboardWorkspaceNumber(t *testing.T, dashboard ControlDashboard, id string) int {
	t.Helper()
	for _, workspace := range dashboard.Workspaces {
		if workspace.ID == id {
			return workspace.Number
		}
	}
	t.Fatalf("workspace %q not found in %#v", id, dashboard.Workspaces)
	return 0
}

func dashboardSessionNumber(t *testing.T, dashboard ControlDashboard, workspaceID string, sessionID string) int {
	t.Helper()
	for _, session := range dashboard.Sessions {
		if session.WorkspaceID == workspaceID && session.ID == sessionID {
			return session.Number
		}
	}
	t.Fatalf("session %q/%q not found in %#v", workspaceID, sessionID, dashboard.Sessions)
	return 0
}

func makeDashboardStressInputs(workspaceCount int, sessionsPerWorkspace int, now time.Time) []DashboardWorkspaceInput {
	workspaces := make([]DashboardWorkspaceInput, 0, workspaceCount)
	for w := 0; w < workspaceCount; w++ {
		workspaceID := fmt.Sprintf("workspace-%03d", w)
		workspacePath := fmt.Sprintf("/home/baka/private/customer-%03d/repo-%03d", w, w)
		sessions := make([]DashboardSessionInput, 0, sessionsPerWorkspace)
		for s := 0; s < sessionsPerWorkspace; s++ {
			sessions = append(sessions, DashboardSessionInput{
				ID:        fmt.Sprintf("session-%03d-%03d", w, s),
				Cwd:       workspacePath,
				Topic:     fmt.Sprintf("investigate /home/baka/private/customer-%03d/repo-%03d issue %03d", w, w, s),
				Status:    "active",
				CreatedAt: now.Add(time.Duration(w*sessionsPerWorkspace+s) * time.Second),
				UpdatedAt: now.Add(time.Duration(w*sessionsPerWorkspace+s) * time.Second),
			})
		}
		workspaces = append(workspaces, DashboardWorkspaceInput{
			ID:        workspaceID,
			Path:      workspacePath,
			UpdatedAt: now.Add(time.Duration(w) * time.Minute),
			Sessions:  sessions,
		})
	}
	return workspaces
}

func assertDashboardViewNumbersUnique(t *testing.T, view DashboardView) {
	t.Helper()
	seen := map[int]DashboardViewItem{}
	for _, item := range view.Items {
		if item.Number <= 0 {
			t.Fatalf("dashboard view item has non-positive number: %#v", item)
		}
		if previous, ok := seen[item.Number]; ok {
			t.Fatalf("duplicate dashboard view number %d: %#v and %#v", item.Number, previous, item)
		}
		seen[item.Number] = item
	}
}

func assertDashboardTitlesDoNotLeakPrivatePaths(t *testing.T, dashboard ControlDashboard) {
	t.Helper()
	for _, workspace := range dashboard.Workspaces {
		if strings.Contains(workspace.DisplayTitle, "/home/") || strings.Contains(workspace.DisplayTitle, "private/customer") {
			t.Fatalf("workspace title leaked path detail: %#v", workspace)
		}
	}
	for _, session := range dashboard.Sessions {
		if strings.Contains(session.DisplayTitle, "/home/") || strings.Contains(session.DisplayTitle, "private/customer") {
			t.Fatalf("session title leaked path detail: %#v", session)
		}
	}
	for _, item := range dashboard.CurrentView.Items {
		if strings.Contains(item.DisplayTitle, "/home/") || strings.Contains(item.DisplayTitle, "private/customer") {
			t.Fatalf("view item title leaked path detail: %#v", item)
		}
	}
}
