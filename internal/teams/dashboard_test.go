package teams

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestControlDashboardRenumbersByCurrentRecencyAcrossRefresh(t *testing.T) {
	now := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	first := BuildControlDashboard(ControlDashboard{}, ControlDashboardInput{
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: "workspace-a",
		Workspaces: []DashboardWorkspaceInput{
			{
				ID:        "workspace-a",
				Path:      "/home/baka/projects/a",
				UpdatedAt: now.Add(-time.Hour),
				Sessions: []DashboardSessionInput{
					{ID: "session-a1", Topic: "first", UpdatedAt: now.Add(-10 * time.Minute)},
					{ID: "session-a2", Topic: "second", UpdatedAt: now.Add(-20 * time.Minute)},
				},
			},
			{ID: "workspace-b", Path: "/home/baka/projects/b", UpdatedAt: now.Add(-2 * time.Hour)},
		},
	}, now)

	if got := dashboardWorkspaceNumber(t, first, "workspace-a"); got != 1 {
		t.Fatalf("workspace-a initial number = %d, want 1", got)
	}
	if got := dashboardSessionNumber(t, first, "workspace-a", "session-a1"); got != 1 {
		t.Fatalf("session-a1 initial number = %d, want 1", got)
	}

	second := BuildControlDashboard(first, ControlDashboardInput{
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: "workspace-a",
		Workspaces: []DashboardWorkspaceInput{
			{ID: "workspace-b", Path: "/home/baka/projects/b", UpdatedAt: now.Add(time.Hour)},
			{
				ID:        "workspace-a",
				Path:      "/home/baka/projects/a",
				UpdatedAt: now.Add(-time.Hour),
				Sessions: []DashboardSessionInput{
					{ID: "session-a2", Topic: "second renamed order", UpdatedAt: now.Add(30 * time.Minute)},
					{ID: "session-a3", Topic: "third", UpdatedAt: now.Add(20 * time.Minute)},
					{ID: "session-a1", Topic: "first renamed order", UpdatedAt: now.Add(-30 * time.Minute)},
				},
			},
		},
	}, now.Add(time.Minute))

	if got := dashboardWorkspaceNumber(t, second, "workspace-b"); got != 1 {
		t.Fatalf("workspace-b number after refresh = %d, want current first row number 1", got)
	}
	if got := dashboardWorkspaceNumber(t, second, "workspace-a"); got != 2 {
		t.Fatalf("workspace-a number after refresh = %d, want current second row number 2", got)
	}
	if got := dashboardSessionNumber(t, second, "workspace-a", "session-a2"); got != 1 {
		t.Fatalf("session-a2 number after refresh = %d, want newest session number 1", got)
	}
	if got := dashboardSessionNumber(t, second, "workspace-a", "session-a3"); got != 2 {
		t.Fatalf("session-a3 number after refresh = %d, want second newest session number 2", got)
	}
	if got := dashboardSessionNumber(t, second, "workspace-a", "session-a1"); got != 3 {
		t.Fatalf("session-a1 number after refresh = %d, want oldest session number 3", got)
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
	if work != "💬 devbox - inspect logs" {
		t.Fatalf("work title = %q, want machine-first work title", work)
	}
	if strings.Contains(work, "Codex Work") || strings.Contains(work, "s001") {
		t.Fatalf("work title = %q, should not include legacy marker or helper session id", work)
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

func TestControlDashboardOrdersWorkspacesAndSessionsByRecentActivity(t *testing.T) {
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	dashboard := BuildControlDashboard(ControlDashboard{}, ControlDashboardInput{
		ViewKind: DashboardViewWorkspaces,
		Workspaces: []DashboardWorkspaceInput{{
			ID:        "workspace-old",
			Path:      "/home/baka/projects/old",
			UpdatedAt: now.Add(-4 * time.Hour),
			Sessions: []DashboardSessionInput{{
				ID:        "session-old",
				Topic:     "old session",
				UpdatedAt: now.Add(-4 * time.Hour),
			}},
		}, {
			ID:        "workspace-new",
			Path:      "/home/baka/projects/new",
			UpdatedAt: now.Add(-10 * time.Minute),
			Sessions: []DashboardSessionInput{{
				ID:        "session-new",
				Topic:     "new session",
				UpdatedAt: now.Add(-10 * time.Minute),
			}},
		}},
	}, now)
	if len(dashboard.Workspaces) != 2 || dashboard.Workspaces[0].ID != "workspace-new" || dashboard.CurrentView.Items[0].WorkspaceID != "workspace-new" {
		t.Fatalf("workspace recency order = %#v, view = %#v", dashboard.Workspaces, dashboard.CurrentView.Items)
	}
	if dashboard.Workspaces[0].Number != 1 || dashboard.Workspaces[1].Number != 2 {
		t.Fatalf("workspace numbers should follow display order: %#v", dashboard.Workspaces)
	}

	dashboard = BuildControlDashboard(dashboard, ControlDashboardInput{
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: "workspace-new",
		Workspaces: []DashboardWorkspaceInput{{
			ID:        "workspace-new",
			Path:      "/home/baka/projects/new",
			UpdatedAt: now,
			Sessions: []DashboardSessionInput{{
				ID:        "session-older",
				Topic:     "older",
				UpdatedAt: now.Add(-2 * time.Hour),
			}, {
				ID:        "session-newer",
				Topic:     "newer",
				UpdatedAt: now.Add(-5 * time.Minute),
			}},
		}},
	}, now.Add(time.Minute))
	if len(dashboard.CurrentView.Items) != 2 || dashboard.CurrentView.Items[0].SessionID != "session-newer" {
		t.Fatalf("session recency order = %#v", dashboard.CurrentView.Items)
	}
	if dashboard.CurrentView.Items[0].Number != 1 || dashboard.CurrentView.Items[1].Number != 2 {
		t.Fatalf("session numbers should follow display order: %#v", dashboard.CurrentView.Items)
	}
}

func TestControlDashboardCountsWorkspaceSessionStatuses(t *testing.T) {
	now := time.Date(2026, 5, 12, 14, 8, 0, 0, time.UTC)
	dashboard := BuildControlDashboard(ControlDashboard{}, ControlDashboardInput{
		ViewKind: DashboardViewWorkspaces,
		Workspaces: []DashboardWorkspaceInput{{
			ID:        "workspace-a",
			Path:      "/home/baka/projects/a",
			UpdatedAt: now,
			Sessions: []DashboardSessionInput{
				{ID: "session-default-active"},
				{ID: "session-explicit-active", Status: "active"},
				{ID: "session-closed", Status: "closed"},
				{ID: "session-local-history", Status: "local"},
			},
		}},
	}, now)
	if len(dashboard.Workspaces) != 1 {
		t.Fatalf("workspaces = %#v, want one workspace", dashboard.Workspaces)
	}
	workspace := dashboard.Workspaces[0]
	if workspace.SessionCount != 4 || workspace.ActiveSessionCount != 2 || workspace.IdleSessionCount != 2 {
		t.Fatalf("workspace session counts = total %d active %d idle %d, want 4/2/2", workspace.SessionCount, workspace.ActiveSessionCount, workspace.IdleSessionCount)
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

	placeholder := NewWorkChatPlaceholderTitle("/home/baka/private/client-alpha/secret-repo")
	if placeholder != "New message in secret-repo" {
		t.Fatalf("placeholder title = %q, want workspace basename only", placeholder)
	}
}

func TestControlDashboardLargeCorpusStressKeepsCurrentViewSequentialPrivateNumbers(t *testing.T) {
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
	assertDashboardViewNumbersSequential(t, first.CurrentView)
	assertDashboardTitlesDoNotLeakPrivatePaths(t, first)

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
				CreatedAt: now.Add(24 * time.Hour),
				UpdatedAt: now.Add(24 * time.Hour),
			})
			break
		}
	}
	second := BuildControlDashboard(first, ControlDashboardInput{
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: "workspace-042",
		Workspaces:          workspaces,
	}, now.Add(time.Minute))
	if got := dashboardWorkspaceNumber(t, second, "workspace-119"); got != 1 {
		t.Fatalf("newest workspace number = %d, want current first row number 1", got)
	}
	if got := dashboardSessionNumber(t, second, "workspace-042", "session-042-new"); got != 1 {
		t.Fatalf("newest session number = %d, want current first row number 1", got)
	}
	assertDashboardViewNumbersUnique(t, second.CurrentView)
	assertDashboardViewNumbersSequential(t, second.CurrentView)
	assertDashboardTitlesDoNotLeakPrivatePaths(t, second)
	if _, err := ResolveDashboardNumber(ChatScopeControl, second.CurrentView, 1, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("current first session number should resolve before expiry: %v", err)
	}
	if _, err := ResolveDashboardNumber(ChatScopeControl, second.CurrentView, 1, now.Add(20*time.Minute)); !errors.Is(err, ErrDashboardViewExpired) {
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

func assertDashboardViewNumbersSequential(t *testing.T, view DashboardView) {
	t.Helper()
	for i, item := range view.Items {
		if want := i + 1; item.Number != want {
			t.Fatalf("dashboard view item %d has number %d, want %d in %#v", i, item.Number, want, view.Items)
		}
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
