package teams

import (
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	DefaultMachineChatMarker = "Codex Machine"
	DefaultControlChatMarker = "🏠 Codex Control"
	DefaultWorkChatMarker    = "💬"

	DefaultDashboardViewTTL = 10 * time.Minute

	maxDashboardTitleRunes = 64
)

type ChatScope string

const (
	ChatScopeControl ChatScope = "control"
	ChatScopeWork    ChatScope = "work"
)

type DashboardViewKind string

const (
	DashboardViewNone       DashboardViewKind = ""
	DashboardViewWorkspaces DashboardViewKind = "workspaces"
	DashboardViewSessions   DashboardViewKind = "sessions"
)

type DashboardSelectionKind string

const (
	DashboardSelectionWorkspace DashboardSelectionKind = "workspace"
	DashboardSelectionSession   DashboardSelectionKind = "session"
)

var (
	ErrDashboardNotBareNumber = errors.New("dashboard selection is not a bare number")
	ErrDashboardWrongScope    = errors.New("dashboard bare number selection is only valid in the control chat")
	ErrDashboardViewMissing   = errors.New("dashboard view is missing")
	ErrDashboardViewExpired   = errors.New("dashboard view expired")
	ErrDashboardNumberMissing = errors.New("dashboard number is not in the current view")
)

type ChatTitleOptions struct {
	Marker       string
	MachineLabel string
	Profile      string
	SessionID    string
	UserTitle    string
	Topic        string
	Cwd          string
}

func MachineChatTitle(opts ChatTitleOptions) string {
	marker := sanitizedTitlePart(opts.Marker, DefaultMachineChatMarker)
	return joinTitleParts(marker, machineTitlePart(opts.MachineLabel), profileTitlePart(opts.Profile))
}

func ControlChatTitle(opts ChatTitleOptions) string {
	marker := sanitizedTitlePart(opts.Marker, DefaultControlChatMarker)
	return joinTitleParts(marker, machineTitlePart(opts.MachineLabel), profileTitlePart(opts.Profile))
}

func WorkChatTitle(opts ChatTitleOptions) string {
	marker := sanitizedTitlePart(opts.Marker, DefaultWorkChatMarker)
	machine := machineTitlePart(opts.MachineLabel)
	return joinTitleParts(strings.TrimSpace(marker+" "+machine), DashboardDisplayTitle(opts.UserTitle, opts.Topic, opts.Cwd))
}

func NewWorkChatPlaceholderTitle(cwd string) string {
	workspace := WorkspaceDisplayTitle("", cwd)
	if workspace == "" {
		workspace = "workspace"
	}
	return "New message in " + workspace
}

func DashboardDisplayTitle(userTitle string, topic string, cwd string) string {
	if title := SanitizeDashboardTitle(userTitle); title != "" {
		return title
	}
	if title := SanitizeDashboardTitle(topic); title != "" {
		return title
	}
	if base := lastPathElement(cwd); base != "" {
		return SanitizeDashboardTitle(base)
	}
	return "untitled"
}

func WorkspaceDisplayTitle(userTitle string, path string) string {
	if title := SanitizeDashboardTitle(userTitle); title != "" {
		return title
	}
	if base := lastPathElement(path); base != "" {
		return SanitizeDashboardTitle(base)
	}
	return "workspace"
}

func SanitizeDashboardTitle(title string) string {
	title = strings.TrimSpace(redactPathLikeTokens(title))
	if title == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range title {
		if unicode.IsControl(r) {
			continue
		}
		switch r {
		case ':', '<', '>', '"', '/', '\\', '|', '?', '*':
			b.WriteByte('-')
		default:
			b.WriteRune(r)
		}
	}
	clean := strings.Join(strings.Fields(b.String()), " ")
	clean = strings.Trim(clean, " -.")
	if clean == "" {
		return ""
	}
	rs := []rune(clean)
	if len(rs) <= maxDashboardTitleRunes {
		return clean
	}
	if maxDashboardTitleRunes <= 3 {
		return string(rs[:maxDashboardTitleRunes])
	}
	return strings.TrimSpace(string(rs[:maxDashboardTitleRunes-3])) + "..."
}

type ControlDashboardInput struct {
	Workspaces          []DashboardWorkspaceInput
	ViewKind            DashboardViewKind
	SelectedWorkspaceID string
	ViewTTL             time.Duration
}

type DashboardWorkspaceInput struct {
	ID        string
	Path      string
	UserTitle string
	UpdatedAt time.Time
	Sessions  []DashboardSessionInput
}

type DashboardSessionInput struct {
	ID            string
	WorkspaceID   string
	Cwd           string
	UserTitle     string
	Topic         string
	Status        string
	TeamsChatID   string
	TeamsChatURL  string
	CodexThreadID string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type ControlDashboard struct {
	Workspaces          []DashboardWorkspace
	Sessions            []DashboardSession
	SelectedWorkspaceID string
	CurrentView         DashboardView
	GeneratedAt         time.Time
}

type DashboardWorkspace struct {
	Number             int
	ID                 string
	Path               string
	DisplayTitle       string
	SessionCount       int
	ActiveSessionCount int
	IdleSessionCount   int
	UpdatedAt          time.Time
}

type DashboardSession struct {
	Number        int
	ID            string
	WorkspaceID   string
	Cwd           string
	DisplayTitle  string
	Status        string
	TeamsChatID   string
	TeamsChatURL  string
	CodexThreadID string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type DashboardView struct {
	Kind        DashboardViewKind
	WorkspaceID string
	Items       []DashboardViewItem
	CreatedAt   time.Time
	ExpiresAt   time.Time
}

type DashboardViewItem struct {
	Number       int
	Kind         DashboardSelectionKind
	WorkspaceID  string
	SessionID    string
	DisplayTitle string
}

type DashboardSelection struct {
	Number      int
	Kind        DashboardSelectionKind
	WorkspaceID string
	SessionID   string
}

func BuildControlDashboard(previous ControlDashboard, input ControlDashboardInput, now time.Time) ControlDashboard {
	viewTTL := input.ViewTTL
	if viewTTL <= 0 {
		viewTTL = DefaultDashboardViewTTL
	}
	workspaces := buildDashboardWorkspaces(previous.Workspaces, input.Workspaces)
	selectedWorkspaceID := strings.TrimSpace(input.SelectedWorkspaceID)
	if selectedWorkspaceID == "" {
		selectedWorkspaceID = previous.SelectedWorkspaceID
	}
	if selectedWorkspaceID == "" && len(workspaces) > 0 {
		selectedWorkspaceID = workspaces[0].ID
	}

	sessions := buildDashboardSessions(previous.Sessions, input.Workspaces, selectedWorkspaceID)
	viewKind := input.ViewKind
	if viewKind == DashboardViewNone {
		viewKind = DashboardViewWorkspaces
	}
	view := buildDashboardView(viewKind, selectedWorkspaceID, workspaces, sessions, now, viewTTL)
	return ControlDashboard{
		Workspaces:          workspaces,
		Sessions:            sessions,
		SelectedWorkspaceID: selectedWorkspaceID,
		CurrentView:         view,
		GeneratedAt:         now,
	}
}

func ResolveBareDashboardNumber(scope ChatScope, view DashboardView, text string, now time.Time) (DashboardSelection, error) {
	n, ok := parseBarePositiveInt(strings.TrimSpace(text))
	if !ok {
		return DashboardSelection{}, ErrDashboardNotBareNumber
	}
	return ResolveDashboardNumber(scope, view, n, now)
}

func ResolveDashboardNumber(scope ChatScope, view DashboardView, number int, now time.Time) (DashboardSelection, error) {
	if scope != ChatScopeControl {
		return DashboardSelection{}, ErrDashboardWrongScope
	}
	if view.Kind == DashboardViewNone || len(view.Items) == 0 {
		return DashboardSelection{}, ErrDashboardViewMissing
	}
	if !view.ExpiresAt.IsZero() && now.After(view.ExpiresAt) {
		return DashboardSelection{}, ErrDashboardViewExpired
	}
	for _, item := range view.Items {
		if item.Number == number {
			return DashboardSelection{
				Number:      item.Number,
				Kind:        item.Kind,
				WorkspaceID: item.WorkspaceID,
				SessionID:   item.SessionID,
			}, nil
		}
	}
	return DashboardSelection{}, ErrDashboardNumberMissing
}

func buildDashboardWorkspaces(previous []DashboardWorkspace, inputs []DashboardWorkspaceInput) []DashboardWorkspace {
	inputs = sortDashboardWorkspaceInputs(inputs)
	workspaces := make([]DashboardWorkspace, 0, len(inputs))
	seen := map[string]bool{}
	for _, input := range inputs {
		id := dashboardWorkspaceID(input)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		active, idle := dashboardSessionStatusCounts(input.Sessions)
		workspaces = append(workspaces, DashboardWorkspace{
			ID:                 id,
			Path:               strings.TrimSpace(input.Path),
			DisplayTitle:       WorkspaceDisplayTitle(input.UserTitle, input.Path),
			SessionCount:       len(input.Sessions),
			ActiveSessionCount: active,
			IdleSessionCount:   idle,
			UpdatedAt:          input.UpdatedAt,
		})
	}
	sort.SliceStable(workspaces, func(i, j int) bool { return dashboardWorkspaceLess(workspaces[i], workspaces[j]) })
	for i := range workspaces {
		workspaces[i].Number = i + 1
	}
	return workspaces
}

func dashboardSessionStatusCounts(sessions []DashboardSessionInput) (active int, idle int) {
	for _, session := range sessions {
		if isActiveSessionStatus(session.Status) {
			active++
		} else {
			idle++
		}
	}
	return active, idle
}

func buildDashboardSessions(previous []DashboardSession, workspaceInputs []DashboardWorkspaceInput, selectedWorkspaceID string) []DashboardSession {
	sessionInputs := map[string]DashboardSessionInput{}
	for _, workspace := range workspaceInputs {
		workspaceID := dashboardWorkspaceID(workspace)
		if workspaceID == "" {
			continue
		}
		for _, session := range workspace.Sessions {
			sessionID := strings.TrimSpace(session.ID)
			if sessionID == "" {
				continue
			}
			session.WorkspaceID = strings.TrimSpace(session.WorkspaceID)
			if session.WorkspaceID == "" {
				session.WorkspaceID = workspaceID
			}
			if session.WorkspaceID != workspaceID {
				continue
			}
			key := sessionKey(workspaceID, sessionID)
			if _, exists := sessionInputs[key]; exists {
				continue
			}
			sessionInputs[key] = session
		}
	}

	var sessions []DashboardSession
	for _, input := range sessionInputs {
		sessionID := strings.TrimSpace(input.ID)
		workspaceID := strings.TrimSpace(input.WorkspaceID)
		if workspaceID == "" {
			workspaceID = selectedWorkspaceID
		}
		sessions = append(sessions, DashboardSession{
			ID:            sessionID,
			WorkspaceID:   workspaceID,
			Cwd:           strings.TrimSpace(input.Cwd),
			DisplayTitle:  DashboardDisplayTitle(input.UserTitle, input.Topic, input.Cwd),
			Status:        strings.TrimSpace(input.Status),
			TeamsChatID:   strings.TrimSpace(input.TeamsChatID),
			TeamsChatURL:  strings.TrimSpace(input.TeamsChatURL),
			CodexThreadID: strings.TrimSpace(input.CodexThreadID),
			CreatedAt:     input.CreatedAt,
			UpdatedAt:     input.UpdatedAt,
		})
	}
	sort.SliceStable(sessions, func(i, j int) bool {
		if sessions[i].WorkspaceID != sessions[j].WorkspaceID {
			return sessions[i].WorkspaceID < sessions[j].WorkspaceID
		}
		return dashboardSessionLess(sessions[i], sessions[j])
	})
	nextByWorkspace := map[string]int{}
	for i := range sessions {
		nextByWorkspace[sessions[i].WorkspaceID]++
		sessions[i].Number = nextByWorkspace[sessions[i].WorkspaceID]
	}
	return sessions
}

func sortDashboardWorkspaceInputs(inputs []DashboardWorkspaceInput) []DashboardWorkspaceInput {
	if len(inputs) <= 1 {
		return inputs
	}
	out := append([]DashboardWorkspaceInput(nil), inputs...)
	sort.SliceStable(out, func(i, j int) bool {
		return dashboardWorkspaceInputLess(out[i], out[j])
	})
	return out
}

func dashboardWorkspaceInputLess(a, b DashboardWorkspaceInput) bool {
	if !a.UpdatedAt.Equal(b.UpdatedAt) {
		if a.UpdatedAt.IsZero() {
			return false
		}
		if b.UpdatedAt.IsZero() {
			return true
		}
		return a.UpdatedAt.After(b.UpdatedAt)
	}
	aTitle := WorkspaceDisplayTitle(a.UserTitle, a.Path)
	bTitle := WorkspaceDisplayTitle(b.UserTitle, b.Path)
	if aTitle != bTitle {
		return aTitle < bTitle
	}
	aPath := strings.TrimSpace(a.Path)
	bPath := strings.TrimSpace(b.Path)
	if aPath != bPath {
		return aPath < bPath
	}
	return dashboardWorkspaceID(a) < dashboardWorkspaceID(b)
}

func dashboardWorkspaceLess(a, b DashboardWorkspace) bool {
	if !a.UpdatedAt.Equal(b.UpdatedAt) {
		if a.UpdatedAt.IsZero() {
			return false
		}
		if b.UpdatedAt.IsZero() {
			return true
		}
		return a.UpdatedAt.After(b.UpdatedAt)
	}
	if a.DisplayTitle != b.DisplayTitle {
		return a.DisplayTitle < b.DisplayTitle
	}
	if a.Path != b.Path {
		return a.Path < b.Path
	}
	return a.ID < b.ID
}

func dashboardSessionInputLess(a, b DashboardSessionInput) bool {
	aTime := dashboardSessionInputRecency(a)
	bTime := dashboardSessionInputRecency(b)
	if !aTime.Equal(bTime) {
		if aTime.IsZero() {
			return false
		}
		if bTime.IsZero() {
			return true
		}
		return aTime.After(bTime)
	}
	aTitle := DashboardDisplayTitle(a.UserTitle, a.Topic, a.Cwd)
	bTitle := DashboardDisplayTitle(b.UserTitle, b.Topic, b.Cwd)
	if aTitle != bTitle {
		return aTitle < bTitle
	}
	return strings.TrimSpace(a.ID) < strings.TrimSpace(b.ID)
}

func dashboardSessionLess(a, b DashboardSession) bool {
	aTime := dashboardSessionRecency(a)
	bTime := dashboardSessionRecency(b)
	if !aTime.Equal(bTime) {
		if aTime.IsZero() {
			return false
		}
		if bTime.IsZero() {
			return true
		}
		return aTime.After(bTime)
	}
	if a.DisplayTitle != b.DisplayTitle {
		return a.DisplayTitle < b.DisplayTitle
	}
	if a.ID != b.ID {
		return a.ID < b.ID
	}
	return a.Number < b.Number
}

func dashboardSessionInputRecency(session DashboardSessionInput) time.Time {
	if !session.UpdatedAt.IsZero() {
		return session.UpdatedAt
	}
	return session.CreatedAt
}

func dashboardSessionRecency(session DashboardSession) time.Time {
	if !session.UpdatedAt.IsZero() {
		return session.UpdatedAt
	}
	return session.CreatedAt
}

func buildDashboardView(kind DashboardViewKind, selectedWorkspaceID string, workspaces []DashboardWorkspace, sessions []DashboardSession, now time.Time, ttl time.Duration) DashboardView {
	view := DashboardView{
		Kind:        kind,
		WorkspaceID: selectedWorkspaceID,
		CreatedAt:   now,
		ExpiresAt:   now.Add(ttl),
	}
	switch kind {
	case DashboardViewSessions:
		for _, session := range sessions {
			if session.WorkspaceID != selectedWorkspaceID {
				continue
			}
			view.Items = append(view.Items, DashboardViewItem{
				Number:       session.Number,
				Kind:         DashboardSelectionSession,
				WorkspaceID:  session.WorkspaceID,
				SessionID:    session.ID,
				DisplayTitle: session.DisplayTitle,
			})
		}
	default:
		view.Kind = DashboardViewWorkspaces
		for _, workspace := range workspaces {
			view.Items = append(view.Items, DashboardViewItem{
				Number:       workspace.Number,
				Kind:         DashboardSelectionWorkspace,
				WorkspaceID:  workspace.ID,
				DisplayTitle: workspace.DisplayTitle,
			})
		}
	}
	return view
}

func previousWorkspaceNumbers(workspaces []DashboardWorkspace) map[string]int {
	numbers := map[string]int{}
	for _, workspace := range workspaces {
		if workspace.ID != "" && workspace.Number > 0 {
			numbers[workspace.ID] = workspace.Number
		}
	}
	return numbers
}

func previousSessionNumbers(sessions []DashboardSession) map[string]int {
	numbers := map[string]int{}
	for _, session := range sessions {
		if session.WorkspaceID != "" && session.ID != "" && session.Number > 0 {
			numbers[sessionKey(session.WorkspaceID, session.ID)] = session.Number
		}
	}
	return numbers
}

func stableNumbers(previous map[string]int, ids []string) map[string]int {
	numbers := map[string]int{}
	used := map[int]bool{}
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if number := previous[id]; number > 0 && !used[number] {
			numbers[id] = number
			used[number] = true
		}
	}
	next := 1
	for _, id := range ids {
		if id == "" || numbers[id] > 0 {
			continue
		}
		for used[next] {
			next++
		}
		numbers[id] = next
		used[next] = true
	}
	return numbers
}

func dashboardWorkspaceID(input DashboardWorkspaceInput) string {
	if id := strings.TrimSpace(input.ID); id != "" {
		return id
	}
	if path := strings.TrimSpace(input.Path); path != "" {
		return filepath.Clean(path)
	}
	return ""
}

func sessionKey(workspaceID string, sessionID string) string {
	return strings.TrimSpace(workspaceID) + "\x00" + strings.TrimSpace(sessionID)
}

func sanitizedTitlePart(value string, fallback string) string {
	if title := SanitizeDashboardTitle(value); title != "" {
		return title
	}
	return fallback
}

func shortTitleIDPart(value string, fallback string) string {
	title := sanitizedTitlePart(value, fallback)
	rs := []rune(title)
	if len(rs) <= 12 {
		return title
	}
	return string(rs[:12])
}

func machineTitlePart(machineLabel string) string {
	return sanitizedTitlePart(machineLabel, "machine")
}

func profileTitlePart(profile string) string {
	profile = strings.TrimSpace(profile)
	if profile == "" || strings.EqualFold(profile, "default") {
		return ""
	}
	return SanitizeDashboardTitle(profile)
}

func joinTitleParts(parts ...string) string {
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			kept = append(kept, part)
		}
	}
	return strings.Join(kept, " - ")
}

func redactPathLikeTokens(text string) string {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return text
	}
	changed := false
	for i, field := range fields {
		prefix, core, suffix := splitTokenPunctuation(field)
		if pathLikeToken(core) {
			if base := lastPathElement(core); base != "" {
				fields[i] = prefix + base + suffix
				changed = true
			}
		}
	}
	if !changed {
		return text
	}
	return strings.Join(fields, " ")
}

func splitTokenPunctuation(token string) (string, string, string) {
	start := 0
	end := len(token)
	for start < end && strings.ContainsRune("'\"([{<", rune(token[start])) {
		start++
	}
	for end > start && strings.ContainsRune("'\".,;:)]}>", rune(token[end-1])) {
		end--
	}
	return token[:start], token[start:end], token[end:]
}

func pathLikeToken(token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	if strings.HasPrefix(token, "/") || strings.HasPrefix(token, "~/") || strings.HasPrefix(token, `~\`) {
		return true
	}
	if len(token) >= 3 && ((token[0] >= 'A' && token[0] <= 'Z') || (token[0] >= 'a' && token[0] <= 'z')) && token[1] == ':' && (token[2] == '\\' || token[2] == '/') {
		return true
	}
	return false
}

func lastPathElement(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	path = strings.ReplaceAll(path, "\\", "/")
	path = strings.TrimRight(path, "/")
	if path == "" {
		return "root"
	}
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		path = path[idx+1:]
	}
	path = strings.TrimSpace(path)
	if path == "." || path == string(filepath.Separator) {
		return ""
	}
	return path
}

func parseBarePositiveInt(text string) (int, bool) {
	if text == "" {
		return 0, false
	}
	n := 0
	for _, r := range text {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
	}
	if n <= 0 {
		return 0, false
	}
	return n, true
}
