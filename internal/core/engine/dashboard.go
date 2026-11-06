package engine

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

// The control dashboard (batch A: F-CTL-3, F-CTL-4, F-CTL-6, F-CTL-7),
// v0.1.20's dashboard.go and bridge.go B:19702-20800 with its texts:
//
//   - `p` lists the workspaces (the working directories of the local Codex
//     sessions), `w <n>` / `p <n|path>` / `s` the sessions of one, and a
//     bare number picks from the list shown last;
//   - the list is state, not a timer: it is stored with the time it
//     expires (10 minutes, v0.1.20 DefaultDashboardViewTTL), and every
//     other control command clears it (controlCommandConsumesDashboardView);
//   - `o`, `d`, `st`, `mkdir`, the path hint and `new` parity.
//
// The sessions are found on demand (one walk of $CODEX_HOME/sessions per
// command, the first record of each rollout cached by path and inode): no
// periodic work.

const (
	// metaDashView is the numbered list the owner's next number refers to
	// (JSON dashView; "" = none).
	metaDashView = "engine_dashboard_view"
	dashViewTTL  = 10 * time.Minute
)

const (
	dashKindWorkspaces = "workspaces"
	dashKindSessions   = "sessions"
	dashItemWorkspace  = "workspace"
	dashItemSession    = "session"
)

type dashState struct {
	mu     sync.Mutex
	loaded bool
	view   *dashView
	metas  map[string]dashMeta // rollout path -> its first record
	titles map[string]string   // rollout path -> first prompt (non-empty only)
}

type dashMeta struct {
	inode int64
	size  int64
	meta  transcript.SessionMeta
	ok    bool
}

// dashView is v0.1.20's DashboardView.
type dashView struct {
	Kind      string     `json:"kind"`
	Workspace string     `json:"workspace,omitempty"` // the selected workspace id
	Items     []dashItem `json:"items,omitempty"`
	Expires   int64      `json:"expires"` // unix ms
}

type dashItem struct {
	N         int    `json:"n"`
	Kind      string `json:"kind"`
	Workspace string `json:"workspace"`
	Thread    string `json:"thread,omitempty"`
}

type dashSession struct {
	thread, path, cwd, workspace string
	title                        string
	updated                      time.Time
	number                       int
}

type dashWorkspace struct {
	id, path, title string
	sessions        []*dashSession // newest first, numbered from 1
	updated         time.Time
	number          int
}

// v0.1.20 controlCommandErrorMessage texts.
var (
	errDashNoView  = errors.New("I do not have a current list yet. Send `projects` or `sessions` first, then choose a number.")
	errDashExpired = errors.New("This numbered list expired. Send `projects` or `sessions` again, then choose one of the newly shown numbers.")
	errDashMissing = errors.New("That number is not in the current list. Send `projects` or `sessions` again, then choose one of the newly shown numbers.")
)

// ---- the stored list ----

func (e *Engine) dashLoad() *dashView {
	e.dash.mu.Lock()
	if e.dash.loaded {
		v := e.dash.view
		e.dash.mu.Unlock()
		return v
	}
	e.dash.mu.Unlock()
	// Read outside dash.mu: dashStore takes it inside a store transaction.
	var view *dashView
	if v, ok, err := e.st.Meta(metaDashView); err == nil && ok && v != "" {
		var dv dashView
		if json.Unmarshal([]byte(v), &dv) == nil {
			view = &dv
		}
	}
	e.dash.mu.Lock()
	defer e.dash.mu.Unlock()
	if !e.dash.loaded {
		e.dash.loaded, e.dash.view = true, view
	}
	return e.dash.view
}

// dashStore returns the transaction step that stores v (nil: clears) and
// remembers it once that transaction commits.
func (e *Engine) dashStore(v *dashView) func(tx *store.Tx) error {
	return func(tx *store.Tx) error {
		val := ""
		if v != nil {
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			val = string(b)
		}
		if err := tx.SetMeta(metaDashView, val); err != nil {
			return err
		}
		e.dash.mu.Lock()
		e.dash.loaded, e.dash.view = true, v
		e.dash.mu.Unlock()
		return nil
	}
}

// dashClear forgets the list (v0.1.20 clearControlDashboardView).
func (e *Engine) dashClear() {
	if e.dashLoad() == nil {
		return
	}
	if err := e.update(e.dashStore(nil)); err != nil {
		e.logf("clear dashboard list: %v", err)
	}
}

// dashConsume clears the list after a control command that does not keep
// it (v0.1.20 controlCommandConsumesDashboardView: every command but the
// lists and a number; non-command text too).
func (e *Engine) dashConsume(p parsed) {
	if p.form == formCommand {
		switch p.name {
		case cmdWorkspaces, cmdWorkspace, cmdSessions, cmdSelect:
			return
		}
	}
	e.dashClear()
}

// joinTx runs the steps in order (nil steps are skipped).
func joinTx(steps ...func(tx *store.Tx) error) func(tx *store.Tx) error {
	return func(tx *store.Tx) error {
		for _, s := range steps {
			if s == nil {
				continue
			}
			if err := s(tx); err != nil {
				return err
			}
		}
		return nil
	}
}

// ---- discovery ----

// dashDiscover lists the workspaces of the local Codex sessions, most
// recently updated first (v0.1.20 BuildControlDashboard order), each with
// its sessions, newest first.
func (e *Engine) dashDiscover() ([]*dashWorkspace, error) {
	if e.cfg.SessionsDir == "" {
		return nil, nil
	}
	found, err := transcript.Walk(e.cfg.SessionsDir)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	hidden := map[string]bool{}
	for id := range e.delegThreads {
		hidden[id] = true // a delegated task: not a session of this machine's owner
	}
	e.mu.Unlock()
	byThread := map[string]*dashSession{}
	seen := map[string]bool{}
	for _, f := range found {
		seen[f.Path] = true
		m, ok := e.dashMeta(f)
		if !ok || m.ID == "" || m.Subagent() || hidden[m.ID] {
			continue
		}
		if s := byThread[m.ID]; s != nil && !f.Info.MTime.After(s.updated) {
			continue
		}
		cwd := strings.TrimSpace(m.Cwd)
		if cwd != "" {
			cwd = filepath.Clean(cwd)
		}
		byThread[m.ID] = &dashSession{thread: m.ID, path: f.Path, cwd: cwd, workspace: workspaceID(cwd), updated: f.Info.MTime}
	}
	e.dash.mu.Lock()
	for p := range e.dash.metas {
		if !seen[p] {
			delete(e.dash.metas, p)
			delete(e.dash.titles, p)
		}
	}
	e.dash.mu.Unlock()
	ws := map[string]*dashWorkspace{}
	for _, s := range byThread {
		w := ws[s.workspace]
		if w == nil {
			w = &dashWorkspace{id: s.workspace, path: s.cwd, title: workspaceTitle(s.cwd)}
			ws[s.workspace] = w
		}
		w.sessions = append(w.sessions, s)
		if s.updated.After(w.updated) {
			w.updated = s.updated
		}
	}
	out := make([]*dashWorkspace, 0, len(ws))
	for _, w := range ws {
		sort.SliceStable(w.sessions, func(i, j int) bool {
			a, b := w.sessions[i], w.sessions[j]
			if !a.updated.Equal(b.updated) {
				return a.updated.After(b.updated)
			}
			return a.thread < b.thread
		})
		for i, s := range w.sessions {
			s.number = i + 1
		}
		out = append(out, w)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if !a.updated.Equal(b.updated) {
			return a.updated.After(b.updated)
		}
		if a.title != b.title {
			return a.title < b.title
		}
		if a.path != b.path {
			return a.path < b.path
		}
		return a.id < b.id
	})
	for i, w := range out {
		w.number = i + 1
	}
	return out, nil
}

// dashMeta reads (or recalls) the first record of a rollout.
func (e *Engine) dashMeta(f transcript.Found) (transcript.SessionMeta, bool) {
	e.dash.mu.Lock()
	if e.dash.metas == nil {
		e.dash.metas, e.dash.titles = map[string]dashMeta{}, map[string]string{}
	}
	c, hit := e.dash.metas[f.Path]
	e.dash.mu.Unlock()
	if hit && c.inode == f.Info.Inode && (c.ok || c.size == f.Info.Size) {
		return c.meta, c.ok
	}
	read := e.readMeta
	if read == nil {
		read = transcript.ReadMeta
	}
	m, err := read(f.Path)
	c = dashMeta{inode: f.Info.Inode, size: f.Info.Size, meta: m, ok: err == nil}
	e.dash.mu.Lock()
	e.dash.metas[f.Path] = c
	e.dash.mu.Unlock()
	return c.meta, c.ok
}

// dashTitles fills the titles of sessions (v0.1.20 DisplayTitle: the
// thread name Codex recorded, else the first prompt, else the id).
func (e *Engine) dashTitles(sessions []*dashSession) {
	if len(sessions) == 0 {
		return
	}
	names := e.sessionIndexNames()
	for _, s := range sessions {
		title := sanitizeTitle(names[s.thread])
		if title == "" {
			e.dash.mu.Lock()
			first := e.dash.titles[s.path]
			e.dash.mu.Unlock()
			if first == "" {
				first = promptTitle(transcript.FirstPrompt(s.path, firstPromptMax))
				if first != "" {
					e.dash.mu.Lock()
					if e.dash.titles == nil { // the dashboard may not have been read yet
						e.dash.titles = map[string]string{}
					}
					e.dash.titles[s.path] = first
					e.dash.mu.Unlock()
				}
			}
			title = sanitizeTitle(first)
		}
		if title == "" {
			title = sanitizeTitle(s.thread)
		}
		if title == "" {
			title = "untitled"
		}
		s.title = title
	}
}

// sessionIndexNames reads Codex's thread names (session_index.jsonl): the
// newest name of each thread.
func (e *Engine) sessionIndexNames() map[string]string {
	out := map[string]string{}
	if e.cfg.SessionsDir == "" {
		return out
	}
	f, err := os.Open(filepath.Join(filepath.Dir(e.cfg.SessionsDir), "session_index.jsonl"))
	if err != nil {
		return out
	}
	defer f.Close()
	if info, err := f.Stat(); err == nil && info.Size() > sessionIndexMax {
		if _, err := f.Seek(info.Size()-sessionIndexMax, io.SeekStart); err != nil {
			return out
		}
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		var rec struct {
			ID   string `json:"id"`
			Name string `json:"thread_name"`
		}
		if json.Unmarshal(sc.Bytes(), &rec) == nil && rec.ID != "" && strings.TrimSpace(rec.Name) != "" {
			out[rec.ID] = strings.TrimSpace(rec.Name)
		}
	}
	return out
}

// workspaceID is v0.1.20's workspaceIDForPath.
func workspaceID(path string) string {
	if path == "" {
		return "workspace:unknown"
	}
	sum := sha256.Sum256([]byte(path))
	return "workspace:" + hex.EncodeToString(sum[:8])
}

func findWorkspace(list []*dashWorkspace, id string) *dashWorkspace {
	for _, w := range list {
		if w.id == id {
			return w
		}
	}
	return nil
}

func findDashSession(list []*dashWorkspace, thread string) *dashSession {
	for _, w := range list {
		for _, s := range w.sessions {
			if s.thread == thread {
				return s
			}
		}
	}
	return nil
}

// workspaceTitle is v0.1.20's WorkspaceDisplayTitle without a user title.
func workspaceTitle(path string) string {
	if base := lastPathElement(path); base != "" {
		if t := sanitizeTitle(base); t != "" {
			return t
		}
	}
	return "workspace"
}

// inlineCode is v0.1.20's dashboardInlineCode.
func inlineCode(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	return "`" + strings.ReplaceAll(v, "`", "'") + "`"
}

func indented(lines ...string) []string {
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		if strings.TrimSpace(l) == "" {
			out = append(out, "")
			continue
		}
		out = append(out, "  "+l)
	}
	return out
}

func dashTime(t time.Time) string { return t.Local().Format("2006-01-02 15:04") }

// ---- linked work chats ----

// dashLink is the work conversation a Codex thread is linked to.
type dashLink struct {
	conv                int64
	label, title, url   string
	state, cwd          string
	parked, open, found bool
}

// linkOf returns the work chat of thread. A discovered session whose chat
// was never created (nothing mirrored yet) is not in Teams.
func (e *Engine) linkOf(thread string) dashLink {
	e.mu.Lock()
	id, ok := e.byThread[thread]
	e.mu.Unlock()
	if !ok {
		return dashLink{}
	}
	return e.linkConv(id)
}

// linkConv is the link of work conversation id.
func (e *Engine) linkConv(id int64) dashLink {
	e.mu.Lock()
	cs := e.convs[id]
	var l dashLink
	if cs != nil && cs.kind == store.KindWork && id != e.inboxID {
		l = dashLink{conv: id, label: cs.label, title: cs.title, url: cs.url, state: cs.state, cwd: cs.cwd, found: true}
	}
	e.mu.Unlock()
	if !l.found {
		return l
	}
	if l.state == store.ConvCreating {
		if v, _, err := e.st.Meta(discoveredKey(l.conv)); err == nil && v == discoveredPending {
			return dashLink{}
		}
	}
	l.open = l.state != store.ConvClosed && l.state != store.ConvHold
	l.parked = l.state == store.ConvParked
	return l
}

// ---- the lists ----

// cmdWorkspaces is `p` (v0.1.20 formatWorkspaceDashboard B:19702).
func (e *Engine) cmdWorkspaces(j cmdJob) {
	list, err := e.dashDiscover()
	if err != nil {
		e.reply(j, "workspace discovery failed: "+err.Error(), e.dashStore(nil))
		return
	}
	selected := ""
	if v := e.dashLoad(); v != nil && findWorkspace(list, v.Workspace) != nil {
		selected = v.Workspace
	}
	if selected == "" && len(list) > 0 {
		selected = list[0].id
	}
	view := &dashView{Kind: dashKindWorkspaces, Workspace: selected, Expires: ms(e.now().Add(dashViewTTL))}
	for i := len(list) - 1; i >= 0; i-- {
		view.Items = append(view.Items, dashItem{N: list[i].number, Kind: dashItemWorkspace, Workspace: list[i].id})
	}
	if len(list) == 0 {
		e.reply(j, "## 📁 Workspaces\n\nNo local Codex workspaces found on this machine.\n\nCodex history is stored locally. If you used Codex on another computer, run this helper there too.\n\n**Next:** send `new <directory>` to create a Work chat for a directory.", e.dashStore(view))
		return
	}
	lines := []string{"## 📁 Workspaces", "", "Reply with a number to open a workspace.", "", "---"}
	for i := len(list) - 1; i >= 0; i-- {
		w := list[i]
		active := 0
		for _, s := range w.sessions {
			if e.linkOf(s.thread).open {
				active++
			}
		}
		title, pathLine := w.title, "Path not recorded by Codex"
		if w.path == "" {
			title = "Unknown workspace"
		} else {
			pathLine = inlineCode(w.path)
		}
		meta := "Sessions: none"
		if len(w.sessions) > 0 {
			meta = fmt.Sprintf("Sessions: %d active, %d idle", active, len(w.sessions)-active)
			if !w.updated.IsZero() {
				meta += ", last updated " + dashTime(w.updated)
			}
		}
		body := []string{pathLine, meta}
		if w.path == "" {
			body = append(body, "Older Codex records without a working directory are grouped here.")
		}
		body = append(body, "", fmt.Sprintf("**Next:** send `%d` or `p %d` to open this workspace", w.number, w.number))
		lines = append(lines, fmt.Sprintf("**%d. %s**", w.number, title))
		lines = append(lines, indented(body...)...)
		lines = append(lines, "---")
	}
	lines = append(lines, "", "**Other options:**", "- Send a workspace number, for example `1`, to open it.", "- `n <directory>` or `new <directory>` - create a new Work chat directly.")
	lines = append(lines, "", "Numbers apply to your next reply and expire after 10 minutes. If a number looks wrong, send `projects` again.")
	e.reply(j, strings.Join(lines, "\n"), e.dashStore(view))
}

// cmdWorkspaceSessions is `w <n>`, `p <n|path>`, `s` (v0.1.20
// formatWorkspaceSessionsDashboard B:19840).
func (e *Engine) cmdWorkspaceSessions(j cmdJob, arg string) {
	arg = strings.TrimSpace(arg)
	list, err := e.dashDiscover()
	if err != nil {
		e.reply(j, "workspace discovery failed: "+err.Error(), e.dashStore(nil))
		return
	}
	selected := ""
	if n, isNum := dashNumber(arg); isNum {
		sel, err := e.dashResolve(list, n)
		if err == nil && sel.kind != dashItemWorkspace {
			err = fmt.Errorf("number %d is not a workspace in the current dashboard view", n)
		}
		if err != nil {
			e.reply(j, err.Error(), e.dashStore(nil))
			return
		}
		selected = sel.workspace.id
	} else if arg != "" {
		p := strings.Trim(arg, "`\"'")
		if p != "" {
			p = filepath.Clean(p)
		}
		selected = workspaceID(p)
	} else if v := e.dashLoad(); v != nil && findWorkspace(list, v.Workspace) != nil {
		selected = v.Workspace
	}
	if selected == "" && len(list) > 0 {
		selected = list[0].id
	}
	view := &dashView{Kind: dashKindSessions, Workspace: selected, Expires: ms(e.now().Add(dashViewTTL))}
	w := findWorkspace(list, selected)
	if w == nil || len(w.sessions) == 0 {
		if w != nil {
			e.reply(j, fmt.Sprintf("## Sessions\n\nWorkspace: %s\n\nNo local Codex sessions were found in this workspace on this machine.\n\nCodex history is stored locally. If you used Codex on another computer, run this helper there too.\n\n**Next:** send `new` to create a new Work chat in this workspace.", workspacePathDisplay(w.path)), e.dashStore(view))
			return
		}
		e.reply(j, "## Sessions\n\nNo local Codex sessions found on this machine.\n\n**Next:** send `projects` to choose a workspace.\n\nTip: send `new <directory>` to create a new Work chat.", e.dashStore(view))
		return
	}
	e.dashTitles(w.sessions)
	lines := []string{"## Sessions", "", "Workspace: " + workspacePathDisplay(w.path), "", "Reply with a number to continue a session in Teams.", "", "---"}
	for i := len(w.sessions) - 1; i >= 0; i-- {
		s := w.sessions[i]
		view.Items = append(view.Items, dashItem{N: s.number, Kind: dashItemSession, Workspace: w.id, Thread: s.thread})
		l := e.linkOf(s.thread)
		action := fmt.Sprintf("send `%d` or `c %d` to continue this session in Teams", s.number, s.number)
		if l.found {
			if l.open {
				action = fmt.Sprintf("send `%d` to open this Teams chat or import updates", s.number)
			} else {
				action = fmt.Sprintf("send `%d` to create a new Work chat from this closed Teams chat", s.number)
			}
		}
		pathLine := "Path not recorded by Codex"
		if w.path != "" {
			pathLine = inlineCode(w.path)
		}
		status := "idle"
		if l.open {
			status = "active"
		}
		meta := "Session: " + status + ", update time not recorded"
		if !s.updated.IsZero() {
			meta = "Session: " + status + ", last updated " + dashTime(s.updated)
		}
		body := []string{pathLine, meta}
		if l.open {
			if l.url != "" {
				body = append(body, "**Chat:** [▶ Open chat]("+l.url+")")
			}
			if l.parked {
				body = append(body, "**Resume parked chat:** send "+inlineCode("r "+l.label))
			}
		}
		body = append(body, "", "**Next:** "+action)
		lines = append(lines, fmt.Sprintf("**%d. %s**", s.number, s.title))
		lines = append(lines, indented(body...)...)
		lines = append(lines, "---")
	}
	lines = append(lines, "", "Need debug IDs? Send `details <number>`. Numbers expire after one reply.", "", "**Next:** send `new` to create a new Work chat in this workspace.")
	e.reply(j, strings.Join(lines, "\n"), e.dashStore(view))
}

func workspacePathDisplay(path string) string {
	if path != "" {
		return inlineCode(path)
	}
	return "**Unknown workspace**\n   Path: not recorded by Codex"
}

func dashNumber(arg string) (int, bool) {
	arg = strings.TrimSpace(arg)
	if arg == "" || arg[0] < '0' || arg[0] > '9' {
		return 0, false
	}
	n, err := strconv.Atoi(arg)
	return n, err == nil && n > 0
}

// dashSel is a number resolved through the list.
type dashSel struct {
	n         int
	kind      string
	workspace *dashWorkspace
	session   *dashSession
}

// dashResolve resolves a number through the stored list (v0.1.20
// resolveDashboardTarget): the list must exist, be unexpired, hold the
// number, and its entry must still exist on disk.
func (e *Engine) dashResolve(list []*dashWorkspace, n int) (dashSel, error) {
	v := e.dashLoad()
	if v == nil || len(v.Items) == 0 {
		return dashSel{}, errDashNoView
	}
	if v.Expires > 0 && e.now().After(fromMS(v.Expires)) {
		return dashSel{}, errDashExpired
	}
	for _, it := range v.Items {
		if it.N != n {
			continue
		}
		w := findWorkspace(list, it.Workspace)
		if w == nil {
			return dashSel{}, errDashMissing
		}
		if it.Kind == dashItemWorkspace {
			return dashSel{n: n, kind: dashItemWorkspace, workspace: w}, nil
		}
		for _, s := range w.sessions {
			if s.thread == it.Thread {
				return dashSel{n: n, kind: dashItemSession, workspace: w, session: s}, nil
			}
		}
		return dashSel{}, errDashMissing
	}
	return dashSel{}, errDashMissing
}

// cmdSelect is a bare number (v0.1.20 resolveControlSelection B:20304): a
// workspace shows its sessions, a session is continued in Teams; the list
// is used up by a session or an error.
func (e *Engine) cmdSelect(ctx context.Context, j cmdJob, arg string) {
	n, _ := dashNumber(arg)
	list, err := e.dashDiscover()
	if err != nil {
		e.reply(j, "workspace discovery failed: "+err.Error(), e.dashStore(nil))
		return
	}
	sel, err := e.dashResolve(list, n)
	if err != nil {
		e.reply(j, err.Error(), e.dashStore(nil))
		return
	}
	if sel.kind == dashItemWorkspace {
		e.cmdWorkspaceSessions(j, strconv.Itoa(n))
		return
	}
	e.dashTitles([]*dashSession{sel.session})
	e.publishSession(ctx, j, sel.session, e.dashStore(nil))
}

// ---- open, details, status ----

// cmdOpen is `o <n|session>` (v0.1.20 formatOpenControlTarget B:20036).
func (e *Engine) cmdOpen(j cmdJob, arg string) {
	arg = strings.TrimSpace(arg)
	if n, isNum := dashNumber(arg); isNum {
		list, err := e.dashDiscover()
		if err != nil {
			e.reply(j, "workspace discovery failed: "+err.Error(), nil)
			return
		}
		sel, err := e.dashResolve(list, n)
		if err == nil && sel.kind != dashItemSession {
			err = fmt.Errorf("number %d is a workspace in the current dashboard view; send `project %d` to list its sessions", n, n)
		}
		if err != nil {
			e.reply(j, err.Error(), nil)
			return
		}
		l := e.linkOf(sel.session.thread)
		switch {
		case !l.found:
			e.reply(j, fmt.Sprintf("This local Codex session is not in Teams yet.\nNext: send `%d` or `continue %d` to create a Work chat and import its history.\nUse `details %d` to show technical IDs.", n, n, n), nil)
		case !l.open:
			e.reply(j, fmt.Sprintf("This Codex session has a closed Teams work chat. The helper no longer polls that chat.\nNext: send `continue %d` to create a new work chat and continue from local history.\nUse `details %d` to show technical IDs.", n, n), nil)
		default:
			e.openLinked(j, l)
		}
		return
	}
	if arg == "" {
		e.reply(j, "usage: `open <number>`", nil)
		return
	}
	if l, ok := e.linkByName(arg); ok {
		e.openLinked(j, l)
		return
	}
	if list, err := e.dashDiscover(); err == nil && findDashSession(list, arg) != nil {
		e.reply(j, "This local Codex session is not in Teams yet.\nNext: send `sessions`, then choose its number to create a Work chat and import its history.\nUse `details <number>` from the sessions list to show technical IDs.", nil)
		return
	}
	e.reply(j, "session not found: "+arg, nil)
}

// linkByName finds a work chat by its session label (s001, 1) or its
// Codex thread id.
func (e *Engine) linkByName(arg string) (dashLink, bool) {
	if cs := e.findLabel(arg); cs != nil {
		e.mu.Lock()
		id := cs.id
		e.mu.Unlock()
		if l := e.linkConv(id); l.found {
			return l, true
		}
	}
	if l := e.linkOf(strings.TrimSpace(arg)); l.found {
		return l, true
	}
	return dashLink{}, false
}

// openLinked is v0.1.20's formatOpenSessionMessage: a parked chat is
// resumed.
func (e *Engine) openLinked(j cmdJob, l dashLink) {
	status := "active"
	if !l.open {
		status = "closed"
	}
	lines := []string{fmt.Sprintf("%s [%s] %s", l.label, status, firstNonEmpty(strings.TrimSpace(l.title), l.label))}
	if l.url != "" {
		lines = append(lines, l.url)
	}
	var extra func(tx *store.Tx) error
	switch {
	case !l.open:
		lines = append(lines, "This work chat is closed, so the helper no longer reads or responds there. Use `sessions` then `continue <number>` to continue the local Codex session in a new work chat.")
	case l.parked:
		lines = append(lines, "This parked work chat was resumed. Next: open this Teams work chat and send a message there to continue.")
		extra = e.wakeStep(l.conv, fmt.Sprintf("resumed:%d", j.inbound))
	default:
		lines = append(lines, "Next: open this Teams work chat and send a message there to continue. `open` only shows the linked chat; it does not import local history.")
	}
	e.reply(j, strings.Join(lines, "\n"), extra)
	if extra != nil {
		e.heatUser(l.conv)
	}
}

// wakeStep wakes a parked conversation with the core's resume notice in
// its chat (as `r <session>` does).
func (e *Engine) wakeStep(conv int64, base string) func(tx *store.Tx) error {
	return func(tx *store.Tx) error {
		c, ok, err := tx.Conversation(conv)
		if err != nil || !ok || c.State != store.ConvParked {
			return err
		}
		if err := tx.Wake(conv); err != nil {
			return err
		}
		if err := e.enqueueNotice(tx, conv, base,
			"This chat was resumed: messages here are read again. Requests sent while it was parked run now."); err != nil {
			return err
		}
		_, err = e.refresh(tx, conv)
		return err
	}
}

// cmdDetails is `d <n|session>` (v0.1.20 formatDetailsControlTarget
// B:15573); with no argument it is the status list.
func (e *Engine) cmdDetails(j cmdJob, arg string) {
	arg = strings.TrimSpace(arg)
	if n, isNum := dashNumber(arg); isNum {
		list, err := e.dashDiscover()
		if err != nil {
			e.reply(j, "workspace discovery failed: "+err.Error(), nil)
			return
		}
		sel, err := e.dashResolve(list, n)
		if err == nil && sel.kind != dashItemSession {
			err = fmt.Errorf("number %d is a directory in the current list; send `project %d` to list its sessions", n, n)
		}
		if err != nil {
			e.reply(j, err.Error(), nil)
			return
		}
		if l := e.linkOf(sel.session.thread); l.found {
			e.reply(j, e.sessionDetails(l.conv), nil)
			return
		}
		s := sel.session
		e.dashTitles([]*dashSession{s})
		lines := []string{"Local Codex session", fmt.Sprintf("List number: %d", n), "Title: " + s.title}
		if s.cwd != "" {
			lines = append(lines, "Working directory: "+s.cwd)
		}
		if s.updated.IsZero() {
			lines = append(lines, "Updated: unknown")
		} else {
			lines = append(lines, "Updated: "+s.updated.Local().Format(time.RFC3339))
		}
		lines = append(lines, "Codex session ID: "+s.thread, "Teams chat: not published",
			fmt.Sprintf("Next: send `continue %d` to create a work chat and import its history.", n))
		e.reply(j, strings.Join(lines, "\n"), nil)
		return
	}
	if arg == "" {
		e.reply(j, e.activeChatsText(), nil)
		return
	}
	if l, ok := e.linkByName(arg); ok {
		e.reply(j, e.sessionDetails(l.conv), nil)
		return
	}
	e.reply(j, "session not found: "+arg, nil)
}

// sessionDetails is v0.1.20's formatSessionDetails (B:15652).
func (e *Engine) sessionDetails(conv int64) string {
	cs := e.conv(conv)
	if cs == nil {
		return "session not found."
	}
	e.mu.Lock()
	label, state, title, cwd, thread, url := cs.label, cs.state, cs.title, cs.cwd, cs.threadID, cs.url
	running := cs.running != nil
	e.mu.Unlock()
	polling := "listening" // parked too (v0.1.20 userFacingChatState)
	switch state {
	case store.ConvClosed:
		polling = "closed"
	case store.ConvHold:
		polling = "held"
	}
	lines := []string{"Session: " + label, "Chat polling: " + polling, "Title: " + firstNonEmpty(strings.TrimSpace(title), label)}
	if cwd != "" {
		lines = append(lines, "Working directory: "+cwd)
	}
	var th store.Thread
	var recent []store.Turn
	_ = e.update(func(tx *store.Tx) error {
		var err error
		if th, _, err = tx.Thread(conv); err != nil {
			return err
		}
		recent, err = tx.RecentTurns(conv, 1)
		return err
	})
	if th.Model != "" {
		lines = append(lines, "Model profile: "+th.Model)
	}
	if thread != "" {
		lines = append(lines, "Codex thread: "+thread)
	}
	if url != "" {
		lines = append(lines, "Teams chat: "+url)
	}
	if len(recent) > 0 {
		t := recent[0]
		lines = append(lines, "Latest turn: "+t.TurnKey)
		activity := "idle"
		if running || t.State == store.TurnQueued || t.State == store.TurnRunning {
			activity = "running"
		}
		lines = append(lines, "Codex status: "+activity)
		if t.CodexTurnID != "" {
			lines = append(lines, "Latest Codex turn: "+t.CodexTurnID)
		}
	} else {
		lines = append(lines, "Codex status: will start when you send a task")
	}
	return strings.Join(lines, "\n")
}

// activeChatsText is control `st` (v0.1.20 formatSessionList B:15242).
func (e *Engine) activeChatsText() string {
	type row struct {
		id                            int64
		label, title, cwd, url, state string
		used                          time.Time
	}
	e.mu.Lock()
	var active, held []row
	closed := 0
	for id, cs := range e.convs {
		if cs.kind != store.KindWork || id == e.inboxID {
			continue
		}
		r := row{id: id, label: cs.label, title: firstNonEmpty(strings.TrimSpace(cs.title), cs.label), cwd: cs.cwd, url: cs.url, state: cs.state, used: cs.lastActivity}
		switch cs.state {
		case store.ConvClosed:
			closed++
		case store.ConvHold:
			held = append(held, r)
		default:
			active = append(active, r)
		}
	}
	e.mu.Unlock()
	// A discovered session with nothing mirrored has no chat yet.
	kept := active[:0]
	for _, r := range active {
		if r.state == store.ConvCreating {
			if v, _, err := e.st.Meta(discoveredKey(r.id)); err == nil && v == discoveredPending {
				continue
			}
		}
		kept = append(kept, r)
	}
	active = kept
	sort.Slice(active, func(a, b int) bool { return active[a].id > active[b].id })
	sort.Slice(held, func(a, b int) bool { return held[a].id < held[b].id })
	asr := "Speech recognition: configured."
	if e.cfg.Transcriber == nil {
		asr = "Speech recognition: not ready in this helper process; update or restart the installed helper to enable automatic local ASR."
	}
	heldLines := func() []string {
		if len(held) == 0 {
			return nil
		}
		out := []string{"## ⚠️ Quarantined Work chats"}
		for _, r := range held {
			out = append(out, fmt.Sprintf("- **%s** — session `%s`; held after the data conversion, so the helper does not read it.", firstNonEmpty(strings.TrimSpace(r.title), r.label), r.label))
		}
		return out
	}
	if len(active) == 0 {
		if len(held) > 0 || closed > 0 {
			lines := []string{"Control status: no active linked work chats."}
			lines = append(lines, heldLines()...)
			if closed > 0 {
				lines = append(lines, fmt.Sprintf("%d closed work chat(s) are hidden because the helper no longer polls them.", closed))
			}
			lines = append(lines, asr, "Send `projects` to choose a workspace, `new <directory>` to create a Work chat, or `sessions` then `continue <number>` to import an existing local Codex session.")
			return strings.Join(lines, "\n\n")
		}
		return "Control status: no linked work chats yet.\n\n" + asr + "\n\nNext: send `projects` to choose a workspace, or `new <directory>` to create a Work chat."
	}
	models := map[int64]string{}
	if ths, err := e.st.Threads(); err == nil {
		for _, th := range ths {
			if th.Model != "" {
				models[th.ConvID] = th.Model
			}
		}
	}
	lines := []string{"## Active Work chats"}
	for _, r := range active {
		status := "active"
		if r.state == store.ConvParked {
			status = "🧊parked"
		}
		lines = append(lines, fmt.Sprintf("**%s** [%s]", r.title, status))
		meta := []string{"**Session:** " + inlineCode(r.label)}
		if r.cwd != "" {
			meta = append(meta, "**Folder:** "+inlineCode(r.cwd))
		} else {
			meta = append(meta, "**Folder:** not recorded")
		}
		if r.used.IsZero() {
			meta = append(meta, "**Last used:** not recorded")
		} else {
			meta = append(meta, "**Last used:** "+dashTime(r.used))
		}
		if r.url != "" {
			meta = append(meta, "**Chat:** [▶ Open chat]("+r.url+")")
		}
		if r.state == store.ConvParked {
			meta = append(meta, "**Resume parked chat:** send "+inlineCode("r "+r.label))
		}
		if m := models[r.id]; m != "" {
			meta = append(meta, "**Model profile:** "+m)
		}
		lines = append(lines, indented(meta...)...)
		lines = append(lines, "---")
	}
	if closed > 0 {
		lines = append(lines, fmt.Sprintf("%d closed work chat(s) hidden. The helper no longer reads or responds in closed chats.", closed))
	}
	lines = append(lines, heldLines()...)
	lines = append(lines, asr, "Next: open one of these Teams chats to continue work, or send `new <directory>` to create another Work chat.")
	return strings.Join(lines, "\n")
}

// ---- directories: mkdir, new, the path hint ----

// resolveUserPath is v0.1.20's resolveUserWorkspacePath: quotes removed,
// `~` and $VARS expanded, a relative path made absolute.
func resolveUserPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if u, ok := unquotePath(raw); ok {
		raw = u
	}
	if raw == "" {
		return "", errors.New("workspace path is required")
	}
	if strings.ContainsRune(raw, 0) {
		return "", errors.New("workspace path contains NUL byte")
	}
	if strings.HasPrefix(raw, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if raw == "~" {
			raw = home
		} else if strings.HasPrefix(raw, "~/") {
			raw = filepath.Join(home, strings.TrimPrefix(raw, "~/"))
		}
	}
	abs, err := filepath.Abs(os.ExpandEnv(raw))
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

// unquotePath is v0.1.20's unquoteTeamsCommandPath (with backticks).
func unquotePath(p string) (string, bool) {
	if len(p) >= 2 {
		switch {
		case p[0] == '"' && p[len(p)-1] == '"':
			if u, err := strconv.Unquote(p); err == nil {
				return u, true
			}
			return p[1 : len(p)-1], true
		case p[0] == '\'' && p[len(p)-1] == '\'', p[0] == '`' && p[len(p)-1] == '`':
			return p[1 : len(p)-1], true
		}
	}
	return p, false
}

// quotePath is v0.1.20's quoteTeamsCommandPath.
func quotePath(p string) string {
	p = strings.TrimSpace(p)
	if strings.ContainsAny(p, " \t\"'") {
		return strconv.Quote(p)
	}
	return p
}

// pathHintText is v0.1.20's controlPathHintMessage (B:7358).
func pathHintText(text string) string {
	p := strings.TrimSpace(text)
	q := quotePath(p)
	return fmt.Sprintf("📁 Detected path: `%s`\n\nChoose one:\n\n- `new %s` - create a 💬 Work chat there\n- `mkdir %s` - only create the directory", p, q, q)
}

// cmdMkdir is `m <dir>` (v0.1.20 createWorkspaceDirectory B:7830).
func (e *Engine) cmdMkdir(j cmdJob, arg string) {
	dir, err := resolveUserPath(arg)
	if err != nil {
		e.reply(j, "usage: `mkdir <directory>`", nil)
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		e.reply(j, "create workspace failed: "+err.Error(), nil)
		return
	}
	e.reply(j, "Directory is ready: "+dir+"\n\nNext: send `new "+quotePath(dir)+"` to create a work chat for this directory.", nil)
}

// newRequest is a parsed `new` (v0.1.20 parseNewSessionRequest).
type newRequest struct {
	dir, title, model string
}

// parseNew parses `new [<dir>] [--model <m>] [-- <title>]`: a missing
// directory is the opened workspace's, a missing one on disk is created
// (0700, v0.1.20 createSession), the model must be one Codex lists.
func (e *Engine) parseNew(ctx context.Context, arg string) (newRequest, string) {
	arg = strings.TrimSpace(arg)
	before, title, _ := strings.Cut(arg, " -- ")
	if strings.HasPrefix(arg, "-- ") {
		before, title = "", strings.TrimPrefix(arg, "-- ")
	}
	var req newRequest
	req.title = strings.TrimSpace(title)
	var keep []string
	words := strings.Fields(before)
	for i := 0; i < len(words); i++ {
		switch strings.ToLower(words[i]) {
		case "--model", "--model-profile", "--modelprofile", "--mp":
			if i+1 >= len(words) {
				return req, words[i] + " requires a model profile name"
			}
			i++
			req.model = words[i]
		default:
			keep = append(keep, words[i])
		}
	}
	if raw := strings.Join(keep, " "); strings.TrimSpace(raw) != "" {
		dir, err := resolveUserPath(raw)
		if err != nil {
			return req, err.Error()
		}
		req.dir = dir
	} else if v := e.dashLoad(); v != nil && v.Workspace != "" {
		if list, err := e.dashDiscover(); err == nil {
			if w := findWorkspace(list, v.Workspace); w != nil {
				req.dir = w.path
			}
		}
	}
	if req.dir == "" {
		return req, "usage: `new <directory>`; after opening a workspace from `projects`, you can also send `new`"
	}
	if req.model != "" {
		models, err := e.ag.Models(ctx)
		if err != nil {
			return req, fmt.Sprintf("cannot create model-profile work chat for %q: Codex did not list its models (%s)", req.model, userError(err.Error()))
		}
		m, ok := findModel(models, req.model)
		if !ok {
			return req, fmt.Sprintf("cannot create model-profile work chat for %q: unknown model; send `model` to list the available models", req.model)
		}
		req.model = m.ID
	}
	if err := os.MkdirAll(req.dir, 0o700); err != nil {
		return req, "create workspace failed: " + err.Error()
	}
	return req, ""
}
