package store

import "slices"

type CLIResult struct {
	Lines []string `json:"lines,omitempty"`
	Error string   `json:"error,omitempty"`
}

type CLIProgress struct {
	Stage  string     `json:"stage,omitempty"`
	Conv   int64      `json:"conv,omitempty"`
	Old    string     `json:"old,omitempty"`
	Chat   string     `json:"chat,omitempty"`
	URL    string     `json:"url,omitempty"`
	Title  string     `json:"title,omitempty"`
	Name   string     `json:"name,omitempty"`
	Import int64      `json:"import,omitempty"`
	Result *CLIResult `json:"result,omitempty"`
}

func (t *Tx) CLIProgress(id string) (CLIProgress, bool, error) {
	var n CLIProgress
	var done bool
	var text string
	_, ok, err := one(t.row(`SELECT stage,conv_id,old_chat,chat,url,title,name,import_id,done,error FROM cli_requests WHERE id=?`, id), func(sc scanner) (bool, error) {
		return true, sc.Scan(&n.Stage, &n.Conv, &n.Old, &n.Chat, &n.URL, &n.Title, &n.Name, &n.Import, &done, &text)
	})
	if err != nil || !ok {
		return n, ok, err
	}
	if done {
		n.Result = &CLIResult{Error: text}
		rows, err := t.rows(`SELECT text FROM cli_result_lines WHERE request_id=? ORDER BY position`, id)
		if err != nil {
			return n, false, err
		}
		defer rows.Close()
		for rows.Next() {
			var text string
			if err := rows.Scan(&text); err != nil {
				return n, false, err
			}
			n.Result.Lines = append(n.Result.Lines, text)
		}
		if err := rows.Err(); err != nil {
			return n, false, err
		}
	}
	return n, true, nil
}

func (t *Tx) SetCLIProgress(id string, n CLIProgress) error {
	old, ok, err := t.CLIProgress(id)
	if err != nil {
		return err
	}
	done, text := n.Result != nil, ""
	if done {
		text = n.Result.Error
	}
	_, err = t.exec(`INSERT INTO cli_requests(id,stage,conv_id,old_chat,chat,url,title,name,import_id,done,error) VALUES(?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET stage=excluded.stage,conv_id=excluded.conv_id,old_chat=excluded.old_chat,chat=excluded.chat,url=excluded.url,title=excluded.title,name=excluded.name,import_id=excluded.import_id,done=excluded.done,error=excluded.error
WHERE (stage,conv_id,old_chat,chat,url,title,name,import_id,done,error) IS NOT (excluded.stage,excluded.conv_id,excluded.old_chat,excluded.chat,excluded.url,excluded.title,excluded.name,excluded.import_id,excluded.done,excluded.error)`, id, n.Stage, n.Conv, n.Old, n.Chat, n.URL, n.Title, n.Name, n.Import, done, text)
	if err != nil {
		return err
	}
	var before, after []string
	if ok && old.Result != nil {
		before = old.Result.Lines
	}
	if done {
		after = n.Result.Lines
	}
	if slices.Equal(before, after) {
		return nil
	}
	if _, err = t.exec(`DELETE FROM cli_result_lines WHERE request_id=?`, id); err != nil {
		return err
	}
	for i, line := range after {
		if _, err = t.exec(`INSERT INTO cli_result_lines(request_id,position,text) VALUES(?,?,?)`, id, i, line); err != nil {
			return err
		}
	}
	return nil
}

type CodexTarget struct {
	Conv    int64 `json:"conv"`
	Inbound int64 `json:"inbound"`
}
type CodexRequest struct {
	ID      string        `json:"id"`
	Conv    int64         `json:"conv,omitempty"`
	Inbound int64         `json:"inbound,omitempty"`
	Quote   string        `json:"quote,omitempty"`
	Targets []CodexTarget `json:"targets,omitempty"`
}

func (t *Tx) CodexRequest() (CodexRequest, bool, error) {
	var n CodexRequest
	_, ok, err := one(t.row(`SELECT request_id,conv_id,inbound_id,quote FROM codex_upgrade WHERE id=1`), func(sc scanner) (bool, error) { return true, sc.Scan(&n.ID, &n.Conv, &n.Inbound, &n.Quote) })
	if err != nil || !ok {
		return n, ok, err
	}
	rows, err := t.rows(`SELECT conv_id,inbound_id FROM codex_upgrade_targets WHERE upgrade_id=1 ORDER BY position`)
	if err != nil {
		return n, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var target CodexTarget
		if err := rows.Scan(&target.Conv, &target.Inbound); err != nil {
			return n, false, err
		}
		n.Targets = append(n.Targets, target)
	}
	return n, true, rows.Err()
}

func (t *Tx) SetCodexRequest(n CodexRequest) error {
	old, _, err := t.CodexRequest()
	if err != nil {
		return err
	}
	_, err = t.exec(`INSERT INTO codex_upgrade(id,request_id,conv_id,inbound_id,quote) VALUES(1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET request_id=excluded.request_id,conv_id=excluded.conv_id,inbound_id=excluded.inbound_id,quote=excluded.quote WHERE (request_id,conv_id,inbound_id,quote) IS NOT (excluded.request_id,excluded.conv_id,excluded.inbound_id,excluded.quote)`, n.ID, n.Conv, n.Inbound, n.Quote)
	if err != nil || slices.Equal(old.Targets, n.Targets) {
		return err
	}
	if _, err = t.exec(`DELETE FROM codex_upgrade_targets WHERE upgrade_id=1`); err != nil {
		return err
	}
	for i, target := range n.Targets {
		if _, err = t.exec(`INSERT INTO codex_upgrade_targets(upgrade_id,position,conv_id,inbound_id) VALUES(1,?,?,?)`, i, target.Conv, target.Inbound); err != nil {
			return err
		}
	}
	return nil
}

func (t *Tx) DeleteCodexRequest() error {
	_, err := t.exec(`DELETE FROM codex_upgrade WHERE id=1`)
	return err
}

type DashboardItem struct {
	N         int    `json:"n"`
	Kind      string `json:"kind"`
	Workspace string `json:"workspace"`
	Thread    string `json:"thread,omitempty"`
}
type DashboardView struct {
	Kind      string          `json:"kind"`
	Workspace string          `json:"workspace,omitempty"`
	Items     []DashboardItem `json:"items,omitempty"`
	Expires   int64           `json:"expires"`
}

func (t *Tx) DashboardView() (*DashboardView, error) {
	var n DashboardView
	_, ok, err := one(t.row(`SELECT kind,workspace,expires FROM dashboard_view WHERE id=1`), func(sc scanner) (bool, error) { return true, sc.Scan(&n.Kind, &n.Workspace, &n.Expires) })
	if err != nil || !ok {
		return nil, err
	}
	rows, err := t.rows(`SELECT number,kind,workspace,thread FROM dashboard_items WHERE view_id=1 ORDER BY position`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item DashboardItem
		if err := rows.Scan(&item.N, &item.Kind, &item.Workspace, &item.Thread); err != nil {
			return nil, err
		}
		n.Items = append(n.Items, item)
	}
	return &n, rows.Err()
}

func (t *Tx) SetDashboardView(n *DashboardView) error {
	if n == nil {
		_, err := t.exec(`DELETE FROM dashboard_view WHERE id=1`)
		return err
	}
	old, err := t.DashboardView()
	if err != nil {
		return err
	}
	_, err = t.exec(`INSERT INTO dashboard_view(id,kind,workspace,expires) VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,workspace=excluded.workspace,expires=excluded.expires WHERE (kind,workspace,expires) IS NOT (excluded.kind,excluded.workspace,excluded.expires)`, n.Kind, n.Workspace, n.Expires)
	if err != nil || old != nil && slices.Equal(old.Items, n.Items) {
		return err
	}
	if _, err = t.exec(`DELETE FROM dashboard_items WHERE view_id=1`); err != nil {
		return err
	}
	for i, item := range n.Items {
		if _, err = t.exec(`INSERT INTO dashboard_items(view_id,position,number,kind,workspace,thread) VALUES(1,?,?,?,?,?)`, i, item.N, item.Kind, item.Workspace, item.Thread); err != nil {
			return err
		}
	}
	return nil
}

// SourceInfo is comparable, including absent versus present empty goals.
type SourceInfo struct {
	Model         string
	Effort        string
	Tier          string
	HasGoal       bool
	GoalObjective string
	GoalStatus    string
}

func (t *Tx) SourceInfo(id int64) (SourceInfo, bool, error) {
	var n SourceInfo
	_, ok, err := one(t.row(`SELECT model,effort,tier,has_goal,goal_objective,goal_status FROM source_info WHERE source_id=?`, id), func(sc scanner) (bool, error) {
		return true, sc.Scan(&n.Model, &n.Effort, &n.Tier, &n.HasGoal, &n.GoalObjective, &n.GoalStatus)
	})
	return n, ok, err
}

func (t *Tx) SetSourceInfo(id int64, n SourceInfo) error {
	_, err := t.exec(`INSERT INTO source_info(source_id,model,effort,tier,has_goal,goal_objective,goal_status) VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET model=excluded.model,effort=excluded.effort,tier=excluded.tier,has_goal=excluded.has_goal,goal_objective=excluded.goal_objective,goal_status=excluded.goal_status WHERE (model,effort,tier,has_goal,goal_objective,goal_status) IS NOT (excluded.model,excluded.effort,excluded.tier,excluded.has_goal,excluded.goal_objective,excluded.goal_status)`, id, n.Model, n.Effort, n.Tier, n.HasGoal, n.GoalObjective, n.GoalStatus)
	return err
}
