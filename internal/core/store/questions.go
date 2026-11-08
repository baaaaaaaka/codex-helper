package store

import (
	"fmt"
	"strconv"
	"strings"
)

var questionSchema = []string{
	`CREATE TABLE question_groups (
 key TEXT PRIMARY KEY, conv_id INTEGER NOT NULL REFERENCES conversations(id),
 thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, async INTEGER NOT NULL,
 blocking INTEGER NOT NULL, deadline INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'open',
 created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
 )`,
	`CREATE INDEX question_groups_open ON question_groups(state, conv_id, async)`,
	`CREATE INDEX question_groups_thread ON question_groups(thread_id, state)`,
	`CREATE INDEX question_groups_retention ON question_groups(updated_at) WHERE state NOT IN ('open','submitting')`,
	`CREATE TABLE questions (
 id INTEGER PRIMARY KEY AUTOINCREMENT, group_key TEXT NOT NULL REFERENCES question_groups(key) ON DELETE CASCADE,
 native_id TEXT NOT NULL, title TEXT NOT NULL, secret INTEGER NOT NULL DEFAULT 0,
 answer TEXT, skipped INTEGER NOT NULL DEFAULT 0, submitted INTEGER NOT NULL DEFAULT 0, inbound_id INTEGER,
 UNIQUE(group_key,native_id)
 )`,
	`CREATE INDEX questions_inbound ON questions(inbound_id) WHERE inbound_id IS NOT NULL`,
	`CREATE TABLE question_options (
 question_id INTEGER NOT NULL REFERENCES questions(id) ON DELETE CASCADE,
 position INTEGER NOT NULL, label TEXT NOT NULL, description TEXT NOT NULL,
 PRIMARY KEY(question_id,position)
 ) WITHOUT ROWID`,
}

// QuestionGroup is one native tool invocation. Only active groups are loaded;
// old terminal records are removed by maintenance, never by a token event.
type QuestionGroup struct {
	Key, ThreadID, TurnID, State string
	ConvID, Deadline, CreatedAt  int64
	Async, Blocking              bool
	Questions                    []Question
}

type Question struct {
	ID              int64
	NativeID, Title string
	Secret          bool
	Answer          *string
	Skipped         bool
	Submitted       bool
	InboundID       int64
	Options         []QuestionOption
}

type QuestionOption struct{ Label, Description string }

func (t *Tx) AddQuestionGroup(g QuestionGroup) (bool, error) {
	var exists int
	if err := t.row(`SELECT EXISTS(SELECT 1 FROM question_groups WHERE key=?)`, g.Key).Scan(&exists); err != nil || exists != 0 {
		return false, err
	}
	var count int
	if err := t.row(`SELECT count(*) FROM question_groups WHERE conv_id=? AND state IN ('open','submitting')`, g.ConvID).Scan(&count); err != nil {
		return false, err
	}
	if count >= 64 {
		return false, fmt.Errorf("too many unanswered question groups")
	}
	if _, err := t.exec(`INSERT INTO question_groups(key,conv_id,thread_id,turn_id,async,blocking,deadline,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)`, g.Key, g.ConvID, g.ThreadID, g.TurnID, g.Async, g.Blocking, g.Deadline, t.now, t.now); err != nil {
		return false, err
	}
	for _, q := range g.Questions {
		var id int64
		if err := t.row(`INSERT INTO questions(group_key,native_id,title,secret) VALUES(?,?,?,?) RETURNING id`, g.Key, q.NativeID, q.Title, q.Secret).Scan(&id); err != nil {
			return false, err
		}
		for i, o := range q.Options {
			if _, err := t.exec(`INSERT INTO question_options VALUES(?,?,?,?)`, id, i, o.Label, o.Description); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (t *Tx) QuestionGroup(key string) (QuestionGroup, bool, error) {
	var g QuestionGroup
	err := t.row(`SELECT key,conv_id,thread_id,turn_id,async,blocking,deadline,state,created_at FROM question_groups WHERE key=?`, key).Scan(&g.Key, &g.ConvID, &g.ThreadID, &g.TurnID, &g.Async, &g.Blocking, &g.Deadline, &g.State, &g.CreatedAt)
	if isNoRows(err) {
		return g, false, nil
	}
	if err != nil {
		return g, false, err
	}
	rows, err := t.rows(`SELECT id,native_id,title,secret,answer,skipped,submitted,COALESCE(inbound_id,0) FROM questions WHERE group_key=? ORDER BY id`, key)
	if err != nil {
		return g, false, err
	}
	for rows.Next() {
		var q Question
		if err = rows.Scan(&q.ID, &q.NativeID, &q.Title, &q.Secret, &q.Answer, &q.Skipped, &q.Submitted, &q.InboundID); err != nil {
			break
		}
		g.Questions = append(g.Questions, q)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return g, false, err
	}
	for i := range g.Questions {
		opts, e := t.rows(`SELECT label,description FROM question_options WHERE question_id=? ORDER BY position`, g.Questions[i].ID)
		if e != nil {
			return g, false, e
		}
		for opts.Next() {
			var o QuestionOption
			if e = opts.Scan(&o.Label, &o.Description); e != nil {
				break
			}
			g.Questions[i].Options = append(g.Questions[i].Options, o)
		}
		if e == nil {
			e = opts.Err()
		}
		opts.Close()
		if e != nil {
			return g, false, e
		}
	}
	return g, true, nil
}

func (t *Tx) QuestionByID(conv, id int64) (QuestionGroup, bool, error) {
	var key string
	err := t.row(`SELECT g.key FROM question_groups g JOIN questions q ON q.group_key=g.key WHERE g.conv_id=? AND q.id=?`, conv, id).Scan(&key)
	if isNoRows(err) {
		return QuestionGroup{}, false, nil
	}
	if err != nil {
		return QuestionGroup{}, false, err
	}
	return t.QuestionGroup(key)
}

func (t *Tx) QuestionByMessage(conv int64, ext string) (QuestionGroup, int64, error) {
	var key string
	err := t.row(`SELECT delivery_key FROM outbox WHERE conv_id=? AND ext_msg_id=? LIMIT 1`, conv, ext).Scan(&key)
	if isNoRows(err) {
		return QuestionGroup{}, 0, nil
	}
	if err != nil {
		return QuestionGroup{}, 0, err
	}
	parts := strings.Split(key, ":")
	if len(parts) != 4 || parts[0] != "question" || parts[2] != "body" {
		return QuestionGroup{}, 0, nil
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return QuestionGroup{}, 0, nil
	}

	g, _, err := t.QuestionByID(conv, id)
	return g, id, err
}

func (t *Tx) OpenQuestionGroups() ([]QuestionGroup, error) {
	return t.questionGroups(`SELECT key FROM question_groups WHERE state IN ('open','submitting') ORDER BY created_at`)
}

func (t *Tx) ThreadQuestionGroups(thread string) ([]QuestionGroup, error) {
	return t.questionGroups(`SELECT key FROM question_groups WHERE thread_id=? AND state IN ('open','submitting') ORDER BY created_at`, thread)
}

// WaitingQuestionConversations needs no question text, options or answers.
func (t *Tx) WaitingQuestionConversations() ([]int64, error) {
	rows, err := t.rows(`SELECT DISTINCT conv_id FROM question_groups WHERE state IN ('open','submitting') AND async=0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var conv int64
		if err := rows.Scan(&conv); err != nil {
			return nil, err
		}
		out = append(out, conv)
	}
	return out, rows.Err()
}

func (t *Tx) questionGroups(query string, args ...any) ([]QuestionGroup, error) {
	rows, err := t.rows(query, args...)
	if err != nil {
		return nil, err
	}
	var keys []string
	for rows.Next() {
		var k string
		if err = rows.Scan(&k); err != nil {
			break
		}
		keys = append(keys, k)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	var out []QuestionGroup
	for _, k := range keys {
		g, _, err := t.QuestionGroup(k)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

func (t *Tx) RecordQuestionAnswer(id, inbound int64, answer string, skip bool) error {
	_, err := t.exec(`UPDATE questions SET answer=?,skipped=?,inbound_id=? WHERE id=?`, answer, skip, inbound, id)
	return err
}

func (t *Tx) SetQuestionState(key, state string) error {
	_, err := t.exec(`UPDATE question_groups SET state=?,updated_at=? WHERE key=? AND state<>?`, state, t.now, key, state)
	return err
}

func (t *Tx) TransitionQuestion(key, from, to string) (bool, error) {
	r, err := t.exec(`UPDATE question_groups SET state=?,updated_at=? WHERE key=? AND state=?`, to, t.now, key, from)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n > 0, err
}

// QuestionMaintenance writes only when a deadline/retention boundary is due.
func (t *Tx) PruneQuestions(before int64) error {
	_, err := t.exec(`DELETE FROM question_groups WHERE state NOT IN ('open','submitting') AND updated_at<?
AND NOT EXISTS (SELECT 1 FROM questions q JOIN inbound i ON i.id=q.inbound_id
WHERE q.group_key=question_groups.key AND i.state IN ('queued','held'))`, before)
	return err
}

// QueueQuestionReply transforms the admitted command into one ordinary input
// in the same transaction as recording the answer. Inbox deduplication and
// the existing steer reconciliation then own delivery.
func (t *Tx) QueueQuestionReply(conv, inbound int64, text string) error {
	if _, err := t.exec(`UPDATE inbound SET kind='prompt',body=?,state='queued',queued_only=0 WHERE id=? AND conv_id=?`, text, inbound, conv); err != nil {
		return err
	}
	_, err := t.newTurn(conv, inbound, false)
	return err
}

func (t *Tx) IsQuestionReply(inbound int64) (bool, error) {
	var yes bool
	err := t.row(`SELECT EXISTS(SELECT 1 FROM questions q JOIN question_groups g ON g.key=q.group_key WHERE q.inbound_id=? AND g.async=1 AND q.skipped=0)`, inbound).Scan(&yes)
	return yes, err
}

func (t *Tx) QuestionInputThread(inbound int64) (string, error) {
	// Keep expired input recognizable, but give it no dispatch target.
	var thread string
	err := t.row(`SELECT g.thread_id FROM questions q JOIN question_groups g ON g.key=q.group_key WHERE q.inbound_id=? AND g.async=1 AND g.state<>'expired' LIMIT 1`, inbound).Scan(&thread)
	if isNoRows(err) {
		err = nil
	}
	return thread, err
}

func (t *Tx) MarkQuestionSubmitted(id int64) error {
	_, err := t.exec(`UPDATE questions SET submitted=1 WHERE id=? AND submitted=0`, id)
	return err
}

func (t *Tx) SetQuestionDeadline(key string, at int64) (bool, error) {
	r, err := t.exec(`UPDATE question_groups SET deadline=? WHERE key=? AND deadline<>?`, at, key, at)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n > 0, err
}
