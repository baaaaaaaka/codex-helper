package store

// SetTitle records a conversation's chat title and where it came from
// (TitleSource: 'user' after a rename by the owner, 'auto' when the
// helper chose it).
func (t *Tx) SetTitle(convID int64, title, source string) error {
	return t.convUpdate(convID, `title = ?, title_source = ?`, nil, nullStr(title), nullStr(source))
}

// HasInbound reports whether a received message was admitted to the
// conversation.
func (t *Tx) HasInbound(convID int64, extMsgID string) (bool, error) {
	var hit int
	err := t.row(`SELECT EXISTS(SELECT 1 FROM inbound WHERE conv_id = ? AND ext_msg_id = ?)`, convID, extMsgID).Scan(&hit)
	return hit != 0, err
}

// SourcePath is the path of the conversation's first local rollout ("" when
// none).
func (t *Tx) SourcePath(convID int64) (string, error) {
	var path string
	err := t.row(`SELECT COALESCE((SELECT path FROM transcript_sources WHERE conv_id = ? ORDER BY id LIMIT 1), '')`, convID).Scan(&path)
	return path, err
}

// FirstPrompt is the body of the conversation's first prompt from the
// owner ("" when none).
func (t *Tx) FirstPrompt(convID int64) (string, error) {
	var body string
	err := t.row(`SELECT COALESCE((SELECT body FROM inbound WHERE conv_id = ? AND kind = 'prompt' AND sender = 'owner'
ORDER BY ext_time, id LIMIT 1), '')`, convID).Scan(&body)
	return body, err
}
