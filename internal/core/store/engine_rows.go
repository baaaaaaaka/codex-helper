package store

type ImportJob struct {
	ID     int64  `json:"id"`
	Conv   int64  `json:"conv"`
	Kind   string `json:"kind"`
	Thread string `json:"thread,omitempty"`
	Path   string `json:"path,omitempty"`
	// Rewind reads the rollout again from its start; else from the cursor.
	Rewind bool `json:"rewind,omitempty"`
	// KeepTeams also posts the helper-run turns (up to Until): a new chat
	// has none of them.
	KeepTeams bool `json:"keep_teams,omitempty"`
	// NS is the delivery key namespace of the source ("" = the thread id).
	NS string `json:"ns,omitempty"`
	// Applied once the cursor was moved; Src is the source, Until the
	// file end then: the job is done when the cursor reaches it.
	Applied bool  `json:"applied,omitempty"`
	Src     int64 `json:"src,omitempty"`
	Until   int64 `json:"until,omitempty"`
	Posted  int   `json:"posted,omitempty"`
	// Resumed: the parked chat was woken by the command (reopen text).
	Resumed bool `json:"resumed,omitempty"`
	// Reached: the cursor reached Until before the chat existed; the job
	// finishes when the chat is created.
	Reached bool `json:"reached,omitempty"`
}

func (t *Tx) SetImportJob(n ImportJob) error {
	_, err := t.exec(`INSERT INTO import_jobs(id,conv_id,kind,thread,path,rewind,keep_teams,namespace,applied,source_id,until_offset,posted,resumed,reached) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET conv_id=excluded.conv_id,kind=excluded.kind,thread=excluded.thread,path=excluded.path,rewind=excluded.rewind,keep_teams=excluded.keep_teams,namespace=excluded.namespace,applied=excluded.applied,source_id=excluded.source_id,until_offset=excluded.until_offset,posted=excluded.posted,resumed=excluded.resumed,reached=excluded.reached WHERE conv_id IS NOT excluded.conv_id OR kind IS NOT excluded.kind OR thread IS NOT excluded.thread OR path IS NOT excluded.path OR rewind IS NOT excluded.rewind OR keep_teams IS NOT excluded.keep_teams OR namespace IS NOT excluded.namespace OR applied IS NOT excluded.applied OR source_id IS NOT excluded.source_id OR until_offset IS NOT excluded.until_offset OR posted IS NOT excluded.posted OR resumed IS NOT excluded.resumed OR reached IS NOT excluded.reached`, n.ID, n.Conv, n.Kind, n.Thread, n.Path, n.Rewind, n.KeepTeams, n.NS, n.Applied, n.Src, n.Until, n.Posted, n.Resumed, n.Reached)
	return err
}

func scanImportJob(sc scanner) (ImportJob, error) {
	var n ImportJob
	err := sc.Scan(&n.ID, &n.Conv, &n.Kind, &n.Thread, &n.Path, &n.Rewind, &n.KeepTeams, &n.NS, &n.Applied, &n.Src, &n.Until, &n.Posted, &n.Resumed, &n.Reached)
	return n, err
}

func (t *Tx) ImportJob(key int64) (ImportJob, bool, error) {
	return one(t.row(`SELECT id,conv_id,kind,thread,path,rewind,keep_teams,namespace,applied,source_id,until_offset,posted,resumed,reached FROM import_jobs WHERE id=?`, key), scanImportJob)
}

func (t *Tx) DeleteImportJob(key int64) error {
	_, err := t.exec(`DELETE FROM import_jobs WHERE id=?`, key)
	return err
}

func (t *Tx) ImportJobs() ([]ImportJob, error) {
	return collect(scanImportJob)(t.rows(`SELECT id,conv_id,kind,thread,path,rewind,keep_teams,namespace,applied,source_id,until_offset,posted,resumed,reached FROM import_jobs ORDER BY seq`))
}

type UpdateRequest struct {
	Tag        string `json:"tag"`
	Prerelease bool   `json:"prerelease,omitempty"`
	Conv       int64  `json:"conv,omitempty"`
	Inbound    int64  `json:"inbound,omitempty"`
	Quote      string `json:"quote,omitempty"`
	From       string `json:"from,omitempty"` // version that installed it
}

func (t *Tx) SetUpdateRequest(n UpdateRequest) error {
	_, err := t.exec(`INSERT INTO update_requests(id,tag,prerelease,conv_id,inbound_id,quote,from_version) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET tag=excluded.tag,prerelease=excluded.prerelease,conv_id=excluded.conv_id,inbound_id=excluded.inbound_id,quote=excluded.quote,from_version=excluded.from_version WHERE tag IS NOT excluded.tag OR prerelease IS NOT excluded.prerelease OR conv_id IS NOT excluded.conv_id OR inbound_id IS NOT excluded.inbound_id OR quote IS NOT excluded.quote OR from_version IS NOT excluded.from_version`, 1, n.Tag, n.Prerelease, n.Conv, n.Inbound, n.Quote, n.From)
	return err
}

func scanUpdateRequest(sc scanner) (UpdateRequest, error) {
	var n UpdateRequest
	err := sc.Scan(&n.Tag, &n.Prerelease, &n.Conv, &n.Inbound, &n.Quote, &n.From)
	return n, err
}

func (t *Tx) UpdateRequest() (UpdateRequest, bool, error) {
	return one(t.row(`SELECT tag,prerelease,conv_id,inbound_id,quote,from_version FROM update_requests WHERE id=?`, 1), scanUpdateRequest)
}

func (t *Tx) DeleteUpdateRequest() error {
	_, err := t.exec(`DELETE FROM update_requests WHERE id=?`, 1)
	return err
}

func (t *Tx) SetUpdatePending(n UpdateRequest) error {
	_, err := t.exec(`INSERT INTO update_pending(id,tag,prerelease,conv_id,inbound_id,quote,from_version) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET tag=excluded.tag,prerelease=excluded.prerelease,conv_id=excluded.conv_id,inbound_id=excluded.inbound_id,quote=excluded.quote,from_version=excluded.from_version WHERE tag IS NOT excluded.tag OR prerelease IS NOT excluded.prerelease OR conv_id IS NOT excluded.conv_id OR inbound_id IS NOT excluded.inbound_id OR quote IS NOT excluded.quote OR from_version IS NOT excluded.from_version`, 1, n.Tag, n.Prerelease, n.Conv, n.Inbound, n.Quote, n.From)
	return err
}

func scanUpdatePending(sc scanner) (UpdateRequest, error) {
	var n UpdateRequest
	err := sc.Scan(&n.Tag, &n.Prerelease, &n.Conv, &n.Inbound, &n.Quote, &n.From)
	return n, err
}

func (t *Tx) UpdatePending() (UpdateRequest, bool, error) {
	return one(t.row(`SELECT tag,prerelease,conv_id,inbound_id,quote,from_version FROM update_pending WHERE id=?`, 1), scanUpdatePending)
}

func (t *Tx) DeleteUpdatePending() error {
	_, err := t.exec(`DELETE FROM update_pending WHERE id=?`, 1)
	return err
}

type RestartRequest struct {
	Conv    int64  `json:"conv"`
	Inbound int64  `json:"inbound"`
	Quote   string `json:"quote,omitempty"`
	Force   bool   `json:"force,omitempty"`
}

func (t *Tx) SetRestartRequest(n RestartRequest) error {
	_, err := t.exec(`INSERT INTO restart_requests(id,conv_id,inbound_id,quote,force) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET conv_id=excluded.conv_id,inbound_id=excluded.inbound_id,quote=excluded.quote,force=excluded.force WHERE conv_id IS NOT excluded.conv_id OR inbound_id IS NOT excluded.inbound_id OR quote IS NOT excluded.quote OR force IS NOT excluded.force`, 1, n.Conv, n.Inbound, n.Quote, n.Force)
	return err
}

func scanRestartRequest(sc scanner) (RestartRequest, error) {
	var n RestartRequest
	err := sc.Scan(&n.Conv, &n.Inbound, &n.Quote, &n.Force)
	return n, err
}

func (t *Tx) RestartRequest() (RestartRequest, bool, error) {
	return one(t.row(`SELECT conv_id,inbound_id,quote,force FROM restart_requests WHERE id=?`, 1), scanRestartRequest)
}

func (t *Tx) DeleteRestartRequest() error {
	_, err := t.exec(`DELETE FROM restart_requests WHERE id=?`, 1)
	return err
}

type ModelForkNote struct {
	Src   int64  `json:"src"`
	Model string `json:"model"`
}

func (t *Tx) SetModelForkNote(key int64, n ModelForkNote) error {
	_, err := t.exec(`INSERT INTO model_fork_notes(conv_id,source_conv,model) VALUES(?,?,?) ON CONFLICT(conv_id) DO UPDATE SET source_conv=excluded.source_conv,model=excluded.model WHERE source_conv IS NOT excluded.source_conv OR model IS NOT excluded.model`, key, n.Src, n.Model)
	return err
}

func scanModelForkNote(sc scanner) (ModelForkNote, error) {
	var n ModelForkNote
	err := sc.Scan(&n.Src, &n.Model)
	return n, err
}

func (t *Tx) ModelForkNote(key int64) (ModelForkNote, bool, error) {
	return one(t.row(`SELECT source_conv,model FROM model_fork_notes WHERE conv_id=?`, key), scanModelForkNote)
}

func (t *Tx) DeleteModelForkNote(key int64) error {
	_, err := t.exec(`DELETE FROM model_fork_notes WHERE conv_id=?`, key)
	return err
}

func (t *Tx) SetAnswerCheck(n LegacyFinalCheck) error {
	_, err := t.exec(`INSERT INTO answer_checks(key,conv_id,legacy_id,ext_msg_id,created,body,reply_to,found) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET conv_id=excluded.conv_id,legacy_id=excluded.legacy_id,ext_msg_id=excluded.ext_msg_id,created=excluded.created,body=excluded.body,reply_to=excluded.reply_to,found=excluded.found WHERE conv_id IS NOT excluded.conv_id OR legacy_id IS NOT excluded.legacy_id OR ext_msg_id IS NOT excluded.ext_msg_id OR created IS NOT excluded.created OR body IS NOT excluded.body OR reply_to IS NOT excluded.reply_to OR found IS NOT excluded.found`, n.Key, n.ConvID, n.LegacyID, n.ExtMsgID, n.Created, n.Body, n.ReplyTo, n.Found)
	return err
}

func scanAnswerCheck(sc scanner) (LegacyFinalCheck, error) {
	var n LegacyFinalCheck
	err := sc.Scan(&n.Key, &n.ConvID, &n.LegacyID, &n.ExtMsgID, &n.Created, &n.Body, &n.ReplyTo, &n.Found)
	return n, err
}

func (t *Tx) AnswerCheck(key string) (LegacyFinalCheck, bool, error) {
	return one(t.row(`SELECT key,conv_id,legacy_id,ext_msg_id,created,body,reply_to,found FROM answer_checks WHERE key=?`, key), scanAnswerCheck)
}

func (t *Tx) DeleteAnswerCheck(key string) error {
	_, err := t.exec(`DELETE FROM answer_checks WHERE key=?`, key)
	return err
}

func (t *Tx) AnswerChecks() ([]LegacyFinalCheck, error) {
	return collect(scanAnswerCheck)(t.rows(`SELECT key,conv_id,legacy_id,ext_msg_id,created,body,reply_to,found FROM answer_checks ORDER BY key`))
}

func (t *Tx) SetHistoryCheck(n HistoryCheck) error {
	_, err := t.exec(`INSERT INTO history_checks(path,conv_id,from_offset,from_line,to_offset,end_offset,done,resume,outcome) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET conv_id=excluded.conv_id,from_offset=excluded.from_offset,from_line=excluded.from_line,to_offset=excluded.to_offset,end_offset=excluded.end_offset,done=excluded.done,resume=excluded.resume,outcome=excluded.outcome WHERE conv_id IS NOT excluded.conv_id OR from_offset IS NOT excluded.from_offset OR from_line IS NOT excluded.from_line OR to_offset IS NOT excluded.to_offset OR end_offset IS NOT excluded.end_offset OR done IS NOT excluded.done OR resume IS NOT excluded.resume OR outcome IS NOT excluded.outcome`, n.Path, n.ConvID, n.From, n.FromLine, n.To, n.End, n.Done, n.Resume, n.Outcome)
	return err
}

func scanHistoryCheck(sc scanner) (HistoryCheck, error) {
	var n HistoryCheck
	err := sc.Scan(&n.Path, &n.ConvID, &n.From, &n.FromLine, &n.To, &n.End, &n.Done, &n.Resume, &n.Outcome)
	return n, err
}

func (t *Tx) HistoryCheck(key string) (HistoryCheck, bool, error) {
	return one(t.row(`SELECT path,conv_id,from_offset,from_line,to_offset,end_offset,done,resume,outcome FROM history_checks WHERE path=?`, key), scanHistoryCheck)
}

func (t *Tx) DeleteHistoryCheck(key string) error {
	_, err := t.exec(`DELETE FROM history_checks WHERE path=?`, key)
	return err
}

func (t *Tx) HistoryChecks() ([]HistoryCheck, error) {
	return collect(scanHistoryCheck)(t.rows(`SELECT path,conv_id,from_offset,from_line,to_offset,end_offset,done,resume,outcome FROM history_checks ORDER BY path`))
}

func (t *Tx) ImportJobForConv(conv int64) (ImportJob, bool, error) {
	return one(t.row(`SELECT id,conv_id,kind,thread,path,rewind,keep_teams,namespace,applied,source_id,until_offset,posted,resumed,reached FROM import_jobs WHERE conv_id=?`, conv), scanImportJob)
}
func (s *Store) ImportJobs() (out []ImportJob, err error) {
	err = s.Read(func(tx *Tx) error { out, err = tx.ImportJobs(); return err })
	return
}
func (s *Store) ImportJobForConv(conv int64) (out ImportJob, ok bool, err error) {
	err = s.Read(func(tx *Tx) error { out, ok, err = tx.ImportJobForConv(conv); return err })
	return
}

func (s *Store) HistoryChecks() (out []HistoryCheck, err error) {
	err = s.Read(func(tx *Tx) error { out, err = tx.HistoryChecks(); return err })
	return
}
func (s *Store) AnswerChecks() (out []LegacyFinalCheck, err error) {
	err = s.Read(func(tx *Tx) error { out, err = tx.AnswerChecks(); return err })
	return
}
func (im *Importer) AnswerCheck(n LegacyFinalCheck) error { return im.t.SetAnswerCheck(n) }
func (im *Importer) HistoryCheck(n HistoryCheck) error    { return im.t.SetHistoryCheck(n) }
