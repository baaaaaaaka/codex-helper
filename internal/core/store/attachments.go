package store

// FileReference contains adapter reference fields, not a serialized Go object.
// Ref is one scalar handle; uploaded reference fields have their own columns.
type FileReference struct {
	Kind         int    `json:"k"`
	Name         string `json:"n"`
	Size         int64  `json:"s,omitempty"`
	ContentType  string `json:"t,omitempty"`
	Ref          string `json:"r"`
	AttachmentID string `json:"attachment_id,omitempty"`
	URL          string `json:"url,omitempty"`
}

type QuoteReference struct {
	N    int    `json:"n"`
	ID   string `json:"id,omitempty"`
	From string `json:"from,omitempty"`
	Time string `json:"time,omitempty"`
	Text string `json:"text,omitempty"`
	Fwd  bool   `json:"fwd,omitempty"`
}

type QuoteText struct {
	From     string `json:"from,omitempty"`
	Time     string `json:"time,omitempty"`
	Text     string `json:"text,omitempty"`
	Fetched  bool   `json:"fetched,omitempty"`
	FullPath string `json:"full,omitempty"`
}

// SetResolvedQuote shares the caller's transaction with the ready outcome.
func (t *Tx) SetResolvedQuote(inbound int64, idx int, n QuoteText) error {
	_, err := t.exec(`UPDATE inbound_files SET quote_resolved=1,resolved_from=?,resolved_time=?,resolved_text=?,resolved_fetched=?,resolved_full_path=? WHERE inbound_id=? AND idx=?`, n.From, n.Time, n.Text, n.Fetched, n.FullPath, inbound, idx)
	return err
}

func (t *Tx) insertInboundFile(inbound int64, idx int, f InboundFile) error {
	var q QuoteReference
	if f.Quote != nil {
		q = *f.Quote
	}
	_, err := t.exec(`INSERT INTO inbound_files(inbound_id,idx,name,ref,local_path,kind,state,error,ref_kind,ref_name,ref_size,ref_content_type,ref_attachment_id,ref_url,quote_n,quote_id,quote_from,quote_time,quote_text,quote_forwarded) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, inbound, idx, f.Name, f.Ref.Ref, nullStr(f.LocalPath), f.Kind, f.State, nullStr(f.Error), f.Ref.Kind, f.Ref.Name, f.Ref.Size, f.Ref.ContentType, f.Ref.AttachmentID, f.Ref.URL, q.N, q.ID, q.From, q.Time, q.Text, q.Fwd)
	if err != nil {
		return err
	}
	if f.ResolvedQuote != nil {
		return t.SetResolvedQuote(inbound, idx, *f.ResolvedQuote)
	}
	return nil
}

func (t *Tx) insertOutboxFile(id int64, f OutFile) error {
	_, err := t.exec(`INSERT INTO outbox_files(outbox_id,local_path,size,upload_url,upload_expires_at,uploaded_bytes,ref,ref_kind,ref_name,ref_size,ref_content_type,ref_attachment_id,ref_url) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, f.LocalPath, f.Size, nullStr(f.UploadURL), nullInt(f.UploadExpiresAt), f.UploadedBytes, nullStr(f.Ref.Ref), f.Ref.Kind, f.Ref.Name, f.Ref.Size, f.Ref.ContentType, f.Ref.AttachmentID, f.Ref.URL)
	return err
}

func (t *Tx) SetUploadedFile(id int64, size int64, ref FileReference) error {
	_, err := t.exec(`UPDATE outbox_files SET upload_url=NULL,upload_expires_at=NULL,uploaded_bytes=?,ref=?,ref_kind=?,ref_name=?,ref_size=?,ref_content_type=?,ref_attachment_id=?,ref_url=? WHERE outbox_id=?`, size, ref.Ref, ref.Kind, ref.Name, ref.Size, ref.ContentType, ref.AttachmentID, ref.URL, id)
	return err
}
