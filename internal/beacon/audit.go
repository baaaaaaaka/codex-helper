package beacon

import (
	"crypto/sha256"
	"fmt"
	"time"
)

func AppendAudit(st *State, action string, target string, now time.Time) (AuditRecord, error) {
	if st == nil {
		return AuditRecord{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	if now.IsZero() {
		now = time.Now()
	}
	prev := st.AuditHead.Hash
	seq := st.AuditHead.Seq + 1
	hash := auditHash(seq, prev, action, target)
	rec := AuditRecord{
		Seq:      seq,
		PrevHash: prev,
		Action:   action,
		Target:   target,
		Secret:   "[redacted]",
		Hash:     hash,
		At:       now,
	}
	st.Audit = append(st.Audit, rec)
	st.AuditHead = AuditHead{Seq: seq, Hash: hash}
	return rec, nil
}

func ValidateAudit(st State) error {
	prev := ""
	for i, rec := range st.Audit {
		seq := i + 1
		if rec.Seq != seq {
			return fmt.Errorf("audit sequence mismatch at %d", seq)
		}
		if rec.PrevHash != prev {
			return fmt.Errorf("audit prev hash mismatch at %d", seq)
		}
		if rec.Secret != "[redacted]" {
			return fmt.Errorf("audit secret was not redacted at %d", seq)
		}
		if got := auditHash(rec.Seq, rec.PrevHash, rec.Action, rec.Target); got != rec.Hash {
			return fmt.Errorf("audit hash mismatch at %d", seq)
		}
		prev = rec.Hash
	}
	if st.AuditHead.Seq != len(st.Audit) || st.AuditHead.Hash != prev {
		return fmt.Errorf("audit head mismatch")
	}
	return nil
}

func auditHash(seq int, prevHash string, action string, target string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s\x00%s", seq, action, target, prevHash))))
}
