package store

type RestartConfirmation struct {
	Conv, Inbound int64
	Quote         string
}

func (t *Tx) SetRestartConfirmation(r RestartConfirmation) error {
	_, err := t.exec(`INSERT INTO restart_confirmation VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET conv_id=excluded.conv_id,inbound_id=excluded.inbound_id,quote=excluded.quote WHERE conv_id IS NOT excluded.conv_id OR inbound_id IS NOT excluded.inbound_id OR quote IS NOT excluded.quote`, r.Conv, r.Inbound, r.Quote)
	return err
}
func (t *Tx) DeleteRestartConfirmation() error {
	_, err := t.exec(`DELETE FROM restart_confirmation WHERE id=1`)
	return err
}
func (s *Store) RestartConfirmation() (r RestartConfirmation, ok bool, err error) {
	err = s.Read(func(t *Tx) error {
		r, ok, err = one(t.row(`SELECT conv_id,inbound_id,quote FROM restart_confirmation WHERE id=1`), func(sc scanner) (r RestartConfirmation, err error) {
			err = sc.Scan(&r.Conv, &r.Inbound, &r.Quote)
			return
		})
		return err
	})
	return
}

type FreezeNotice struct {
	ParkedAt int64
	Done     bool
}

func (t *Tx) SetFreezeNotice(conv int64, r FreezeNotice) error {
	_, err := t.exec(`INSERT INTO freeze_notices VALUES(?,?,?) ON CONFLICT(conv_id) DO UPDATE SET parked_at=excluded.parked_at,done=excluded.done WHERE parked_at IS NOT excluded.parked_at OR done IS NOT excluded.done`, conv, r.ParkedAt, r.Done)
	return err
}
func (t *Tx) CompleteFreezeNotice(conv, at int64) error {
	_, err := t.exec(`UPDATE freeze_notices SET done=1 WHERE conv_id=? AND parked_at=? AND done=0`, conv, at)
	return err
}
func (s *Store) FreezeNotice(conv int64) (r FreezeNotice, ok bool, err error) {
	err = s.Read(func(t *Tx) error {
		r, ok, err = one(t.row(`SELECT parked_at,done FROM freeze_notices WHERE conv_id=?`, conv), func(sc scanner) (r FreezeNotice, err error) { err = sc.Scan(&r.ParkedAt, &r.Done); return })
		return err
	})
	return
}
