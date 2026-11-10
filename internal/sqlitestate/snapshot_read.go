package sqlitestate

import (
	"context"
	"database/sql"
	"fmt"
	"io"
)

// OpenSnapshot reads a bounded, immutable published database. The source
// handle permits replacement on Windows and closes before any SQL runs.
// This is only for small atomic snapshots, never mutable business databases.
func OpenSnapshot(path string, maxBytes int64) (*sql.DB, error) {
	if maxBytes < 16 {
		return nil, fmt.Errorf("invalid snapshot size limit")
	}
	info, err := Inspect(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxBytes {
		return nil, fmt.Errorf("invalid or oversized SQLite snapshot: %s", path)
	}
	f, err := openSharedReadFile(path)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	f.Close()
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes || len(data) < 16 || string(data[:16]) != "SQLite format 3\x00" {
		return nil, fmt.Errorf("invalid SQLite snapshot: %s", path)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(context.Background())
	if err == nil {
		err = conn.Raw(func(raw any) error {
			decoder, ok := raw.(interface{ Deserialize([]byte) error })
			if !ok {
				return fmt.Errorf("SQLite driver cannot open native snapshots")
			}
			return decoder.Deserialize(data)
		})
		conn.Close()
	}
	if err == nil {
		_, err = db.Exec(`PRAGMA query_only=ON`)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
