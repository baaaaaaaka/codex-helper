package store

import (
	"context"
	"strings"
	"testing"
)

// VacuumStep frees at most the pages it is asked for.
func TestVacuumStepIsBounded(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()
	freelist := func() int {
		var n int
		if err := s.w.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, q := range []string{
		`CREATE TABLE scratch(b BLOB)`,
		`INSERT INTO scratch SELECT zeroblob(1024) FROM (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 2000) SELECT x FROM c)`,
		`DROP TABLE scratch`,
	} {
		if _, err := s.w.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", strings.Fields(q)[0], err)
		}
	}
	before := freelist()
	if before < 64 {
		t.Fatalf("free list %d pages, want a large one", before)
	}
	mustT(t, s.VacuumStep(16))
	if after := freelist(); after != before-16 {
		t.Fatalf("free list %d -> %d, want %d", before, after, before-16)
	}
}
