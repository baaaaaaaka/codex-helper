package store

import (
	"context"
	"errors"
	"testing"
)

func TestSQLiteHotPollCanonicalFallbackBudgetIsCumulative(t *testing.T) {
	budget := &sqliteHotPollCanonicalFallbackBudget{maxRows: 3, maxJSONBytes: 10}
	ctx := context.WithValue(context.Background(), sqliteHotPollCanonicalFallbackBudgetKey{}, budget)

	if err := sqliteHotPollChargeCanonicalFallbackBudget(ctx, 1, 4); err != nil {
		t.Fatalf("first aggregate charge: %v", err)
	}
	if err := sqliteHotPollChargeCanonicalFallbackBudget(ctx, 1, 4); err != nil {
		t.Fatalf("second aggregate charge: %v", err)
	}
	if err := sqliteHotPollChargeCanonicalFallbackBudget(ctx, 1, 3); err == nil || !errors.Is(err, errSQLiteHotPollAdmissionIndeterminate) {
		t.Fatalf("third aggregate charge = %v, want an indeterminate-budget error", err)
	}
	if budget.rows != 2 || budget.jsonBytes != 8 {
		t.Fatalf("budget after rejected charge = rows:%d bytes:%d, want rows:2 bytes:8", budget.rows, budget.jsonBytes)
	}
	if err := sqliteHotPollChargeCanonicalFallbackBudget(context.Background(), 100, 100); err != nil {
		t.Fatalf("context without fallback budget unexpectedly failed: %v", err)
	}
}
