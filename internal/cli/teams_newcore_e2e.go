//go:build e2e

package cli

// The e2e build of cxp (go build -tags e2e) runs `cxp teams run`
// inside the E2E harness (E2E_SUT=cxp): Graph and login traffic goes to the
// fake Graph through the sandbox transport, and the poll timing is the
// scenario's. Everything else is the production wiring. Production builds
// do not contain this file.

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sandbox"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut/override"
)

// e2eBlockedLinePrefix is harness.BlockedLinePrefix: the harness fails a
// scenario on any such stderr line.
const e2eBlockedLinePrefix = "E2E-BLOCKED "

func init() {
	teamsNewCoreTestOverrides = teamsNewCoreE2EOverrides
}

func teamsNewCoreE2EOverrides(stderr io.Writer) (teamsNewCoreOverrides, error) {
	graphURL, instance := os.Getenv("E2E_SUT_GRAPH_URL"), os.Getenv("E2E_SUT_INSTANCE")
	if graphURL == "" || instance == "" {
		return teamsNewCoreOverrides{}, fmt.Errorf("e2e build: E2E_SUT_GRAPH_URL and E2E_SUT_INSTANCE are required")
	}
	var mu sync.Mutex
	tr, err := sandbox.New(graphURL, instance, func(b fakegraph.BlockedRequest) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = fmt.Fprintf(stderr, "%s%s\n", e2eBlockedLinePrefix, b.URL)
	})
	if err != nil {
		return teamsNewCoreOverrides{}, err
	}
	interval := 250 * time.Millisecond
	if v := os.Getenv("E2E_SUT_INTERVAL"); v != "" {
		if interval, err = time.ParseDuration(v); err != nil {
			return teamsNewCoreOverrides{}, fmt.Errorf("E2E_SUT_INTERVAL: %w", err)
		}
	}
	if v := os.Getenv("E2E_SUT_REFUSAL_RECHECK"); v != "" {
		if teamsRefusalRecheck, err = time.ParseDuration(v); err != nil {
			return teamsNewCoreOverrides{}, fmt.Errorf("E2E_SUT_REFUSAL_RECHECK: %w", err)
		}
	}
	timing := engine.FastTiming(interval)
	readRate := 4 * float64(time.Second) / float64(interval)
	if os.Getenv("E2E_SUT_PRODUCTION_TIMING") == "1" {
		timing, readRate = engine.ProductionTiming(), 0
	}
	if err := override.Timing(&timing); err != nil {
		return teamsNewCoreOverrides{}, err
	}
	webhook := os.Getenv("E2E_SUT_WORKFLOW_WEBHOOK_URL")
	return teamsNewCoreOverrides{
		HTTP:     tr.Client(),
		Timing:   &timing,
		ReadRate: readRate,
		Host:     override.Hostname(""),
		// The scenario's Workflow webhook is configured the way
		// `cxp teams migrate` leaves it, so the production reader is used.
		Prepare: func(st *corestore.Store, dir string) error {
			if webhook == "" {
				return nil
			}
			secret := filepath.Join(dir, "workflow-webhook-url")
			if err := os.WriteFile(secret, []byte(webhook+"\n"), 0o600); err != nil {
				return err
			}
			return st.Update(func(tx *corestore.Tx) error {
				if err := tx.SetMeta(migrate.MetaWorkflowEnabled, "true"); err != nil {
					return err
				}
				return tx.SetMeta(migrate.MetaWorkflowSecretFile, secret)
			})
		},
	}, nil
}
