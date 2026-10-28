package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appgateway"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
)

const (
	proxyListProbeTimeout = time.Second // Includes the server's 750ms SOCKS probe.
	proxyListTimeout      = 5 * time.Second
	proxyListWorkers      = 4
)

// Both daemon and app-gateway listeners expose this local health protocol.
// Unlike the reuse check, a diagnostic probe must retain 503 recovery details
// and must not open CONNECT routes to external destinations.
var proxyListProbe = appgateway.Probe

type proxyListRow struct {
	id, kind, profile string
	pid, http, socks  int
	lastSeen          time.Time
	status, detail    string
}

func runProxyList(cmd *cobra.Command, root *rootOptions) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), proxyListTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	configOverride := ""
	if root != nil {
		configOverride = root.configPath
	}
	paths, err := resolveEffectivePaths(configOverride, "", "")
	if err != nil {
		return err
	}
	cfg, err := config.LoadPathReadOnly(paths.ConfigPath)
	if err != nil {
		return err
	}
	rows, loadErr := proxyListRows(ctx, cfg, proxyNow())
	// A small bounded pool keeps independent local probes from multiplying
	// latency. Each worker owns a different row; output retains registry order.
	jobs := make(chan int, len(rows))
	for i := range rows {
		if rows[i].status == "" {
			jobs <- i
		}
	}
	close(jobs)
	var wg sync.WaitGroup
	for range min(proxyListWorkers, len(jobs)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				inspectProxyListRow(ctx, &rows[i])
			}
		}()
	}
	wg.Wait()

	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "INSTANCE\tTYPE\tPROFILE\tPID\tHTTP\tSOCKS\tSTATUS\tLAST_SEEN\tDETAIL"); err != nil {
		return err
	}
	for _, row := range rows {
		lastSeen := "-"
		if !row.lastSeen.IsZero() {
			lastSeen = row.lastSeen.Format(time.RFC3339)
		}
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\n",
			proxyListCell(row.id), proxyListCell(row.kind), proxyListCell(row.profile), row.pid, row.http, row.socks,
			row.status, lastSeen, proxyListCell(row.detail)); err != nil {
			return err
		}
	}
	return errors.Join(w.Flush(), loadErr, ctx.Err())
}

func proxyListRows(ctx context.Context, cfg config.Config, now time.Time) ([]proxyListRow, error) {
	names := make(map[string]string, len(cfg.Profiles))
	for _, profile := range cfg.Profiles {
		names[profile.ID] = profile.Name
	}
	rows := make([]proxyListRow, 0, len(cfg.Instances)+len(cfg.Profiles))
	for _, inst := range cfg.Instances {
		row := proxyListRow{id: inst.ID, kind: inst.Kind, profile: inst.ProfileID,
			pid: inst.DaemonPID, http: inst.HTTPPort, socks: inst.SocksPort, lastSeen: inst.LastSeenAt}
		if row.kind == "" {
			row.kind = "legacy"
		}
		if name := names[inst.ProfileID]; name != "" {
			row.profile = name
		}
		if inst.Kind == config.InstanceKindModelAdapter {
			if inst.ModelUnified {
				row.profile = "unified models"
			} else if inst.ModelProfileName != "" {
				row.profile = inst.ModelProfileName
			}
		}
		switch {
		case inst.RecoveryBudget.Blocked:
			row.status, row.detail = "blocked", inst.RecoveryBudget.LastReason
		case inst.DaemonPID <= 0 && inst.HTTPPort <= 0 && proxyInstanceStarting(inst, now):
			row.status, row.detail = "starting", "HTTP listener not registered"
		case inst.DaemonPID <= 0:
			row.status = "dead"
		default:
			alive, err := proxyProcessStatus(inst.DaemonPID)
			switch {
			case err != nil:
				row.status, row.detail = "unknown", fmt.Sprintf("inspect pid %d: %v", inst.DaemonPID, err)
			case !alive:
				row.status = "dead"
			case inst.HTTPPort <= 0 && proxyInstanceStarting(inst, now):
				row.status, row.detail = "starting", "HTTP listener not registered"
			case inst.HTTPPort <= 0:
				row.status, row.detail = "unknown", "HTTP listener not registered; PID ownership unverified"
			}
		}
		rows = append(rows, row)
	}
	if len(cfg.Profiles) == 0 {
		return rows, nil
	}
	dir, err := appGatewayRegistryDirFn()
	if err != nil {
		return rows, err
	}
	var loadErr error
	for _, profile := range cfg.Profiles {
		if err := ctx.Err(); err != nil {
			return rows, errors.Join(loadErr, err)
		}
		reg, err := appgateway.LoadReadOnly(dir, profile.ID)
		if errors.Is(err, appgateway.ErrRegistrationNotFound) {
			continue
		}
		if err != nil {
			loadErr = errors.Join(loadErr, fmt.Errorf("read app gateway for profile %q: %w", profile.Name, err))
			continue
		}
		row := proxyListRow{id: reg.ID, kind: "app-gateway", profile: profile.Name,
			pid: reg.OwnerPID, http: reg.HTTPPort, lastSeen: reg.UpdatedAt}
		if row.profile == "" {
			row.profile = profile.ID
		}
		if reg.RecoveryBlocked || reg.State == appgateway.StateBlocked {
			row.status, row.detail = "blocked", reg.LastError
		}
		// Probe the stable listener even when a saved PID is absent or stale.
		// Its identity is authoritative during legacy migration and owner changes.
		rows = append(rows, row)
	}
	return rows, loadErr
}

func inspectProxyListRow(ctx context.Context, row *proxyListRow) {
	if err := ctx.Err(); err != nil {
		row.status, row.detail = "unknown", err.Error()
		return
	}
	health, err := proxyListProbe(ctx, row.http, proxyListProbeTimeout)
	switch {
	case err != nil:
		row.status, row.detail = "unknown", err.Error()
	case health.InstanceID != row.id:
		row.status, row.detail = "stale", fmt.Sprintf("listener belongs to instance %q", health.InstanceID)
	case health.OK:
		row.status = "alive"
	default:
		row.status, row.detail = "unhealthy", health.Error
		if health.Recovery == appgateway.StateBlocked {
			row.status = "blocked"
		}
		if health.Recovery != "" {
			row.detail = strings.TrimSpace(health.Recovery + ": " + health.Error)
		}
	}
}

// Startup placeholders have a bounded grace period, including model adapters
// which predate owner leases. Heartbeat age alone never disproves a live owner.
func proxyInstanceStarting(inst config.Instance, now time.Time) bool {
	if !inst.OwnerLeaseExpiresAt.IsZero() {
		return inst.OwnerLeaseExpiresAt.After(now)
	}
	started := inst.StartedAt
	if inst.LastSeenAt.After(started) {
		started = inst.LastSeenAt
	}
	// A backwards wall-clock adjustment is not evidence that startup expired.
	return !started.IsZero() && now.Sub(started) < manager.DefaultProxyOwnerLease
}

func proxyListCell(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
}
