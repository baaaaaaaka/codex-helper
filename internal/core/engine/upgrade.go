package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Updates (SPEC R-16, R-17): the helper updates itself from its releases,
// and the Codex CLI is upgraded when Codex rejects a request as too old or
// on `codex update now`. Both wait for running turns to finish (new turns
// are held meanwhile) and never interrupt one; both end with a restart on
// the same state (ErrRestart, exit 75), so every Codex app-server and the
// helper itself run the new binaries. The outcome is announced once, by
// the process that runs after the restart.

// Updater updates the helper (cxp) itself.
type Updater interface {
	// Check returns the release to install, or a zero offer when the
	// running helper is current. prerelease includes pre-releases; manual
	// is a `helper update` command (no release-age or priority filter).
	Check(ctx context.Context, prerelease, manual bool) (UpdateOffer, error)
	// Install downloads, validates and puts the release in place of the
	// installed helper. The running process keeps its code until the
	// restart that follows.
	Install(ctx context.Context, offer UpdateOffer) error
}

// UpdateOffer is a helper release to install.
type UpdateOffer struct {
	Tag string // release tag, e.g. "v0.3.1"
}

// CodexUpgrader upgrades the Codex CLI from its install source.
type CodexUpgrader interface {
	Upgrade(ctx context.Context) error
}

// Meta keys of updates.
const (
	metaUpdateNext    = "engine_update_next_check" // unix ms of the next automatic release check
	metaUpdateRequest = "engine_update_request"    // JSON updateRequest: a release waiting for drain and install
	metaUpdatePending = "engine_update_pending"    // JSON updateRequest: installed, announced by the next process
	metaUpdateSkip    = "engine_update_skip"       // tag whose activation failed: automatic checks skip it
	metaCodexUpgrade  = "engine_codex_upgrade"     // JSON codexRequest: a Codex CLI upgrade waiting for drain
	metaRetryPrefix   = "engine_retry:"            // + conv id: inbound id of the request `helper retry last` runs
)

// updateRequest is a helper release to install, and the command that asked
// for it (Conv 0: an automatic update, announced in the control chat).
type updateRequest struct {
	Tag        string `json:"tag"`
	Prerelease bool   `json:"prerelease,omitempty"`
	Conv       int64  `json:"conv,omitempty"`
	Inbound    int64  `json:"inbound,omitempty"`
	Quote      string `json:"quote,omitempty"`
	From       string `json:"from,omitempty"` // version that installed it
}

// codexRequest is a pending Codex CLI upgrade: the command that asked for
// it (Conv 0: none) and the requests Codex rejected as too old.
type codexRequest struct {
	ID      string        `json:"id"`
	Conv    int64         `json:"conv,omitempty"`
	Inbound int64         `json:"inbound,omitempty"`
	Quote   string        `json:"quote,omitempty"`
	Targets []codexTarget `json:"targets,omitempty"`
}

type codexTarget struct {
	Conv    int64 `json:"conv"`
	Inbound int64 `json:"inbound"`
}

// User-visible update texts (the old core's, without paths).
const (
	textCodexTooOld = "⚠️ Codex CLI needs an update\n\n" +
		"Codex rejected this request because the installed CLI is too old for the selected model.\n\n" +
		"I will upgrade Codex after current Codex requests finish. I will not retry this request automatically.\n" +
		"After the upgrade finishes, send `helper retry last` here."
	textCodexTooOldManual = "⚠️ Codex CLI needs an update\n\n" +
		"Codex rejected this request because the installed CLI is too old for the selected model.\n\n" +
		"Automatic Codex upgrade isn't enabled here, so an administrator needs to update Codex — no action needed from you. Once it's updated, send `helper retry last` here."
	textCodexUpdateHelp = "## ⬆️ Codex CLI update\n\n" +
		"This updates the local Codex CLI used by the Teams helper. It does not update cxp/codex-helper.\n" +
		"The helper waits for active Codex work to finish and keeps the detected Codex install source.\n\n" +
		"Command:\n" +
		"- `codex update now` - update Codex CLI to the latest version available from its install source"
	textCodexScheduled = "⬆️ Codex CLI update scheduled\n\n" +
		"I will wait for active Codex requests to finish, then update the local Codex CLI using its detected install source.\n" +
		"I will post the completion or failure here. Existing requests are not retried automatically."
	textCodexTargetDone   = "✅ Codex CLI upgraded.\n\nRetry this request with `helper retry last`."
	textCodexTargetFailed = "⚠️ Codex CLI upgrade failed.\n\nI could not upgrade Codex automatically; an administrator needs to update it — no action needed from you. Once it's updated, send `helper retry last` here."
)

func helperUpdateHelp(prerelease bool) string {
	mode := "stable releases only"
	if prerelease {
		mode = "stable releases plus eligible pre-releases"
	}
	return "## ⬇️ Helper update\n\n" +
		"This updates the local Teams helper from GitHub releases.\n" +
		"It waits until active Codex work is idle before replacing the helper.\n\n" +
		"Commands:\n" +
		"- `helper update now` - update to the latest stable release\n" +
		"- `helper update prerelease` - update to the newest release or pre-release\n" +
		"- `helper update status` - show a pending update\n\n" +
		"Automatic updates currently check: " + mode + "."
}

func helperUpdateScheduled(tag string, prerelease bool) string {
	ch := "stable release"
	if prerelease {
		ch = "release/pre-release"
	}
	return "⬇️ Helper update scheduled\n\nTarget: `" + tag + "`\nChannel: " + ch +
		"\n\nI will install it when active Codex work is idle, then restart the helper."
}

// codexTooOld reports whether a Codex error says the CLI is too old for the
// request (the old core's codexErrorRequiresUpgrade).
func codexTooOld(text string) bool {
	text = strings.ToLower(text)
	if !strings.Contains(text, "codex") {
		return false
	}
	return strings.Contains(text, "requires a newer version") ||
		strings.Contains(text, "newer version of codex") ||
		strings.Contains(text, "upgrade to the latest") ||
		strings.Contains(text, "latest app or cli") ||
		(strings.Contains(text, "model") && strings.Contains(text, "upgrade") && strings.Contains(text, "cli"))
}

// sameVersion compares release versions with or without the "v" prefix.
func sameVersion(a, b string) bool {
	norm := func(s string) string { return strings.TrimPrefix(strings.TrimSpace(s), "v") }
	return norm(a) != "" && norm(a) == norm(b)
}

func metaJSON(tx *store.Tx, key string, v any) (bool, error) {
	raw, ok, err := tx.Meta(key)
	if err != nil || !ok || strings.TrimSpace(raw) == "" {
		return false, err
	}
	if err := json.Unmarshal([]byte(raw), v); err != nil {
		return false, nil // unreadable: treated as absent
	}
	return true, nil
}

func setMetaJSON(tx *store.Tx, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return tx.SetMeta(key, string(raw))
}

// announceUpdate posts, once, the outcome of the helper update the
// previous process installed: completed when this process runs the target
// version, else activation failed (R-16, R-17). The notice and the end of
// the pending record are one transaction.
func (e *Engine) announceUpdate() error {
	return e.update(func(tx *store.Tx) error {
		var p updateRequest
		ok, err := metaJSON(tx, metaUpdatePending, &p)
		if err != nil || !ok {
			return err
		}
		var text string
		if sameVersion(p.Tag, e.cfg.Version) {
			text = "✅ Helper update completed\n\nVersion: `" + p.Tag + "`\n\nThe Teams helper is back online and running the updated code."
		} else {
			text = "⚠️ Helper update activation failed\n\nTarget: `" + p.Tag + "`\n\nRunning helper: `" + e.cfg.Version + "`\n\n" +
				"The helper is still running the previous version. Run `cxp teams service status` on that machine to check the installed and running versions."
			if err := tx.SetMeta(metaUpdateSkip, p.Tag); err != nil {
				return err
			}
		}
		conv, quote := p.Conv, p.Quote
		if conv == 0 || e.conv(conv) == nil {
			conv, quote = e.controlID, ""
		}
		msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("update-done:%s:%s:%d", p.Tag, p.From, p.Inbound), text, quote)
		if err != nil {
			return err
		}
		if _, err := tx.Enqueue(conv, msgs); err != nil {
			return err
		}
		return tx.SetMeta(metaUpdatePending, "")
	})
}

// updateLoop runs automatic release checks and the pending helper and
// Codex updates, one at a time.
func (e *Engine) updateLoop(ctx context.Context) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-e.updateWake:
		}
		next := e.updateStep(ctx)
		if ctx.Err() != nil {
			return
		}
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(next)
	}
}

// updateStep runs what is due and returns the wait until the next check.
func (e *Engine) updateStep(ctx context.Context) time.Duration {
	var (
		creq        codexRequest
		hreq        updateRequest
		haveC, have bool
		next        int64
	)
	err := e.update(func(tx *store.Tx) error {
		var err error
		if haveC, err = metaJSON(tx, metaCodexUpgrade, &creq); err != nil {
			return err
		}
		if have, err = metaJSON(tx, metaUpdateRequest, &hreq); err != nil {
			return err
		}
		v, _, err := tx.Meta(metaUpdateNext)
		next, _ = strconv.ParseInt(v, 10, 64)
		return err
	})
	if err != nil {
		e.logf("update: %v", err)
		return e.t.UpdateCheck
	}
	switch {
	case haveC && e.cfg.CodexUpgrader != nil:
		e.runCodexUpgrade(ctx, creq)
		return 0
	case have && e.cfg.Updater != nil:
		e.runHelperUpdate(ctx, hreq)
		return 0
	case e.cfg.Updater == nil:
		return e.t.UpdateCheck
	}
	now := e.now()
	if wait := time.UnixMilli(next).Sub(now); next > 0 && wait > 0 {
		return min(wait, e.t.UpdateCheck)
	}
	offer, err := e.cfg.Updater.Check(ctx, e.cfg.UpdatePrerelease, false)
	if ctx.Err() != nil {
		return 0
	}
	if err != nil {
		e.logf("update: release check: %v", err)
	}
	var skip string
	err = e.update(func(tx *store.Tx) error {
		if err := tx.SetMeta(metaUpdateNext, strconv.FormatInt(ms(now.Add(e.t.UpdateCheck)), 10)); err != nil {
			return err
		}
		var err error
		if skip, _, err = tx.Meta(metaUpdateSkip); err != nil || offer.Tag == "" || sameVersion(offer.Tag, skip) || sameVersion(offer.Tag, e.cfg.Version) {
			return err
		}
		// Automatic: no scheduled notice; the next process announces
		// the outcome once.
		return setMetaJSON(tx, metaUpdateRequest, updateRequest{Tag: offer.Tag, Prerelease: e.cfg.UpdatePrerelease})
	})
	if err != nil {
		e.logf("update: %v", err)
		return e.t.UpdateCheck
	}
	if offer.Tag != "" && !sameVersion(offer.Tag, skip) && !sameVersion(offer.Tag, e.cfg.Version) {
		return 0
	}
	return e.t.UpdateCheck
}

// runHelperUpdate drains, installs req and restarts. A drain that does not
// finish in Timing.UpdateDrain, or a failed install, gives up until the
// next check (a `helper update` command is told).
func (e *Engine) runHelperUpdate(ctx context.Context, req updateRequest) {
	failed := func(text string) {
		e.undrain()
		err := e.update(func(tx *store.Tx) error {
			if err := tx.SetMeta(metaUpdateRequest, ""); err != nil {
				return err
			}
			if req.Conv == 0 || e.conv(req.Conv) == nil {
				return nil // automatic: logged; retried at the next check
			}
			msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("update-failed:%s:%d", req.Tag, req.Inbound), text, req.Quote)
			if err != nil {
				return err
			}
			_, err = tx.Enqueue(req.Conv, msgs)
			return err
		})
		if err != nil {
			e.logf("update: %v", err)
		}
		e.wakeSender()
	}
	e.logf("update: %s waits for running turns", req.Tag)
	if !e.drain(ctx, e.t.UpdateDrain) {
		if ctx.Err() != nil {
			return
		}
		e.logf("update: %s not installed: turns still running after %s", req.Tag, e.t.UpdateDrain)
		failed(fmt.Sprintf("⚠️ Helper update failed\n\nCodex work was still running after %s, so `%s` was not installed. Send `helper update now` again when it is idle.",
			fmtDuration(e.t.UpdateDrain), req.Tag))
		return
	}
	if err := e.cfg.Updater.Install(ctx, UpdateOffer{Tag: req.Tag}); err != nil {
		if ctx.Err() != nil {
			return
		}
		e.logf("update: install %s: %v", req.Tag, err)
		failed("⚠️ Helper update failed\n\n" + userError(err.Error()))
		return
	}
	req.From = e.cfg.Version
	err := e.update(func(tx *store.Tx) error {
		if err := setMetaJSON(tx, metaUpdatePending, req); err != nil {
			return err
		}
		return tx.SetMeta(metaUpdateRequest, "")
	})
	if err != nil {
		e.logf("update: record %s: %v", req.Tag, err)
		e.undrain()
		return
	}
	e.logf("update: %s installed; restarting", req.Tag)
	// The drain stays on: nothing starts before the restart.
	e.fail(ErrRestart)
}

// requestCodexUpgrade records a Codex CLI upgrade in tx: for the request
// Codex rejected (target), or for a command (conv/inbound/quote).
func (e *Engine) requestCodexUpgrade(tx *store.Tx, target *codexTarget, cmd *cmdJob) error {
	var req codexRequest
	if _, err := metaJSON(tx, metaCodexUpgrade, &req); err != nil {
		return err
	}
	if req.ID == "" {
		req.ID = randHex(4)
	}
	if target != nil {
		req.Targets = append(req.Targets, *target)
	}
	if cmd != nil && req.Conv == 0 {
		req.Conv, req.Inbound, req.Quote = cmd.conv, cmd.inbound, cmd.quote
	}
	return setMetaJSON(tx, metaCodexUpgrade, req)
}

// runCodexUpgrade drains, upgrades the Codex CLI, posts the outcome to the
// control chat and to each rejected request's chat, and restarts so every
// app-server runs the new CLI.
func (e *Engine) runCodexUpgrade(ctx context.Context, req codexRequest) {
	e.logf("codex upgrade: waiting for running turns")
	var cause string
	ok := e.drain(ctx, e.t.CodexDrain)
	if ctx.Err() != nil {
		return
	}
	if !ok {
		cause = fmt.Sprintf("Codex requests were still running after %s, so Codex was not upgraded. Send `codex update now` in the control chat to try again.", fmtDuration(e.t.CodexDrain))
	} else if err := e.cfg.CodexUpgrader.Upgrade(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		e.logf("codex upgrade: %v", err)
		cause = userError(err.Error())
	}
	done := cause == ""
	var control, target string
	switch {
	case !done:
		control, target = "⚠️ Codex CLI upgrade failed\n\n"+cause, textCodexTargetFailed+"\n\nError: "+cause
	case len(req.Targets) > 0:
		control, target = "✅ Codex CLI upgraded.\n\nRetry the failed Work chat request with `helper retry last`.", textCodexTargetDone
	default:
		control = "✅ Codex CLI upgraded.\n\nNew Teams requests will use the updated Codex CLI."
	}
	err := e.update(func(tx *store.Tx) error {
		conv, quote := req.Conv, req.Quote
		if conv == 0 || e.conv(conv) == nil {
			conv, quote = e.controlID, ""
		}
		msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("codex-upgrade:%s:control", req.ID), control, quote)
		if err != nil {
			return err
		}
		if _, err := tx.Enqueue(conv, msgs); err != nil {
			return err
		}
		seen := map[int64]bool{}
		for _, t := range req.Targets {
			if t.Conv == e.controlID || seen[t.Conv] || e.conv(t.Conv) == nil {
				continue
			}
			seen[t.Conv] = true
			if err := e.enqueueNotice(tx, t.Conv, fmt.Sprintf("codex-upgrade:%s:%d", req.ID, t.Conv), target); err != nil {
				return err
			}
		}
		return tx.SetMeta(metaCodexUpgrade, "")
	})
	if err != nil {
		e.logf("codex upgrade: record outcome: %v", err)
	}
	e.wakeSender()
	if !done || err != nil {
		e.undrain()
		return
	}
	// The notices are sent by the next process; the restart replaces
	// every app-server, which still runs the old CLI.
	e.logf("codex upgrade: done; restarting")
	e.fail(ErrRestart)
}

// drain holds new turn starts and waits until no turn runs, at most limit.
// It reports whether the engine is idle; on false the hold is lifted
// (unless the engine is stopping).
func (e *Engine) drain(ctx context.Context, limit time.Duration) bool {
	e.drainFlag.Store(true)
	wake(e.registryWake)
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	tick := time.NewTicker(e.t.Tick)
	defer tick.Stop()
	for {
		if !e.busy() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			if !e.busy() {
				return true
			}
			e.undrain()
			return false
		case <-tick.C:
		}
	}
}

// undrain lets turns start again.
func (e *Engine) undrain() {
	e.drainFlag.Store(false)
	wake(e.registryWake)
	e.wakeAll()
}

// busy reports whether a turn runs or is being started.
func (e *Engine) busy() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.starting > 0 {
		return true
	}
	for _, cs := range e.convs {
		if cs.running != nil {
			return true
		}
	}
	return false
}

// cmdHelperUpdate is `helper update [now|prerelease|status]`.
func (e *Engine) cmdHelperUpdate(ctx context.Context, j cmdJob, arg string) {
	prerelease, status, ok := parseUpdateArg(arg)
	if !ok {
		e.reply(j, helperUpdateHelp(e.cfg.UpdatePrerelease), nil)
		return
	}
	if status {
		e.reply(j, e.updateStatus(), nil)
		return
	}
	if e.cfg.Updater == nil {
		e.reply(j, "⚠️ Helper update is not available in this helper process. Start it with the normal Teams service command, then try again.", nil)
		return
	}
	offer, err := e.cfg.Updater.Check(ctx, prerelease, true)
	if err != nil {
		e.reply(j, "⚠️ Helper update check failed\n\n"+userError(err.Error()), nil)
		return
	}
	if offer.Tag == "" || sameVersion(offer.Tag, e.cfg.Version) {
		if prerelease {
			e.reply(j, "✅ Helper is already on the latest available release or pre-release for this machine.", nil)
		} else {
			e.reply(j, "✅ Helper is already on the latest stable release for this machine.", nil)
		}
		return
	}
	e.reply(j, helperUpdateScheduled(offer.Tag, prerelease), func(tx *store.Tx) error {
		return setMetaJSON(tx, metaUpdateRequest, updateRequest{Tag: offer.Tag, Prerelease: prerelease,
			Conv: j.conv, Inbound: j.inbound, Quote: j.quote})
	})
	wake(e.updateWake)
}

// parseUpdateArg reads the old core's `helper update` arguments.
func parseUpdateArg(arg string) (prerelease, status, ok bool) {
	var toks []string
	for _, t := range strings.Fields(strings.ToLower(arg)) {
		if t != "update" && t != "upgrade" {
			toks = append(toks, t)
		}
	}
	if len(toks) == 0 {
		return false, false, false
	}
	for _, t := range toks {
		switch t {
		case "status":
			if len(toks) != 1 {
				return false, false, false
			}
			status = true
		case "now", "--now", "latest", "release", "stable":
		case "prerelease", "pre-release", "pre", "rc", "--pre", "--prerelease", "--include-prerelease":
			prerelease = true
		default:
			return false, false, false
		}
	}
	return prerelease, status, true
}

// updateStatus describes pending updates (R-17: an installed version that
// is not running yet is shown as pending).
func (e *Engine) updateStatus() string {
	var (
		req, pend updateRequest
		creq      codexRequest
		hr, hp    bool
		hc        bool
	)
	_ = e.update(func(tx *store.Tx) error {
		var err error
		if hr, err = metaJSON(tx, metaUpdateRequest, &req); err != nil {
			return err
		}
		if hp, err = metaJSON(tx, metaUpdatePending, &pend); err != nil {
			return err
		}
		hc, err = metaJSON(tx, metaCodexUpgrade, &creq)
		return err
	})
	lines := []string{"## ⬇️ Helper update status", "", "Running: `" + e.cfg.Version + "`"}
	switch {
	case hp:
		lines = append(lines, "Installed, waiting for the restart: `"+pend.Tag+"`")
	case hr:
		phase := "waiting for its turn"
		if e.draining() {
			phase = "waiting for running Codex work to finish"
		}
		lines = append(lines, "Scheduled: `"+req.Tag+"` ("+phase+")")
	default:
		lines = append(lines, "No helper update is pending.")
	}
	if hc {
		lines = append(lines, "A Codex CLI upgrade is scheduled.")
	}
	if e.cfg.Updater == nil {
		lines = append(lines, "Automatic updates are off for this helper process.")
	}
	return strings.Join(lines, "\n")
}

// cmdCodexUpdate is `codex update now`.
func (e *Engine) cmdCodexUpdate(j cmdJob, arg string) {
	f := strings.Fields(strings.ToLower(arg))
	if len(f) == 0 || (f[0] != "update" && f[0] != "upgrade") || len(f) != 2 || f[1] != "now" {
		e.reply(j, textCodexUpdateHelp, nil)
		return
	}
	if e.cfg.CodexUpgrader == nil {
		e.reply(j, "⚠️ Codex CLI update is not available in this helper process: it runs a Codex CLI chosen with `--codex-path`. Update that Codex installation directly.", nil)
		return
	}
	e.reply(j, textCodexScheduled, func(tx *store.Tx) error { return e.requestCodexUpgrade(tx, nil, &j) })
	wake(e.updateWake)
}

func fmtDuration(d time.Duration) string {
	if d >= time.Minute && d%time.Minute == 0 {
		return plural(int(d/time.Minute), "minute", "minutes")
	}
	return d.Round(time.Second).String()
}
