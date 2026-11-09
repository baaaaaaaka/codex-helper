package asr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ManagedConfig configures the managed llama.cpp backend. Zero values pick
// the defaults noted on each field.
type ManagedConfig struct {
	CacheDir string    // default <user cache dir>/codex-helper/asr
	Device   string    // "cpu" (default, as before) or "gpu"/"auto"/"vulkan"/"metal": GPU when one loads
	Manifest *Manifest // default DefaultManifest(runtime.GOOS, runtime.GOARCH)
	// MirrorBaseURLs hold byte-identical copies of every asset in Manifest.
	MirrorBaseURLs []string
	Logf           func(string, ...any) // stage/progress diagnostics; never logs signed URLs

	// Local overrides for offline hosts; each one skips its download.
	ServerPath, ModelPath, MMProjPath, FFmpegPath string

	SetupTimeout     time.Duration // whole first-run setup; default 30 min
	SetupIdleTimeout time.Duration // setup gives up after this long without progress; default 10 min
	IntegrityBackoff time.Duration // first retry delay after a checksum failure, doubling up to 1 h; default 1 min
	RequestTimeout   time.Duration // one transcription; default 15 min
	LoadTimeout      time.Duration // llama-server start until healthy; default 5 min
	IdleUnload       time.Duration // stop llama-server after this idle time; default 30 min
	StallTimeout     time.Duration // abort a download that receives nothing; default 60 s
	Speed            float64       // audio speed-up before recognition; default 1.25
	SegmentSeconds   int           // split long audio (after speed-up); default 60
	HTTPClient       *http.Client
}

// Server sizing that keeps memory at about 1.4 GB (Q8_0 0.6B model +
// projector + a 2048-token context, one slot) and CPU at MaxThreads.
const (
	contextTokens = 2048
	maxTokens     = 1024
	stickyFor     = time.Hour // native-load / unsupported failures are not retried sooner
	transcribeMsg = "Transcribe the audio exactly. Do not translate. Preserve the spoken language. Preserve English words, acronyms, code identifiers, file names, and paths as spoken."
)

// Managed is the managed llama.cpp backend. It is safe for concurrent use;
// transcriptions run one at a time.
type Managed struct {
	cfg      ManagedConfig
	in       *installer
	closeCtx context.Context
	closeFn  context.CancelFunc

	mu             sync.Mutex
	setup          *setupRun
	mf             Manifest // resolved by the first setup
	ready          *paths
	failErr        error     // sticky failure, returned until failUntil
	failUntil      time.Time //
	integrityFails int       // consecutive checksum failures (exponential backoff)
	setupFailures  map[string]error
	lastProgress   time.Time
	closed         bool

	runMu   sync.Mutex // serializes transcriptions and server lifecycle
	srv     *llamaServer
	lastUse time.Time
	idle    *time.Timer
}

type paths struct{ server, model, mmproj, ffmpeg string }

type setupRun struct {
	done      chan struct{}
	preparing chan struct{} // closed when the run has to download something
	bytes     int64
	notified  map[string]bool // voice files already told about this run (by path)
	pending   map[string]bool // voice files that went back to the caller as KindPreparing
	lastDL    error           // last download failure, retried while setup makes progress
	paths     *paths
	err       error
}

var (
	errSetupStalled = errors.New("setup stopped making progress")
	errSetupTotal   = errors.New("setup took too long")
)

// NewManaged returns the managed backend. Nothing is downloaded until the
// first Transcribe.
func NewManaged(cfg ManagedConfig) *Managed {
	if cfg.CacheDir == "" {
		base, err := os.UserCacheDir()
		if err != nil {
			base = os.TempDir()
		}
		cfg.CacheDir = filepath.Join(base, "codex-helper", "asr")
	}
	if abs, err := filepath.Abs(cfg.CacheDir); err == nil {
		cfg.CacheDir = abs
	}
	setDefault(&cfg.SetupTimeout, 30*time.Minute)
	setDefault(&cfg.SetupIdleTimeout, 10*time.Minute)
	setDefault(&cfg.IntegrityBackoff, time.Minute)
	setDefault(&cfg.RequestTimeout, 15*time.Minute)
	setDefault(&cfg.LoadTimeout, 5*time.Minute)
	setDefault(&cfg.IdleUnload, 30*time.Minute)
	setDefault(&cfg.StallTimeout, time.Minute)
	if cfg.Speed <= 0 {
		cfg.Speed = 1.25
	}
	if cfg.SegmentSeconds <= 0 {
		cfg.SegmentSeconds = 60
	}
	m := &Managed{cfg: cfg, in: newInstaller(cfg.CacheDir, cfg.HTTPClient),
		setupFailures: map[string]error{}}
	m.in.stall = cfg.StallTimeout
	m.in.logf = cfg.Logf
	reapOrphans(cfg.CacheDir)
	m.in.progress = m.touch
	m.closeCtx, m.closeFn = context.WithCancel(context.Background())
	return m
}

// New picks the backend: the external command when one is configured,
// otherwise the managed runtime.
func New(command string, args []string, cfg ManagedConfig) Transcriber {
	if c := NewCommand(command, args); c != nil {
		return c
	}
	return NewManaged(cfg)
}

func setDefault(d *time.Duration, v time.Duration) {
	if *d <= 0 {
		*d = v
	}
}

// Close stops setup and the resident server.
func (m *Managed) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.closeFn()
	m.runMu.Lock()
	defer m.runMu.Unlock()
	m.stopServerLocked()
	return nil
}

func (m *Managed) Transcribe(ctx context.Context, req Request) (Result, error) {
	if info, err := os.Stat(req.Path); err != nil || info.IsDir() {
		return Result{}, newError(KindMedia, "open voice file", err, req.Path)
	}
	for attempt := 0; ; attempt++ {
		p, err := m.waitReady(ctx, req)
		if err != nil {
			return Result{}, err
		}
		res, repaired, err := m.transcribeOnce(ctx, p, req)
		if !repaired || attempt > 0 {
			return res, err
		}
		// A model file failed to load and was quarantined: set up again
		// (one download) and retry this voice message once.
	}
}

func (m *Managed) transcribeOnce(ctx context.Context, p *paths, req Request) (res Result, repaired bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.RequestTimeout)
	defer cancel()
	m.runMu.Lock()
	defer m.runMu.Unlock()
	if m.closeCtx.Err() != nil {
		return Result{}, false, newError(KindCanceled, "transcribe", m.closeCtx.Err(), "")
	}
	res, err = m.transcribeLocked(ctx, p, req)
	m.lastUse = time.Now()
	m.armIdleLocked()
	if err != nil && ctx.Err() != nil {
		err = ctxError(ctx, "transcribe")
	}
	if KindOf(err) == KindNativeLoad {
		m.stick(err)
	}
	return res, err != nil && m.quarantineBadModel(err), err
}

// waitReady returns the installed paths, starting first-run setup if needed.
// Setup runs detached from ctx, so a caller whose ctx ends gets the
// retryable KindPreparing and keeps its voice message pending while setup
// makes progress. Setup ends only on a hard error, after SetupIdleTimeout
// without progress, or after SetupTimeout in total; voice files that were
// pending on it then get that error once, on their next call. The request
// is told once via EventPreparing when it must wait for a download.
func (m *Managed) waitReady(ctx context.Context, req Request) (*paths, error) {
	m.mu.Lock()
	switch {
	case m.closed:
		m.mu.Unlock()
		return nil, newError(KindCanceled, "prepare voice recognition", errors.New("transcriber closed"), "")
	case m.ready != nil:
		p := m.ready
		m.mu.Unlock()
		return p, nil
	}
	if err, ok := m.setupFailures[req.Path]; ok {
		delete(m.setupFailures, req.Path)
		m.mu.Unlock()
		return nil, err
	}
	if m.failErr != nil && time.Now().Before(m.failUntil) {
		err := m.failErr
		m.mu.Unlock()
		return nil, err
	}
	run := m.setup
	if run == nil {
		run = &setupRun{done: make(chan struct{}), preparing: make(chan struct{}), notified: map[string]bool{}, pending: map[string]bool{}}
		m.setup = run
		go m.runSetup(run)
	}
	m.mu.Unlock()

	preparing := run.preparing
	for {
		select {
		case <-run.done:
			if run.err != nil { // delivered now; do not deliver it again
				m.mu.Lock()
				delete(m.setupFailures, req.Path)
				m.mu.Unlock()
			}
			return run.paths, run.err
		case <-preparing:
			preparing = nil
			m.mu.Lock()
			first := !run.notified[req.Path]
			run.notified[req.Path] = true
			m.mu.Unlock()
			if first { // one notice per voice file, however often the caller retries
				req.notify(Event{Kind: EventPreparing, CacheDir: m.cfg.CacheDir, DownloadBytes: run.bytes})
			}
		case <-ctx.Done():
			m.mu.Lock()
			run.pending[req.Path] = true
			m.mu.Unlock()
			return nil, newError(KindPreparing, "prepare voice recognition", ctx.Err(), "")
		}
	}
}

func (m *Managed) runSetup(run *setupRun) {
	total, cancelTotal := context.WithTimeoutCause(m.closeCtx, m.cfg.SetupTimeout, errSetupTotal)
	defer cancelTotal()
	ctx, stop := context.WithCancelCause(total)
	defer stop(nil)
	m.touch()
	go m.watchProgress(ctx, stop)
	p, err := m.install(ctx, run)
	if err != nil && ctx.Err() != nil {
		err = m.setupStopped(ctx, run)
	}
	m.mu.Lock()
	run.paths, run.err = p, err
	if err == nil {
		m.ready, m.integrityFails, m.setupFailures = p, 0, map[string]error{}
	} else {
		m.recordFailureLocked(err)
		for path := range run.pending {
			m.setupFailures[path] = err
		}
	}
	m.setup = nil
	m.mu.Unlock()
	close(run.done)
}

// touch records setup progress: bytes arrived or an install step finished.
// A mirror that keeps failing after some bytes is still bounded by
// SetupTimeout.
func (m *Managed) touch() {
	m.mu.Lock()
	m.lastProgress = time.Now()
	m.mu.Unlock()
}

func (m *Managed) watchProgress(ctx context.Context, stop context.CancelCauseFunc) {
	tick := time.NewTicker(min(m.cfg.SetupIdleTimeout/4, time.Second))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			m.mu.Lock()
			idle := time.Since(m.lastProgress)
			m.mu.Unlock()
			if idle >= m.cfg.SetupIdleTimeout {
				stop(errSetupStalled)
				return
			}
		}
	}
}

// setupStopped explains why setup's context ended.
func (m *Managed) setupStopped(ctx context.Context, run *setupRun) error {
	const op = "prepare voice recognition"
	switch cause := context.Cause(ctx); {
	case errors.Is(cause, errSetupStalled) && run.lastDL != nil:
		return newError(KindDownload, op, run.lastDL, "no download progress for "+m.cfg.SetupIdleTimeout.String())
	case errors.Is(cause, errSetupStalled):
		return newError(KindTimeout, op, cause, "setup made no progress for "+m.cfg.SetupIdleTimeout.String())
	case errors.Is(cause, errSetupTotal):
		return newError(KindTimeout, op, cause, "setup did not finish within "+m.cfg.SetupTimeout.String())
	}
	return ctxError(ctx, op)
}

// recordFailureLocked makes failures that retrying soon cannot fix sticky:
// checksum failures back off exponentially from IntegrityBackoff up to an
// hour, native-load and unsupported-platform failures wait an hour. The
// error carries the retry time for the user message.
func (m *Managed) recordFailureLocked(err error) {
	var e *Error
	if !errors.As(err, &e) {
		return
	}
	var wait time.Duration
	switch e.Kind {
	case KindIntegrity:
		m.integrityFails++
		wait = backoff(m.cfg.IntegrityBackoff, m.integrityFails)
	case KindNativeLoad, KindUnsupported:
		wait = stickyFor
	default:
		return
	}
	m.failErr, m.failUntil = err, time.Now().Add(wait)
	e.RetryAt = m.failUntil
}

// backoff is base doubled per earlier failure, capped at an hour.
func backoff(base time.Duration, failures int) time.Duration {
	d := base
	for i := 1; i < failures && d < time.Hour; i++ {
		d *= 2
	}
	return min(d, time.Hour)
}

func (m *Managed) stick(err error) {
	m.mu.Lock()
	m.recordFailureLocked(err)
	m.ready = nil
	m.mu.Unlock()
}

// install makes every needed file present and verified.
func (m *Managed) install(ctx context.Context, run *setupRun) (*paths, error) {
	c := m.cfg
	p := &paths{server: c.ServerPath, model: c.ModelPath, mmproj: c.MMProjPath, ffmpeg: c.FFmpegPath}
	mf := m.mf
	if c.Manifest != nil {
		mf = *c.Manifest
	} else if p.server == "" || p.model == "" || p.mmproj == "" || p.ffmpeg == "" {
		var err error
		if mf, err = DefaultManifest(runtime.GOOS, runtime.GOARCH); err != nil {
			return nil, err
		}
	}
	var mirrorErr error
	mf, mirrorErr = withMirrorBases(mf, c.MirrorBaseURLs)
	if mirrorErr != nil {
		return nil, mirrorErr
	}
	m.mu.Lock()
	m.mf = mf
	m.mu.Unlock()
	if err := os.MkdirAll(c.CacheDir, 0o700); err != nil {
		return nil, newError(KindCache, "create cache dir", err, c.CacheDir)
	}
	m.in.cleanTemp(time.Now())

	type job struct {
		asset    Asset
		target   *string
		file     string
		validate func(string) error
	}
	// Small binaries first: a host that cannot run them fails before the
	// large model download, and the model's download is the last step.
	var jobs []job
	if p.server == "" {
		jobs = append(jobs, job{mf.Runtime, &p.server, exeName("llama-server"), m.validateBinary("llama-server", "--version")})
	}
	if p.ffmpeg == "" && mf.FFmpeg.URL != "" {
		jobs = append(jobs, job{mf.FFmpeg, &p.ffmpeg, exeName("ffmpeg"), m.validateBinary("ffmpeg", "-version")})
	}
	if p.mmproj == "" {
		jobs = append(jobs, job{mf.MMProj, &p.mmproj, mf.MMProj.Name, nil})
	}
	if p.model == "" {
		jobs = append(jobs, job{mf.Model, &p.model, mf.Model.Name, nil})
	}

	var missing int64
	for _, j := range jobs {
		if !m.in.installed(j.asset) {
			missing += j.asset.Size
		}
	}
	if missing > 0 {
		need := uint64(missing)*2 + 512<<20 // archive + extracted copy, plus headroom
		if free, err := freeBytes(c.CacheDir); err == nil && free < need {
			return nil, newError(KindDiskSpace, "check free space", nil,
				fmt.Sprintf("%s has %d MiB free, setup needs %d MiB", c.CacheDir, free>>20, need>>20))
		}
		run.bytes = missing
		close(run.preparing)
	}
	for _, j := range jobs {
		for attempt := 1; ; attempt++ {
			dir, err := m.in.ensure(ctx, j.asset, j.validate)
			if err == nil {
				*j.target = filepath.Join(dir, j.file)
				break
			}
			if KindOf(err) != KindDownload || errors.Is(err, errPermanent) || ctx.Err() != nil {
				return nil, err
			}
			run.lastDL = err // transient: keep trying while setup makes progress
			select {
			case <-ctx.Done():
				return nil, err
			case <-time.After(min(time.Duration(attempt)*5*m.in.backoff, time.Minute)):
			}
		}
	}
	if p.ffmpeg == "" {
		path, err := systemFFmpeg(ctx)
		if err != nil {
			return nil, err
		}
		p.ffmpeg = path
	}
	for _, f := range []string{p.server, p.model, p.mmproj, p.ffmpeg} {
		if _, err := os.Stat(f); err != nil {
			return nil, newError(KindNotConfigured, "check configured path", err, "")
		}
	}
	return p, nil
}

// validateBinary runs "<dir>/<name> <arg>" before an install is published.
// The 60 s bound covers first-run antivirus scans (Windows) and slow
// first exec (macOS), which earlier needed 30 s and 60 s.
func (m *Managed) validateBinary(name, arg string) func(string) error {
	return func(dir string) error {
		ctx, cancel := context.WithTimeout(m.closeCtx, time.Minute)
		defer cancel()
		cmd := exec.Command(filepath.Join(dir, exeName(name)), arg)
		cmd.Env = childEnv()
		_, stderr, err := run(ctx, cmd)
		if err != nil {
			if ctx.Err() != nil {
				return newError(KindTimeout, "check "+name, ctx.Err(), "")
			}
			return classifyRun("check "+name, err, stderr, KindCache)
		}
		return nil
	}
}

// systemFFmpeg finds an ffmpeg on PATH that has the filters we use.
func systemFFmpeg(ctx context.Context) (string, error) {
	path, err := exec.LookPath("ffmpeg")
	if err != nil {
		return "", newError(KindUnsupported, "find ffmpeg", err, "no managed ffmpeg for this platform; install ffmpeg on PATH")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	cmd := exec.Command(path, "-hide_banner", "-filters")
	cmd.Env = childEnv()
	out, stderr, err := run(ctx, cmd)
	if err != nil {
		return "", classifyRun("check system ffmpeg", err, stderr, KindUnsupported)
	}
	if !strings.Contains(out, "atempo") || !strings.Contains(out, "aresample") {
		return "", newError(KindUnsupported, "check system ffmpeg", nil, path+" lacks the atempo/aresample filters")
	}
	return path, nil
}

func (m *Managed) transcribeLocked(ctx context.Context, p *paths, req Request) (Result, error) {
	if err := os.MkdirAll(m.in.tmp(), 0o700); err != nil {
		return Result{}, newError(KindCache, "create temp dir", err, "")
	}
	dir, err := os.MkdirTemp(m.in.tmp(), "req-")
	if err != nil {
		return Result{}, newError(KindCache, "create temp dir", err, "")
	}
	defer os.RemoveAll(dir)
	segments, err := m.decode(ctx, p.ffmpeg, req.Path, dir)
	if err != nil {
		return Result{}, err
	}
	var texts []string
	language := ""
	for _, seg := range segments {
		wav, err := os.ReadFile(seg)
		if err != nil {
			return Result{}, newError(KindCache, "read decoded audio", err, "")
		}
		text, lang, err := m.recognize(ctx, p, wav)
		if err != nil {
			return Result{}, err
		}
		if text != "" {
			texts = append(texts, text)
		}
		language = firstNonEmpty(language, lang)
	}
	res := Result{Text: strings.Join(texts, " "), Language: firstNonEmpty(language, req.language()),
		Model: "Qwen/Qwen3-ASR-0.6B", Backend: "llama.cpp/" + m.device()}
	if res.Text == "" {
		return Result{}, newError(KindNoSpeech, "transcribe", nil, "")
	}
	return res, nil
}

// recognize sends one segment, restarting a crashed server once.
func (m *Managed) recognize(ctx context.Context, p *paths, wav []byte) (string, string, error) {
	for attempt := 0; ; attempt++ {
		if err := m.startServerLocked(ctx, p); err != nil {
			return "", "", err
		}
		text, lang, err := m.srv.transcribe(ctx, wav)
		if err == nil || ctx.Err() != nil {
			return text, lang, err
		}
		if !m.srv.exitedWithin(2*time.Second) || attempt > 0 {
			stderr := m.srv.stderr.String()
			m.stopServerLocked()
			return "", "", newError(KindBackend, "transcribe with llama-server", err, tailLines(stderr, 8))
		}
		m.stopServerLocked() // crashed mid-request: start a fresh one and retry once
	}
}

// decode converts any Teams audio/video into 16 kHz mono WAV segments,
// sped up by cfg.Speed (owner request: shorter audio, lower latency).
func (m *Managed) decode(ctx context.Context, ffmpeg, input, dir string) ([]string, error) {
	filter := "aresample=16000"
	if m.cfg.Speed != 1 {
		filter = "atempo=" + strconv.FormatFloat(m.cfg.Speed, 'f', -1, 64) + "," + filter
	}
	cmd := exec.Command(ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-i", input, "-vn", "-map", "0:a:0", "-af", filter, "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le",
		"-f", "segment", "-segment_time", strconv.Itoa(m.cfg.SegmentSeconds), "-reset_timestamps", "1",
		filepath.Join(dir, "seg-%03d.wav"))
	cmd.Env = childEnv()
	_, stderr, err := run(ctx, cmd)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctxError(ctx, "decode audio")
		}
		e := classifyRun("decode audio with ffmpeg", err, stderr, KindCache)
		if e.Kind == KindBackend {
			e.Kind = KindMedia
		}
		return nil, e
	}
	segments, _ := filepath.Glob(filepath.Join(dir, "seg-*.wav"))
	var out []string
	for _, s := range segments { // skip slivers shorter than 0.1 s
		if info, err := os.Stat(s); err == nil && info.Size() > 44+3200 {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, newError(KindMedia, "decode audio", nil, "the recording contains no audible audio")
	}
	return out, nil
}

func (m *Managed) device() string {
	if d := strings.ToLower(strings.TrimSpace(m.cfg.Device)); d != "" && d != "cpu" && d != "none" {
		return "gpu"
	}
	return "cpu"
}

func (m *Managed) startServerLocked(ctx context.Context, p *paths) error {
	if m.srv != nil && m.srv.alive() {
		return nil
	}
	m.stopServerLocked()
	reapOrphans(m.cfg.CacheDir)
	srv, err := startLlamaServer(ctx, p, m.device(), m.cfg.CacheDir, m.in.tmp(), m.cfg.LoadTimeout)
	if err != nil {
		return err
	}
	m.srv = srv
	return nil
}

// quarantineBadModel moves aside a managed model file that llama-server
// could not parse, so the next setup downloads it again. Local overrides
// are never touched.
func (m *Managed) quarantineBadModel(err error) bool {
	var e *Error
	if !errors.As(err, &e) || e.Op != opLoadModel || e.Kind != KindIntegrity {
		return false
	}
	m.mu.Lock()
	mf := m.mf
	m.mu.Unlock()
	switch {
	case strings.HasPrefix(e.Detail, "multimodal") && m.cfg.MMProjPath == "":
		m.in.forget(mf.MMProj, true)
	case !strings.HasPrefix(e.Detail, "multimodal") && m.cfg.ModelPath == "":
		m.in.forget(mf.Model, true)
	default:
		return false
	}
	m.mu.Lock()
	m.ready = nil
	m.mu.Unlock()
	return true
}

func (m *Managed) stopServerLocked() {
	if m.srv != nil {
		m.srv.stop()
		m.srv = nil
	}

}

// armIdleLocked schedules unloading the model after IdleUnload without use
// (owner request: resident at most 30 minutes after the last voice message).
func (m *Managed) armIdleLocked() {
	if m.idle != nil {
		m.idle.Stop()
	}
	m.idle = time.AfterFunc(m.cfg.IdleUnload, func() {
		m.runMu.Lock()
		defer m.runMu.Unlock()
		if time.Since(m.lastUse) >= m.cfg.IdleUnload {
			m.stopServerLocked()
		}
	})
}
