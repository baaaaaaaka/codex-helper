package teams

import (
	"bytes"
	"cmp"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

// NodeMath renders formulas with MathJax + resvg in a managed Node.js
// runtime (mathjs/). The runtime is installed on first use with
// `npm ci --ignore-scripts` into a private cache directory; an install from
// an earlier release with an identical renderer is reused as is.
//
// Text MathJax cannot draw itself (CJK inside \text{}, for example) needs a
// font: pinned fonts (Fonts) are downloaded on demand, see mathfont.go. A
// formula whose text is still not covered by an installed font is refused:
// resvg would draw boxes, and a wrong image is worse than showing the TeX.
type NodeMath struct {
	Dir     string // cache root; default <user cache>/codex-helper/teams-math
	Node    string // node binary; default from PATH
	Npm     string // npm binary; default from PATH
	Timeout time.Duration

	// Fonts are the pinned fonts that may be downloaded; nil means
	// DefaultMathFonts, an empty slice disables downloads.
	Fonts       []FontAsset
	HTTP        *http.Client  // font downloads; default http.DefaultClient
	FontTimeout time.Duration // default 2 minutes

	// Install state (runtime): the runtime installs in the background; a
	// failed install is retried after mathInstallBackoff.
	mu       sync.Mutex
	root     string
	pending  chan struct{} // closed when the running install ends
	failures int
	retryAt  time.Time
	lastErr  error

	fontMu sync.Mutex
	fonts  map[string]fontStamp // verified font files
}

//go:embed mathjs/package.json mathjs/package-lock.json mathjs/renderer.mjs mathjs/fonts/*.txt
var mathjsFiles embed.FS

const (
	mathRuntimeName   = "mathjax-4.1.2_resvg-js-2.6.2_ch1"
	mathRuntimeLegacy = "mathjax-4.1.2_resvg-js-2.6.2_noto-serif-sc-2.003_v3"
	mathInstallTime   = 2 * time.Minute
	mathRenderTime    = 20 * time.Second
	maxMathOutput     = 64 << 20
)

func (n *NodeMath) Render(ctx context.Context, sources []string) ([]MathResult, error) {
	root, err := n.runtime(ctx)
	if err != nil {
		return nil, err
	}
	all := make([]int, len(sources))
	for i := range all {
		all[i] = i
	}
	fonts := n.installedFonts(root)
	res, texts, err := n.run(ctx, root, sources, all, fonts)
	if err != nil {
		return nil, err
	}
	// Formulas with text no installed font covers: fetch fonts, render again.
	var retry []int
	var need []string
	for i, t := range texts {
		if res[i].Err == nil && len(n.missingGlyphs(fonts, t)) > 0 {
			retry, need = append(retry, i), append(need, t)
		}
	}
	var fontErr error
	if len(retry) > 0 {
		var added bool
		fonts, added, fontErr = n.ensureFonts(ctx, root, fonts, need)
		if added {
			again, againTexts, err := n.run(ctx, root, sources, retry, fonts)
			for k, i := range retry {
				switch {
				case err != nil:
					res[i], texts[i] = MathResult{Err: err}, ""
				default:
					res[i], texts[i] = again[k], againTexts[k]
				}
			}
		}
	}
	for i, t := range texts {
		if res[i].Err != nil {
			continue
		}
		if missing := n.missingGlyphs(fonts, t); len(missing) > 0 {
			msg := fmt.Sprintf("formula needs glyphs no math font has (%s)", runeList(missing))
			if fontErr != nil {
				msg += ": " + fontErr.Error()
			}
			res[i] = MathResult{Err: errors.New(msg)}
		}
	}
	return res, nil
}

// run renders sources[idx...] and returns results and renderer text in idx order.
func (n *NodeMath) run(ctx context.Context, root string, sources []string, idx []int, fonts []renderFont) ([]MathResult, []string, error) {
	node := n.Node
	if node == "" {
		var err error
		if node, err = exec.LookPath("node"); err != nil {
			return nil, nil, fmt.Errorf("teams math: node not found: %w", err)
		}
	}
	type item struct {
		Index  int    `json:"index"`
		Source string `json:"source"`
	}
	req := struct {
		Items []item       `json:"items"`
		Fonts []renderFont `json:"fonts"`
	}{Fonts: append([]renderFont{}, fonts...)}
	for k, i := range idx {
		req.Items = append(req.Items, item{k, sources[i]})
	}
	in, _ := json.Marshal(req)
	timeout := n.Timeout
	if timeout <= 0 {
		timeout = mathRenderTime
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(rctx, node, filepath.Join(root, "renderer.mjs"))
	cmd.Dir = root
	cmd.Stdin = bytes.NewReader(in)
	var out, stderr limitedBuffer
	out.max, stderr.max = maxMathOutput, 4<<10
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return nil, nil, fmt.Errorf("teams math: renderer failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return parseMathOutput(out.Bytes(), len(idx))
}

// parseMathOutput decodes the renderer response. texts[i] is the text the
// renderer drew with external fonts (to be checked against font coverage).
func parseMathOutput(raw []byte, n int) ([]MathResult, []string, error) {
	var resp struct {
		Results []struct {
			Index  int    `json:"index"`
			PNG    string `json:"png"`
			Width  int    `json:"width"`
			Height int    `json:"height"`
			Text   string `json:"text"`
			Error  *struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		} `json:"results"`
		Error *struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, nil, fmt.Errorf("teams math: bad renderer output: %w", err)
	}
	if resp.Error != nil {
		return nil, nil, fmt.Errorf("teams math: renderer error %s", resp.Error.Code)
	}
	out := make([]MathResult, n)
	texts := make([]string, n)
	for i := range out {
		out[i].Err = errors.New("no result")
	}
	for _, r := range resp.Results {
		if r.Index < 0 || r.Index >= n {
			continue
		}
		res := MathResult{Width: r.Width, Height: r.Height}
		if r.Error != nil {
			res.Err = fmt.Errorf("%s: %s", r.Error.Code, r.Error.Message)
		} else if png, err := base64.StdEncoding.DecodeString(r.PNG); err != nil {
			res.Err = err
		} else {
			res.PNG = png
			texts[r.Index] = r.Text
		}
		out[r.Index] = res
	}
	return out, texts, nil
}

// errMathInstalling: the renderer runtime is being installed in the
// background; the formula is shown as code meanwhile.
var errMathInstalling = errors.New("teams math: the renderer is being installed")

// mathNow is the clock of the install backoff (tests replace it).
var mathNow = time.Now

// mathInstallBackoff is the pause after the n-th consecutive failed install:
// 10 min doubling to 24 h.
func mathInstallBackoff(n int) time.Duration {
	d := 10 * time.Minute
	for i := 1; i < n && d < 24*time.Hour; i++ {
		d *= 2
	}
	return min(d, 24*time.Hour)
}

// runtime returns a directory holding renderer.mjs and node_modules.
//
// It never waits for an install: a runtime already on disk is used at once;
// otherwise one install (npm ci, up to mathInstallTime) starts in the
// background and errMathInstalling is returned, so the caller shows the
// formula as code instead of holding the answer. After a failed install the
// next one waits mathInstallBackoff (10 min doubling to 24 h); until then
// every call fails fast with the last install error.
func (n *NodeMath) runtime(ctx context.Context) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.root != "" {
		return n.root, nil
	}
	if n.pending != nil {
		return "", errMathInstalling
	}
	if mathNow().Before(n.retryAt) {
		return "", fmt.Errorf("teams math: renderer install failed; next try after %s: %w", n.retryAt.Format(time.RFC3339), n.lastErr)
	}
	base, err := n.base()
	if err != nil {
		return "", err
	}
	if root, ok := existingMathRuntime(base); ok {
		n.root = root
		return root, nil
	}
	done := make(chan struct{})
	n.pending = done
	go func() {
		defer close(done)
		root, err := n.install(context.WithoutCancel(ctx), base)
		n.mu.Lock()
		defer n.mu.Unlock()
		n.pending = nil
		if err != nil {
			n.failures++
			n.lastErr = err
			n.retryAt = mathNow().Add(mathInstallBackoff(n.failures))
			return
		}
		n.root, n.failures, n.lastErr, n.retryAt = root, 0, nil, time.Time{}
	}()
	return "", errMathInstalling
}

// installDone returns a channel closed when no install is running.
func (n *NodeMath) installDone() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.pending != nil {
		return n.pending
	}
	c := make(chan struct{})
	close(c)
	return c
}

func (n *NodeMath) base() (string, error) {
	if n.Dir != "" {
		return n.Dir, nil
	}
	c, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(c, "codex-helper", "teams-math"), nil
}

// mathRuntimeReady reports whether dir holds the current renderer and its
// modules.
func mathRuntimeReady(dir string) bool {
	want, _ := mathjsFiles.ReadFile("mathjs/renderer.mjs")
	have, err := os.ReadFile(filepath.Join(dir, "renderer.mjs"))
	if err != nil || !bytes.Equal(have, want) {
		return false
	}
	for _, p := range []string{"node_modules/mathjax/package.json", "node_modules/@resvg/resvg-js/package.json"} {
		if _, err := os.Stat(filepath.Join(dir, p)); err != nil {
			return false
		}
	}
	return true
}

// existingMathRuntime finds a ready runtime: the current one, or one from an
// earlier release with an identical renderer.
func existingMathRuntime(base string) (string, bool) {
	for _, d := range []string{filepath.Join(base, mathRuntimeName), filepath.Join(base, mathRuntimeLegacy)} {
		if mathRuntimeReady(d) {
			return d, true
		}
	}
	return "", false
}

func (n *NodeMath) install(ctx context.Context, base string) (string, error) {
	root := filepath.Join(base, mathRuntimeName)
	ctx, cancel := context.WithTimeout(ctx, mathInstallTime)
	defer cancel()
	npm := n.Npm
	if npm == "" {
		var err error
		if npm, err = exec.LookPath("npm"); err != nil {
			return "", fmt.Errorf("teams math: npm is needed to install the renderer: %w", err)
		}
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		return "", err
	}
	// Another process may be installing: wait for it within the same bound.
	lock := flock.New(filepath.Join(base, "install.lock"))
	if locked, err := lock.TryLockContext(ctx, 250*time.Millisecond); err != nil || !locked {
		return "", fmt.Errorf("teams math: install lock: %v", cmp.Or(err, ctx.Err()))
	}
	defer lock.Unlock()
	if mathRuntimeReady(root) {
		return root, nil
	}
	stage, err := os.MkdirTemp(base, ".install-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(stage)
	for _, name := range []string{"package.json", "package-lock.json", "renderer.mjs"} {
		data, _ := mathjsFiles.ReadFile("mathjs/" + name)
		if err := os.WriteFile(filepath.Join(stage, name), data, 0o600); err != nil {
			return "", err
		}
	}
	cmd := exec.CommandContext(ctx, npm, "ci", "--ignore-scripts", "--no-audit", "--no-fund", "--include=optional")
	cmd.Dir = stage
	var log limitedBuffer
	log.max = 8 << 10
	cmd.Stdout, cmd.Stderr = &log, &log
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("teams math: npm ci: %w: %s", err, strings.TrimSpace(log.String()))
	}
	_ = os.RemoveAll(root)
	if err := os.Rename(stage, root); err != nil {
		return "", err
	}
	if !mathRuntimeReady(root) {
		return "", errors.New("teams math: installed runtime is incomplete")
	}
	return root, nil
}

type limitedBuffer struct {
	bytes.Buffer
	max int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := b.max - b.Len(); room > 0 {
		b.Buffer.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}
