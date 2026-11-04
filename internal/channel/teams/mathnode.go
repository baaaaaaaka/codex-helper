package teams

import (
	"bytes"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
// A formula whose SVG needs glyphs outside the MathJax fonts (for example
// CJK text inside \text{}) is refused: resvg would draw them as boxes, and
// a wrong image is worse than showing the TeX.
type NodeMath struct {
	Dir     string // cache root; default <user cache>/codex-helper/teams-math
	Node    string // node binary; default from PATH
	Npm     string // npm binary; default from PATH
	Timeout time.Duration

	mu   sync.Mutex
	root string
}

//go:embed mathjs/package.json mathjs/package-lock.json mathjs/renderer.mjs
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
	node := n.Node
	if node == "" {
		if node, err = exec.LookPath("node"); err != nil {
			return nil, fmt.Errorf("teams math: node not found: %w", err)
		}
	}
	type item struct {
		Index  int    `json:"index"`
		Source string `json:"source"`
	}
	req := struct {
		Items []item `json:"items"`
		Fonts []any  `json:"fonts"`
	}{Fonts: []any{}}
	for i, s := range sources {
		req.Items = append(req.Items, item{i, s})
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
		return nil, fmt.Errorf("teams math: renderer failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return parseMathOutput(out.Bytes(), len(sources))
}

func parseMathOutput(raw []byte, n int) ([]MathResult, error) {
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
		return nil, fmt.Errorf("teams math: bad renderer output: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("teams math: renderer error %s", resp.Error.Code)
	}
	out := make([]MathResult, n)
	for i := range out {
		out[i].Err = errors.New("no result")
	}
	for _, r := range resp.Results {
		if r.Index < 0 || r.Index >= n {
			continue
		}
		res := MathResult{Width: r.Width, Height: r.Height}
		switch {
		case r.Error != nil:
			res.Err = fmt.Errorf("%s: %s", r.Error.Code, r.Error.Message)
		case strings.TrimSpace(r.Text) != "":
			res.Err = fmt.Errorf("formula needs glyphs the math fonts do not have (%q)", truncate(r.Text, 40))
		default:
			png, err := base64.StdEncoding.DecodeString(r.PNG)
			if err != nil {
				res.Err = err
			} else {
				res.PNG = png
			}
		}
		out[r.Index] = res
	}
	return out, nil
}

func truncate(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

// runtime returns a directory holding renderer.mjs and node_modules,
// installing it once if needed.
func (n *NodeMath) runtime(ctx context.Context) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.root != "" {
		return n.root, nil
	}
	root, err := n.install(ctx)
	if err == nil {
		n.root = root
	}
	return root, err
}

func (n *NodeMath) install(ctx context.Context) (string, error) {
	base := n.Dir
	if base == "" {
		c, err := os.UserCacheDir()
		if err != nil {
			return "", err
		}
		base = filepath.Join(c, "codex-helper", "teams-math")
	}
	want, _ := mathjsFiles.ReadFile("mathjs/renderer.mjs")
	ready := func(dir string) bool {
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
	root := filepath.Join(base, mathRuntimeName)
	for _, d := range []string{root, filepath.Join(base, mathRuntimeLegacy)} {
		if ready(d) {
			return d, nil
		}
	}
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
	lock := flock.New(filepath.Join(base, "install.lock"))
	if err := lock.Lock(); err != nil {
		return "", err
	}
	defer lock.Unlock()
	if ready(root) {
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
	ictx, cancel := context.WithTimeout(ctx, mathInstallTime)
	defer cancel()
	cmd := exec.CommandContext(ictx, npm, "ci", "--ignore-scripts", "--no-audit", "--no-fund", "--include=optional")
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
	if !ready(root) {
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
