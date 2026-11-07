package teams

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image/png"
	"regexp"
	"strings"
)

// MathRenderer turns TeX formulas into PNG images.
//
// Render returns one result per source, in order. A per-formula failure is
// reported in MathResult.Err; a returned error fails the whole batch. Either
// way the formula falls back to a code block with its TeX, so a renderer
// must only return an image it is sure is faithful (no missing glyphs, no
// error boxes).
type MathRenderer interface {
	Render(ctx context.Context, sources []string) ([]MathResult, error)
}

// MathResult is one rendered formula.
type MathResult struct {
	PNG    []byte
	Width  int
	Height int
	Err    error
}

const (
	maxMathSourceBytes  = 8 << 10  // per formula (N-19)
	maxMathEscapedBytes = 16 << 10 // per formula, HTML-escaped (v0.1.20)
	maxMathTotalBytes   = 32 << 10 // per document
	maxMathFormulas     = 32       // markers per document
	maxMathBraceDepth   = 64
	// Formula images per part. Graph documents no count limit for a
	// message's hostedContents (owner question 2026-11-07: v0.1.20's 5
	// was the helper's own choice, the same as its inbound image limit),
	// so a part takes as many as its HTML budget and maxPartImageBytes allow;
	// a refused part is posted again with fallbackImagesPerPart images,
	// then with none (Adapter.Send).
	maxImagesPerPart      = maxMathFormulas
	fallbackImagesPerPart = 5
	maxPartImageBytes     = 2 << 20 // PNG bytes per part (base64 grows it by 4/3; Graph takes ~4 MB per request)
	maxPNGBytes           = 4 << 20
	maxPNGSide            = 4096
	maxPNGPixels          = 8 << 20
	mathOpen, mathClose   = "<m>", "</m>"
)

var (
	mathEmailRE   = regexp.MustCompile(`\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b`)
	mathWinPathRE = regexp.MustCompile(`\b[A-Za-z]:\\`)
	mathUnixRE    = regexp.MustCompile(`(?:^|\s)/(?:home|usr|var|etc|tmp)/`)
	mathUnsafeRE  = regexp.MustCompile(`(?i)\\(?:require|includegraphics|href|url|htmlclass|htmlstyle|style|class|def|gdef|edef|xdef|newcommand|renewcommand|let|futurelet|csname|endcsname|loop|repeat|input|include|openin|openout|read|write|catcode|usepackage)\b`)
)

// safeTeX reports whether src may be sent to the renderer. Anything that
// looks like a path, URL, address, nested delimiter or a macro definition is
// refused and shown as code instead.
func safeTeX(src string) bool {
	if strings.TrimSpace(src) == "" || len(src) > maxMathSourceBytes || len(esc(src)) > maxMathEscapedBytes || strings.ContainsRune(src, 0) {
		return false
	}
	if strings.Contains(src, mathOpen) || strings.Contains(src, mathClose) {
		return false
	}
	for _, tok := range []string{`\(`, `\)`, `\[`, `\]`, "${", "$(", "$[", "://"} {
		if strings.Contains(src, tok) {
			return false
		}
	}
	if hasUnescapedDollar(src) || mathEmailRE.MatchString(src) || mathWinPathRE.MatchString(src) ||
		mathUnixRE.MatchString(src) || mathUnsafeRE.MatchString(src) {
		return false
	}
	depth, peak, escaped := 0, 0, false
	for _, r := range src {
		switch {
		case escaped:
			escaped = false
		case r == '\\':
			escaped = true
		case r == '{':
			depth++
			peak = max(peak, depth)
		case r == '}':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0 && peak <= maxMathBraceDepth
}

func hasUnescapedDollar(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '$' {
			continue
		}
		n := 0
		for j := i - 1; j >= 0 && s[j] == '\\'; j-- {
			n++
		}
		if n%2 == 0 {
			return true
		}
	}
	return false
}

// checkPNG validates a renderer result before it is embedded.
func checkPNG(r MathResult) error {
	if r.Err != nil {
		return r.Err
	}
	if len(r.PNG) == 0 || len(r.PNG) > maxPNGBytes {
		return fmt.Errorf("png size %d out of range", len(r.PNG))
	}
	if !bytes.HasPrefix(r.PNG, []byte("\x89PNG\r\n\x1a\n")) {
		return errors.New("not a PNG")
	}
	cfg, err := png.DecodeConfig(bytes.NewReader(r.PNG))
	if err != nil {
		return err
	}
	if cfg.Width <= 0 || cfg.Height <= 0 || cfg.Width > maxPNGSide || cfg.Height > maxPNGSide || cfg.Width*cfg.Height > maxPNGPixels {
		return fmt.Errorf("png %dx%d out of range", cfg.Width, cfg.Height)
	}
	if _, err := png.Decode(bytes.NewReader(r.PNG)); err != nil {
		return fmt.Errorf("png does not decode: %w", err)
	}
	return nil
}
