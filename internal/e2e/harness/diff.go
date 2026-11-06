package harness

import (
	"fmt"
	"strings"
)

// UnifiedDiff returns a unified line diff of want and got (3 lines of
// context), or "" when they are equal.
func UnifiedDiff(want, got, wantName, gotName string) string {
	if want == got {
		return ""
	}
	a := strings.Split(want, "\n")
	b := strings.Split(got, "\n")
	// Trim the common prefix and suffix; the middle is usually small.
	pre := 0
	for pre < len(a) && pre < len(b) && a[pre] == b[pre] {
		pre++
	}
	suf := 0
	for suf < len(a)-pre && suf < len(b)-pre && a[len(a)-1-suf] == b[len(b)-1-suf] {
		suf++
	}
	ma, mb := a[pre:len(a)-suf], b[pre:len(b)-suf]
	type op struct {
		kind byte // ' ', '-', '+'
		text string
		ai   int // line index in a (for ' ' and '-')
		bi   int
	}
	var ops []op
	if len(ma)*len(mb) > 16_000_000 {
		for i, l := range ma {
			ops = append(ops, op{'-', l, pre + i, -1})
		}
		for i, l := range mb {
			ops = append(ops, op{'+', l, -1, pre + i})
		}
	} else {
		// LCS table over the middle.
		n, m := len(ma), len(mb)
		lcs := make([][]int32, n+1)
		for i := range lcs {
			lcs[i] = make([]int32, m+1)
		}
		for i := n - 1; i >= 0; i-- {
			for j := m - 1; j >= 0; j-- {
				if ma[i] == mb[j] {
					lcs[i][j] = lcs[i+1][j+1] + 1
				} else if lcs[i+1][j] >= lcs[i][j+1] {
					lcs[i][j] = lcs[i+1][j]
				} else {
					lcs[i][j] = lcs[i][j+1]
				}
			}
		}
		i, j := 0, 0
		for i < n || j < m {
			switch {
			case i < n && j < m && ma[i] == mb[j]:
				ops = append(ops, op{' ', ma[i], pre + i, pre + j})
				i++
				j++
			case j < m && (i == n || lcs[i][j+1] >= lcs[i+1][j]):
				ops = append(ops, op{'+', mb[j], -1, pre + j})
				j++
			default:
				ops = append(ops, op{'-', ma[i], pre + i, -1})
				i++
			}
		}
	}
	// Add the trimmed context around the middle.
	var all []op
	for i := max(0, pre-3); i < pre; i++ {
		all = append(all, op{' ', a[i], i, i})
	}
	all = append(all, ops...)
	for k := 0; k < min(3, suf); k++ {
		all = append(all, op{' ', a[len(a)-suf+k], len(a) - suf + k, len(b) - suf + k})
	}
	var out strings.Builder
	fmt.Fprintf(&out, "--- %s\n+++ %s\n", wantName, gotName)
	// Hunks: changed lines with up to 3 lines of context.
	const ctx = 3
	changed := make([]bool, len(all))
	for i, o := range all {
		changed[i] = o.kind != ' '
	}
	i := 0
	for i < len(all) {
		if !changed[i] {
			i++
			continue
		}
		start := max(0, i-ctx)
		end := i
		for end < len(all) {
			if changed[end] {
				end++
				continue
			}
			// look ahead: another change within 2*ctx keeps the hunk open
			next := end
			for next < len(all) && !changed[next] && next-end < 2*ctx {
				next++
			}
			if next < len(all) && changed[next] {
				end = next
				continue
			}
			end = min(len(all), end+ctx)
			break
		}
		aStart, bStart := -1, -1
		for _, o := range all[start:end] {
			if aStart < 0 && o.ai >= 0 {
				aStart = o.ai
			}
			if bStart < 0 && o.bi >= 0 {
				bStart = o.bi
			}
		}
		fmt.Fprintf(&out, "@@ -%d +%d @@\n", aStart+1, bStart+1)
		for _, o := range all[start:end] {
			out.WriteByte(o.kind)
			out.WriteString(o.text)
			out.WriteByte('\n')
		}
		i = end
	}
	return out.String()
}
