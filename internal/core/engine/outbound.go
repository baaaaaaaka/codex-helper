package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Outbound files (the artifact manifest contract, SPEC F-RND-10). Every
// turn gets its own output directory, named in the prompt. Codex writes the
// files it wants to send there and lists them in a fenced
// `codex-helper-artifacts` block of its final answer. The engine strips the
// block, snapshots each listed file into a staging directory and enqueues
// one outbox row per file after the final text; the sender uploads it
// (resumably, outbox_files) and posts it as an attachment under exactly the
// manifest name.

// artifactFence is the info string of the manifest block (unchanged from
// the legacy helper, so older sessions keep working).
const artifactFence = "codex-helper-artifacts"

// maxArtifactBytes bounds one outbound file.
const maxArtifactBytes = 250 << 20

// mathContract asks Codex for formulas the channel can render.
const mathContract = "Put fractions, sums, integrals, matrices, and display math in <m>TeX</m> on a separate line, never in prose, tables, or lists—even if asked for one line. Use backticks for simple inline/literal TeX. Never use $ or \\(...\\). Math markers are not artifacts."

// helperSafety keeps a turn from managing the helper that runs it, and
// points cross-machine work at `cxp delegate` (the old core's prompt).
const helperSafety = `Helper safety:
- You are running inside a Codex turn launched by the chat helper. The current request is the user message above; treat earlier requests in resumed history as completed context unless the user asks about them.
- Do not restart, reload, kill, replace, or background the helper process, binary, or service from this turn.
- If the helper needs to restart after an update or local repair, finish the answer and tell the user to send ` + "`helper restart now`" + ` in the control chat.
- If the helper needs a release update, tell the user to send ` + "`helper update now`" + `, or ` + "`helper update prerelease`" + ` only when they asked for prereleases.
- Subagent requests use native Codex subagents. For cross-machine agent delegation, use the cxp skill and its ` + "`cxp delegate`" + ` workflow when the task needs another signed-in machine.`

// outboundDir is the output directory of a turn ("" when files are off).
func (e *Engine) outboundDir(turnKey string) string {
	if e.cfg.FilesDir == "" || turnKey == "" {
		return ""
	}
	return filepath.Join(e.cfg.FilesDir, "outbound", turnKey)
}

// outboundInstructions is appended to a prompt so Codex knows how to send
// files back.
func outboundInstructions(dir string) string {
	s := helperSafety + "\n\n" + mathContract
	if dir == "" {
		return s
	}
	return s + "\n\n" +
		"If you need to return generated files or images to the Teams user, write them under this local directory:\n" +
		dir + "\n\n" +
		"Then include this fenced manifest in your final answer:\n" +
		"```" + artifactFence + "\n" +
		`{"version":1,"files":[{"path":"relative/path.ext","name":"display-name.ext"}]}` + "\n" +
		"```\n\n" +
		"Use only relative manifest paths under that directory. Keep your normal answer visible; the helper attaches the listed files separately."
}

// manifestBlocks splits text into the visible part and the manifest blocks.
func manifestBlocks(text string) (string, [][]byte) {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	var blocks [][]byte
	var cur []string
	in := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !in {
			if strings.HasPrefix(trimmed, "```") && strings.EqualFold(strings.TrimSpace(strings.TrimPrefix(trimmed, "```")), artifactFence) {
				in, cur = true, nil
				continue
			}
			out = append(out, line)
			continue
		}
		if strings.HasPrefix(trimmed, "```") {
			blocks = append(blocks, []byte(strings.Join(cur, "\n")))
			in, cur = false, nil
			continue
		}
		cur = append(cur, line)
	}
	if in && len(cur) > 0 { // unterminated block at the end
		blocks = append(blocks, []byte(strings.Join(cur, "\n")))
	}
	return strings.TrimSpace(strings.Join(out, "\n")), blocks
}

// artifact is one validated manifest entry.
type artifact struct {
	src  string // file under the turn's output directory
	name string // display name
}

var windowsDrive = regexp.MustCompile(`^[A-Za-z]:(?:[\\/]|$)`)

// parseManifest validates one manifest block against the output directory.
// Entries that cannot be sent are reported by display name with a reason
// (never a path).
func parseManifest(block []byte, dir string) ([]artifact, []string) {
	text := string(block)
	if strings.Contains(text, `"relative/path.ext"`) && strings.Contains(text, `"display-name.ext"`) {
		return nil, nil // the placeholder echoed back
	}
	var m struct {
		Version int `json:"version"`
		Files   []struct {
			Path string `json:"path"`
			Name string `json:"name"`
		} `json:"files"`
	}
	dec := json.NewDecoder(bytes.NewReader(block))
	if err := dec.Decode(&m); err != nil {
		return nil, []string{"the file list could not be read"}
	}
	if m.Version != 0 && m.Version != 1 {
		return nil, []string{fmt.Sprintf("unsupported file list version %d", m.Version)}
	}
	var out []artifact
	var problems []string
	for i, f := range m.Files {
		name := strings.TrimSpace(f.Name)
		if name == "" {
			name = path.Base(strings.ReplaceAll(strings.TrimSpace(f.Path), `\`, "/"))
		}
		name = safeName(name)
		if name == "attachment" && f.Name == "" && f.Path == "" {
			name = fmt.Sprintf("file %d", i+1)
		}
		clean, err := cleanManifestPath(f.Path)
		if err != nil {
			problems = append(problems, name+": "+err.Error())
			continue
		}
		if dir == "" {
			problems = append(problems, name+": files cannot be sent from this chat")
			continue
		}
		src := filepath.Join(dir, filepath.FromSlash(clean))
		if err := underDir(dir, src); err != nil {
			problems = append(problems, name+": "+err.Error())
			continue
		}
		out = append(out, artifact{src: src, name: name})
	}
	return out, problems
}

func cleanManifestPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	switch {
	case raw == "":
		return "", errors.New("no path given")
	case strings.ContainsRune(raw, 0), windowsDrive.MatchString(raw):
		return "", errors.New("the path must be relative to the output directory")
	}
	p := strings.ReplaceAll(raw, `\`, "/")
	if strings.HasPrefix(p, "/") {
		return "", errors.New("the path must be relative to the output directory")
	}
	p = path.Clean(p)
	if p == "." || p == ".." || strings.HasPrefix(p, "../") {
		return "", errors.New("the path must stay inside the output directory")
	}
	return p, nil
}

// underDir checks that p, with symlinks resolved, stays inside dir.
func underDir(dir, p string) error {
	rd, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return errors.New("the output directory does not exist")
	}
	rp, err := filepath.EvalSymlinks(p)
	if err != nil {
		return errors.New("the file was not found")
	}
	rel, err := filepath.Rel(rd, rp)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return errors.New("the file is outside the output directory")
	}
	return nil
}

// stageArtifacts parses the manifests of a final answer, snapshots every
// listed file into the staging area and returns the visible answer, one
// outbox row per file, and the problems to show. Runs outside any
// transaction (file I/O).
func (e *Engine) stageArtifacts(tr store.Turn, final string) (string, []store.OutMsg, []string) {
	visible, blocks := manifestBlocks(final)
	if len(blocks) == 0 {
		return visible, nil, nil
	}
	dir := e.outboundDir(tr.TurnKey)
	var arts []artifact
	var problems []string
	seen := map[string]bool{}
	for _, b := range blocks {
		a, p := parseManifest(b, dir)
		problems = append(problems, p...)
		for _, x := range a {
			if !seen[x.src] {
				seen[x.src] = true
				arts = append(arts, x)
			}
		}
	}
	var msgs []store.OutMsg
	for i, a := range arts {
		staged := filepath.Join(e.cfg.FilesDir, "send", fmt.Sprint(tr.ID), fmt.Sprint(i+1), a.name)
		size, err := snapshot(a.src, staged)
		if err != nil {
			problems = append(problems, a.name+": "+err.Error())
			continue
		}
		msgs = append(msgs, store.OutMsg{
			DeliveryKey: fmt.Sprintf("turn:%d:file:%d", tr.ID, i+1), Kind: store.OutFinal,
			File: &store.OutFile{LocalPath: staged, Size: size},
		})
	}
	return visible, msgs, problems
}

// snapshot copies a regular file to dst (replacing an earlier partial copy).
func snapshot(src, dst string) (int64, error) {
	fi, err := os.Lstat(src)
	switch {
	case err != nil:
		return 0, errors.New("the file was not found")
	case fi.Mode()&os.ModeSymlink != 0:
		if fi, err = os.Stat(src); err != nil {
			return 0, errors.New("the file was not found")
		}
	}
	if !fi.Mode().IsRegular() {
		return 0, errors.New("it is not a regular file")
	}
	if fi.Size() > maxArtifactBytes {
		return 0, fmt.Errorf("it is larger than %d MB", maxArtifactBytes>>20)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return 0, errors.New("it could not be prepared for sending")
	}
	in, err := os.Open(src)
	if err != nil {
		return 0, errors.New("it could not be read")
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, errors.New("it could not be prepared for sending")
	}
	n, err := io.Copy(out, in)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, dst)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return 0, errors.New("it could not be read")
	}
	return n, nil
}
