package candidateupdate

import (
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

const PostParentRepairCommand = "__internal-update-repair-entry-after-parent-exit"

type postParentRepairRequest struct {
	ParentPID             int
	TargetPath            string
	ReadyPath             string
	ExpectedCurrentSHA256 string
	SourceSHA256          string
	// Candidate-owned updates do not materialize the separate canonical
	// codex-proxy.exe until the stable entry is repaired. Legacy bridge
	// updates retain the stricter canonical-target commit check.
	AllowUncommittedManagedTarget bool
}

// HandlePostParentRepairCommand runs before immutable-runtime dispatch. The
// worker executable is itself the already-published immutable runtime, so it
// remains available after the historical updater moves its download into
// codex-proxy.exe and exits.
func HandlePostParentRepairCommand(args []string, stdout io.Writer, stderr io.Writer) (int, bool) {
	if len(args) < 2 || args[1] != PostParentRepairCommand {
		return 0, false
	}
	request, err := parsePostParentRepairRequest(args[2:])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "post-parent entry repair: %v\n", err)
		return 2, true
	}
	source, err := helperpath.RawExecutable()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "post-parent entry repair: inspect worker executable: %v\n", err)
		return 1, true
	}
	if err := repairPostParentEntry(source, request, stdout); err != nil {
		_, _ = fmt.Fprintf(stderr, "post-parent entry repair: %v\n", err)
		return 1, true
	}
	_, _ = fmt.Fprintf(stdout, "post-parent entry repair completed: %s\n", request.TargetPath)
	return 0, true
}

func parsePostParentRepairRequest(args []string) (postParentRepairRequest, error) {
	var request postParentRepairRequest
	seen := map[string]bool{}
	for _, arg := range args {
		name, value, ok := strings.Cut(arg, "=")
		if !ok || !strings.HasPrefix(name, "--") {
			return request, fmt.Errorf("unsupported argument %q", arg)
		}
		if seen[name] {
			return request, fmt.Errorf("duplicate argument %q", name)
		}
		seen[name] = true
		switch name {
		case "--parent-pid":
			pid, err := strconv.Atoi(value)
			if err != nil || pid <= 0 {
				return request, fmt.Errorf("invalid parent pid %q", value)
			}
			request.ParentPID = pid
		case "--target-path":
			request.TargetPath = strings.TrimSpace(value)
		case "--ready-path":
			request.ReadyPath = strings.TrimSpace(value)
		case "--expected-current-sha256":
			request.ExpectedCurrentSHA256 = strings.ToLower(strings.TrimSpace(value))
		case "--source-sha256":
			request.SourceSHA256 = strings.ToLower(strings.TrimSpace(value))
		case "--allow-uncommitted-managed-target":
			if value != "true" {
				return request, fmt.Errorf("--allow-uncommitted-managed-target must be true")
			}
			request.AllowUncommittedManagedTarget = true
		default:
			return request, fmt.Errorf("unsupported argument %q", name)
		}
	}
	if request.ParentPID <= 0 || request.TargetPath == "" || request.ReadyPath == "" {
		return request, fmt.Errorf("parent pid, target path, and ready path are required")
	}
	for name, value := range map[string]string{
		"expected current sha256": request.ExpectedCurrentSHA256,
		"source sha256":           request.SourceSHA256,
	} {
		if len(value) != 64 {
			return request, fmt.Errorf("%s is invalid", name)
		}
		if _, err := hex.DecodeString(value); err != nil {
			return request, fmt.Errorf("%s is invalid: %w", name, err)
		}
	}
	return request, nil
}
