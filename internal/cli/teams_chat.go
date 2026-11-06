package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

// v0.1.20's maintenance commands that create Teams chats or send to them
// (gap batch G). They act through the running helper
// (teams_newcore_clireq.go); the texts and flags are v0.1.20's.

// teamsDrainTimeoutFlag accepts v0.1.20's --drain-timeout: the helper
// switches chats without stopping, so there is nothing to drain.
func teamsDrainTimeoutFlag(cmd *cobra.Command) {
	cmd.Flags().String("drain-timeout", "", "Ignored (the running helper switches without draining)")
	_ = cmd.Flags().MarkHidden("drain-timeout")
}

func runTeamsControlRecreate(out io.Writer) error {
	return submitTeamsCLIRequest(out, teamsCLISubmission{req: engine.CLIRequest{Op: engine.CLIRecreateControl}})
}

func newTeamsChatCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chat",
		Short: "Developer maintenance for Teams work chats",
		Long:  "Developer maintenance for Teams work chats. These commands may create Teams chats and send messages; they never delete old Teams chats. The running Teams helper carries them out.",
	}
	cmd.AddCommand(newTeamsChatRecreateCmd(), newTeamsChatPublishHistoryCmd())
	return cmd
}

func newTeamsChatRecreateCmd() *cobra.Command {
	var (
		yes         bool
		historyMode string
		activate    bool
	)
	cmd := &cobra.Command{
		Use:   "recreate <session-id|codex-thread-id|teams-chat-id>",
		Short: "Create a fresh Teams work chat for an existing session",
		Long:  "Create a fresh meeting-based Teams work chat for an existing helper session and rebind it there. The old Teams chat is retained and receives a migration link; it is never deleted.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !yes {
				return fmt.Errorf("recreating a work chat creates a new Teams chat and sends messages; rerun with --yes")
			}
			historyMode = strings.ToLower(strings.TrimSpace(historyMode))
			if historyMode != "none" && historyMode != "full" {
				return fmt.Errorf("invalid --history %q: expected none or full", historyMode)
			}
			return submitTeamsCLIRequest(cmd.OutOrStdout(), teamsCLISubmission{req: engine.CLIRequest{
				Op: engine.CLIRecreateChat, Selector: strings.TrimSpace(args[0]), History: historyMode == "full", Activate: activate,
			}})
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm that this may create a Teams chat, send messages, and publish history when requested")
	cmd.Flags().StringVar(&historyMode, "history", "none", "History to publish into the new chat: none or full")
	cmd.Flags().BoolVar(&activate, "activate", false, "Reactivate a held (quarantined) session while recreating its chat")
	teamsDrainTimeoutFlag(cmd)
	return cmd
}

func newTeamsChatPublishHistoryCmd() *cobra.Command {
	var yes, full, force bool
	cmd := &cobra.Command{
		Use:   "publish-history <session-id|codex-thread-id|teams-chat-id>",
		Short: "Publish complete local Codex history to a Teams work chat",
		Long:  "Publish the complete linked local Codex transcript to the selected session's current Teams chat. Normal runs are idempotent per target chat; --force deliberately creates a fresh replay namespace.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !full {
				return fmt.Errorf("publishing complete local history requires --full")
			}
			if !yes {
				return fmt.Errorf("publishing local history sends messages to Teams; rerun with --yes")
			}
			return submitTeamsCLIRequest(cmd.OutOrStdout(), teamsCLISubmission{req: engine.CLIRequest{
				Op: engine.CLIPublishHistory, Selector: strings.TrimSpace(args[0]), Force: force,
			}})
		},
	}
	cmd.Flags().BoolVar(&full, "full", false, "Publish the complete linked local Codex transcript")
	cmd.Flags().BoolVar(&force, "force", false, "Deliberately replay the full transcript even if this target chat was already published")
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm that this sends local Codex history to Teams")
	teamsDrainTimeoutFlag(cmd)
	return cmd
}

// teamsDefaultUploadFolder is the OneDrive folder the helper uploads to
// (channel/teams adapter default, as in v0.1.20).
const teamsDefaultUploadFolder = "Microsoft Teams Chat Files"

func newTeamsSendFileCmd() *cobra.Command {
	var (
		sessionID, chatID, message, uploadFolder, outboundRoot string
		allowLocalPath, yes, verbose                           bool
	)
	cmd := &cobra.Command{
		Use:   "send-file <path>",
		Short: "Upload a local file to OneDrive and send it as a Teams attachment",
		Long:  "Upload a local file to OneDrive and send it as a Teams attachment in a work chat. Relative paths are read from the Teams outbound root (the directory `helper file` reads); other paths need --allow-local-path. The running Teams helper sends it.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(chatID) != "" && !yes {
				return fmt.Errorf("refusing explicit --chat-id without --yes; prefer --session to avoid sending a file to the wrong Teams chat")
			}
			sessionID, chatID = strings.TrimSpace(sessionID), strings.TrimSpace(chatID)
			switch {
			case chatID != "" && sessionID != "":
				return fmt.Errorf("use only one of --chat-id or --session")
			case chatID == "" && sessionID == "":
				return fmt.Errorf("choose a target with --session or --chat-id")
			}
			if f := strings.TrimSpace(uploadFolder); f != "" && f != teamsDefaultUploadFolder {
				return fmt.Errorf("--upload-folder is not supported by this version; files are uploaded to %q", teamsDefaultUploadFolder)
			}
			root := strings.TrimSpace(outboundRoot)
			if root == "" {
				anchor, _, err := teamsNewCoreCLIStorePath()
				if err != nil {
					return err
				}
				root = teamsOutboundRoot(anchor)
			}
			src, err := resolveTeamsSendFilePath(args[0], root, allowLocalPath)
			if err != nil {
				if !allowLocalPath {
					return fmt.Errorf("%w (use --allow-local-path for an explicit local upload)", err)
				}
				return err
			}
			out := &lineFilter{out: cmd.OutOrStdout(), drop: func(l string) bool {
				return !verbose && strings.HasPrefix(l, "Teams message id: ")
			}}
			return submitTeamsCLIRequest(out, teamsCLISubmission{
				req: engine.CLIRequest{Op: engine.CLISendFile, Selector: sessionID, ChatID: chatID, Message: message},
				stage: func(dir string) (string, error) {
					dst := filepath.Join(dir, filepath.Base(src))
					return dst, copyTeamsSendFile(src, dst)
				},
			})
		},
	}
	cmd.Flags().StringVar(&sessionID, "session", "", "Send to a known Teams session id")
	cmd.Flags().StringVar(&chatID, "chat-id", "", "Send to an explicit Teams chat id (a chat of this helper)")
	cmd.Flags().BoolVar(&allowLocalPath, "allow-local-path", false, "Allow uploading the explicit local path instead of the Teams outbound root")
	cmd.Flags().StringVar(&message, "message", "", "Message text to send with the attachment")
	cmd.Flags().StringVar(&uploadFolder, "upload-folder", teamsDefaultUploadFolder, "OneDrive folder to upload into (only the default is supported)")
	cmd.Flags().StringVar(&outboundRoot, "outbound-root", "", "Root directory for relative upload paths")
	cmd.Flags().BoolVar(&yes, "yes", false, "Allow an explicit --chat-id target")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Print the Teams message id after the upload")
	return cmd
}

// teamsOutboundRoot is the helper's outbound root next to its store (the
// engine's FilesDir/outbound).
func teamsOutboundRoot(anchor string) string {
	return filepath.Join(filepath.Dir(anchor), "core-files", "outbound")
}

// resolveTeamsSendFilePath is v0.1.20's resolveOutboundPath: a relative
// path under the outbound root, or any path with --allow-local-path; the
// file must be a regular file, not a symlink.
func resolveTeamsSendFilePath(path, root string, allowAny bool) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("file path is required")
	}
	var resolved string
	switch {
	case allowAny:
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", err
		}
		resolved = abs
	case filepath.IsAbs(path):
		return "", fmt.Errorf("absolute paths require --allow-local-path")
	default:
		clean := filepath.Clean(path)
		if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("path must stay under Teams outbound root")
		}
		rootAbs, err := filepath.Abs(root)
		if err != nil {
			return "", err
		}
		resolved = filepath.Join(rootAbs, clean)
		if real, err := filepath.EvalSymlinks(resolved); err == nil {
			realRoot, rerr := filepath.EvalSymlinks(rootAbs)
			if rerr != nil {
				return "", fmt.Errorf("path must stay under Teams outbound root")
			}
			rel, err := filepath.Rel(realRoot, real)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
				return "", fmt.Errorf("path must stay under Teams outbound root")
			}
		}
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return "", fmt.Errorf("open Teams upload file: %s: no such file", filepath.Base(resolved))
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("refusing to upload symlink: %s", filepath.Base(resolved))
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("refusing to upload non-regular file: %s", filepath.Base(resolved))
	}
	return resolved, nil
}

// copyTeamsSendFile copies the file into the request's directory, so the
// helper (possibly on another host of a shared home) reads what the
// command checked.
func copyTeamsSendFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("read Teams upload file: %w", err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("read Teams upload file: %w", err)
	}
	return out.Close()
}

// lineFilter drops the lines drop selects from what it writes.
type lineFilter struct {
	out  io.Writer
	drop func(string) bool
}

func (f *lineFilter) Write(p []byte) (int, error) {
	for _, l := range strings.SplitAfter(string(p), "\n") {
		if l == "" || f.drop(strings.TrimSuffix(l, "\n")) {
			continue
		}
		if _, err := io.WriteString(f.out, l); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}
