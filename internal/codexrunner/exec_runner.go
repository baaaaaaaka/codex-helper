package codexrunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const defaultCodexCommand = "codex"

type ExecRunner struct {
	Launcher   CommandLauncher
	Command    string
	ExtraArgs  []string
	WorkingDir string
	Timeout    time.Duration
}

func NewExecRunner(launcher CommandLauncher) *ExecRunner {
	return &ExecRunner{Launcher: launcher, Command: defaultCodexCommand}
}

func (r *ExecRunner) StartThread(ctx context.Context, input TurnInput) (TurnResult, error) {
	if err := validatePrompt(input.Prompt); err != nil {
		return TurnResult{}, err
	}
	args, err := r.startArgs(input)
	if err != nil {
		return TurnResult{}, err
	}
	result, err := r.run(ctx, launchInput{
		args:         args,
		prompt:       input.Prompt,
		workingDir:   firstNonEmpty(input.WorkingDir, r.WorkingDir),
		timeout:      firstDuration(input.Timeout, r.Timeout),
		eventHandler: input.EventHandler,
	})
	if err != nil {
		return result, err
	}
	if strings.TrimSpace(result.ThreadID) == "" {
		return result, &Error{Kind: ErrorParse, Message: "codex exec did not emit thread.started with thread_id"}
	}
	return result, nil
}

func (r *ExecRunner) ResumeThread(ctx context.Context, threadID string, input TurnInput) (TurnResult, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return TurnResult{}, &Error{Kind: ErrorInvalidRequest, Message: "thread id is required"}
	}
	if err := validatePrompt(input.Prompt); err != nil {
		return TurnResult{}, err
	}
	args, err := r.resumeArgs(threadID, input)
	if err != nil {
		return TurnResult{}, err
	}
	result, err := r.run(ctx, launchInput{
		args:         args,
		prompt:       input.Prompt,
		workingDir:   firstNonEmpty(input.WorkingDir, r.WorkingDir),
		timeout:      firstDuration(input.Timeout, r.Timeout),
		eventHandler: input.EventHandler,
	})
	if result.ThreadID == "" {
		result.ThreadID = threadID
	} else if result.ThreadID != threadID {
		if err == nil {
			err = &Error{Kind: ErrorParse, Message: fmt.Sprintf("resume emitted thread_id %q, expected %q", result.ThreadID, threadID)}
		}
	}
	return result, err
}

func (r *ExecRunner) StartTurn(ctx context.Context, input StartTurnInput) (TurnResult, error) {
	if strings.TrimSpace(input.ThreadID) == "" {
		return r.StartThread(ctx, input.TurnInput)
	}
	return r.ResumeThread(ctx, input.ThreadID, input.TurnInput)
}

func (r *ExecRunner) InterruptTurn(context.Context, TurnRef) error {
	return unsupported("interrupt turn")
}

func (r *ExecRunner) ReadThread(context.Context, string) (Thread, error) {
	return Thread{}, unsupported("read thread")
}

func (r *ExecRunner) ListThreads(context.Context, ListThreadsOptions) ([]Thread, error) {
	return nil, unsupported("list threads")
}

type launchInput struct {
	args         []string
	prompt       string
	workingDir   string
	timeout      time.Duration
	eventHandler EventHandler
}

func (r *ExecRunner) run(ctx context.Context, input launchInput) (TurnResult, error) {
	launcher := r.Launcher
	if launcher == nil {
		launcher = DirectLauncher{}
	}
	command := strings.TrimSpace(r.Command)
	if command == "" {
		command = defaultCodexCommand
	}
	req := LaunchRequest{
		Command:      command,
		Args:         input.args,
		Dir:          input.workingDir,
		Stdin:        input.prompt,
		Timeout:      input.timeout,
		EventHandler: input.eventHandler,
	}
	output, launchErr := launcher.Launch(ctx, req)
	result, parseErr := ParseJSONL(bytes.NewReader(output.Stdout))
	if launchErr != nil && (errors.Is(launchErr, context.DeadlineExceeded) || errors.Is(launchErr, context.Canceled)) {
		return result, classifyLaunchError(launchErr)
	}
	if parseErr != nil {
		return result, parseErr
	}
	if launchErr != nil {
		return result, classifyLaunchError(launchErr)
	}
	if output.ExitCode != 0 {
		return result, codexFailure(output, result)
	}
	if result.Failure != nil {
		return result, &Error{Kind: ErrorCodex, Message: result.Failure.Message}
	}
	return result, nil
}

func (r *ExecRunner) startArgs(input TurnInput) ([]string, error) {
	args, err := r.baseArgs(input.ExtraArgs)
	if err != nil {
		return nil, err
	}
	workingDir := firstNonEmpty(input.WorkingDir, r.WorkingDir)
	if workingDir != "" {
		args = append(args, "-C", workingDir)
	}
	args = append(args, "-")
	return args, nil
}

func (r *ExecRunner) resumeArgs(threadID string, input TurnInput) ([]string, error) {
	extraArgs := append([]string{}, r.ExtraArgs...)
	extraArgs = append(extraArgs, input.ExtraArgs...)
	if err := rejectLastArg(extraArgs); err != nil {
		return nil, err
	}
	extraArgs = translateResumeArgs(extraArgs)
	args := []string{"exec", "resume", "--json"}
	args = append(args, extraArgs...)
	args = append(args, threadID, "-")
	return args, nil
}

func (r *ExecRunner) baseArgs(extra []string) ([]string, error) {
	merged := append([]string{}, r.ExtraArgs...)
	merged = append(merged, extra...)
	if err := rejectLastArg(merged); err != nil {
		return nil, err
	}
	args := []string{"exec", "--json"}
	args = append(args, merged...)
	return args, nil
}

func translateResumeArgs(args []string) []string {
	if len(args) == 0 {
		return nil
	}
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := strings.TrimSpace(args[i])
		switch {
		case arg == "--sandbox" || arg == "-s":
			if i+1 >= len(args) {
				out = append(out, args[i])
				continue
			}
			i++
			out = append(out, "-c", "sandbox_mode="+strconv.Quote(strings.TrimSpace(args[i])))
		case strings.HasPrefix(arg, "--sandbox="):
			mode := strings.TrimSpace(strings.TrimPrefix(arg, "--sandbox="))
			out = append(out, "-c", "sandbox_mode="+strconv.Quote(mode))
		default:
			out = append(out, args[i])
		}
	}
	return out
}

func validatePrompt(prompt string) error {
	if strings.TrimSpace(prompt) == "" {
		return &Error{Kind: ErrorInvalidRequest, Message: "prompt is required"}
	}
	return nil
}

func rejectLastArg(args []string) error {
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == "--last" || strings.HasPrefix(arg, "--last=") {
			return &Error{Kind: ErrorInvalidRequest, Message: "resume automation must use an exact thread id, not --last"}
		}
	}
	return nil
}

func classifyLaunchError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Kind: ErrorTimeout, Err: err}
	case errors.Is(err, context.Canceled):
		return &Error{Kind: ErrorCanceled, Err: err}
	default:
		return &Error{Kind: ErrorLaunch, Err: err}
	}
}

func codexFailure(output LaunchResult, result TurnResult) error {
	if result.Failure != nil {
		return &Error{Kind: ErrorCodex, Message: result.Failure.Message}
	}
	message := strings.TrimSpace(string(output.Stderr))
	if message == "" {
		message = strings.TrimSpace(string(output.Stdout))
	}
	if message == "" {
		message = fmt.Sprintf("codex exited with status %d", output.ExitCode)
	}
	return &Error{Kind: ErrorCodex, Message: message}
}

func firstDuration(values ...time.Duration) time.Duration {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

type DirectLauncher struct{}

func (DirectLauncher) Launch(ctx context.Context, req LaunchRequest) (LaunchResult, error) {
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, req.Command, req.Args...)
	if req.Dir != "" {
		cmd.Dir = req.Dir
	}
	if req.Stdin != "" {
		cmd.Stdin = strings.NewReader(req.Stdin)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = NewEventStreamWriter(&stdout, req.EventHandler)
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := LaunchResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if err == nil {
		return result, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, ctxErr
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, nil
	}
	return result, err
}
