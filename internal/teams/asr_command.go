package teams

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const teamsASRMaxCPUThreads = 4

var errASRCommandNotConfigured = errors.New("ASR command is not configured")

type CommandASRTranscriber struct {
	Command string
	Args    []string
	Env     []string
	Model   string
	Backend string
}

func NewCommandASRTranscriber(command string, args []string) *CommandASRTranscriber {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil
	}
	copiedArgs := append([]string(nil), args...)
	return &CommandASRTranscriber{Command: command, Args: copiedArgs, Backend: "command"}
}

func (t *CommandASRTranscriber) TranscribeTeamsMedia(ctx context.Context, input ASRTranscribeInput) (ASRTranscript, error) {
	if t == nil || strings.TrimSpace(t.Command) == "" {
		return ASRTranscript{}, errASRCommandNotConfigured
	}
	sourcePath := strings.TrimSpace(input.File.Path)
	if sourcePath == "" {
		return ASRTranscript{}, fmt.Errorf("ASR source file path is empty")
	}
	args := t.commandArgs(input)
	cmd := exec.Command(strings.TrimSpace(t.Command), args...)
	cmd.Env = asrCommandEnv(os.Environ(), t.Env, input)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := runASRCommand(ctx, cmd); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return ASRTranscript{}, fmt.Errorf("%w: %s", err, shortenTeamsLine(detail, 600))
		}
		return ASRTranscript{}, err
	}
	transcript := ASRTranscript{
		SourceIndex: input.SourceIndex,
		SourceName:  asrTranscriptDisplayName(ASRTranscript{SourceIndex: input.SourceIndex, SourcePath: firstNonEmptyString(input.File.PromptPath, input.File.Path)}),
		SourcePath:  firstNonEmptyString(input.File.PromptPath, input.File.Path),
		ContentType: input.File.ContentType,
		Language:    input.Language,
		Speed:       input.Speed,
		Model:       strings.TrimSpace(t.Model),
		Backend:     firstNonEmptyString(t.Backend, "command"),
	}
	out := strings.TrimSpace(stdout.String())
	if out == "" {
		return transcript, nil
	}
	var decoded ASRTranscript
	if err := json.Unmarshal([]byte(out), &decoded); err == nil && asrTranscriptJSONLooksUsable(decoded) {
		return mergeASRTranscriptDefaults(decoded, transcript), nil
	}
	transcript.Text = out
	return transcript, nil
}

func (t *CommandASRTranscriber) commandArgs(input ASRTranscribeInput) []string {
	if len(t.Args) == 0 {
		return []string{input.File.Path}
	}
	args := make([]string, 0, len(t.Args))
	for _, arg := range t.Args {
		args = append(args, replaceASRCommandPlaceholders(arg, input))
	}
	return args
}

func replaceASRCommandPlaceholders(value string, input ASRTranscribeInput) string {
	replacer := strings.NewReplacer(
		"{input}", input.File.Path,
		"{path}", input.File.Path,
		"{prompt_path}", firstNonEmptyString(input.File.PromptPath, input.File.Path),
		"{content_type}", input.File.ContentType,
		"{language}", input.Language,
		"{speed}", input.Speed,
		"{threads}", strconv.Itoa(teamsASRMaxCPUThreads),
		"{source_index}", strconv.Itoa(input.SourceIndex),
	)
	return replacer.Replace(value)
}

func asrCommandEnv(base []string, extra []string, input ASRTranscribeInput) []string {
	env := envSliceToMap(base)
	for _, item := range extra {
		key, value, ok := strings.Cut(item, "=")
		if !ok || strings.TrimSpace(key) == "" {
			continue
		}
		env[strings.TrimSpace(key)] = value
	}
	for _, key := range []string{
		"GOMAXPROCS",
		"OMP_NUM_THREADS",
		"OMP_THREAD_LIMIT",
		"OPENBLAS_NUM_THREADS",
		"BLIS_NUM_THREADS",
		"GOTO_NUM_THREADS",
		"MKL_NUM_THREADS",
		"NUMEXPR_NUM_THREADS",
		"NUMBA_NUM_THREADS",
		"ONNX_NUM_THREADS",
		"ORT_NUM_THREADS",
		"RAYON_NUM_THREADS",
		"VECLIB_MAXIMUM_THREADS",
		"TORCH_NUM_THREADS",
	} {
		env[key] = boundedASRThreadEnv(env[key])
	}
	env["CODEX_HELPER_TEAMS_ASR_LANGUAGE"] = input.Language
	env["CODEX_HELPER_TEAMS_ASR_SPEED"] = input.Speed
	env["CODEX_HELPER_TEAMS_ASR_SOURCE_INDEX"] = strconv.Itoa(input.SourceIndex)
	env["CODEX_HELPER_TEAMS_ASR_THREADS"] = strconv.Itoa(teamsASRMaxCPUThreads)
	env["TOKENIZERS_PARALLELISM"] = "false"
	return envMapToSlice(env)
}

func boundedASRThreadEnv(value string) string {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err == nil && parsed > 0 && parsed <= teamsASRMaxCPUThreads {
		return strconv.Itoa(parsed)
	}
	return strconv.Itoa(teamsASRMaxCPUThreads)
}

func envSliceToMap(values []string) map[string]string {
	out := make(map[string]string, len(values))
	for _, value := range values {
		key, val, ok := strings.Cut(value, "=")
		if !ok || key == "" {
			continue
		}
		out[key] = val
	}
	return out
}

func envMapToSlice(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for key, value := range values {
		out = append(out, key+"="+value)
	}
	return out
}

func asrTranscriptJSONLooksUsable(t ASRTranscript) bool {
	return strings.TrimSpace(t.Text) != "" ||
		strings.TrimSpace(t.Language) != "" ||
		strings.TrimSpace(t.Duration) != "" ||
		strings.TrimSpace(t.Model) != "" ||
		strings.TrimSpace(t.Backend) != "" ||
		strings.TrimSpace(t.Warning) != ""
}

func mergeASRTranscriptDefaults(value ASRTranscript, defaults ASRTranscript) ASRTranscript {
	if value.SourceIndex == 0 {
		value.SourceIndex = defaults.SourceIndex
	}
	if strings.TrimSpace(value.SourceName) == "" {
		value.SourceName = defaults.SourceName
	}
	if strings.TrimSpace(value.SourcePath) == "" {
		value.SourcePath = defaults.SourcePath
	}
	if strings.TrimSpace(value.ContentType) == "" {
		value.ContentType = defaults.ContentType
	}
	if strings.TrimSpace(value.Language) == "" {
		value.Language = defaults.Language
	}
	if strings.TrimSpace(value.Speed) == "" {
		value.Speed = defaults.Speed
	}
	if strings.TrimSpace(value.Model) == "" {
		value.Model = defaults.Model
	}
	if strings.TrimSpace(value.Backend) == "" {
		value.Backend = defaults.Backend
	}
	return value
}
