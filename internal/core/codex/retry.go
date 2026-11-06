package codex

import (
	"bytes"
	"encoding/json"
	"strings"
)

// retryNotice is an `error` notification Codex will retry by itself.
type retryNotice struct {
	turnID, message, code string
}

// parseRetry reads an `error` notification with willRetry (a dropped
// stream Codex reconnects, a retried request). The message and code are
// composed and sanitized as v0.1.20 did (codexrunner appserver.go "error",
// parser.go composeCodexFailureMessage): app-server error data is an
// untrusted upstream boundary.
func parseRetry(raw json.RawMessage) (retryNotice, bool) {
	var p struct {
		TurnID    string `json:"turnId"`
		Message   string `json:"message"`
		Code      string `json:"code"`
		WillRetry bool   `json:"willRetry"`
		Error     struct {
			Message           string          `json:"message"`
			Code              string          `json:"code"`
			AdditionalDetails string          `json:"additionalDetails"`
			CodexErrorInfo    json.RawMessage `json:"codexErrorInfo"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &p) != nil || !p.WillRetry {
		return retryNotice{}, false
	}
	r := retryNotice{
		turnID:  p.TurnID,
		code:    sanitizeErrorText(firstNonBlank(p.Error.Code, p.Code, errorInfoCode(p.Error.CodexErrorInfo)), 128),
		message: composeErrorMessage(p.Error.Message, p.Message, p.Error.AdditionalDetails),
	}
	if r.message == "" {
		r.message = "Codex stream disconnected; reconnecting"
	}
	return r, true
}

func composeErrorMessage(primary, fallback, additional string) string {
	message := sanitizeErrorText(firstNonBlank(primary, fallback), 2048)
	detail := sanitizeErrorText(additional, 512)
	if message == "" {
		return detail
	}
	if detail == "" || strings.Contains(message, detail) {
		return message
	}
	return message + ": " + detail
}

// errorInfoCode is the variant name of a codexErrorInfo value: the string
// itself, or the key of a one-key object.
func errorInfoCode(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var name string
	if json.Unmarshal(raw, &name) == nil {
		return name
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return ""
	}
	for key := range object {
		return key
	}
	return ""
}

// sanitizeErrorText redacts a cxp proxy token, drops HTML bodies, keeps
// the text on one line and bounds it to maxRunes.
func sanitizeErrorText(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = redactCXPToken(value)
	if looksLikeHTML(value) {
		value = "HTML response omitted"
	}
	value = strings.Join(strings.Fields(value), " ")
	if runes := []rune(value); maxRunes > 0 && len(runes) > maxRunes {
		value = string(runes[:maxRunes]) + "..."
	}
	return value
}

func redactCXPToken(value string) string {
	const marker = "/_cxp/"
	for offset := 0; ; {
		index := strings.Index(value[offset:], marker)
		if index < 0 {
			return value
		}
		start := offset + index + len(marker)
		end := strings.IndexByte(value[start:], '/')
		if end < 0 {
			return value[:start] + "<redacted>"
		}
		end += start
		value = value[:start] + "<redacted>" + value[end:]
		offset = start + len("<redacted>")
	}
}

func looksLikeHTML(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "<!doctype html") ||
		strings.Contains(lower, "<html") ||
		strings.Contains(lower, "<script")
}

func firstNonBlank(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
