package codexrunner

import (
	"bytes"
	"encoding/json"
)

const appServerJSONSmallValueLimit = 8 * 1024

type appServerJSONScanState uint8

const (
	appServerJSONScanStart appServerJSONScanState = iota
	appServerJSONScanKeyOrEnd
	appServerJSONScanKeyAfterComma
	appServerJSONScanKeyString
	appServerJSONScanColon
	appServerJSONScanValueStart
	appServerJSONScanValueString
	appServerJSONScanValueComposite
	appServerJSONScanValuePrimitive
	appServerJSONScanAfterValue
	appServerJSONScanDone
	appServerJSONScanInvalid
)

type appServerJSONScanEvent struct {
	FieldStart    string
	FieldComplete string
	RawValue      []byte
	Invalid       bool
}

// appServerTopLevelJSONScanner walks only the JSON envelope. It decodes
// top-level member names and small id/method values while skipping large
// nested values byte by byte. Full validation is intentionally left to
// encoding/json for records that are captured as server requests.
type appServerTopLevelJSONScanner struct {
	state appServerJSONScanState

	keyRaw        []byte
	keyTooLarge   bool
	key           string
	valueRaw      []byte
	valueTooLarge bool
	captureValue  bool
	valueName     string

	escaped  bool
	inString bool
	depth    int
}

func (s *appServerTopLevelJSONScanner) FeedByte(value byte) appServerJSONScanEvent {
	if s.state == appServerJSONScanInvalid {
		return appServerJSONScanEvent{Invalid: true}
	}
	if s.state == appServerJSONScanDone {
		if isJSONSpace(value) {
			return appServerJSONScanEvent{}
		}
		s.state = appServerJSONScanInvalid
		return appServerJSONScanEvent{Invalid: true}
	}

	switch s.state {
	case appServerJSONScanStart:
		if isJSONSpace(value) {
			return appServerJSONScanEvent{}
		}
		if value != '{' {
			return s.invalidate()
		}
		s.state = appServerJSONScanKeyOrEnd
	case appServerJSONScanKeyOrEnd:
		if isJSONSpace(value) {
			return appServerJSONScanEvent{}
		}
		if value == '}' {
			s.state = appServerJSONScanDone
			return appServerJSONScanEvent{}
		}
		if value != '"' {
			return s.invalidate()
		}
		s.keyRaw = append(s.keyRaw[:0], value)
		s.keyTooLarge = false
		s.escaped = false
		s.state = appServerJSONScanKeyString
	case appServerJSONScanKeyAfterComma:
		if isJSONSpace(value) {
			return appServerJSONScanEvent{}
		}
		if value != '"' {
			return s.invalidate()
		}
		s.keyRaw = append(s.keyRaw[:0], value)
		s.keyTooLarge = false
		s.escaped = false
		s.state = appServerJSONScanKeyString
	case appServerJSONScanKeyString:
		s.appendKeyByte(value)
		if s.escaped {
			s.escaped = false
			return appServerJSONScanEvent{}
		}
		if value == '\\' {
			s.escaped = true
			return appServerJSONScanEvent{}
		}
		if value != '"' {
			return appServerJSONScanEvent{}
		}
		s.key = ""
		if !s.keyTooLarge {
			if err := json.Unmarshal(s.keyRaw, &s.key); err != nil {
				return s.invalidate()
			}
		}
		s.state = appServerJSONScanColon
	case appServerJSONScanColon:
		if isJSONSpace(value) {
			return appServerJSONScanEvent{}
		}
		if value != ':' {
			return s.invalidate()
		}
		s.state = appServerJSONScanValueStart
		return appServerJSONScanEvent{FieldStart: s.key}
	case appServerJSONScanValueStart:
		if isJSONSpace(value) {
			return appServerJSONScanEvent{}
		}
		s.valueName = s.key
		s.captureValue = s.key == "id" || s.key == "method"
		s.valueRaw = s.valueRaw[:0]
		s.valueTooLarge = false
		s.escaped = false
		s.inString = false
		s.depth = 0
		s.appendValueByte(value)
		switch value {
		case '"':
			s.inString = true
			s.state = appServerJSONScanValueString
		case '{', '[':
			s.depth = 1
			s.state = appServerJSONScanValueComposite
		default:
			if value == ',' || value == '}' || value == ']' {
				return s.invalidate()
			}
			s.state = appServerJSONScanValuePrimitive
		}
	case appServerJSONScanValueString:
		s.appendValueByte(value)
		if s.escaped {
			s.escaped = false
			return appServerJSONScanEvent{}
		}
		if value == '\\' {
			s.escaped = true
			return appServerJSONScanEvent{}
		}
		if value == '"' {
			return s.completeValue(appServerJSONScanAfterValue)
		}
	case appServerJSONScanValueComposite:
		s.appendValueByte(value)
		if s.inString {
			if s.escaped {
				s.escaped = false
				return appServerJSONScanEvent{}
			}
			if value == '\\' {
				s.escaped = true
				return appServerJSONScanEvent{}
			}
			if value == '"' {
				s.inString = false
			}
			return appServerJSONScanEvent{}
		}
		switch value {
		case '"':
			s.inString = true
		case '{', '[':
			s.depth++
		case '}', ']':
			s.depth--
			if s.depth == 0 {
				return s.completeValue(appServerJSONScanAfterValue)
			}
			if s.depth < 0 {
				return s.invalidate()
			}
		}
	case appServerJSONScanValuePrimitive:
		if isJSONSpace(value) {
			return s.completeValue(appServerJSONScanAfterValue)
		}
		switch value {
		case ',':
			return s.completeValue(appServerJSONScanKeyAfterComma)
		case '}':
			return s.completeValue(appServerJSONScanDone)
		case ']', '{', '[', '"':
			return s.invalidate()
		default:
			s.appendValueByte(value)
		}
	case appServerJSONScanAfterValue:
		if isJSONSpace(value) {
			return appServerJSONScanEvent{}
		}
		if value == ',' {
			s.state = appServerJSONScanKeyAfterComma
			return appServerJSONScanEvent{}
		}
		if value == '}' {
			s.state = appServerJSONScanDone
			return appServerJSONScanEvent{}
		}
		return s.invalidate()
	}
	return appServerJSONScanEvent{}
}

func (s *appServerTopLevelJSONScanner) appendKeyByte(value byte) {
	if s.keyTooLarge {
		return
	}
	if len(s.keyRaw) >= appServerJSONSmallValueLimit {
		s.keyTooLarge = true
		s.keyRaw = nil
		return
	}
	s.keyRaw = append(s.keyRaw, value)
}

func (s *appServerTopLevelJSONScanner) appendValueByte(value byte) {
	if !s.captureValue || s.valueTooLarge {
		return
	}
	if len(s.valueRaw) >= appServerJSONSmallValueLimit {
		s.valueTooLarge = true
		s.valueRaw = nil
		return
	}
	s.valueRaw = append(s.valueRaw, value)
}

func (s *appServerTopLevelJSONScanner) completeValue(next appServerJSONScanState) appServerJSONScanEvent {
	event := appServerJSONScanEvent{FieldComplete: s.valueName}
	if s.captureValue && !s.valueTooLarge {
		event.RawValue = append([]byte(nil), s.valueRaw...)
	}
	s.state = next
	s.valueName = ""
	s.captureValue = false
	s.valueRaw = s.valueRaw[:0]
	s.valueTooLarge = false
	return event
}

func (s *appServerTopLevelJSONScanner) invalidate() appServerJSONScanEvent {
	s.state = appServerJSONScanInvalid
	return appServerJSONScanEvent{Invalid: true}
}

func (s *appServerTopLevelJSONScanner) Complete() bool {
	return s.state == appServerJSONScanDone
}

func (s *appServerTopLevelJSONScanner) Invalid() bool {
	return s.state == appServerJSONScanInvalid
}

func isJSONSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

func decodeAppServerMethod(raw []byte) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var method string
	if err := json.Unmarshal(raw, &method); err != nil {
		return "", false
	}
	return method, true
}

func canonicalAppServerID(raw []byte) string {
	return string(bytes.TrimSpace(raw))
}
