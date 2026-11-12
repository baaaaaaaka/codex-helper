package teams

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"unicode"
)

// ReadTarget describes a resource, never an executable Graph URL or credential.
type ReadTarget struct {
	Kind    string `json:"kind"`
	Tenant  string `json:"tenant,omitempty"`
	Chat    string `json:"chat,omitempty"`
	Team    string `json:"team,omitempty"`
	Channel string `json:"channel,omitempty"`
	Message string `json:"message,omitempty"`
	Parent  string `json:"parent,omitempty"`
}

const refPrefix = "cxp-teams-v1:"
const cursorPrefix = "cxp-teams-cursor-v1:"
const maxReadToken = 32 << 10

func encodeReadToken(prefix string, v any) string {
	b, _ := json.Marshal(v)
	return prefix + base64.RawURLEncoding.EncodeToString(b)
}
func decodeReadToken(raw, prefix string, v any) error {
	if len(raw) > maxReadToken || !strings.HasPrefix(raw, prefix) {
		return errors.New("invalid or oversized Teams reference/cursor")
	}
	b, e := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(raw, prefix))
	if e != nil {
		return errors.New("invalid Teams reference/cursor encoding")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(v); e != nil {
		return errors.New("invalid Teams reference/cursor structure")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("invalid Teams reference/cursor trailing data")
	}
	return nil
}
func validReadID(s string) bool {
	return len(s) > 0 && len(s) <= 2048 && s != "." && s != ".." && !strings.ContainsAny(s, "/\\?#%") && strings.IndexFunc(s, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) < 0
}
func (t ReadTarget) validate(tenant string) error {
	for _, s := range []string{t.Tenant, t.Chat, t.Team, t.Channel, t.Message, t.Parent} {
		if s != "" && !validReadID(s) {
			return errors.New("invalid resource identifier")
		}
	}
	if tenant != "" && t.Tenant != "" && !strings.EqualFold(tenant, t.Tenant) {
		return errors.New("Teams target tenant differs from current read identity")
	}
	switch t.Kind {
	case "chat":
		if t.Chat == "" || t.Team != "" || t.Channel != "" || t.Message != "" || t.Parent != "" {
			return errors.New("invalid chat target")
		}
	case "chat_message":
		if t.Chat == "" || t.Message == "" || t.Team != "" || t.Channel != "" || t.Parent != "" {
			return errors.New("invalid chat message target")
		}
	case "channel":
		if t.Team == "" || t.Channel == "" || t.Chat != "" || t.Message != "" || t.Parent != "" {
			return errors.New("invalid channel target")
		}
	case "channel_message":
		if t.Team == "" || t.Channel == "" || t.Message == "" || t.Chat != "" {
			return errors.New("invalid channel message target")
		}
	default:
		return errors.New("unsupported Teams resource kind")
	}
	return nil
}
func (t ReadTarget) Ref() string { return encodeReadToken(refPrefix, t) }
func (t ReadTarget) path() string {
	var p string
	if t.Chat != "" {
		p = "/chats/" + url.PathEscape(t.Chat)
	} else {
		p = "/teams/" + url.PathEscape(t.Team) + "/channels/" + url.PathEscape(t.Channel)
	}
	if t.Message != "" {
		if t.Parent != "" && t.Parent != t.Message {
			p += "/messages/" + url.PathEscape(t.Parent) + "/replies/" + url.PathEscape(t.Message)
		} else {
			p += "/messages/" + url.PathEscape(t.Message)
		}
	}
	return p
}
func (t ReadTarget) root() ReadTarget {
	if t.Parent != "" {
		t.Message = t.Parent
		t.Parent = ""
	}
	return t
}
func (t ReadTarget) collection() string { t.Message = ""; t.Parent = ""; return t.path() + "/messages" }
func (t ReadTarget) webURL() string {
	q := url.Values{}
	if t.Tenant != "" {
		q.Set("tenantId", t.Tenant)
	}
	if t.Chat != "" {
		if t.Message == "" {
			return ChatURL(t.Chat, t.Tenant)
		}
		q.Set("context", `{"contextType":"chat"}`)
		return "https://teams.microsoft.com/l/message/" + url.PathEscape(t.Chat) + "/" + url.PathEscape(t.Message) + "?" + q.Encode()
	}
	q.Set("groupId", t.Team)
	if t.Parent != "" {
		q.Set("parentMessageId", t.Parent)
	}
	if t.Message != "" {
		return "https://teams.microsoft.com/l/message/" + url.PathEscape(t.Channel) + "/" + url.PathEscape(t.Message) + "?" + q.Encode()
	}
	return "https://teams.microsoft.com/l/channel/" + url.PathEscape(t.Channel) + "/channel?" + q.Encode()
}

// ParseReadTarget accepts existing-resource Teams links and our bounded refs.
func ParseReadTarget(raw, tenant string) (ReadTarget, error) {
	var t ReadTarget
	if strings.HasPrefix(raw, refPrefix) {
		if e := decodeReadToken(raw, refPrefix, &t); e != nil {
			return t, e
		}
		if e := t.validate(tenant); e != nil {
			return t, e
		}
		if t.Tenant == "" {
			t.Tenant = tenant
		}
		if t.Parent == t.Message {
			t.Parent = ""
		}
		return t, nil
	}
	if len(raw) > maxReadToken {
		return t, errors.New("Teams URL is too long")
	}
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.Host != "teams.microsoft.com" || u.User != nil || u.Fragment != "" {
		return t, errors.New("expected an HTTPS teams.microsoft.com resource link or Teams ref")
	}
	q, e := url.ParseQuery(u.RawQuery)
	if e != nil {
		return t, errors.New("invalid Teams URL query")
	}
	for _, vs := range q {
		if len(vs) != 1 {
			return t, errors.New("duplicate Teams URL parameter")
		}
	}
	t.Tenant = q.Get("tenantId")
	t.Team = q.Get("groupId")
	seg := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(seg) != 4 || seg[0] != "l" {
		return t, errors.New("unsupported Teams link shape")
	}
	switch seg[1] {
	case "chat":
		if seg[3] != "0" || t.Team != "" || q.Get("users") != "" || q.Get("message") != "" {
			return t, errors.New("expected an existing chat link")
		}
		t.Kind = "chat"
		t.Chat = seg[2]
	case "channel":
		t.Kind = "channel"
		t.Channel = seg[2]
	case "message":
		t.Message = seg[3]
		t.Parent = q.Get("parentMessageId")
		var c struct {
			Type string `json:"contextType"`
			Chat string `json:"chatId"`
		}
		if s := q.Get("context"); s != "" {
			if json.Unmarshal([]byte(s), &c) != nil {
				return t, errors.New("invalid Teams message context")
			}
		}
		if t.Team != "" {
			if c.Type == "chat" || c.Chat != "" {
				return t, errors.New("conflicting channel and chat context")
			}
			t.Kind = "channel_message"
			t.Channel = seg[2]
		} else {
			if t.Parent != "" || c.Type != "" && c.Type != "chat" || strings.HasSuffix(seg[2], "@thread.tacv2") {
				return t, errors.New("channel message link lacks groupId")
			}
			if c.Chat != "" && c.Chat != seg[2] {
				return t, errors.New("conflicting chat identifiers")
			}
			t.Kind = "chat_message"
			t.Chat = seg[2]
		}
	default:
		return t, fmt.Errorf("unsupported Teams link type %q", seg[1])
	}
	if t.Parent == t.Message {
		t.Parent = ""
	}
	if e = t.validate(tenant); e != nil {
		return t, e
	}
	if t.Tenant == "" {
		t.Tenant = tenant
	}
	return t, nil
}
