package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/teamshtml"
)

// Receive walks the whole window of chat messages modified after
// cursor-Overlap (every nextLink page), then returns it oldest first.
//
// Graph lists chat messages only newest first (lastModifiedDateTime desc),
// so nothing is returned until the walk is complete: an older message
// behind a page boundary must not be admitted after a newer one.
//
// The cursor is the lastModifiedDateTime watermark of the newest message
// seen, as fixed-width RFC 3339 (so cursors also sort as strings); "" lists
// the whole chat.
const cursorLayout = "2006-01-02T15:04:05.000000000Z07:00"

func (a *Adapter) Receive(ctx context.Context, stream, cursor string) (channel.Window, error) {
	var since, watermark time.Time
	if cursor != "" {
		t, err := time.Parse(time.RFC3339Nano, cursor)
		if err != nil {
			return channel.Window{}, &channel.Rejected{Permanent: true, Err: fmt.Errorf("teams: bad cursor %q", cursor)}
		}
		watermark = t
		since = t.Add(-a.cfg.Overlap)
	}
	msgs, newest, pages, err := a.walk(ctx, stream, since)
	if err != nil {
		return channel.Window{}, err
	}
	w := channel.Window{Msgs: msgs, Requests: pages, Cursor: cursor}
	if newest.After(watermark) {
		// Unchanged input stays byte-identical so the core can skip the write.
		w.Cursor = newest.UTC().Format(cursorLayout)
	}
	return w, nil
}

// walk lists every message of chat with lastModifiedDateTime > since
// (all messages when since is zero). It returns them sorted by
// (createdDateTime, id), the newest lastModifiedDateTime seen and the
// number of pages fetched.
func (a *Adapter) walk(ctx context.Context, chat string, since time.Time) ([]channel.InboundMsg, time.Time, int, error) {
	me, err := a.me(ctx)
	if err != nil {
		return nil, time.Time{}, 0, err
	}
	q := url.Values{}
	q.Set("$top", strconv.Itoa(pageSize))
	q.Set("$orderby", "lastModifiedDateTime desc")
	if !since.IsZero() {
		// Never send an empty or epoch bound; Graph rejects/ignores those.
		floor := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		if since.Before(floor) {
			since = floor
		}
		q.Set("$filter", "lastModifiedDateTime gt "+since.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano))
	}
	next := "/chats/" + url.PathEscape(chat) + "/messages?" + q.Encode()
	seenLinks := map[string]bool{}
	seenIDs := map[string]bool{}
	var out []channel.InboundMsg
	var newest time.Time
	pages := 0
	for next != "" {
		if pages >= a.cfg.MaxPages {
			return nil, time.Time{}, pages, &channel.Rejected{Err: fmt.Errorf("teams: receive window for chat exceeds %d pages", a.cfg.MaxPages)}
		}
		if seenLinks[next] {
			return nil, time.Time{}, pages, &channel.Rejected{Err: fmt.Errorf("teams: nextLink loop")}
		}
		seenLinks[next] = true
		var page struct {
			Value    []graphMessage `json:"value"`
			NextLink string         `json:"@odata.nextLink"`
		}
		if err := a.g.getJSON(ctx, next, pageBodyLimit, &page); err != nil {
			return nil, time.Time{}, pages, err
		}
		pages++
		for _, gm := range page.Value {
			if gm.ID == "" || seenIDs[gm.ID] {
				continue
			}
			seenIDs[gm.ID] = true
			m := a.convert(chat, gm, me.ID)
			if m.Modified.After(newest) {
				newest = m.Modified
			}
			out = append(out, m)
		}
		next = page.NextLink
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].Created.Equal(out[j].Created) {
			return out[i].Created.Before(out[j].Created)
		}
		return idLess(out[i].ID, out[j].ID)
	})
	return out, newest, pages, nil
}

// idLess orders Teams message ids (decimal millisecond timestamps) numerically.
func idLess(a, b string) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}

type graphMessage struct {
	ID          string  `json:"id"`
	MessageType string  `json:"messageType"`
	Created     string  `json:"createdDateTime"`
	Modified    string  `json:"lastModifiedDateTime"`
	Edited      *string `json:"lastEditedDateTime"`
	Deleted     *string `json:"deletedDateTime"`
	From        *struct {
		User        *graphIdentity `json:"user"`
		Application *graphIdentity `json:"application"`
	} `json:"from"`
	Body struct {
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
	} `json:"body"`
	Attachments []graphAttachment `json:"attachments"`
	Mentions    []struct {
		Mentioned *struct {
			User *graphIdentity `json:"user"`
		} `json:"mentioned"`
	} `json:"mentions"`
}

type graphIdentity struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

type graphAttachment struct {
	ID          string `json:"id"`
	ContentType string `json:"contentType"`
	ContentURL  string `json:"contentUrl"`
	Content     string `json:"content"`
	Name        string `json:"name"`
}

var hostedRE = regexp.MustCompile(`(?i)/hostedContents/([^/"'<>\s]+)/\$value`)

const maxInboundFiles = 16

func (a *Adapter) convert(chat string, gm graphMessage, owner string) channel.InboundMsg {
	m := channel.InboundMsg{Chat: chat, ID: gm.ID}
	m.Created, _ = time.Parse(time.RFC3339Nano, gm.Created)
	m.Modified, _ = time.Parse(time.RFC3339Nano, gm.Modified)
	if m.Modified.IsZero() {
		m.Modified = m.Created
	}
	m.Edited = gm.Edited != nil && *gm.Edited != ""
	switch {
	case gm.Deleted != nil && *gm.Deleted != "":
		m.Kind = channel.KindDeleted
	case gm.MessageType != "" && gm.MessageType != "message":
		m.Kind = channel.KindSystem
	}
	if gm.From != nil {
		switch {
		case gm.From.User != nil:
			m.From, m.FromName = gm.From.User.ID, gm.From.User.DisplayName
			m.ByOwner = owner != "" && strings.EqualFold(m.From, owner)
		case gm.From.Application != nil:
			m.From, m.FromName = "app:"+gm.From.Application.ID, gm.From.Application.DisplayName
		}
	}
	for _, mn := range gm.Mentions {
		if mn.Mentioned != nil && mn.Mentioned.User != nil && owner != "" && strings.EqualFold(mn.Mentioned.User.ID, owner) {
			m.MentionsOwner = true
		}
	}
	content := gm.Body.Content
	if sm := markerRE.FindStringSubmatch(content); sm != nil {
		m.Marker = sm[1]
	}
	if strings.EqualFold(gm.Body.ContentType, "html") {
		m.Text = teamshtml.PlainTextFromTeamsHTML(content)
	} else {
		m.Text = strings.TrimSpace(markerRE.ReplaceAllString(content, ""))
	}
	m.Hash = channel.TextHash(m.Text)
	if m.Kind != channel.KindMessage {
		return m
	}
	m.Files, m.Quotes = a.inboundFiles(chat, gm)
	return m
}

func (a *Adapter) inboundFiles(chat string, gm graphMessage) ([]channel.FileRef, []channel.Quote) {
	var files []channel.FileRef
	var quotes []channel.Quote
	seen := map[string]bool{}
	hosted := func(kind channel.FileKind, id, name string) {
		if seen[id] {
			return
		}
		seen[id] = true
		files = append(files, channel.FileRef{Kind: kind, Name: name, Ref: hostedRef(chat, gm.ID, id)})
	}
	for _, sm := range hostedRE.FindAllStringSubmatch(gm.Body.Content, -1) {
		hosted(channel.FileImage, sm[1], "")
	}
	for _, at := range gm.Attachments {
		ct := strings.ToLower(strings.TrimSpace(strings.Split(at.ContentType, ";")[0]))
		switch ct {
		case "reference":
			u, err := url.Parse(strings.TrimSpace(at.ContentURL))
			if err != nil || u.Scheme != "https" || u.User != nil || !a.allowedHost(u.Hostname()) {
				files = append(files, channel.FileRef{Kind: channel.FileUnsupported, Name: at.Name, Note: "file link is not on an allowed SharePoint host"})
				continue
			}
			files = append(files, channel.FileRef{Kind: channel.FileDoc, Name: at.Name, Ref: "share:" + u.String()})
		case "application/vnd.microsoft.card.audio", "application/vnd.microsoft.card.video":
			var card struct {
				Media []struct {
					URL string `json:"url"`
				} `json:"media"`
			}
			_ = json.Unmarshal([]byte(at.Content), &card)
			found := false
			for _, md := range card.Media {
				for _, sm := range hostedRE.FindAllStringSubmatch(md.URL, -1) {
					hosted(channel.FileVoice, sm[1], at.Name)
					found = true
				}
			}
			if !found {
				files = append(files, channel.FileRef{Kind: channel.FileUnsupported, Name: at.Name, Note: "media card without hosted content"})
			}
		case "messagereference", "forwardedmessagereference":
			quotes = append(quotes, quoteFrom(at))
		default:
			files = append(files, channel.FileRef{Kind: channel.FileUnsupported, Name: at.Name, Note: "unsupported attachment type " + ct})
		}
	}
	if len(files) > maxInboundFiles {
		extra := len(files) - maxInboundFiles
		files = append(files[:maxInboundFiles], channel.FileRef{Kind: channel.FileUnsupported, Note: fmt.Sprintf("%d more attachments not fetched", extra)})
	}
	return files, quotes
}

func quoteFrom(at graphAttachment) channel.Quote {
	type who struct {
		User *struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		} `json:"user"`
		Application *struct {
			DisplayName string `json:"displayName"`
		} `json:"application"`
	}
	var c struct {
		MessageID      string `json:"messageId"`
		MessagePreview string `json:"messagePreview"`
		MessageSender  who    `json:"messageSender"`
		OriginalID     string `json:"originalMessageId"`
		OriginalText   string `json:"originalMessageContent"`
		OriginalSender who    `json:"originalMessageSender"`
	}
	_ = json.Unmarshal([]byte(at.Content), &c)
	name := func(w who) string {
		if w.User != nil {
			return first(w.User.DisplayName, w.User.ID)
		}
		if w.Application != nil {
			return w.Application.DisplayName
		}
		return ""
	}
	return channel.Quote{
		ID:       first(c.MessageID, c.OriginalID, at.ID),
		FromName: first(name(c.MessageSender), name(c.OriginalSender)),
		Text:     first(c.MessagePreview, teamshtml.PlainTextFromTeamsHTML(c.OriginalText)),
	}
}

func (a *Adapter) allowedHost(host string) bool {
	host = strings.ToLower(host)
	allowed := a.cfg.SharePointHosts
	if len(allowed) == 0 {
		allowed = []string{".sharepoint.com"}
	}
	for _, h := range allowed {
		h = strings.ToLower(strings.TrimSpace(h))
		if h != "" && (host == h || strings.HasPrefix(h, ".") && strings.HasSuffix(host, h)) {
			return true
		}
	}
	return false
}

// SharePointHostsFromEnv parses CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS.
func SharePointHostsFromEnv(v string) []string {
	return strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ';' || r == ' ' || r == '\n' || r == '\t' })
}

func hostedRef(chat, msg, id string) string {
	return "hosted:" + url.PathEscape(chat) + "/" + url.PathEscape(msg) + "/" + url.PathEscape(id)
}
