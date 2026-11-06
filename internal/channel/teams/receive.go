package teams

import (
	"context"
	"encoding/json"
	"errors"
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
	var tl *channel.WindowTooLarge
	if errors.As(err, &tl) && cursor != "" {
		if skip, _ := time.Parse(time.RFC3339Nano, tl.SkipCursor); !skip.After(watermark) {
			tl.SkipCursor = cursor // never behind the input cursor
		}
	}
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

// messagesURL is the first page of the messages of chat modified after
// since (all messages when since is zero), newest modification first.
func messagesURL(chat string, since time.Time) string {
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
	return "/chats/" + url.PathEscape(chat) + "/messages?" + q.Encode()
}

// HistoryPage lists one page of the messages of chat modified after since,
// newest modification first (channel.HistoryPager): one Graph request.
func (a *Adapter) HistoryPage(ctx context.Context, chat string, since time.Time, pos string) ([]channel.InboundMsg, string, error) {
	me, err := a.me(ctx)
	if err != nil {
		return nil, "", err
	}
	next := pos
	if next == "" {
		next = messagesURL(chat, since)
	}
	var page struct {
		Value    []graphMessage `json:"value"`
		NextLink string         `json:"@odata.nextLink"`
	}
	if err := a.g.getJSON(ctx, next, pageBodyLimit, &page); err != nil {
		return nil, "", err
	}
	if page.NextLink == next {
		return nil, "", &channel.Rejected{Err: fmt.Errorf("teams: nextLink loop")}
	}
	out := make([]channel.InboundMsg, 0, len(page.Value))
	for _, gm := range page.Value {
		if gm.ID != "" {
			out = append(out, a.convert(chat, gm, me.ID))
		}
	}
	return out, page.NextLink, nil
}

// PartText is the visible text of a rendered part, as a message read back
// carries it (channel.PartTexter). A part body is the chatMessage request
// JSON; its text is that of body.content.
func (a *Adapter) PartText(body string) string {
	var m struct {
		Body struct {
			Content string `json:"content"`
		} `json:"body"`
	}
	if json.Unmarshal([]byte(body), &m) != nil {
		return ""
	}
	return plainText(m.Body.Content)
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
	next := messagesURL(chat, since)
	seenLinks := map[string]bool{}
	seenIDs := map[string]bool{}
	var out []channel.InboundMsg
	var newest time.Time
	pages := 0
	for next != "" {
		if pages >= a.cfg.MaxPages {
			// Admitting only the newest pages would break ordering; let the
			// core decide (visible notice, retry or skip).
			oldest := newest
			for _, m := range out {
				if m.Modified.Before(oldest) {
					oldest = m.Modified
				}
			}
			return nil, time.Time{}, pages, &channel.WindowTooLarge{Pages: pages, Seen: len(out), OldestSeen: oldest, NewestSeen: newest, SkipCursor: newest.UTC().Format(cursorLayout)}
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
		m.Text = plainText(content)
	} else {
		m.Text = strings.TrimSpace(markerRE.ReplaceAllString(content, ""))
	}
	m.Hash = channel.TextHash(m.Text)
	if m.Kind != channel.KindMessage {
		return m
	}
	m.Files, m.Quotes = a.inboundFiles(chat, gm)
	m.Attachments = attachmentTypes(gm)
	return m
}

// attachmentTypes lists a message's attachments as v0.1.20's
// unsupportedControlAttachments did: the attachments in Graph order, then
// each inline hosted content of the body, else a bare placeholder.
func attachmentTypes(gm graphMessage) []string {
	var types []string
	for _, at := range gm.Attachments {
		types = append(types, strings.TrimSpace(at.ContentType))
	}
	seen := map[string]bool{}
	for _, sm := range hostedRE.FindAllStringSubmatch(gm.Body.Content, -1) {
		if !seen[sm[1]] {
			seen[sm[1]] = true
			types = append(types, "Teams-hosted inline content")
		}
	}
	if len(types) == 0 && strings.Contains(strings.ToLower(gm.Body.Content), "<attachment") {
		types = append(types, "Teams attachment placeholder")
	}
	return types
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
			q := quoteFrom(at)
			q.Forwarded = ct == "forwardedmessagereference"
			quotes = append(quotes, q)
		default:
			files = append(files, channel.FileRef{Kind: channel.FileUnsupported, Name: at.Name, Note: "unsupported attachment type " + ct})
		}
	}
	// The per-message limits (5 media, 5 files, 3 quotes) are the engine's
	// (SPEC F-RND-8): an over-limit message is refused, not truncated.
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
		OriginalTime   string `json:"originalSentDateTime"`
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
		Text:     first(c.MessagePreview, plainText(c.OriginalText)),
		Time:     strings.TrimSpace(c.OriginalTime),
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

// Teams stores emoji as markup: <emoji id=".." alt="😀" title=".."></emoji>,
// <customemoji .. alt=":name:">, or (older clients) <img
// itemtype="http://schema.skype.com/Emoji" alt="😀">. They are part of what
// the user wrote, so the plain text keeps their alt text.
var (
	emojiPairRE  = regexp.MustCompile(`(?is)<(emoji|customemoji)\b([^>]*)>[^<]*</(?:emoji|customemoji)\s*>`)
	emojiOpenRE  = regexp.MustCompile(`(?is)<(?:emoji|customemoji)\b([^>]*)>`)
	emojiCloseRE = regexp.MustCompile(`(?i)</(?:emoji|customemoji)\s*>`)
	emojiImgRE   = regexp.MustCompile(`(?is)<img\b([^>]*itemtype\s*=\s*["']https?://schema\.skype\.com/(?:Emoji|Emoticon)["'][^>]*)>`)
	altAttrRE    = regexp.MustCompile(`(?is)\balt\s*=\s*(?:"([^"]*)"|'([^']*)')`)
	titleAttrRE  = regexp.MustCompile(`(?is)\btitle\s*=\s*(?:"([^"]*)"|'([^']*)')`)
)

func emojiAlt(attrs string) string {
	for _, re := range []*regexp.Regexp{altAttrRE, titleAttrRE} {
		if m := re.FindStringSubmatch(attrs); m != nil {
			return m[1] + m[2]
		}
	}
	return ""
}

// plainText is the inbound text of a Teams HTML body: emoji become their
// Unicode (alt) text, the rest as teamshtml renders it.
func plainText(content string) string {
	if strings.Contains(content, "<") {
		content = emojiPairRE.ReplaceAllStringFunc(content, func(t string) string {
			return emojiAlt(emojiPairRE.FindStringSubmatch(t)[2])
		})
		content = emojiOpenRE.ReplaceAllStringFunc(content, func(t string) string {
			return emojiAlt(emojiOpenRE.FindStringSubmatch(t)[1])
		})
		content = emojiCloseRE.ReplaceAllString(content, "")
		content = emojiImgRE.ReplaceAllStringFunc(content, func(t string) string {
			return emojiAlt(emojiImgRE.FindStringSubmatch(t)[1])
		})
	}
	return teamshtml.PlainTextFromTeamsHTML(content)
}

func hostedRef(chat, msg, id string) string {
	return "hosted:" + url.PathEscape(chat) + "/" + url.PathEscape(msg) + "/" + url.PathEscape(id)
}

// Recent lists up to limit of the chat's most recently modified messages,
// newest first (Graph's order), following nextLink pages until limit is
// reached. Unlike Receive it does not need the whole window: it is for
// readers of a bounded tail (the delegation registry and inbox chats).
func (a *Adapter) Recent(ctx context.Context, chat string, limit int) ([]channel.InboundMsg, error) {
	if limit <= 0 {
		return nil, nil
	}
	me, err := a.me(ctx)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Set("$top", strconv.Itoa(min(limit, pageSize)))
	q.Set("$orderby", "lastModifiedDateTime desc")
	next := "/chats/" + url.PathEscape(chat) + "/messages?" + q.Encode()
	seenLinks := map[string]bool{}
	seenIDs := map[string]bool{}
	var out []channel.InboundMsg
	for next != "" && len(out) < limit {
		if seenLinks[next] {
			return nil, &channel.Rejected{Err: fmt.Errorf("teams: nextLink loop")}
		}
		seenLinks[next] = true
		var page struct {
			Value    []graphMessage `json:"value"`
			NextLink string         `json:"@odata.nextLink"`
		}
		if err := a.g.getJSON(ctx, next, pageBodyLimit, &page); err != nil {
			return nil, err
		}
		for _, gm := range page.Value {
			if gm.ID == "" || seenIDs[gm.ID] || len(out) >= limit {
				continue
			}
			seenIDs[gm.ID] = true
			out = append(out, a.convert(chat, gm, me.ID))
		}
		next = page.NextLink
	}
	return out, nil
}
