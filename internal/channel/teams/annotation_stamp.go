package teams

import (
	"bytes"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
)

// Ordinary message links survive Teams HTML normalization. Event fragments
// distinguish annotations on the same message without changing navigation.
func annotationLink(chat, id, event string) string {
	link := "https://teams.microsoft.com/l/message/" + url.PathEscape(chat) + "/" + url.PathEscape(id) + "?context=%7B%22contextType%22%3A%22chat%22%7D"
	if event != "" {
		link += "#cxp-" + url.QueryEscape(event)
	}
	return link
}

func eventTime(t time.Time) string { return t.Format("2006-01-02 15:04:05 UTC-07:00") }

func timeStamp(prefix, link string, at time.Time) string {
	return "<p>" + prefix + ` <a href="` + esc(link) + `">` + esc(eventTime(at)) + "</a></p>"
}

// Match the annotation paragraph, not any URL or matching text in the body.
// Quoted conversations and code examples cannot acknowledge this message.
// The legacy form put the link around the Helper header, followed by its body.
func hasAnnotationStamp(content, link, prefix string, legacyText string) bool {
	z := html.NewTokenizer(strings.NewReader(content))
	text := make([]byte, 0, 256)
	blocked, paragraph, linked, legacyHeader := 0, false, false, false
	for {
		switch z.Next() {
		case html.ErrorToken:
			return false
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			switch t.Data {
			case "blockquote", "pre", "code":
				blocked++
			case "br":
				if paragraph && blocked == 0 {
					text = append(text, ' ')
				}
			case "hr":
				legacyHeader = false
			case "p":
				paragraph = blocked == 0
				text = text[:0]
				linked = false
			case "a":
				if paragraph && blocked == 0 && attr(t, "href") == link {
					linked = true
				}
			}
		case html.TextToken:
			if paragraph && blocked == 0 {
				text = append(text, z.Text()...)
			}
		case html.EndTagToken:
			switch z.Token().Data {
			case "blockquote", "pre", "code":
				if blocked > 0 {
					blocked--
				}
			case "p":
				if !paragraph {
					continue
				}
				value := bytes.TrimSpace(text)
				if linked && bytes.HasPrefix(value, []byte(prefix+" ")) {
					return true
				}
				if legacyHeader && strings.Join(strings.Fields(string(value)), " ") == legacyText {
					return true
				}
				legacyHeader = legacyText != "" && linked && bytes.Equal(value, []byte("🔧 Helper:"))
				paragraph = false
			}
		}
	}
}
