package teams

import (
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"golang.org/x/net/html"
)

// namedLinks runs only for HTML containing an anchor. Keep plainText unchanged:
// it is also the contract for persisted outbox hashes and send reconciliation.
func namedLinks(content string) []channel.Link {
	if !containsFold(content, "<a", 1) {
		return nil
	}
	z := html.NewTokenizer(strings.NewReader(content))
	var links []channel.Link
	var href string
	var label strings.Builder
	seen := map[channel.Link]bool{}
	flush := func() {
		if href == "" {
			return
		}
		text := strings.TrimSpace(plainText(label.String()))
		link := channel.Link{Text: text, URL: href}
		if text != href && !seen[link] {
			links = append(links, link)
			seen[link] = true
		}
		href = ""
		label.Reset()
	}
	for {
		switch z.Next() {
		case html.ErrorToken:
			flush()
			return links
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			if t.Data == "a" {
				flush()
				for _, a := range t.Attr {
					if a.Key == "href" {
						href, _ = safeURL(strings.TrimSpace(a.Val))
						break
					}
				}
			} else if href != "" {
				label.WriteString(string(z.Raw()))
			}
		case html.EndTagToken:
			t := z.Token()
			if t.Data == "a" {
				flush()
			} else if href != "" {
				label.WriteString(string(z.Raw()))
			}
		case html.TextToken:
			if href != "" {
				label.Write(z.Raw())
			}
		}
	}
}
