package teams

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"golang.org/x/net/html"
)

const pendingQuestionPrefix = "❓Pending agent question"

// splitPendingQuestions recognizes only a top-level helper paragraph identified
// by this message's self-link. Keep original bytes: reserializing Teams HTML can
// damage hosted images, attachment placeholders and existing annotations.
func splitPendingQuestions(content, chat, id string) (body, prefix, section string) {
	link := annotationLink(chat, id, "pending-questions")
	z := html.NewTokenizer(strings.NewReader(content))
	offset, blocked, paragraph, rule := 0, 0, -1, -1
	matched := false
	for {
		token := z.Next()
		start := offset
		offset += len(z.Raw())
		switch token {
		case html.ErrorToken:
			return content, "", ""
		case html.StartTagToken, html.SelfClosingTagToken:
			t := z.Token()
			switch t.Data {
			case "blockquote", "pre", "code":
				blocked++
			case "hr":
				if blocked == 0 {
					rule = start
				}
			case "p":
				if blocked == 0 {
					paragraph = start
					matched = false
				}
			case "a":
				if blocked == 0 && paragraph >= 0 && attr(t, "href") == link {
					matched = true
				}
			}
		case html.EndTagToken:
			switch z.Token().Data {
			case "blockquote", "pre", "code":
				if blocked > 0 {
					blocked--
				}
			case "p":
				if blocked == 0 && paragraph >= 0 && matched {
					begin := paragraph
					if rule >= 0 && strings.TrimSpace(content[rule:paragraph]) == "<hr/>" {
						begin = rule
					}
					// Teams can normalize <hr/> into <hr> or <hr />.
					if rule >= 0 {
						between := strings.TrimSpace(content[rule:paragraph])
						if between == "<hr>" || between == "<hr />" {
							begin = rule
						}
					}
					section = content[begin:offset]
					body = content[:begin] + content[offset:]
					clean, changed := messagePrefixEdit(body, pendingQuestionPrefix, false)
					if changed {
						prefix = "<p><strong>" + esc(pendingQuestionPrefix) + "</strong></p>"
						body = clean
					}
					return body, prefix, section
				}
				paragraph = -1
				matched = false
			}
		}
	}
}

func pendingQuestionsHTML(chat, id string, pending channel.PendingQuestions, titles bool) string {
	var out strings.Builder
	questions := pending.Items
	count := pending.Count
	if count == 0 {
		count = len(questions)
	}
	fmt.Fprintf(&out, `<hr/><p><strong><a href="%s">❓ Pending questions · %d</a></strong>`, esc(annotationLink(chat, id, "pending-questions")), count)
	for _, q := range questions {
		out.WriteString("<br/>")
		title := ""
		if titles {
			title = strings.Join(strings.Fields(q.Title), " ")
			r := []rune(title)
			if len(r) > 80 {
				title = string(r[:80]) + "…"
			}
			if title != "" {
				title = " · " + title
			}
		}
		label := q.ID + title
		if q.MessageID == "" {
			out.WriteString(esc(label + " · Sending question…"))
		} else {
			fmt.Fprintf(&out, `<a href="%s">%s</a>`, esc(annotationLink(chat, q.MessageID, "")), esc(label))
			if q.MessageID == id {
				out.WriteString(" (this question)")
			}
		}
	}
	out.WriteString("</p>")
	return out.String()
}

// SetPendingQuestions shares the ownership checks and lossless PATCH used by
// prefix edits. The whole decoration changes in one request.
func (a *Adapter) SetPendingQuestions(ctx context.Context, chat, id string, pending channel.PendingQuestions) error {
	return a.editMessageDecoration(ctx, chat, id, func(content string) (string, error) {
		return pendingQuestionsEdit(content, chat, id, pending)
	})
}

// pendingQuestionsEdit is shared by standalone refreshes and annotations.
func pendingQuestionsEdit(content, chat, id string, pending channel.PendingQuestions) (string, error) {
	questions := pending.Items
	body, prefix, section := splitPendingQuestions(content, chat, id)
	body, _ = messagePrefixEdit(body, pendingQuestionPrefix, false) // upgrade legacy prefix
	if len(questions) == 0 {
		return body, nil
	}
	head := "<p><strong>" + esc(pendingQuestionPrefix) + "</strong></p>"
	tail := pendingQuestionsHTML(chat, id, pending, true)
	if prefix != "" && section != "" && strings.HasSuffix(content, section) &&
		strings.Join(strings.Fields(plainText(section)), " ") == strings.Join(strings.Fields(plainText(tail)), " ") && slices.Equal(namedLinks(section), namedLinks(tail)) {
		return content, nil
	}
	next := head + body + tail
	if len(next)+markerReserve > maxPartBytes {
		next = head + body + pendingQuestionsHTML(chat, id, pending, false)
	}
	if len(next)+markerReserve > maxPartBytes {
		return "", channel.ErrQuestionDecorationTooLarge
	}
	return next, nil
}

// RenderQuestionDirectory uses the ordinary bounded message splitter.
func (a *Adapter) RenderQuestionDirectory(ctx context.Context, chat string, questions []channel.PendingQuestion) ([]channel.Part, error) {
	var body strings.Builder
	for _, q := range questions {
		title := strings.Join(strings.Fields(q.Title), " ")
		title = string([]rune(title)[:min(80, len([]rune(title)))])
		if q.MessageID == "" {
			fmt.Fprintf(&body, "- %s · Sending question…\n", q.ID)
			continue
		}
		fmt.Fprintf(&body, "- [%s](%s) %s\n", q.ID, annotationLink(chat, q.MessageID, ""), strings.NewReplacer("[", "\\[", "]", "\\]", "<", "&lt;", ">", "&gt;").Replace(title))
	}
	return a.Render(ctx, channel.RichDoc{Label: "❓ Pending question directory", Markdown: body.String()})
}
