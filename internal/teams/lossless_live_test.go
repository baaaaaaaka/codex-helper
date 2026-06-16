package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	xhtml "golang.org/x/net/html"
)

func TestLiveGraphLosslessAttachmentEditOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_LOSSLESS_EDIT")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_LOSSLESS_EDIT=1 to create a live single-member Teams chat and test lossless message edit")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before live Teams write tests", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "teams-lossless-attachment-edit")

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	writeCfg, err := DefaultEffectiveAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveAuthConfig error: %v", err)
	}
	fileCfg, err := DefaultEffectiveFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveFileWriteAuthConfig error: %v", err)
	}
	writeGraph := NewGraphClient(NewAuthManager(writeCfg), io.Discard)
	fileGraph := NewGraphClient(NewAuthManager(fileCfg), io.Discard)

	me, err := writeGraph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me failed: %v", err)
	}
	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	chat, err := writeGraph.CreateSingleMemberGroupChat(ctx, me.ID, "Codex Lossless Edit "+nonce)
	if err != nil {
		t.Fatalf("create live lossless edit chat failed: %v", err)
	}
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, writeGraph, chat.ID)
	if refreshed, err := writeGraph.GetChat(ctx, chat.ID); err == nil && strings.TrimSpace(refreshed.WebURL) != "" {
		chat = refreshed
	}
	t.Logf("LIVE_LOSSLESS_EDIT_CHAT_ID=%s", chat.ID)
	t.Logf("LIVE_LOSSLESS_EDIT_CHAT_URL=%s", chat.WebURL)

	base, err := writeGraph.SendHTML(ctx, chat.ID, "<p>lossless edit quoted source "+htmlText(nonce)+"</p>")
	if err != nil {
		t.Fatalf("send quoted source failed: %v", err)
	}
	if msg, err := sendLiveMessageReferenceFixture(ctx, writeGraph, chat.ID, base, me); err != nil {
		t.Logf("LIVE_LOSSLESS_EDIT_MESSAGE_REFERENCE_CREATE=unsupported err=%v", err)
	} else {
		verifyLiveLosslessEdit(ctx, t, writeGraph, chat.ID, msg.ID, "messageReference")
	}
	time.Sleep(450 * time.Millisecond)

	fileName := "lossless-edit-" + nonce + ".txt"
	item, err := fileGraph.UploadSmallDriveItem(ctx, DefaultOutboundUploadFolder(), fileName, []byte("lossless edit fixture "+nonce+"\n"), "text/plain")
	if err != nil {
		t.Fatalf("upload lossless edit file fixture failed: %v", err)
	}
	meta, err := fileGraph.GetDriveItemMetadata(ctx, item.ID)
	if err != nil {
		t.Fatalf("read uploaded file metadata failed: %v", err)
	}
	fileMsg, err := fileGraph.SendDriveItemAttachment(ctx, chat.ID, meta, "lossless edit file fixture")
	if err != nil {
		t.Fatalf("send live file attachment fixture failed: %v", err)
	}
	verifyLiveLosslessEdit(ctx, t, writeGraph, chat.ID, fileMsg.ID, "referenceFile")
	time.Sleep(450 * time.Millisecond)

	sourceA, err := writeGraph.SendHTML(ctx, chat.ID, liveComplexQuoteSourceHTML("A", nonce))
	if err != nil {
		t.Fatalf("send complex quote source A failed: %v", err)
	}
	time.Sleep(450 * time.Millisecond)
	sourceB, err := writeGraph.SendHTML(ctx, chat.ID, liveComplexQuoteSourceHTML("B", nonce))
	if err != nil {
		t.Fatalf("send complex quote source B failed: %v", err)
	}
	time.Sleep(450 * time.Millisecond)

	complexQuoteBody := liveComplexAttachmentBody(nonce, []string{"quote-complex-1"})
	complexQuote, err := sendLiveMessageReferencesFixture(ctx, writeGraph, chat.ID, complexQuoteBody, []liveMessageReferenceFixture{{
		ID:      "quote-complex-1",
		Source:  sourceA,
		Preview: "complex quote preview A " + nonce,
	}}, me)
	if err != nil {
		t.Fatalf("send complex messageReference fixture failed: %v", err)
	}
	verifyLiveLosslessEdit(ctx, t, writeGraph, chat.ID, complexQuote.ID, "complexMessageReference")
	time.Sleep(450 * time.Millisecond)

	multiQuoteBody := liveComplexAttachmentBody(nonce, []string{"quote-multi-1", "quote-multi-2"})
	multiQuote, err := sendLiveMessageReferencesFixture(ctx, writeGraph, chat.ID, multiQuoteBody, []liveMessageReferenceFixture{
		{ID: "quote-multi-1", Source: sourceA, Preview: "multi quote preview A " + nonce},
		{ID: "quote-multi-2", Source: sourceB, Preview: "multi quote preview B " + nonce},
	}, me)
	if err != nil {
		t.Fatalf("send multi-messageReference stress fixture failed: %v", err)
	}
	verifyLiveLosslessEdit(ctx, t, writeGraph, chat.ID, multiQuote.ID, "multiMessageReference")
	time.Sleep(450 * time.Millisecond)

	complexFileName := "lossless-edit-complex-" + nonce + ".txt"
	complexFile, err := fileGraph.UploadSmallDriveItem(ctx, DefaultOutboundUploadFolder(), complexFileName, []byte("lossless complex file fixture "+nonce+"\n"), "text/plain")
	if err != nil {
		t.Fatalf("upload complex lossless file fixture failed: %v", err)
	}
	complexFileMeta, err := fileGraph.GetDriveItemMetadata(ctx, complexFile.ID)
	if err != nil {
		t.Fatalf("read complex uploaded file metadata failed: %v", err)
	}
	mentionText := strings.TrimSpace(firstNonEmptyString(me.DisplayName, me.UserPrincipalName, "owner"))
	complexFileBody := `<p><at id="0">` + htmlText(mentionText) + `</at> mention before complex attachment body.</p>` + liveComplexAttachmentBody(nonce, []string{driveItemAttachmentID(complexFileMeta)})
	complexFileMsg, err := sendLiveReferenceFileFixture(ctx, fileGraph, chat.ID, complexFileMeta, complexFileBody, []ChatMention{{ID: 0, Text: mentionText, User: me}})
	if err != nil {
		t.Fatalf("send complex reference file stress fixture failed: %v", err)
	}
	verifyLiveLosslessEdit(ctx, t, writeGraph, chat.ID, complexFileMsg.ID, "complexReferenceFileWithMention")
	time.Sleep(450 * time.Millisecond)

	mixedFileName := "lossless-edit-mixed-" + nonce + ".txt"
	mixedFile, err := fileGraph.UploadSmallDriveItem(ctx, DefaultOutboundUploadFolder(), mixedFileName, []byte("lossless mixed file fixture "+nonce+"\n"), "text/plain")
	if err != nil {
		t.Fatalf("upload mixed lossless file fixture failed: %v", err)
	}
	mixedFileMeta, err := fileGraph.GetDriveItemMetadata(ctx, mixedFile.ID)
	if err != nil {
		t.Fatalf("read mixed uploaded file metadata failed: %v", err)
	}
	mixedBody := liveComplexAttachmentBody(nonce, []string{"quote-mixed-1", driveItemAttachmentID(mixedFileMeta)})
	mixedMsg, err := sendLiveMixedReferenceAndFileFixture(ctx, fileGraph, chat.ID, mixedBody, liveMessageReferenceFixture{
		ID:      "quote-mixed-1",
		Source:  sourceA,
		Preview: "mixed quote preview " + nonce,
	}, mixedFileMeta, me)
	if err != nil {
		t.Fatalf("send mixed messageReference/reference stress fixture failed: %v", err)
	}
	verifyLiveLosslessEdit(ctx, t, writeGraph, chat.ID, mixedMsg.ID, "mixedMessageReferenceAndFile")
}

func sendLiveMessageReferenceFixture(ctx context.Context, graph *GraphClient, chatID string, source ChatMessage, me User) (ChatMessage, error) {
	return sendLiveMessageReferencesFixture(ctx, graph, chatID, `<p>lossless edit quote fixture</p><attachment id="quote-1"></attachment>`, []liveMessageReferenceFixture{{
		ID:      "quote-1",
		Source:  source,
		Preview: "lossless edit quoted source preview",
	}}, me)
}

type liveMessageReferenceFixture struct {
	ID      string
	Source  ChatMessage
	Preview string
}

func sendLiveMessageReferencesFixture(ctx context.Context, graph *GraphClient, chatID string, bodyHTML string, refs []liveMessageReferenceFixture, me User) (ChatMessage, error) {
	attachments := make([]map[string]any, 0, len(refs))
	for _, ref := range refs {
		content := fmt.Sprintf(
			`{"messageId":%q,"messagePreview":%q,"messageSender":{"user":{"id":%q,"displayName":%q}}}`,
			ref.Source.ID,
			ref.Preview,
			me.ID,
			firstNonEmptyString(me.DisplayName, me.UserPrincipalName, "owner"),
		)
		attachments = append(attachments, map[string]any{
			"id":          ref.ID,
			"contentType": "messageReference",
			"content":     content,
		})
	}
	body := map[string]any{
		"body": map[string]any{
			"contentType": "html",
			"content":     bodyHTML,
		},
		"attachments": attachments,
	}
	var msg ChatMessage
	err := graph.do(ctx, http.MethodPost, "/chats/"+url.PathEscape(chatID)+"/messages", body, &msg)
	return msg, err
}

func sendLiveReferenceFileFixture(ctx context.Context, graph *GraphClient, chatID string, item DriveItem, bodyHTML string, mentions []ChatMention) (ChatMessage, error) {
	attachmentID := driveItemAttachmentID(item)
	if attachmentID == "" {
		return ChatMessage{}, fmt.Errorf("drive item %q has no eTag GUID for Teams attachment id", strings.TrimSpace(item.ID))
	}
	contentURL := strings.TrimSpace(firstNonEmptyString(item.WebDavURL, item.WebURL))
	if contentURL == "" {
		return ChatMessage{}, fmt.Errorf("drive item %q has no webDavUrl or webUrl", strings.TrimSpace(item.ID))
	}
	body := map[string]any{
		"body": map[string]any{
			"contentType": "html",
			"content":     bodyHTML,
		},
		"attachments": []map[string]any{{
			"id":          attachmentID,
			"contentType": "reference",
			"contentUrl":  contentURL,
			"name":        strings.TrimSpace(firstNonEmptyString(item.Name, "attachment")),
		}},
	}
	if len(mentions) > 0 {
		graphMentions, err := liveGraphMentionPayload(mentions)
		if err != nil {
			return ChatMessage{}, err
		}
		body["mentions"] = graphMentions
	}
	var msg ChatMessage
	err := graph.do(ctx, http.MethodPost, "/chats/"+url.PathEscape(chatID)+"/messages", body, &msg)
	return msg, err
}

func sendLiveMixedReferenceAndFileFixture(ctx context.Context, graph *GraphClient, chatID string, bodyHTML string, ref liveMessageReferenceFixture, item DriveItem, me User) (ChatMessage, error) {
	fileID := driveItemAttachmentID(item)
	if fileID == "" {
		return ChatMessage{}, fmt.Errorf("drive item %q has no eTag GUID for Teams attachment id", strings.TrimSpace(item.ID))
	}
	contentURL := strings.TrimSpace(firstNonEmptyString(item.WebDavURL, item.WebURL))
	if contentURL == "" {
		return ChatMessage{}, fmt.Errorf("drive item %q has no webDavUrl or webUrl", strings.TrimSpace(item.ID))
	}
	refContent := fmt.Sprintf(
		`{"messageId":%q,"messagePreview":%q,"messageSender":{"user":{"id":%q,"displayName":%q}}}`,
		ref.Source.ID,
		ref.Preview,
		me.ID,
		firstNonEmptyString(me.DisplayName, me.UserPrincipalName, "owner"),
	)
	body := map[string]any{
		"body": map[string]any{
			"contentType": "html",
			"content":     bodyHTML,
		},
		"attachments": []map[string]any{
			{
				"id":          ref.ID,
				"contentType": "messageReference",
				"content":     refContent,
			},
			{
				"id":          fileID,
				"contentType": "reference",
				"contentUrl":  contentURL,
				"name":        strings.TrimSpace(firstNonEmptyString(item.Name, "attachment")),
			},
		},
	}
	var msg ChatMessage
	err := graph.do(ctx, http.MethodPost, "/chats/"+url.PathEscape(chatID)+"/messages", body, &msg)
	return msg, err
}

func liveGraphMentionPayload(mentions []ChatMention) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(mentions))
	for _, mention := range mentions {
		if strings.TrimSpace(mention.User.ID) == "" {
			return nil, fmt.Errorf("mention user id is required")
		}
		text := strings.TrimSpace(mention.Text)
		if text == "" {
			text = firstNonEmptyString(mention.User.DisplayName, mention.User.UserPrincipalName, "owner")
		}
		out = append(out, map[string]any{
			"id":          mention.ID,
			"mentionText": text,
			"mentioned": map[string]any{
				"user": map[string]any{
					"id":               mention.User.ID,
					"displayName":      firstNonEmptyString(mention.User.DisplayName, text),
					"userIdentityType": "aadUser",
				},
			},
		})
	}
	return out, nil
}

func liveComplexQuoteSourceHTML(label string, nonce string) string {
	return strings.Join([]string{
		`<p><strong>Source ` + htmlText(label) + `</strong> for ` + htmlText(nonce) + `</p>`,
		`<blockquote><p>nested quote source ` + htmlText(label) + `</p></blockquote>`,
		`<ul><li>source bullet one</li><li>source bullet two with <code>x|y</code></li></ul>`,
	}, "")
}

func liveComplexAttachmentBody(nonce string, attachmentIDs []string) string {
	var b strings.Builder
	b.WriteString(`<p><strong>Stress heading ` + htmlText(nonce) + `</strong> with <em>emphasis</em>, <s>strike</s>, <code>x|y &amp; z</code>, and <a href="https://example.com/a?x=1&amp;y=2">safe link</a>.</p>`)
	b.WriteString(`<blockquote><p>quoted block before attachment</p>`)
	if len(attachmentIDs) > 0 {
		b.WriteString(`<attachment id="` + htmlText(attachmentIDs[0]) + `"></attachment>`)
	}
	b.WriteString(`</blockquote>`)
	b.WriteString(`<ul><li>bullet alpha</li><li>bullet beta with <code>&lt;tag&gt;</code></li></ul>`)
	b.WriteString(`<ol><li>ordered one</li><li>ordered two</li></ol>`)
	b.WriteString(`<pre><code>line 1&#10;line 2 with | and &lt;xml&gt;</code></pre>`)
	b.WriteString(`<table><thead><tr><th>Column</th><th>Value</th></tr></thead><tbody><tr><td>A</td><td>1 &amp; 2</td></tr><tr><td>B</td><td><code>cell|code</code></td></tr></tbody></table>`)
	for i, id := range attachmentIDs[1:] {
		b.WriteString(`<p>extra attachment ` + fmt.Sprint(i+2) + `</p><attachment id="` + htmlText(id) + `"></attachment>`)
	}
	return b.String()
}

func verifyLiveLosslessEdit(ctx context.Context, t *testing.T, graph *GraphClient, chatID string, messageID string, label string) {
	t.Helper()
	original, err := graph.GetMessage(ctx, chatID, messageID)
	if err != nil {
		t.Fatalf("%s original GET failed: %v", label, err)
	}
	if len(original.Attachments) == 0 {
		t.Fatalf("%s original message has no attachments: %#v", label, original)
	}
	originalBody := strings.TrimSpace(original.Body.Content)
	originalPlaceholders := attachmentPlaceholderIDs(originalBody)
	originalPlaceholderOrder := attachmentPlaceholderIDsInOrder(originalBody)
	originalAttachments := liveAttachmentSignatures(original.Attachments)
	originalMentions := liveMentionSignatures(original.Mentions)
	originalFormat := liveBodyFormatSignature(t, originalBody, false)
	originalPlain := normalizeLivePlainForFormat(PlainTextFromTeamsHTML(originalBody))
	updatedBody := `<p><strong>` + htmlText(incomingUserLabel()) + `:</strong></p>` + originalBody
	original.Body.ContentType = "html"
	bridge := &Bridge{graph: graph}
	outcome := bridge.applyTeamsMessageEdit(ctx, chatID, original, updatedBody, teamsMessageEditOptions{
		ProtectAttachmentContext: true,
	})
	if !outcome.Applied {
		t.Fatalf("%s lossless PATCH outcome = %#v, want applied", label, outcome)
	}
	after, err := graph.GetMessage(ctx, chatID, messageID)
	if err != nil {
		t.Fatalf("%s after PATCH GET failed: %v", label, err)
	}
	if plain := PlainTextFromTeamsHTML(after.Body.Content); !strings.Contains(plain, incomingUserLabel()+":") {
		t.Fatalf("%s patched plain text missing user label: %q", label, plain)
	}
	if got := attachmentPlaceholderIDs(after.Body.Content); !sameStringSet(got, originalPlaceholders) {
		t.Fatalf("%s placeholders changed: before=%v after=%v body=%q", label, originalPlaceholders, got, after.Body.Content)
	}
	if got := attachmentPlaceholderIDsInOrder(after.Body.Content); !sameStringList(got, originalPlaceholderOrder) {
		t.Fatalf("%s placeholder order changed: before=%v after=%v body=%q", label, originalPlaceholderOrder, got, after.Body.Content)
	}
	if got := liveAttachmentSignatures(after.Attachments); !sameStringSet(got, originalAttachments) {
		t.Fatalf("%s attachments changed:\nbefore=%v\nafter=%v", label, originalAttachments, got)
	}
	if got := liveMentionSignatures(after.Mentions); !sameStringSet(got, originalMentions) {
		t.Fatalf("%s mentions changed:\nbefore=%v\nafter=%v", label, originalMentions, got)
	}
	afterCore := liveBodyAfterUserAnnotationCore(after.Body.Content)
	afterFormat := liveBodyFormatSignature(t, after.Body.Content, true)
	if !sameStringList(afterFormat, originalFormat) {
		t.Fatalf("%s body format signature changed:\nbefore=%v\nafter=%v\nbody=%q", label, originalFormat, afterFormat, after.Body.Content)
	}
	if got := normalizeLivePlainForFormat(stripUserAnnotationPrefix(PlainTextFromTeamsHTML(after.Body.Content))); got != originalPlain {
		t.Fatalf("%s body plain text changed:\nbefore=%q\nafter=%q\nbody=%q", label, originalPlain, got, after.Body.Content)
	}
	t.Logf("LIVE_LOSSLESS_EDIT_RESULT label=%s message=%s raw_core_equal=%v attachments=%v mentions=%v placeholders=%v", label, messageID, strings.TrimSpace(afterCore) == originalBody, originalAttachments, originalMentions, originalPlaceholderOrder)
}

var attachmentPlaceholderPattern = regexp.MustCompile(`(?i)<attachment\b[^>]*\bid=["']?([^"'\s>/]+)["']?[^>]*>`)

func attachmentPlaceholderIDs(content string) []string {
	var out []string
	for _, match := range attachmentPlaceholderPattern.FindAllStringSubmatch(content, -1) {
		if len(match) > 1 && strings.TrimSpace(match[1]) != "" {
			out = append(out, strings.TrimSpace(match[1]))
		}
	}
	sort.Strings(out)
	return out
}

func attachmentPlaceholderIDsInOrder(content string) []string {
	var out []string
	for _, match := range attachmentPlaceholderPattern.FindAllStringSubmatch(content, -1) {
		if len(match) > 1 && strings.TrimSpace(match[1]) != "" {
			out = append(out, strings.TrimSpace(match[1]))
		}
	}
	return out
}

func liveAttachmentSignatures(attachments []MessageAttachment) []string {
	out := make([]string, 0, len(attachments))
	for _, attachment := range attachments {
		out = append(out, strings.Join([]string{
			strings.TrimSpace(attachment.ID),
			strings.TrimSpace(attachment.ContentType),
			strings.TrimSpace(attachment.ContentURL),
			strings.TrimSpace(attachment.Name),
			strings.TrimSpace(attachment.Content),
		}, "\x1f"))
	}
	sort.Strings(out)
	return out
}

func liveMentionSignatures(mentions []json.RawMessage) []string {
	out := make([]string, 0, len(mentions))
	for _, mention := range mentions {
		out = append(out, strings.TrimSpace(string(mention)))
	}
	sort.Strings(out)
	return out
}

func liveBodyAfterUserAnnotationCore(content string) string {
	content = strings.TrimSpace(content)
	prefix := `<p><strong>` + htmlText(incomingUserLabel()) + `:</strong></p>`
	if strings.HasPrefix(content, prefix) {
		return strings.TrimSpace(strings.TrimPrefix(content, prefix))
	}
	return content
}

func liveBodyFormatSignature(t *testing.T, content string, skipUserAnnotation bool) []string {
	t.Helper()
	root, err := xhtml.Parse(strings.NewReader("<div>" + content + "</div>"))
	if err != nil {
		t.Fatalf("parse live body HTML: %v\n%s", err, content)
	}
	var out []string
	skippedUserAnnotation := false
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n == nil {
			return
		}
		if n.Type == xhtml.ElementNode {
			if skipUserAnnotation && !skippedUserAnnotation && strings.EqualFold(n.Data, "p") && strings.TrimSpace(liveNodeText(n)) == incomingUserLabel()+":" {
				skippedUserAnnotation = true
				return
			}
			tag := strings.ToLower(n.Data)
			switch tag {
			case "a":
				out = append(out, "a[href="+liveNodeAttr(n, "href")+"]")
			case "attachment":
				out = append(out, "attachment[id="+liveNodeAttr(n, "id")+"]")
			case "at":
				out = append(out, "at[id="+liveNodeAttr(n, "id")+"]")
			case "blockquote", "br", "code", "codeblock", "div", "em", "i", "li", "ol", "p", "pre", "s", "strike", "strong", "table", "tbody", "td", "th", "thead", "tr", "u", "ul":
				out = append(out, tag)
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return out
}

func liveNodeText(n *xhtml.Node) string {
	var b strings.Builder
	var walk func(*xhtml.Node)
	walk = func(cur *xhtml.Node) {
		if cur == nil {
			return
		}
		if cur.Type == xhtml.TextNode {
			b.WriteString(cur.Data)
		}
		for child := cur.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(n)
	return b.String()
}

func liveNodeAttr(n *xhtml.Node, key string) string {
	for _, attr := range n.Attr {
		if strings.EqualFold(attr.Key, key) {
			return strings.TrimSpace(attr.Val)
		}
	}
	return ""
}

func normalizeLivePlainForFormat(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func sameStringSet(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func sameStringList(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func htmlText(text string) string {
	text = strings.ReplaceAll(text, "&", "&amp;")
	text = strings.ReplaceAll(text, "<", "&lt;")
	text = strings.ReplaceAll(text, ">", "&gt;")
	return text
}
