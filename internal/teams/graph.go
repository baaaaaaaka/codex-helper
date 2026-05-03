package teams

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	graphBaseURL          = "https://graph.microsoft.com/v1.0"
	defaultGraphRetries   = 3
	defaultBackoffBase    = 200 * time.Millisecond
	defaultBackoffMax     = 2 * time.Second
	maxHostedContentBytes = 20 << 20
	maxSharedFileBytes    = 20 << 20
	maxDriveItemJSONBytes = 2 << 20
)

var guidPattern = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

type graphAuth interface {
	AccessToken(ctx context.Context, out io.Writer, forceLogin bool) (string, error)
	RefreshAccessToken(ctx context.Context) (string, error)
}

type GraphClient struct {
	auth       graphAuth
	client     *http.Client
	out        io.Writer
	baseURL    string
	maxRetries int
	backoffMin time.Duration
	backoffMax time.Duration
	sleep      func(context.Context, time.Duration) error
	jitter     func(time.Duration) time.Duration
}

type User struct {
	ID                string `json:"id"`
	DisplayName       string `json:"displayName"`
	UserPrincipalName string `json:"userPrincipalName"`
}

type Chat struct {
	ID              string `json:"id"`
	Topic           string `json:"topic"`
	ChatType        string `json:"chatType"`
	CreatedDateTime string `json:"createdDateTime"`
	WebURL          string `json:"webUrl"`
}

type OnlineMeeting struct {
	ID         string `json:"id"`
	Subject    string `json:"subject"`
	JoinWebURL string `json:"joinWebUrl"`
	ChatInfo   struct {
		ThreadID  string `json:"threadId"`
		MessageID string `json:"messageId,omitempty"`
	} `json:"chatInfo"`
}

type ChatMember struct {
	ID          string   `json:"id"`
	Roles       []string `json:"roles,omitempty"`
	DisplayName string   `json:"displayName,omitempty"`
	Email       string   `json:"email,omitempty"`
	UserID      string   `json:"userId,omitempty"`
}

type ChatMessage struct {
	ID                   string `json:"id"`
	CreatedDateTime      string `json:"createdDateTime"`
	LastModifiedDateTime string `json:"lastModifiedDateTime"`
	MessageType          string `json:"messageType"`
	From                 struct {
		User *struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		} `json:"user"`
	} `json:"from"`
	Body struct {
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
	} `json:"body"`
	Attachments []MessageAttachment `json:"attachments,omitempty"`
}

type MessageAttachment struct {
	ID          string `json:"id,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	ContentURL  string `json:"contentUrl,omitempty"`
	Name        string `json:"name,omitempty"`
}

type HostedContentValue struct {
	Bytes       []byte
	ContentType string
}

type DriveItem struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	ETag      string `json:"eTag"`
	WebURL    string `json:"webUrl"`
	WebDavURL string `json:"webDavUrl"`
}

type MessageWindow struct {
	Messages  []ChatMessage
	Truncated bool
	NextPath  string
}

type ChatMention struct {
	ID   int
	Text string
	User User
}

type OpenURLCardAction struct {
	Title string
	URL   string
}

type GraphStatusError struct {
	Method     string
	Path       string
	StatusCode int
	Code       string
	Message    string
	RetryAfter time.Duration
}

func (e *GraphStatusError) Error() string {
	detail := ""
	if e.Code != "" {
		detail = ": code=" + e.Code
	}
	if e.Message != "" {
		detail += ": " + e.Message
	}
	return fmt.Sprintf("Graph %s %s failed: HTTP %d %s%s", e.Method, redactGraphPath(pathWithoutQuery(e.Path)), e.StatusCode, http.StatusText(e.StatusCode), detail)
}

func NewGraphClient(auth *AuthManager, out io.Writer) *GraphClient {
	return newGraphClient(auth, out)
}

func NewGraphClientWithHTTPClient(auth *AuthManager, out io.Writer, client *http.Client) *GraphClient {
	return newGraphClientWithHTTPClient(auth, out, client)
}

func NewReadGraphClient(out io.Writer) (*GraphClient, error) {
	return NewReadGraphClientWithHTTPClient(out, nil)
}

func NewReadGraphClientWithHTTPClient(out io.Writer, client *http.Client) (*GraphClient, error) {
	cfg, err := DefaultEffectiveReadAuthConfig()
	if err != nil {
		return nil, err
	}
	return newGraphClientWithHTTPClient(newNonInteractiveAuthManagerWithHTTPClient(cfg, client, "Teams message read", loginCommandForAuthCache(cfg.CachePath, "codex-proxy teams auth read")), out, client), nil
}

func newGraphClient(auth graphAuth, out io.Writer) *GraphClient {
	return newGraphClientWithHTTPClient(auth, out, nil)
}

func newGraphClientWithHTTPClient(auth graphAuth, out io.Writer, client *http.Client) *GraphClient {
	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}
	return &GraphClient{
		auth:       auth,
		client:     client,
		out:        out,
		baseURL:    graphBaseURL,
		maxRetries: defaultGraphRetries,
		backoffMin: defaultBackoffBase,
		backoffMax: defaultBackoffMax,
		sleep:      sleepContext,
		jitter:     jitterDuration,
	}
}

func (g *GraphClient) Me(ctx context.Context) (User, error) {
	var user User
	err := g.do(ctx, http.MethodGet, "/me?$select=id,displayName,userPrincipalName", nil, &user)
	return user, err
}

func (g *GraphClient) ListChats(ctx context.Context, top int) ([]Chat, error) {
	if top <= 0 || top > 50 {
		top = 50
	}
	var payload struct {
		Value []Chat `json:"value"`
	}
	err := g.do(ctx, http.MethodGet, "/me/chats?$top="+strconv.Itoa(top), nil, &payload)
	return payload.Value, err
}

func (g *GraphClient) CreateSingleMemberGroupChat(ctx context.Context, userID string, topic string) (Chat, error) {
	body := map[string]any{
		"chatType": "group",
		"topic":    SanitizeTopic(topic),
		"members": []map[string]any{
			{
				"@odata.type":     "#microsoft.graph.aadUserConversationMember",
				"roles":           []string{"owner"},
				"user@odata.bind": fmt.Sprintf("https://graph.microsoft.com/v1.0/users('%s')", userID),
			},
		},
	}
	var chat Chat
	err := g.do(ctx, http.MethodPost, "/chats", body, &chat)
	return chat, err
}

func (g *GraphClient) CreateMeetingChat(ctx context.Context, topic string) (Chat, error) {
	subject := SanitizeTopic(topic)
	now := time.Now().UTC()
	body := map[string]any{
		"subject":       subject,
		"startDateTime": now.Format(time.RFC3339),
		"endDateTime":   now.Add(24 * time.Hour).Format(time.RFC3339),
	}
	var meeting OnlineMeeting
	if err := g.do(ctx, http.MethodPost, "/me/onlineMeetings", body, &meeting); err != nil {
		return Chat{}, err
	}
	threadID := strings.TrimSpace(meeting.ChatInfo.ThreadID)
	if threadID == "" {
		return Chat{}, fmt.Errorf("onlineMeeting response did not include chatInfo.threadId")
	}
	webURL := TeamsChatURL(threadID, g.tenantID())
	if webURL == "" {
		webURL = meeting.JoinWebURL
	}
	return Chat{
		ID:       threadID,
		Topic:    firstNonEmptyString(SanitizeTopic(meeting.Subject), subject),
		ChatType: "meeting",
		WebURL:   webURL,
	}, nil
}

type graphAuthWithTenant interface {
	TenantID() string
}

func (g *GraphClient) tenantID() string {
	if g == nil || g.auth == nil {
		return ""
	}
	if auth, ok := g.auth.(graphAuthWithTenant); ok {
		return strings.TrimSpace(auth.TenantID())
	}
	return ""
}

func (g *GraphClient) GetChat(ctx context.Context, chatID string) (Chat, error) {
	var chat Chat
	err := g.do(ctx, http.MethodGet, "/chats/"+url.PathEscape(chatID)+"?$select=id,topic,chatType,webUrl", nil, &chat)
	return chat, err
}

func (g *GraphClient) ListChatMembers(ctx context.Context, chatID string) ([]ChatMember, error) {
	var payload struct {
		Value []ChatMember `json:"value"`
	}
	err := g.do(ctx, http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/members", nil, &payload)
	return payload.Value, err
}

func (g *GraphClient) SendHTML(ctx context.Context, chatID string, html string) (ChatMessage, error) {
	body := map[string]any{
		"body": map[string]any{
			"contentType": "html",
			"content":     html,
		},
	}
	var msg ChatMessage
	err := g.do(ctx, http.MethodPost, "/chats/"+url.PathEscape(chatID)+"/messages", body, &msg)
	return msg, err
}

func (g *GraphClient) SendHTMLWithMentions(ctx context.Context, chatID string, html string, mentions []ChatMention) (ChatMessage, error) {
	body := map[string]any{
		"body": map[string]any{
			"contentType": "html",
			"content":     html,
		},
	}
	if len(mentions) > 0 {
		graphMentions := make([]map[string]any, 0, len(mentions))
		for _, mention := range mentions {
			if strings.TrimSpace(mention.User.ID) == "" {
				return ChatMessage{}, fmt.Errorf("mention user id is required")
			}
			text := strings.TrimSpace(mention.Text)
			if text == "" {
				text = firstNonEmptyString(mention.User.DisplayName, mention.User.UserPrincipalName, "owner")
			}
			graphMentions = append(graphMentions, map[string]any{
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
		body["mentions"] = graphMentions
	}
	var msg ChatMessage
	err := g.do(ctx, http.MethodPost, "/chats/"+url.PathEscape(chatID)+"/messages", body, &msg)
	return msg, err
}

func (g *GraphClient) SendOpenURLAdaptiveCard(ctx context.Context, chatID string, title string, text string, actions []OpenURLCardAction) (ChatMessage, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return ChatMessage{}, fmt.Errorf("chat id is required")
	}
	title = strings.TrimSpace(title)
	if title == "" {
		return ChatMessage{}, fmt.Errorf("card title is required")
	}
	cardActions := make([]map[string]any, 0, len(actions))
	for _, action := range actions {
		actionTitle := strings.TrimSpace(action.Title)
		actionURL := strings.TrimSpace(action.URL)
		if actionTitle == "" || actionURL == "" {
			return ChatMessage{}, fmt.Errorf("card action title and URL are required")
		}
		if !safeTeamsOpenURL(actionURL) {
			return ChatMessage{}, fmt.Errorf("refusing unsafe Teams card URL")
		}
		cardActions = append(cardActions, map[string]any{
			"type":  "Action.OpenUrl",
			"title": actionTitle,
			"url":   actionURL,
		})
	}
	if len(cardActions) == 0 {
		return ChatMessage{}, fmt.Errorf("at least one card action is required")
	}
	bodyBlocks := []map[string]any{
		{
			"type":   "TextBlock",
			"text":   title,
			"weight": "Bolder",
			"size":   "Medium",
			"wrap":   true,
		},
	}
	if trimmed := strings.TrimSpace(text); trimmed != "" {
		bodyBlocks = append(bodyBlocks, map[string]any{
			"type": "TextBlock",
			"text": trimmed,
			"wrap": true,
		})
	}
	card := map[string]any{
		"$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
		"type":    "AdaptiveCard",
		"version": "1.4",
		"body":    bodyBlocks,
		"actions": cardActions,
	}
	cardJSON, err := json.Marshal(card)
	if err != nil {
		return ChatMessage{}, err
	}
	message := map[string]any{
		"body": map[string]any{
			"contentType": "html",
			"content":     `<attachment id="card-1"></attachment>`,
		},
		"attachments": []map[string]any{
			{
				"id":          "card-1",
				"contentType": "application/vnd.microsoft.card.adaptive",
				"content":     string(cardJSON),
			},
		},
	}
	var msg ChatMessage
	err = g.do(ctx, http.MethodPost, "/chats/"+url.PathEscape(chatID)+"/messages", message, &msg)
	return msg, err
}

func safeTeamsOpenURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return parsed.Scheme == "https" && strings.EqualFold(parsed.Hostname(), "teams.microsoft.com")
}

func (g *GraphClient) UpdateChatMessageHTML(ctx context.Context, chatID string, messageID string, html string) error {
	body := map[string]any{
		"body": map[string]any{
			"contentType": "html",
			"content":     html,
		},
	}
	return g.do(ctx, http.MethodPatch, "/chats/"+url.PathEscape(chatID)+"/messages/"+url.PathEscape(messageID), body, nil)
}

func (g *GraphClient) UpdateChatTopic(ctx context.Context, chatID string, topic string) error {
	body := map[string]any{
		"topic": SanitizeTopic(topic),
	}
	return g.do(ctx, http.MethodPatch, "/chats/"+url.PathEscape(chatID), body, nil)
}

func (g *GraphClient) UnhideChatForUser(ctx context.Context, chatID string, user User) error {
	chatID = strings.TrimSpace(chatID)
	userID := strings.TrimSpace(user.ID)
	if chatID == "" {
		return fmt.Errorf("chat id is required")
	}
	if userID == "" {
		return fmt.Errorf("user id is required")
	}
	userBody := map[string]any{
		"id": userID,
	}
	if tenantID := strings.TrimSpace(g.tenantID()); tenantID != "" {
		userBody["tenantId"] = tenantID
	}
	body := map[string]any{
		"user": userBody,
	}
	return g.do(ctx, http.MethodPost, "/chats/"+url.PathEscape(chatID)+"/unhideForUser", body, nil)
}

func HTMLMessageMentioningOwner(prefix string, text string, owner User) (string, []ChatMention) {
	mentionText := strings.TrimSpace(firstNonEmptyString(owner.DisplayName, owner.UserPrincipalName, "owner"))
	mention := `<at id="0">` + html.EscapeString(mentionText) + `</at>`
	prefix = strings.TrimSpace(prefix)
	text = strings.TrimSpace(text)
	if prefix != "" {
		return "<p><strong>" + html.EscapeString(prefix) + ":</strong> " + mention + " " + html.EscapeString(text) + "</p>", []ChatMention{{
			ID:   0,
			Text: mentionText,
			User: owner,
		}}
	}
	return "<p>" + mention + " " + html.EscapeString(text) + "</p>", []ChatMention{{
		ID:   0,
		Text: mentionText,
		User: owner,
	}}
}

func (g *GraphClient) SendDriveItemAttachment(ctx context.Context, chatID string, item DriveItem, message string) (ChatMessage, error) {
	contentURL := strings.TrimSpace(firstNonEmptyString(item.WebDavURL, item.WebURL))
	if contentURL == "" {
		return ChatMessage{}, fmt.Errorf("drive item %q has no webDavUrl or webUrl", item.ID)
	}
	name := strings.TrimSpace(item.Name)
	if name == "" {
		name = "attachment"
	}
	attachmentID := driveItemAttachmentID(item)
	bodyText := html.EscapeString(helperAttachmentMessage(message))
	if bodyText != "" {
		bodyText += " "
	}
	body := map[string]any{
		"body": map[string]any{
			"contentType": "html",
			"content":     bodyText + `<attachment id="` + html.EscapeString(attachmentID) + `"></attachment>`,
		},
		"attachments": []map[string]any{{
			"id":          attachmentID,
			"contentType": "reference",
			"contentUrl":  contentURL,
			"name":        name,
		}},
	}
	var msg ChatMessage
	err := g.do(ctx, http.MethodPost, "/chats/"+url.PathEscape(chatID)+"/messages", body, &msg)
	return msg, err
}

func helperAttachmentMessage(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "file attached"
	}
	if IsHelperText(message) {
		return message
	}
	return "Codex: " + message
}

func (g *GraphClient) GetMessage(ctx context.Context, chatID string, messageID string) (ChatMessage, error) {
	var msg ChatMessage
	err := g.do(ctx, http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/messages/"+url.PathEscape(messageID), nil, &msg)
	return msg, err
}

func (g *GraphClient) GetHostedContentValue(ctx context.Context, chatID string, messageID string, hostedContentID string) (HostedContentValue, error) {
	path := "/chats/" + url.PathEscape(chatID) + "/messages/" + url.PathEscape(messageID) + "/hostedContents/" + url.PathEscape(hostedContentID) + "/$value"
	data, contentType, err := g.doRaw(ctx, http.MethodGet, path, maxHostedContentBytes)
	if err != nil {
		return HostedContentValue{}, err
	}
	return HostedContentValue{Bytes: data, ContentType: contentType}, nil
}

func (g *GraphClient) UploadSmallDriveItem(ctx context.Context, folder string, name string, data []byte, contentType string) (DriveItem, error) {
	path, err := meDriveRootContentPath(folder, name)
	if err != nil {
		return DriveItem{}, err
	}
	raw, err := g.doBytes(ctx, http.MethodPut, path, data, contentType, maxDriveItemJSONBytes)
	if err != nil {
		return DriveItem{}, err
	}
	var item DriveItem
	if err := json.Unmarshal(raw, &item); err != nil {
		return DriveItem{}, err
	}
	if item.ID == "" {
		return DriveItem{}, fmt.Errorf("drive upload response did not include item id")
	}
	return item, nil
}

func (g *GraphClient) GetDriveItemMetadata(ctx context.Context, itemID string) (DriveItem, error) {
	itemID = strings.TrimSpace(itemID)
	if itemID == "" {
		return DriveItem{}, fmt.Errorf("drive item id is required")
	}
	path := "/me/drive/items/" + url.PathEscape(itemID) + "?$select=id,name,eTag,webUrl,webDavUrl"
	var item DriveItem
	err := g.do(ctx, http.MethodGet, path, nil, &item)
	return item, err
}

func (g *GraphClient) GetSharedDriveItemContent(ctx context.Context, rawURL string) (HostedContentValue, error) {
	shareID := graphShareID(rawURL)
	if shareID == "" {
		return HostedContentValue{}, fmt.Errorf("sharing URL is required")
	}
	path := "/shares/" + url.PathEscape(shareID) + "/driveItem/content"
	data, contentType, err := g.doRaw(ctx, http.MethodGet, path, maxSharedFileBytes)
	if err != nil {
		return HostedContentValue{}, err
	}
	return HostedContentValue{Bytes: data, ContentType: contentType}, nil
}

func (g *GraphClient) ListMessages(ctx context.Context, chatID string, top int) ([]ChatMessage, error) {
	window, err := g.ListMessagesWindow(ctx, chatID, top, time.Time{})
	if err != nil {
		return nil, err
	}
	return window.Messages, nil
}

func (g *GraphClient) ListMessagesWindow(ctx context.Context, chatID string, top int, modifiedAfter time.Time) (MessageWindow, error) {
	path := chatMessagesPath(chatID, top, modifiedAfter)
	return g.ListMessagesWindowFromPath(ctx, path)
}

func (g *GraphClient) ListMessagesWindowFromPath(ctx context.Context, path string) (MessageWindow, error) {
	messages, next, err := g.listMessagesPage(ctx, path)
	if err != nil {
		return MessageWindow{}, err
	}
	out := MessageWindow{Messages: messages}
	if strings.TrimSpace(next) == "" {
		return out, nil
	}
	nextPath, err := g.relativeGraphPath(next)
	if err != nil {
		return MessageWindow{}, err
	}
	out.Truncated = true
	out.NextPath = nextPath
	return out, nil
}

func (g *GraphClient) listMessagesPage(ctx context.Context, path string) ([]ChatMessage, string, error) {
	var payload struct {
		Value    []ChatMessage `json:"value"`
		NextLink string        `json:"@odata.nextLink"`
	}
	if err := g.do(ctx, http.MethodGet, path, nil, &payload); err != nil {
		return nil, "", err
	}
	return payload.Value, payload.NextLink, nil
}

func chatMessagesPath(chatID string, top int, modifiedAfter time.Time) string {
	top = normalizedGraphMessagesTop(top)
	values := url.Values{}
	values.Set("$top", strconv.Itoa(top))
	if !modifiedAfter.IsZero() {
		values.Set("$orderby", "lastModifiedDateTime desc")
		values.Set("$filter", "lastModifiedDateTime gt "+modifiedAfter.UTC().Format(time.RFC3339Nano))
	}
	return "/chats/" + url.PathEscape(chatID) + "/messages?" + values.Encode()
}

func normalizedGraphMessagesTop(top int) int {
	const minTop = 10
	const defaultTop = 20
	const maxTop = 50
	if top <= 0 {
		return defaultTop
	}
	if top < minTop {
		return minTop
	}
	if top > maxTop {
		return maxTop
	}
	return top
}

func (g *GraphClient) relativeGraphPath(nextLink string) (string, error) {
	nextLink = strings.TrimSpace(nextLink)
	if nextLink == "" {
		return "", fmt.Errorf("Graph nextLink is empty")
	}
	if strings.HasPrefix(nextLink, "/") {
		return trimGraphBasePath(nextLink, g.baseURL), nil
	}
	nextURL, err := url.Parse(nextLink)
	if err != nil {
		return "", err
	}
	baseURL := g.baseURL
	if baseURL == "" {
		baseURL = graphBaseURL
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(nextURL.Scheme, base.Scheme) || !strings.EqualFold(nextURL.Host, base.Host) {
		return "", fmt.Errorf("Graph nextLink host mismatch")
	}
	return trimGraphBasePath(nextURL.RequestURI(), baseURL), nil
}

func trimGraphBasePath(requestURI string, baseURL string) string {
	if baseURL == "" {
		baseURL = graphBaseURL
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return requestURI
	}
	basePath := strings.TrimRight(base.EscapedPath(), "/")
	if basePath == "" || basePath == "/" {
		return requestURI
	}
	if requestURI == basePath {
		return "/"
	}
	if strings.HasPrefix(requestURI, basePath+"/") {
		return strings.TrimPrefix(requestURI, basePath)
	}
	return requestURI
}

func (g *GraphClient) do(ctx context.Context, method string, path string, body any, out any) error {
	if !isAllowedGraphRequest(method, path) {
		return fmt.Errorf("refusing non-allowlisted Graph request: %s %s", method, path)
	}
	var payload []byte
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = raw
	}
	token, err := g.auth.AccessToken(ctx, g.out, false)
	if err != nil {
		return err
	}

	retries := 0
	refreshedAfterUnauthorized := false
	for {
		req, err := http.NewRequestWithContext(ctx, method, g.graphURL(path), bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := g.httpClient().Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusUnauthorized && !refreshedAfterUnauthorized {
			discardAndClose(resp.Body)
			token, err = g.auth.RefreshAccessToken(ctx)
			if err != nil {
				return err
			}
			refreshedAfterUnauthorized = true
			continue
		}
		if shouldRetryGraphRequest(method, resp.StatusCode) && retries < g.retryLimit() {
			delay := g.retryDelay(resp, retries)
			discardAndClose(resp.Body)
			if err := g.sleepFor(ctx, delay); err != nil {
				return err
			}
			retries++
			continue
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		closeErr := resp.Body.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if resp.StatusCode >= 400 {
			return graphStatusError(method, path, resp, raw)
		}
		if out == nil || len(bytes.TrimSpace(raw)) == 0 {
			return nil
		}
		return json.Unmarshal(raw, out)
	}
}

func (g *GraphClient) doRaw(ctx context.Context, method string, path string, maxBytes int64) ([]byte, string, error) {
	if !isAllowedGraphRequest(method, path) {
		return nil, "", fmt.Errorf("refusing non-allowlisted Graph request: %s %s", method, path)
	}
	token, err := g.auth.AccessToken(ctx, g.out, false)
	if err != nil {
		return nil, "", err
	}

	retries := 0
	refreshedAfterUnauthorized := false
	for {
		req, err := http.NewRequestWithContext(ctx, method, g.graphURL(path), nil)
		if err != nil {
			return nil, "", err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := g.httpClient().Do(req)
		if err != nil {
			return nil, "", err
		}
		if resp.StatusCode == http.StatusUnauthorized && !refreshedAfterUnauthorized {
			discardAndClose(resp.Body)
			token, err = g.auth.RefreshAccessToken(ctx)
			if err != nil {
				return nil, "", err
			}
			refreshedAfterUnauthorized = true
			continue
		}
		if shouldRetryGraphRequest(method, resp.StatusCode) && retries < g.retryLimit() {
			delay := g.retryDelay(resp, retries)
			discardAndClose(resp.Body)
			if err := g.sleepFor(ctx, delay); err != nil {
				return nil, "", err
			}
			retries++
			continue
		}
		raw, err := readLimited(resp.Body, maxBytes)
		closeErr := resp.Body.Close()
		if err != nil {
			return nil, "", err
		}
		if closeErr != nil {
			return nil, "", closeErr
		}
		if resp.StatusCode >= 400 {
			return nil, "", graphStatusError(method, path, resp, raw)
		}
		return raw, resp.Header.Get("Content-Type"), nil
	}
}

func (g *GraphClient) doBytes(ctx context.Context, method string, path string, data []byte, contentType string, maxResponseBytes int64) ([]byte, error) {
	if !isAllowedGraphRequest(method, path) {
		return nil, fmt.Errorf("refusing non-allowlisted Graph request: %s %s", method, path)
	}
	token, err := g.auth.AccessToken(ctx, g.out, false)
	if err != nil {
		return nil, err
	}

	retries := 0
	refreshedAfterUnauthorized := false
	for {
		req, err := http.NewRequestWithContext(ctx, method, g.graphURL(path), bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := g.httpClient().Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusUnauthorized && !refreshedAfterUnauthorized {
			discardAndClose(resp.Body)
			token, err = g.auth.RefreshAccessToken(ctx)
			if err != nil {
				return nil, err
			}
			refreshedAfterUnauthorized = true
			continue
		}
		if shouldRetryGraphRequest(method, resp.StatusCode) && retries < g.retryLimit() {
			delay := g.retryDelay(resp, retries)
			discardAndClose(resp.Body)
			if err := g.sleepFor(ctx, delay); err != nil {
				return nil, err
			}
			retries++
			continue
		}
		raw, err := readLimited(resp.Body, maxResponseBytes)
		closeErr := resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if resp.StatusCode >= 400 {
			return nil, graphStatusError(method, path, resp, raw)
		}
		return raw, nil
	}
}

func isAllowedGraphRequest(method string, path string) bool {
	clean := pathWithoutQuery(path)
	if method == http.MethodGet && clean == "/me" {
		if q, ok := allowedGraphQuery(path); !ok || !allowedMeQuery(q) {
			return false
		}
		return true
	}
	if method == http.MethodGet && clean == "/me/chats" {
		q, ok := allowedGraphQuery(path)
		return ok && allowedListChatsQuery(q)
	}
	if method == http.MethodPost && clean == "/chats" {
		if q, ok := allowedGraphQuery(path); !ok || len(q) != 0 {
			return false
		}
		return true
	}
	if method == http.MethodPost && clean == "/me/onlineMeetings" {
		if q, ok := allowedGraphQuery(path); !ok || len(q) != 0 {
			return false
		}
		return true
	}
	if method == http.MethodPost && clean == "/me/onlineMeetings/createOrGet" {
		if q, ok := allowedGraphQuery(path); !ok || len(q) != 0 {
			return false
		}
		return true
	}
	if method == http.MethodPatch && isChatPath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && len(q) == 0
	}
	if method == http.MethodPost && isChatUnhideForUserPath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && len(q) == 0
	}
	if method == http.MethodPatch && isChatMessagePath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && len(q) == 0
	}
	if method == http.MethodGet && isChatPath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && allowedChatQuery(q)
	}
	if method == http.MethodGet && isChatMembersPath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && allowedChatMembersQuery(q)
	}
	if method == http.MethodGet && isChatMessagePath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && len(q) == 0
	}
	if method == http.MethodGet && isChatMessageHostedContentValuePath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && len(q) == 0
	}
	if method == http.MethodGet && isShareDriveItemContentPath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && len(q) == 0
	}
	if method == http.MethodPut && isMeDriveRootContentPath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && len(q) == 0
	}
	if method == http.MethodGet && isMeDriveItemMetadataPath(clean) {
		q, ok := allowedGraphQuery(path)
		return ok && allowedDriveItemMetadataQuery(q)
	}
	if !isChatMessagesPath(clean) {
		return false
	}
	q, ok := allowedGraphQuery(path)
	if !ok {
		return false
	}
	switch method {
	case http.MethodGet:
		return allowedMessagesQuery(q)
	case http.MethodPost:
		return len(q) == 0
	default:
		return false
	}
}

func pathWithoutQuery(path string) string {
	clean, _, _ := strings.Cut(path, "?")
	return clean
}

func redactGraphPath(path string) string {
	if strings.HasPrefix(path, "/me/drive/root:/") && strings.HasSuffix(path, ":/content") {
		return "/me/drive/root:/{path}:/content"
	}
	parts := strings.Split(path, "/")
	for i := 1; i < len(parts); i++ {
		switch parts[i-1] {
		case "chats":
			if parts[i] != "" {
				parts[i] = "{chat-id}"
			}
		case "messages":
			if parts[i] != "" {
				parts[i] = "{message-id}"
			}
		case "hostedContents":
			if parts[i] != "" && parts[i] != "$value" {
				parts[i] = "{hosted-content-id}"
			}
		case "shares":
			if parts[i] != "" {
				parts[i] = "{share-id}"
			}
		case "items":
			if parts[i] != "" {
				parts[i] = "{item-id}"
			}
		}
	}
	return strings.Join(parts, "/")
}

func retryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func (g *GraphClient) graphURL(path string) string {
	baseURL := g.baseURL
	if baseURL == "" {
		baseURL = graphBaseURL
	}
	return strings.TrimRight(baseURL, "/") + path
}

func (g *GraphClient) httpClient() *http.Client {
	if g.client != nil {
		return g.client
	}
	return http.DefaultClient
}

func (g *GraphClient) retryLimit() int {
	if g.maxRetries <= 0 {
		return defaultGraphRetries
	}
	return g.maxRetries
}

func (g *GraphClient) retryDelay(resp *http.Response, attempt int) time.Duration {
	if resp.StatusCode == http.StatusTooManyRequests {
		if delay := retryAfter(resp.Header.Get("Retry-After")); delay > 0 {
			return delay
		}
	}
	delay := boundedBackoff(g.backoffMin, g.backoffMax, attempt)
	if g.jitter != nil {
		return g.jitter(delay)
	}
	return jitterDuration(delay)
}

func (g *GraphClient) sleepFor(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if g.sleep != nil {
		return g.sleep(ctx, delay)
	}
	return sleepContext(ctx, delay)
}

func boundedBackoff(minDelay, maxDelay time.Duration, attempt int) time.Duration {
	if minDelay <= 0 {
		minDelay = defaultBackoffBase
	}
	if maxDelay <= 0 {
		maxDelay = defaultBackoffMax
	}
	delay := minDelay
	for i := 0; i < attempt && delay < maxDelay; i++ {
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
	return delay
}

func jitterDuration(delay time.Duration) time.Duration {
	if delay <= 1 {
		return delay
	}
	half := delay / 2
	if half <= 0 {
		return delay
	}
	n, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(half)+1))
	if err != nil {
		return delay
	}
	return half + time.Duration(n.Int64())
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func shouldRetryGraphStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500 && status <= 599
}

func shouldRetryGraphRequest(method string, status int) bool {
	if status == http.StatusTooManyRequests {
		return true
	}
	if status < 500 || status > 599 {
		return false
	}
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut:
		return true
	default:
		return false
	}
}

func discardAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 64<<10))
	_ = body.Close()
}

func readLimited(body io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = maxHostedContentBytes
	}
	raw, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("Graph response exceeds %d bytes", maxBytes)
	}
	return raw, nil
}

func graphStatusError(method string, path string, resp *http.Response, raw []byte) error {
	err := &GraphStatusError{
		Method:     method,
		Path:       path,
		StatusCode: resp.StatusCode,
		Code:       safeGraphErrorCode(raw),
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		err.RetryAfter = retryAfter(resp.Header.Get("Retry-After"))
	}
	return err
}

func safeGraphErrorCode(raw []byte) string {
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return ""
	}
	code := strings.TrimSpace(payload.Error.Code)
	if code == "" || len(code) > 80 {
		return ""
	}
	for _, r := range code {
		if !(r == '_' || r == '-' || r == '.' || r == '/' || r == ':' || r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z') {
			return ""
		}
	}
	return code
}

func safeGraphErrorMessage(raw []byte) string {
	var payload struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return ""
	}
	message := strings.Join(strings.Fields(payload.Error.Message), " ")
	if message == "" || len(message) > 240 {
		return ""
	}
	for _, r := range message {
		if r < 0x20 || r == '<' || r == '>' {
			return ""
		}
	}
	return message
}

func allowedGraphQuery(path string) (url.Values, bool) {
	_, query, _ := strings.Cut(path, "?")
	if strings.Contains(query, "#") {
		return nil, false
	}
	values, err := url.ParseQuery(query)
	if err != nil {
		return nil, false
	}
	return values, true
}

func allowedMeQuery(values url.Values) bool {
	if len(values) == 0 {
		return true
	}
	selectValues := values["$select"]
	return len(values) == 1 && len(selectValues) == 1 && selectValues[0] == "id,displayName,userPrincipalName"
}

func allowedListChatsQuery(values url.Values) bool {
	if len(values) == 0 {
		return true
	}
	topValues := values["$top"]
	if len(values) != 1 || len(topValues) != 1 {
		return false
	}
	top, err := strconv.Atoi(topValues[0])
	return err == nil && top > 0 && top <= 50
}

func allowedDriveItemMetadataQuery(values url.Values) bool {
	selectValues := values["$select"]
	return len(values) == 1 && len(selectValues) == 1 && selectValues[0] == "id,name,eTag,webUrl,webDavUrl"
}

func allowedChatQuery(values url.Values) bool {
	selectValues := values["$select"]
	return len(values) == 1 && len(selectValues) == 1 && selectValues[0] == "id,topic,chatType,webUrl"
}

func allowedChatMembersQuery(values url.Values) bool {
	return len(values) == 0
}

func allowedMessagesQuery(values url.Values) bool {
	if len(values) == 0 {
		return true
	}
	for key := range values {
		switch key {
		case "$top", "$orderby", "$filter", "$skiptoken":
		default:
			return false
		}
	}
	if topValues := values["$top"]; len(topValues) > 0 {
		if len(topValues) != 1 {
			return false
		}
		top, err := strconv.Atoi(topValues[0])
		if err != nil || top <= 0 || top > 50 {
			return false
		}
	}
	if orderValues := values["$orderby"]; len(orderValues) > 0 {
		if len(orderValues) != 1 || orderValues[0] != "lastModifiedDateTime desc" {
			return false
		}
	}
	if filterValues := values["$filter"]; len(filterValues) > 0 {
		if len(filterValues) != 1 || !allowedLastModifiedFilter(filterValues[0]) {
			return false
		}
		if orderValues := values["$orderby"]; len(orderValues) != 1 || orderValues[0] != "lastModifiedDateTime desc" {
			return false
		}
	}
	if skipValues := values["$skiptoken"]; len(skipValues) > 0 {
		if len(skipValues) != 1 || len(skipValues[0]) > 2048 || strings.ContainsAny(skipValues[0], "\r\n") {
			return false
		}
	}
	return true
}

func allowedLastModifiedFilter(value string) bool {
	value = strings.TrimSpace(value)
	const prefix = "lastModifiedDateTime gt "
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	raw := strings.TrimSpace(strings.TrimPrefix(value, prefix))
	if raw == "" || strings.ContainsAny(raw, "'\"()") {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, raw)
	return err == nil
}

func isChatMessagesPath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 4 || parts[0] != "" || parts[1] != "chats" || parts[2] == "" || parts[3] != "messages" {
		return false
	}
	return safeGraphDynamicID(parts[2])
}

func isChatPath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 3 || parts[0] != "" || parts[1] != "chats" || parts[2] == "" {
		return false
	}
	return safeGraphDynamicID(parts[2])
}

func isChatMembersPath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 4 || parts[0] != "" || parts[1] != "chats" || parts[2] == "" || parts[3] != "members" {
		return false
	}
	return safeGraphDynamicID(parts[2])
}

func isChatUnhideForUserPath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 4 || parts[0] != "" || parts[1] != "chats" || parts[2] == "" || parts[3] != "unhideForUser" {
		return false
	}
	return safeGraphDynamicID(parts[2])
}

func isChatMessagePath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 5 || parts[0] != "" || parts[1] != "chats" || parts[2] == "" || parts[3] != "messages" || parts[4] == "" {
		return false
	}
	return safeGraphDynamicID(parts[2]) && safeGraphDynamicID(parts[4])
}

func isChatMessageHostedContentValuePath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 8 || parts[0] != "" || parts[1] != "chats" || parts[2] == "" || parts[3] != "messages" || parts[4] == "" || parts[5] != "hostedContents" || parts[6] == "" || parts[7] != "$value" {
		return false
	}
	return safeGraphDynamicID(parts[2]) && safeGraphDynamicID(parts[4]) && safeGraphDynamicID(parts[6])
}

func isShareDriveItemContentPath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 5 || parts[0] != "" || parts[1] != "shares" || parts[2] == "" || parts[3] != "driveItem" || parts[4] != "content" {
		return false
	}
	return safeGraphDynamicID(parts[2])
}

func isMeDriveRootContentPath(path string) bool {
	if !strings.HasPrefix(path, "/me/drive/root:/") || !strings.HasSuffix(path, ":/content") {
		return false
	}
	inside := strings.TrimSuffix(strings.TrimPrefix(path, "/me/drive/root:/"), ":/content")
	return safeColonPathSegments(inside)
}

func isMeDriveItemMetadataPath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 5 || parts[0] != "" || parts[1] != "me" || parts[2] != "drive" || parts[3] != "items" || parts[4] == "" {
		return false
	}
	itemID, err := url.PathUnescape(parts[4])
	if err != nil {
		return false
	}
	return safeDriveItemID(itemID)
}

func safeGraphDynamicID(value string) bool {
	decoded, err := url.PathUnescape(value)
	if err != nil {
		return false
	}
	return safeDriveItemID(decoded)
}

func meDriveRootContentPath(folder string, name string) (string, error) {
	segments := safeDrivePathSegments(folder)
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("upload file name is required")
	}
	segments = append(segments, name)
	if len(segments) == 0 {
		return "", fmt.Errorf("upload path is required")
	}
	escaped := make([]string, 0, len(segments))
	for _, segment := range segments {
		if !safeDrivePathSegment(segment) {
			return "", fmt.Errorf("unsafe upload path segment %q", segment)
		}
		escaped = append(escaped, url.PathEscape(segment))
	}
	return "/me/drive/root:/" + strings.Join(escaped, "/") + ":/content", nil
}

func safeDrivePathSegments(folder string) []string {
	folder = strings.Trim(strings.TrimSpace(folder), "/\\")
	if folder == "" {
		return nil
	}
	parts := strings.FieldsFunc(folder, func(r rune) bool { return r == '/' || r == '\\' })
	segments := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			segments = append(segments, part)
		}
	}
	return segments
}

func safeColonPathSegments(path string) bool {
	if path == "" || strings.ContainsAny(path, "\r\n") {
		return false
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "" {
			return false
		}
		decoded, err := url.PathUnescape(segment)
		if err != nil || !safeDrivePathSegment(decoded) {
			return false
		}
	}
	return true
}

func safeDrivePathSegment(segment string) bool {
	segment = strings.TrimSpace(segment)
	if segment == "" || segment == "." || segment == ".." || len(segment) > 180 {
		return false
	}
	for _, r := range segment {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return !strings.ContainsAny(segment, `/\:*?"<>|`)
}

func safeDriveItemID(itemID string) bool {
	itemID = strings.TrimSpace(itemID)
	if itemID == "" || itemID == "." || itemID == ".." || len(itemID) > 512 {
		return false
	}
	if strings.ContainsAny(itemID, "/\\") {
		return false
	}
	for _, r := range itemID {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func graphShareID(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	return "u!" + base64.RawURLEncoding.EncodeToString([]byte(rawURL))
}

func driveItemAttachmentID(item DriveItem) string {
	if match := guidPattern.FindString(item.ETag); match != "" {
		return strings.ToLower(match)
	}
	if id := randomAttachmentID(); id != "" {
		return id
	}
	return "attachment-" + safePathPart(firstNonEmptyString(item.ID, item.Name, "file"))
}

func randomAttachmentID() string {
	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return ""
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
