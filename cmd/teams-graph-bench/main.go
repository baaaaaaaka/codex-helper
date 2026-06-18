package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"io"
	"os"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

const requiredConfirm = "create-live-teams-group-chat"

type benchSummary struct {
	StartedAt           string   `json:"started_at"`
	ElapsedMillis       int64    `json:"elapsed_millis"`
	Topic               string   `json:"topic"`
	ChatID              string   `json:"chat_id"`
	ChatURL             string   `json:"chat_url,omitempty"`
	OwnerUserID         string   `json:"owner_user_id"`
	TestUserID          string   `json:"test_user_id"`
	Members             []string `json:"members"`
	PlainMessageID      string   `json:"plain_message_id"`
	RequestMessageID    string   `json:"request_message_id"`
	QuotedAckMessageID  string   `json:"quoted_ack_message_id"`
	QuotedAckHasRef     bool     `json:"quoted_ack_has_reference_attachment"`
	RecentMessageCount  int      `json:"recent_message_count"`
	RecentContainsPlain bool     `json:"recent_contains_plain"`
	RecentContainsAck   bool     `json:"recent_contains_ack"`
}

type discoverSummary struct {
	Me         string               `json:"me"`
	Candidates []discoverCandidate  `json:"candidates"`
	Chats      []discoverChatResult `json:"chats"`
	Errors     []string             `json:"errors,omitempty"`
}

type discoverCandidate struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name,omitempty"`
	Email       string `json:"email,omitempty"`
	ChatID      string `json:"chat_id"`
	ChatTopic   string `json:"chat_topic,omitempty"`
	ChatType    string `json:"chat_type,omitempty"`
}

type discoverChatResult struct {
	ChatID   string   `json:"chat_id"`
	Topic    string   `json:"topic,omitempty"`
	ChatType string   `json:"chat_type,omitempty"`
	Members  []string `json:"members,omitempty"`
}

func main() {
	var (
		testUserID         = flag.String("test-user-id", strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_GROUP_TEST_USER_ID")), "Microsoft Graph user id for a disposable Teams test account")
		confirm            = flag.String("confirm", "", "must be "+requiredConfirm+" to create a real Teams group chat")
		nonce              = flag.String("nonce", time.Now().UTC().Format("20060102T150405Z"), "unique marker for chat topic and messages")
		timeout            = flag.Duration("timeout", 3*time.Minute, "overall timeout")
		discoverCandidates = flag.Bool("discover-candidates", false, "list non-self members from recent Teams chats and exit without writing")
		discoverTop        = flag.Int("discover-top", 20, "number of recent chats to scan for candidate members")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if *discoverCandidates {
		if err := discover(ctx, *discoverTop); err != nil {
			failf("%v", err)
		}
		return
	}

	if strings.TrimSpace(*confirm) != requiredConfirm {
		failf("--confirm %s is required; this bench creates a real Teams group chat", requiredConfirm)
	}
	if strings.TrimSpace(*testUserID) == "" {
		failf("--test-user-id or CODEX_HELPER_TEAMS_LIVE_GROUP_TEST_USER_ID is required")
	}

	start := time.Now()
	if err := run(ctx, benchInput{
		TestUserID: strings.TrimSpace(*testUserID),
		Nonce:      sanitizeMarker(*nonce),
		StartedAt:  start,
	}); err != nil {
		failf("%v", err)
	}
}

func discover(ctx context.Context, top int) error {
	readCfg, err := teams.DefaultReadAuthConfig()
	if err != nil {
		return fmt.Errorf("load Teams read auth config: %w", err)
	}
	graph := teams.NewGraphClient(teams.NewAuthManager(readCfg), io.Discard)
	me, err := graph.Me(ctx)
	if err != nil {
		return fmt.Errorf("Graph /me failed: %w", err)
	}
	chats, err := graph.ListChats(ctx, top)
	if err != nil {
		return fmt.Errorf("list chats: %w", err)
	}

	seen := map[string]bool{}
	summary := discoverSummary{Me: me.ID}
	for _, chat := range chats {
		members, err := graph.ListChatMembers(ctx, chat.ID)
		if err != nil {
			summary.Errors = append(summary.Errors, chat.ID+": "+err.Error())
			continue
		}
		chatResult := discoverChatResult{
			ChatID:   chat.ID,
			Topic:    chat.Topic,
			ChatType: chat.ChatType,
			Members:  memberSummaries(members),
		}
		summary.Chats = append(summary.Chats, chatResult)
		for _, member := range members {
			userID := strings.TrimSpace(member.UserID)
			if userID == "" || strings.EqualFold(userID, me.ID) || seen[userID] {
				continue
			}
			seen[userID] = true
			summary.Candidates = append(summary.Candidates, discoverCandidate{
				UserID:      userID,
				DisplayName: strings.TrimSpace(member.DisplayName),
				Email:       strings.TrimSpace(member.Email),
				ChatID:      chat.ID,
				ChatTopic:   chat.Topic,
				ChatType:    chat.ChatType,
			})
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		return err
	}
	if len(summary.Candidates) == 0 {
		return fmt.Errorf("no non-self chat members found in the %d most recent chats", len(chats))
	}
	return nil
}

type benchInput struct {
	TestUserID string
	Nonce      string
	StartedAt  time.Time
}

func run(ctx context.Context, input benchInput) error {
	writeCfg, err := teams.DefaultAuthConfig()
	if err != nil {
		return fmt.Errorf("load Teams chat-write auth config: %w", err)
	}
	readCfg, err := teams.DefaultReadAuthConfig()
	if err != nil {
		return fmt.Errorf("load Teams read auth config: %w", err)
	}
	writeGraph := teams.NewGraphClient(teams.NewAuthManager(writeCfg), io.Discard)
	readGraph := teams.NewGraphClient(teams.NewAuthManager(readCfg), io.Discard)

	me, err := writeGraph.Me(ctx)
	if err != nil {
		return fmt.Errorf("Graph /me failed: %w", err)
	}
	if strings.TrimSpace(me.ID) == "" {
		return fmt.Errorf("Graph /me returned no user id")
	}
	if strings.TrimSpace(me.ID) == input.TestUserID {
		return fmt.Errorf("test user id must be different from the signed-in user id")
	}

	topic := "Codex Graph Bench - " + input.Nonce
	chat, err := writeGraph.CreateGroupChat(ctx, topic, []teams.GroupChatMemberBinding{
		{UserID: me.ID, Roles: []string{"owner"}},
		{UserID: input.TestUserID, Roles: []string{"owner"}},
	})
	if err != nil {
		return fmt.Errorf("create group chat: %w", err)
	}
	if refreshed, err := readGraph.GetChat(ctx, chat.ID); err == nil {
		chat = refreshed
	}
	members, err := readGraph.ListChatMembers(ctx, chat.ID)
	if err != nil {
		return fmt.Errorf("list created chat members: %w", err)
	}
	if err := validateMembers(members, me.ID, input.TestUserID); err != nil {
		return err
	}

	plainText := "graph bench plain message " + input.Nonce
	plainMsg, err := writeGraph.SendHTML(ctx, chat.ID, "<p>"+html.EscapeString(plainText)+"</p>")
	if err != nil {
		return fmt.Errorf("send plain message: %w", err)
	}
	waitedPlain, err := waitForMessageID(ctx, readGraph, chat.ID, plainMsg.ID)
	if err != nil {
		return err
	}
	plainMsg = waitedPlain

	requestText := "please @codex validate graph bench " + input.Nonce
	requestMsg, err := writeGraph.SendHTML(ctx, chat.ID, "<p>"+html.EscapeString(requestText)+"</p>")
	if err != nil {
		return fmt.Errorf("send request message: %w", err)
	}
	waitedRequest, err := waitForMessageID(ctx, readGraph, chat.ID, requestMsg.ID)
	if err != nil {
		return err
	}
	requestMsg = waitedRequest

	ackHTML := "<p>Codex graph bench accepted " + html.EscapeString(input.Nonce) + "</p>"
	ackMsg, err := writeGraph.SendHTMLReplyWithQuote(ctx, chat.ID, requestMsg.ID, ackHTML, nil)
	if err != nil {
		return fmt.Errorf("send replyWithQuote ack: %w", err)
	}
	waitedAck, err := waitForMessageID(ctx, readGraph, chat.ID, ackMsg.ID)
	if err != nil {
		return err
	}
	ackMsg = waitedAck

	recent, err := readGraph.ListMessages(ctx, chat.ID, 20)
	if err != nil {
		return fmt.Errorf("list recent messages: %w", err)
	}
	summary := benchSummary{
		StartedAt:           input.StartedAt.UTC().Format(time.RFC3339Nano),
		ElapsedMillis:       time.Since(input.StartedAt).Milliseconds(),
		Topic:               topic,
		ChatID:              chat.ID,
		ChatURL:             chat.WebURL,
		OwnerUserID:         me.ID,
		TestUserID:          input.TestUserID,
		Members:             memberSummaries(members),
		PlainMessageID:      plainMsg.ID,
		RequestMessageID:    requestMsg.ID,
		QuotedAckMessageID:  ackMsg.ID,
		QuotedAckHasRef:     hasMessageReferenceAttachment(ackMsg),
		RecentMessageCount:  len(recent),
		RecentContainsPlain: recentContains(recent, plainText),
		RecentContainsAck:   recentContains(recent, "Codex graph bench accepted "+input.Nonce),
	}
	if !summary.QuotedAckHasRef {
		return fmt.Errorf("quoted ack %s did not include a messageReference attachment", ackMsg.ID)
	}
	if !summary.RecentContainsPlain || !summary.RecentContainsAck {
		return fmt.Errorf("recent messages did not include expected bench plain/ack content")
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(summary)
}

func validateMembers(members []teams.ChatMember, ownerUserID string, testUserID string) error {
	if len(members) != 2 {
		return fmt.Errorf("created chat has %d members, want exactly 2", len(members))
	}
	want := map[string]bool{
		strings.TrimSpace(ownerUserID): false,
		strings.TrimSpace(testUserID):  false,
	}
	for _, member := range members {
		userID := strings.TrimSpace(member.UserID)
		if _, ok := want[userID]; !ok {
			return fmt.Errorf("created chat includes unexpected member userId=%q displayName=%q", member.UserID, member.DisplayName)
		}
		want[userID] = true
	}
	for userID, ok := range want {
		if !ok {
			return fmt.Errorf("created chat missing expected member %q", userID)
		}
	}
	return nil
}

func waitForMessageID(ctx context.Context, graph *teams.GraphClient, chatID string, messageID string) (teams.ChatMessage, error) {
	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for {
		messages, err := graph.ListMessages(ctx, chatID, 20)
		if err != nil {
			lastErr = err
		} else {
			for _, msg := range messages {
				if strings.TrimSpace(msg.ID) == strings.TrimSpace(messageID) {
					return msg, nil
				}
			}
		}
		if time.Now().After(deadline) {
			return teams.ChatMessage{}, fmt.Errorf("message %s did not appear in chat %s; last error: %v", messageID, chatID, lastErr)
		}
		select {
		case <-ctx.Done():
			return teams.ChatMessage{}, fmt.Errorf("context expired waiting for message %s: %w", messageID, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func hasMessageReferenceAttachment(msg teams.ChatMessage) bool {
	for _, attachment := range msg.Attachments {
		if strings.EqualFold(strings.TrimSpace(attachment.ContentType), "messageReference") {
			return true
		}
	}
	return false
}

func memberSummaries(members []teams.ChatMember) []string {
	out := make([]string, 0, len(members))
	for _, member := range members {
		label := strings.TrimSpace(member.UserID)
		if name := strings.TrimSpace(member.DisplayName); name != "" {
			label += ":" + name
		}
		out = append(out, label)
	}
	return out
}

func recentContains(messages []teams.ChatMessage, needle string) bool {
	for _, msg := range messages {
		if strings.Contains(teams.PlainTextFromTeamsHTML(msg.Body.Content), needle) {
			return true
		}
	}
	return false
}

func sanitizeMarker(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Now().UTC().Format("20060102T150405Z")
	}
	var out strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out.WriteRune(r)
		case r == '-' || r == '_':
			out.WriteRune(r)
		}
		if out.Len() >= 64 {
			break
		}
	}
	if out.Len() == 0 {
		return time.Now().UTC().Format("20060102T150405Z")
	}
	return out.String()
}

func failf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "teams-graph-bench: "+format+"\n", args...)
	os.Exit(2)
}
