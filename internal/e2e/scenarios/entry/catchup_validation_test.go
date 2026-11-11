package entry

import (
	"strings"
	"testing"
)

func TestValidateCatchUpPosts(t *testing.T) {
	tokens := []string{"backlog-p-01-0", "backlog-a-01-0", "backlog-p-02-0"}
	good := []catchUpPost{
		{"chat1", "Backlog prompt backlog-p-01-0", tokens[0]},
		{"chat1", "Backlog prompt backlog-p-01-0", tokens[1]},
		{"chat2", "Backlog prompt backlog-p-02-0", tokens[2]},
	}
	if err := validateCatchUpPosts(tokens, good); err != nil {
		t.Fatal(err)
	}
	missingAndDuplicate := append([]catchUpPost(nil), good...)
	missingAndDuplicate[1].text = tokens[0]
	if err := validateCatchUpPosts(tokens, missingAndDuplicate); err == nil {
		t.Fatal("accepted missing and duplicated record")
	}
	wrongChat := append([]catchUpPost(nil), good...)
	wrongChat[1].chatID, wrongChat[1].topic = wrongChat[2].chatID, wrongChat[2].topic
	if err := validateCatchUpPosts(tokens, wrongChat); err == nil {
		t.Fatal("accepted record in another session")
	}
}

func TestValidateCatchUpSummary(t *testing.T) {
	posts := []catchUpPost{{"chat1", "", "backlog-p-00-0"}, {"chat2", "", "backlog-p-01-0"}}
	first := `<li><strong>s001</strong><a href="/chat/chat1">backlog-p-00-0</a></li>`
	second := `<li><strong>s002</strong><a href="/chat/chat2">backlog-p-01-0</a></li>`
	if err := validateCatchUpSummary(2, posts, []string{first + second}); err != nil {
		t.Fatal(err)
	}
	if err := validateCatchUpSummary(2, posts, []string{strings.ReplaceAll(first, "chat1", "chat2") + second}); err == nil {
		t.Fatal("accepted swapped chat link")
	}
	if err := validateCatchUpSummary(2, posts, []string{first + strings.ReplaceAll(second, "s002", "s001")}); err == nil {
		t.Fatal("accepted duplicate alias")
	}
}
