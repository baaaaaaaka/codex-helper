package teams

import "testing"

func TestExtractChatID(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "raw chat id",
			raw:  "19:abc123@thread.v2",
			want: "19:abc123@thread.v2",
		},
		{
			name: "percent encoded chat link",
			raw:  "https://teams.microsoft.com/l/chat/19%3Aabc123%40thread.v2/0?tenantId=tenant",
			want: "19:abc123@thread.v2",
		},
		{
			name: "meeting chat link",
			raw:  "https://teams.microsoft.com/l/chat/19%3Ameeting_MmE4ZDE%40thread.v2/0?tenantId=tenant",
			want: "19:meeting_MmE4ZDE@thread.v2",
		},
		{
			name: "channel style thread id",
			raw:  "https://teams.microsoft.com/l/channel/19%3Ag-D85%40thread.tacv2/test?groupId=team",
			want: "19:g-D85@thread.tacv2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractChatID(tt.raw)
			if err != nil {
				t.Fatalf("ExtractChatID error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("ExtractChatID = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractChatIDRejectsMissingID(t *testing.T) {
	if _, err := ExtractChatID("https://teams.microsoft.com/l/chat/no-chat-id"); err == nil {
		t.Fatal("expected error")
	}
}
