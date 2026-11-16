package types

import (
	"testing"

	"github.com/gotd/td/tg"
)

func TestExtractThreadID_TopicFromReplyToTopID(t *testing.T) {
	msg := &tg.Message{
		ID:    100,
		Flags: 1 << 3,
		ReplyTo: &tg.MessageReplyHeader{
			ForumTopic:   true,
			ReplyToTopID: 42,
			ReplyToMsgID: 99,
		},
	}
	got := ExtractThreadID(msg)
	if got != 42 {
		t.Errorf("ExtractThreadID() = %d, want %d (ReplyToTopID takes priority)", got, 42)
	}
}

func TestExtractThreadID_TopicFallbackToReplyToMsgID(t *testing.T) {
	msg := &tg.Message{
		ID:    100,
		Flags: 1 << 3,
		ReplyTo: &tg.MessageReplyHeader{
			ForumTopic:   true,
			ReplyToTopID: 0,
			ReplyToMsgID: 456,
		},
	}
	got := ExtractThreadID(msg)
	if got != 456 {
		t.Errorf("ExtractThreadID() = %d, want %d (fallback to ReplyToMsgID when ForumTopic)", got, 456)
	}
}

func TestExtractThreadID_GeneralTopicNoReplyTo(t *testing.T) {
	msg := &tg.Message{ID: 100}
	got := ExtractThreadID(msg)
	if got != 0 {
		t.Errorf("ExtractThreadID() = %d, want 0 (general topic has no reply_to)", got)
	}
}

func TestExtractThreadID_ForumTopicZeroIDs(t *testing.T) {
	msg := &tg.Message{
		ID:    100,
		Flags: 1 << 3,
		ReplyTo: &tg.MessageReplyHeader{
			ForumTopic:   true,
			ReplyToTopID: 0,
			ReplyToMsgID: 0,
		},
	}
	got := ExtractThreadID(msg)
	if got != 0 {
		t.Errorf("ExtractThreadID() = %d, want 0 (ForumTopic but no valid topic ID)", got)
	}
}

func TestExtractThreadID_ReplyToTopIDWithoutForumTopic(t *testing.T) {
	msg := &tg.Message{
		ID:    100,
		Flags: 1 << 3,
		ReplyTo: &tg.MessageReplyHeader{
			ForumTopic:   false,
			ReplyToTopID: 77,
			ReplyToMsgID: 0,
		},
	}
	got := ExtractThreadID(msg)
	if got != 77 {
		t.Errorf("ExtractThreadID() = %d, want %d (ReplyToTopID is definitive topic ID)", got, 77)
	}
}

func TestExtractThreadID_MessageIDNeverTreatedAsTopic(t *testing.T) {
	msg := &tg.Message{
		ID:    9999,
		Flags: 1 << 3,
		ReplyTo: &tg.MessageReplyHeader{
			ForumTopic:   false,
			ReplyToTopID: 0,
			ReplyToMsgID: 0,
		},
	}
	got := ExtractThreadID(msg)
	if got != 0 {
		t.Errorf("ExtractThreadID() = %d, want 0 (Message.ID must never be treated as Topic ID)", got)
	}
}

func TestParseCommand(t *testing.T) {
	cases := []struct {
		text, prefix string
		wantName     string
		wantArgs     string
		wantOK       bool
	}{
		{"!Ping hello world", "!", "ping", "hello world", true},
		{"!  ping\narg1", "!", "ping", "arg1", true},
		{"/start", "/", "start", "", true},
		{"hello", "!", "", "", false},
		{"!", "!", "", "", false},
		{"ping 123", "", "ping", "123", true},
	}
	for _, tc := range cases {
		name, args, ok := ParseCommand(tc.text, tc.prefix)
		if ok != tc.wantOK || name != tc.wantName || args != tc.wantArgs {
			t.Errorf("ParseCommand(%q, %q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.text, tc.prefix, name, args, ok, tc.wantName, tc.wantArgs, tc.wantOK)
		}
	}
}

func TestConvertMessageToUpdate(t *testing.T) {
	msg := &tg.Message{
		ID:       7,
		Out:      true,
		PeerID:   &tg.PeerChannel{ChannelID: 42},
		FromID:   &tg.PeerUser{UserID: 99},
		Message:  "hello",
		Date:     1700000000,
		Entities: []tg.MessageEntityClass{&tg.MessageEntityBold{Offset: 0, Length: 5}},
	}
	e := tg.Entities{
		Users:    map[int64]*tg.User{99: {ID: 99, FirstName: "Alice", Username: "alice"}},
		Channels: map[int64]*tg.Channel{42: {ID: 42, Title: "Dev", Megagroup: true}},
	}
	u := ConvertMessageToUpdate(msg, e)
	if u == nil || u.Message == nil {
		t.Fatal("expected non-nil converted update and message")
	}
	if u.Message.Chat.ID != -1000000000042 || u.Message.Chat.Type != "supergroup" {
		t.Fatalf("unexpected chat: %+v", u.Message.Chat)
	}
	if !u.Message.Out || u.Message.From == nil || u.Message.From.Username != "alice" {
		t.Fatalf("unexpected message sender/out: %+v", u.Message)
	}
	if len(u.Message.Entities) != 1 || u.Message.Entities[0].Type != "bold" {
		t.Fatalf("unexpected entities: %+v", u.Message.Entities)
	}
}
