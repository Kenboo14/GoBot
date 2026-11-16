package filters

import (
	"regexp"
	"testing"

	"github.com/Kenboo14/GoBot/types"
)

func TestFilters(t *testing.T) {
	privOut := &types.Message{
		Out:  true,
		Chat: types.Chat{ID: 123, Type: "private"},
		Text: "/Start@mybot hello",
	}
	grpIn := &types.Message{
		Out:  false,
		Chat: types.Chat{ID: -100123, Type: "supergroup"},
		Text: "!ping",
	}

	if !Me.And(Private).And(Command("start"))(privOut) {
		t.Fatal("expected Me.And(Private).And(Command(start)) to match privOut")
	}
	if Incoming(privOut) || Group(privOut) || Channel(privOut) {
		t.Fatal("unexpected match on privOut")
	}
	if !Incoming.And(Group).And(CommandWithPrefixes([]string{"!", "."}, "ping"))(grpIn) {
		t.Fatal("expected CommandWithPrefixes to match !ping")
	}
	if Me.Not()(privOut) {
		t.Fatal("Not() should negate Me")
	}
	if !Me.Or(Group)(grpIn) {
		t.Fatal("Or() should match Group")
	}
	re := Regex(regexp.MustCompile(`^!p`))
	if !re(grpIn) || re(privOut) {
		t.Fatal("Regex filter mismatch")
	}
}
