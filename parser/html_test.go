package parser

import (
	"strings"
	"testing"

	"github.com/gotd/td/tg"
)

func TestParseHTML(t *testing.T) {
	plain, ents := ParseHTML("<h1>Title</h1><p>Hello <b>world</b> and <code>123</code></p><hr/>")
	if !strings.Contains(plain, "Title\nHello world and 123") {
		t.Fatalf("unexpected plain text: %q", plain)
	}
	if len(ents) != 3 {
		t.Fatalf("expected 3 entities, got %d: %#v", len(ents), ents)
	}
	if _, ok := ents[0].(*tg.MessageEntityBold); !ok {
		t.Fatalf("expected first entity bold, got %T", ents[0])
	}
}

func TestParseHTML_Plain(t *testing.T) {
	plain, ents := ParseHTML("plain text without tags")
	if plain != "plain text without tags" || len(ents) != 0 {
		t.Fatalf("unexpected result: %q %#v", plain, ents)
	}
}

func TestFormatRichHTMLAndIsRichContent(t *testing.T) {
	in := "<blockquote expandable>line1\nline2<br/>\nline3</blockquote>"
	if !IsRichContent(in) {
		t.Fatal("expected IsRichContent to detect expandable blockquote")
	}
	if IsRichContent("<b>bold only</b>") {
		t.Fatal("standard <b> tag should not trigger rich content")
	}
	out := FormatRichHTML(in)
	if out != "<blockquote expandable>line1<br/>line2<br/>line3</blockquote>" {
		t.Fatalf("unexpected FormatRichHTML output: %q", out)
	}
}

func TestSanitizeEntities(t *testing.T) {
	// "hi 😀" has UTF-16 length 5 (h=1, i=1, ' '=1, 😀=2).
	ents := []tg.MessageEntityClass{
		&tg.MessageEntityBold{Offset: 0, Length: 20},
		&tg.MessageEntityItalic{Offset: 10, Length: 2},
	}
	got := SanitizeEntities("hi 😀", ents)
	if len(got) != 1 {
		t.Fatalf("expected 1 valid entity, got %d", len(got))
	}
	if got[0].GetLength() != 5 {
		t.Fatalf("expected clamped length 5, got %d", got[0].GetLength())
	}
}
