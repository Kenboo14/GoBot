// Package parser converts HTML and Telegram rich-HTML markup into plain text
// and MTProto message entities.
package parser

import (
	"regexp"
	"strings"
	"unicode/utf16"

	"github.com/gotd/td/telegram/message/entity"
	"github.com/gotd/td/telegram/message/html"
	"github.com/gotd/td/telegram/message/styling"
	"github.com/gotd/td/tg"
)

var (
	reImg          = regexp.MustCompile(`<img[^>]+src=["']([^"']+)["'][^>]*>`)
	reVid          = regexp.MustCompile(`<video[^>]+src=["']([^"']+)["'][^>]*>.*?</video>`)
	reBlockquote   = regexp.MustCompile(`(?s)(<blockquote[^>]*>)(.*?)(</blockquote>)`)
	reLineBreak    = regexp.MustCompile(`(?i)(<br\s*/?>)\s*\n`)
	reMultiNewline = regexp.MustCompile(`\n{3,}`)
)

// ParseHTML parses Telegram-compatible HTML into plain text and sanitized MTProto
// message entities.
func ParseHTML(rawHTML string) (string, []tg.MessageEntityClass) {
	text := CleanHTML(rawHTML)
	if !strings.Contains(text, "<") || !strings.Contains(text, ">") {
		return text, nil
	}
	var builder entity.Builder
	if err := styling.Perform(&builder, html.String(nil, text)); err != nil {
		return text, nil
	}
	plain, entities := builder.Complete()
	return plain, SanitizeEntities(plain, entities)
}

// SanitizeEntities clamps entity offsets and lengths to the UTF-16 code unit
// bounds of plain text, dropping out-of-range or empty entities.
func SanitizeEntities(plain string, entities []tg.MessageEntityClass) []tg.MessageEntityClass {
	if len(entities) == 0 {
		return nil
	}
	u16Len := len(utf16.Encode([]rune(plain)))
	valid := make([]tg.MessageEntityClass, 0, len(entities))
	for _, e := range entities {
		offset := e.GetOffset()
		length := e.GetLength()
		if offset < 0 || offset >= u16Len {
			continue
		}
		if offset+length > u16Len {
			newLen := u16Len - offset
			if newLen <= 0 {
				continue
			}
			setEntityLength(e, newLen)
		}
		valid = append(valid, e)
	}
	return valid
}

func setEntityLength(e tg.MessageEntityClass, newLen int) {
	switch ent := e.(type) {
	case *tg.MessageEntityBlockquote:
		ent.Length = newLen
	case *tg.MessageEntityBold:
		ent.Length = newLen
	case *tg.MessageEntityItalic:
		ent.Length = newLen
	case *tg.MessageEntityCode:
		ent.Length = newLen
	case *tg.MessageEntityPre:
		ent.Length = newLen
	case *tg.MessageEntityTextURL:
		ent.Length = newLen
	case *tg.MessageEntityMentionName:
		ent.Length = newLen
	case *tg.MessageEntityMention:
		ent.Length = newLen
	case *tg.MessageEntityHashtag:
		ent.Length = newLen
	case *tg.MessageEntityBotCommand:
		ent.Length = newLen
	case *tg.MessageEntityURL:
		ent.Length = newLen
	case *tg.MessageEntityEmail:
		ent.Length = newLen
	case *tg.MessageEntityUnderline:
		ent.Length = newLen
	case *tg.MessageEntityStrike:
		ent.Length = newLen
	case *tg.MessageEntitySpoiler:
		ent.Length = newLen
	case *tg.MessageEntityCustomEmoji:
		ent.Length = newLen
	case *tg.MessageEntityBankCard:
		ent.Length = newLen
	case *tg.MessageEntityPhone:
		ent.Length = newLen
	case *tg.MessageEntityCashtag:
		ent.Length = newLen
	}
}

// FormatRichHTML ensures newlines inside blockquotes are converted to <br/>
// so Telegram rich messages do not collapse them into a single line.
func FormatRichHTML(htmlText string) string {
	return reBlockquote.ReplaceAllStringFunc(htmlText, func(match string) string {
		parts := reBlockquote.FindStringSubmatch(match)
		if len(parts) < 4 {
			return match
		}
		openTag := parts[1]
		content := parts[2]
		closeTag := parts[3]

		content = strings.ReplaceAll(content, "\r\n", "\n")
		content = reLineBreak.ReplaceAllString(content, "$1")
		content = strings.ReplaceAll(content, "\n", "<br/>")

		return openTag + content + closeTag
	})
}

// IsRichContent reports whether text contains extended HTML tags intended for
// MTProto SetRichMessage / Bot 2.0 rich messages.
func IsRichContent(text string) bool {
	return strings.Contains(text, "<h1") ||
		strings.Contains(text, "<h2") ||
		strings.Contains(text, "<h3") ||
		strings.Contains(text, "<table") ||
		strings.Contains(text, "<tg-button") ||
		strings.Contains(text, "expandable") ||
		strings.Contains(text, "<hr") ||
		strings.Contains(text, "<img") ||
		strings.Contains(text, "<video")
}

// CleanHTML normalizes extended HTML tags (paragraphs, headings, tables,
// line breaks, hr, media embeds) into standard Telegram HTML.
func CleanHTML(text string) string {
	s := strings.ReplaceAll(text, "\r\n", "\n")
	s = strings.ReplaceAll(s, "<blockquote>\n", "<blockquote>")
	s = strings.ReplaceAll(s, "\n</blockquote>", "</blockquote>")
	s = strings.ReplaceAll(s, "<br/>\n", "\n")
	s = strings.ReplaceAll(s, "<br />\n", "\n")
	s = strings.ReplaceAll(s, "<br>\n", "\n")
	s = strings.ReplaceAll(s, "<br/>", "\n")
	s = strings.ReplaceAll(s, "<br />", "\n")
	s = strings.ReplaceAll(s, "<br>", "\n")
	s = strings.ReplaceAll(s, "<p>", "")
	s = strings.ReplaceAll(s, "</p>", "\n")
	s = strings.ReplaceAll(s, "<h1>", "<b>")
	s = strings.ReplaceAll(s, "</h1>", "</b>\n")
	s = strings.ReplaceAll(s, "<h2>", "<b>")
	s = strings.ReplaceAll(s, "</h2>", "</b>\n")
	for {
		idx := strings.Index(s, "<tg-button-row>")
		if idx == -1 {
			break
		}
		endIdx := strings.Index(s[idx:], "</tg-button-row>")
		if endIdx == -1 {
			break
		}
		s = s[:idx] + s[idx+endIdx+len("</tg-button-row>"):]
	}
	s = strings.ReplaceAll(s, "<tr><th>", "• <b>")
	s = strings.ReplaceAll(s, "</th><th>", " | ")
	s = strings.ReplaceAll(s, "</th></tr>", "</b>\n")
	s = strings.ReplaceAll(s, "<tr><td>", "• ")
	s = strings.ReplaceAll(s, "</td><td>", ": ")
	s = strings.ReplaceAll(s, "</td></tr>", "\n")
	tableTags := []string{
		`<table border="1">`, "<table>", "</table>",
		"<tr>", "</tr>", "<th>", "</th>", "<td>", "</td>",
		"<thead>", "</thead>", "<tbody>", "</tbody>",
	}
	for _, tag := range tableTags {
		s = strings.ReplaceAll(s, tag, "")
	}
	s = reImg.ReplaceAllString(s, `<a href="$1">&#8204;</a>`)
	s = reVid.ReplaceAllString(s, `<a href="$1">&#8204;</a>`)
	s = strings.ReplaceAll(s, "<hr/>", "")
	s = strings.ReplaceAll(s, "<hr />", "")
	s = strings.ReplaceAll(s, "<hr>", "")
	s = reMultiNewline.ReplaceAllString(s, "\n\n")
	return strings.TrimSpace(s)
}
