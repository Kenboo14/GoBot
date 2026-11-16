package types

import (
	"strconv"
	"strings"

	"github.com/gotd/td/tg"
)

// ExtractThreadID extracts the forum topic thread ID from msg.
// ReplyToTopID takes precedence; ReplyToMsgID is used only when ForumTopic is
// true so regular replies are never mistaken for topic IDs.
func ExtractThreadID(msg *tg.Message) int {
	if msg == nil {
		return 0
	}
	if replyTo, ok := msg.GetReplyTo(); ok {
		if header, ok := replyTo.(*tg.MessageReplyHeader); ok {
			if header.ReplyToTopID != 0 {
				return header.ReplyToTopID
			}
			if header.ForumTopic && header.ReplyToMsgID != 0 {
				return header.ReplyToMsgID
			}
		}
	}
	return 0
}

// ConvertTGEntities converts MTProto entities into Bot API style MessageEntity slice.
func ConvertTGEntities(source []tg.MessageEntityClass) []MessageEntity {
	if len(source) == 0 {
		return nil
	}
	result := make([]MessageEntity, 0, len(source))
	for _, item := range source {
		var e MessageEntity
		switch val := item.(type) {
		case *tg.MessageEntityBold:
			e.Type, e.Offset, e.Length = "bold", val.Offset, val.Length
		case *tg.MessageEntityItalic:
			e.Type, e.Offset, e.Length = "italic", val.Offset, val.Length
		case *tg.MessageEntityUnderline:
			e.Type, e.Offset, e.Length = "underline", val.Offset, val.Length
		case *tg.MessageEntityStrike:
			e.Type, e.Offset, e.Length = "strikethrough", val.Offset, val.Length
		case *tg.MessageEntityCode:
			e.Type, e.Offset, e.Length = "code", val.Offset, val.Length
		case *tg.MessageEntityPre:
			e.Type, e.Offset, e.Length, e.Language = "pre", val.Offset, val.Length, val.Language
		case *tg.MessageEntityTextURL:
			e.Type, e.Offset, e.Length, e.URL = "text_link", val.Offset, val.Length, val.URL
		case *tg.MessageEntityMention:
			e.Type, e.Offset, e.Length = "mention", val.Offset, val.Length
		case *tg.MessageEntityHashtag:
			e.Type, e.Offset, e.Length = "hashtag", val.Offset, val.Length
		case *tg.MessageEntityURL:
			e.Type, e.Offset, e.Length = "url", val.Offset, val.Length
		case *tg.MessageEntityBlockquote:
			e.Type, e.Offset, e.Length = "blockquote", val.Offset, val.Length
		case *tg.MessageEntitySpoiler:
			e.Type, e.Offset, e.Length = "spoiler", val.Offset, val.Length
		case *tg.MessageEntityCustomEmoji:
			e.Type, e.Offset, e.Length, e.CustomEmojiID = "custom_emoji", val.Offset, val.Length, strconv.FormatInt(val.DocumentID, 10)
		default:
			continue
		}
		result = append(result, e)
	}
	return result
}

// ConvertMessageToUpdate converts a raw *tg.Message and update entities into an
// Update containing a populated Message.
func ConvertMessageToUpdate(msg *tg.Message, e tg.Entities) *Update {
	if msg == nil {
		return nil
	}
	m := ConvertMessage(msg, e)
	return &Update{
		ID:      msg.ID,
		Message: m,
	}
}

// ConvertMessage converts a raw *tg.Message and update entities into a *Message.
func ConvertMessage(msg *tg.Message, e tg.Entities) *Message {
	if msg == nil {
		return nil
	}
	var chat Chat
	var fromUser *User

	switch p := msg.PeerID.(type) {
	case *tg.PeerUser:
		chat.ID = p.UserID
		chat.Type = "private"
		if u, ok := e.Users[p.UserID]; ok && u != nil {
			chat.Username = u.Username
			chat.Title = strings.TrimSpace(u.FirstName + " " + u.LastName)
		}
	case *tg.PeerChat:
		chat.ID = -p.ChatID
		chat.Type = "group"
		if ch, ok := e.Chats[p.ChatID]; ok && ch != nil {
			chat.Title = ch.Title
		}
	case *tg.PeerChannel:
		chat.ID = -1000000000000 - p.ChannelID
		chat.Type = "channel"
		if ch, ok := e.Channels[p.ChannelID]; ok && ch != nil {
			chat.Title = ch.Title
			chat.Username = ch.Username
			if ch.Megagroup {
				chat.Type = "supergroup"
			}
		}
	}

	if fromPeer, ok := msg.FromID.(*tg.PeerUser); ok {
		fromUser = &User{ID: fromPeer.UserID}
		if u, ok := e.Users[fromPeer.UserID]; ok && u != nil {
			fromUser.FirstName = u.FirstName
			fromUser.LastName = u.LastName
			fromUser.Username = u.Username
			fromUser.IsBot = u.Bot
		}
	} else if chat.ID > 0 {
		fromUser = &User{ID: chat.ID}
		if u, ok := e.Users[chat.ID]; ok && u != nil {
			fromUser.FirstName = u.FirstName
			fromUser.LastName = u.LastName
			fromUser.Username = u.Username
			fromUser.IsBot = u.Bot
		}
	}

	m := &Message{
		ID:   msg.ID,
		Date: msg.Date,
		Out:  msg.Out,
		Text: msg.Message,
		Chat: chat,
		From: fromUser,
	}

	if gid, ok := msg.GetGroupedID(); ok && gid != 0 {
		m.MediaGroupID = strconv.FormatInt(gid, 10)
	}

	if msg.Media != nil {
		if msg.Message != "" {
			m.Caption = msg.Message
		}
		switch media := msg.Media.(type) {
		case *tg.MessageMediaContact:
			m.Contact = &Contact{
				PhoneNumber: media.PhoneNumber,
				FirstName:   media.FirstName,
				LastName:    media.LastName,
				UserID:      media.UserID,
				VCard:       media.Vcard,
			}
		case *tg.MessageMediaPhoto:
			if photo, ok := media.Photo.(*tg.Photo); ok {
				m.Photo = []PhotoSize{{FileID: strconv.FormatInt(photo.ID, 10), Width: 100, Height: 100}}
			}
		case *tg.MessageMediaDocument:
			if doc, ok := media.Document.(*tg.Document); ok {
				docID := strconv.FormatInt(doc.ID, 10)
				m.Document = &Document{FileID: docID, MimeType: doc.MimeType, FileSize: doc.Size}
				for _, attr := range doc.Attributes {
					switch a := attr.(type) {
					case *tg.DocumentAttributeVideo:
						m.Video = &Video{FileID: docID, Duration: int(a.Duration), Width: a.W, Height: a.H, FileSize: doc.Size}
					case *tg.DocumentAttributeAudio:
						if a.Voice {
							m.Voice = &Voice{FileID: docID, Duration: a.Duration, FileSize: doc.Size}
						} else {
							m.Audio = &Audio{FileID: docID, Duration: a.Duration, FileSize: doc.Size}
						}
					case *tg.DocumentAttributeAnimated:
						m.Animation = &Document{FileID: docID, MimeType: doc.MimeType, FileSize: doc.Size}
					}
				}
			}
		}
	}

	entities := ConvertTGEntities(msg.Entities)
	if msg.Media != nil {
		m.CaptionEntities = entities
	} else {
		m.Entities = entities
	}

	if topicID := ExtractThreadID(msg); topicID != 0 {
		m.MessageThreadID = topicID
	}

	return m
}

// ParseCommand parses a command with the given prefix (empty prefix matches
// prefixless commands). Command name is returned in lowercase.
func ParseCommand(text, prefix string) (name, args string, ok bool) {
	if text == "" {
		return "", "", false
	}
	if prefix != "" {
		if !strings.HasPrefix(text, prefix) {
			return "", "", false
		}
		text = text[len(prefix):]
	}
	// Trim leading ASCII whitespace without allocations.
	for len(text) > 0 && isASCIIWS(text[0]) {
		text = text[1:]
	}
	if text == "" {
		return "", "", false
	}
	idx := -1
	for i := 0; i < len(text); i++ {
		if isASCIIWS(text[i]) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return strings.ToLower(text), "", true
	}
	name = strings.ToLower(text[:idx])
	args = strings.TrimSpace(text[idx+1:])
	return name, args, true
}

func isASCIIWS(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}
