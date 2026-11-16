// Package types defines lightweight, shared Telegram data models and MTProto
// conversion helpers used across GoBot and GotdBot.
package types

import "github.com/gotd/td/tg"

// ParseMode specifies how outgoing message text should be parsed.
type ParseMode string

const (
	ParseModeHTML       ParseMode = "HTML"
	ParseModeMarkdown   ParseMode = "Markdown"
	ParseModeMarkdownV2 ParseMode = "MarkdownV2"
)

// User represents a Telegram user or bot.
type User struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name,omitempty"`
	Username  string `json:"username,omitempty"`
	IsBot     bool   `json:"is_bot,omitempty"`
}

// Chat represents a Telegram private chat, group, supergroup, or channel.
type Chat struct {
	ID       int64  `json:"id"`
	Type     string `json:"type"`
	Title    string `json:"title,omitempty"`
	Username string `json:"username,omitempty"`
}

// MessageEntity represents a special entity in a text message (bold, code, etc.).
type MessageEntity struct {
	Type          string `json:"type"`
	Offset        int    `json:"offset"`
	Length        int    `json:"length"`
	URL           string `json:"url,omitempty"`
	User          *User  `json:"user,omitempty"`
	Language      string `json:"language,omitempty"`
	CustomEmojiID string `json:"custom_emoji_id,omitempty"`
}

// Contact represents a phone contact.
type Contact struct {
	PhoneNumber string `json:"phone_number"`
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name,omitempty"`
	UserID      int64  `json:"user_id,omitempty"`
	VCard       string `json:"vcard,omitempty"`
}

// PhotoSize represents one size of a photo or file thumbnail.
type PhotoSize struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	FileSize     int    `json:"file_size,omitempty"`
}

// Document represents a general file.
type Document struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
	FileSize int64  `json:"file_size,omitempty"`
}

// Video represents a video file.
type Video struct {
	FileID   string `json:"file_id"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
	Duration int    `json:"duration"`
	FileSize int64  `json:"file_size,omitempty"`
}

// Audio represents an audio file.
type Audio struct {
	FileID   string `json:"file_id"`
	Duration int    `json:"duration"`
	FileSize int64  `json:"file_size,omitempty"`
}

// Voice represents a voice note.
type Voice struct {
	FileID   string `json:"file_id"`
	Duration int    `json:"duration"`
	FileSize int64  `json:"file_size,omitempty"`
}

// Message represents a Telegram message.
type Message struct {
	ID              int             `json:"message_id"`
	From            *User           `json:"from,omitempty"`
	Chat            Chat            `json:"chat"`
	Date            int             `json:"date"`
	Out             bool            `json:"out,omitempty"`
	MessageThreadID int             `json:"message_thread_id,omitempty"`
	Text            string          `json:"text,omitempty"`
	Caption         string          `json:"caption,omitempty"`
	Entities        []MessageEntity `json:"entities,omitempty"`
	CaptionEntities []MessageEntity `json:"caption_entities,omitempty"`
	Photo           []PhotoSize     `json:"photo,omitempty"`
	Video           *Video          `json:"video,omitempty"`
	Document        *Document       `json:"document,omitempty"`
	Audio           *Audio          `json:"audio,omitempty"`
	Voice           *Voice          `json:"voice,omitempty"`
	Animation       *Document       `json:"animation,omitempty"`
	VideoNote       *Video          `json:"video_note,omitempty"`
	Sticker         any             `json:"sticker,omitempty"`
	Contact         *Contact        `json:"contact,omitempty"`
	ReplyToMessage  *Message        `json:"reply_to_message,omitempty"`
	MediaGroupID    string          `json:"media_group_id,omitempty"`
}

// MaybeInaccessibleMessage wraps a Message that may be inaccessible.
type MaybeInaccessibleMessage struct {
	Message *Message
}

// CallbackQuery represents an incoming callback query from an inline keyboard button.
type CallbackQuery struct {
	ID                 string                          `json:"id"`
	From               User                            `json:"from"`
	Message            *MaybeInaccessibleMessage       `json:"message,omitempty"`
	InlineMessageID    string                          `json:"inline_message_id,omitempty"`
	RawInlineMessageID tg.InputBotInlineMessageIDClass `json:"-"`
	Data               string                          `json:"data,omitempty"`
}

// Update represents an incoming Telegram update.
type Update struct {
	ID            int            `json:"update_id"`
	Message       *Message       `json:"message,omitempty"`
	ChannelPost   *Message       `json:"channel_post,omitempty"`
	CallbackQuery *CallbackQuery `json:"callback_query,omitempty"`
	InlineQuery   *InlineQuery   `json:"inline_query,omitempty"`
}

// InlineQuery represents an incoming inline query.
type InlineQuery struct {
	ID     string `json:"id"`
	From   User   `json:"from"`
	Query  string `json:"query"`
	Offset string `json:"offset"`
}

// InlineQueryResult is an inline query result item.
type InlineQueryResult interface{}

// InputMessageContent represents the content of a message sent via inline query.
type InputMessageContent interface{}

// InputTextMessageContent represents text message content for an inline result.
type InputTextMessageContent struct {
	MessageText string    `json:"message_text"`
	ParseMode   ParseMode `json:"parse_mode,omitempty"`
}

// InlineQueryResultArticle represents a link to an article or web page.
type InlineQueryResultArticle struct {
	Type                string                `json:"type"`
	ID                  string                `json:"id"`
	Title               string                `json:"title"`
	Description         string                `json:"description,omitempty"`
	InputMessageContent InputMessageContent   `json:"input_message_content"`
	ReplyMarkup         *InlineKeyboardMarkup `json:"reply_markup,omitempty"`
}

// AnswerInlineQueryParams configures AnswerInlineQuery.
type AnswerInlineQueryParams struct {
	InlineQueryID string              `json:"inline_query_id"`
	Results       []InlineQueryResult `json:"results"`
	CacheTime     int                 `json:"cache_time,omitempty"`
	IsPersonal    bool                `json:"is_personal,omitempty"`
}

// InlineKeyboardButton represents one button of an inline keyboard.
type InlineKeyboardButton struct {
	Text                         string `json:"text"`
	URL                          string `json:"url,omitempty"`
	CallbackData                 string `json:"callback_data,omitempty"`
	SwitchInlineQueryCurrentChat string `json:"switch_inline_query_current_chat,omitempty"`
}

// InlineKeyboardMarkup represents an inline keyboard attached to a message.
type InlineKeyboardMarkup struct {
	InlineKeyboard [][]InlineKeyboardButton `json:"inline_keyboard"`
}

// KeyboardButton represents one button of a reply keyboard.
type KeyboardButton struct {
	Text           string `json:"text"`
	RequestContact bool   `json:"request_contact,omitempty"`
}

// ReplyKeyboardMarkup represents a custom reply keyboard.
type ReplyKeyboardMarkup struct {
	Keyboard        [][]KeyboardButton `json:"keyboard"`
	ResizeKeyboard  bool               `json:"resize_keyboard,omitempty"`
	OneTimeKeyboard bool               `json:"one_time_keyboard,omitempty"`
}

// ReplyKeyboardRemove removes a custom reply keyboard.
type ReplyKeyboardRemove struct {
	RemoveKeyboard bool `json:"remove_keyboard"`
}

// ReplyParameters describes reply target parameters.
type ReplyParameters struct {
	MessageID int `json:"message_id"`
}

// ReplyMarkup is a polymorphic markup value (*InlineKeyboardMarkup,
// *ReplyKeyboardMarkup, *ReplyKeyboardRemove, or tg.ReplyMarkupClass).
type ReplyMarkup interface{}

// MessageId identifies a single message.
type MessageId struct {
	ID int `json:"message_id"`
}

// SendMessageParams configures SendMessage.
type SendMessageParams struct {
	ChatID          any              `json:"chat_id"`
	MessageThreadID int              `json:"message_thread_id,omitempty"`
	Text            string           `json:"text"`
	ParseMode       ParseMode        `json:"parse_mode,omitempty"`
	Entities        []MessageEntity  `json:"entities,omitempty"`
	ReplyMarkup     ReplyMarkup      `json:"reply_markup,omitempty"`
	ReplyParameters *ReplyParameters `json:"reply_parameters,omitempty"`
}

// CopyMessageParams configures CopyMessage.
type CopyMessageParams struct {
	ChatID      any         `json:"chat_id"`
	FromChatID  any         `json:"from_chat_id"`
	MessageID   int         `json:"message_id"`
	Caption     string      `json:"caption,omitempty"`
	ParseMode   ParseMode   `json:"parse_mode,omitempty"`
	ReplyMarkup ReplyMarkup `json:"reply_markup,omitempty"`
}

// CopyMessagesParams configures CopyMessages.
type CopyMessagesParams struct {
	ChatID     any   `json:"chat_id"`
	FromChatID any   `json:"from_chat_id"`
	MessageIDs []int `json:"message_ids"`
}

// EditMessageTextParams configures EditMessageText.
type EditMessageTextParams struct {
	ChatID             any                             `json:"chat_id,omitempty"`
	MessageID          int                             `json:"message_id,omitempty"`
	InlineMessageID    string                          `json:"inline_message_id,omitempty"`
	RawInlineMessageID tg.InputBotInlineMessageIDClass `json:"-"`
	Text               string                          `json:"text"`
	ParseMode          ParseMode                       `json:"parse_mode,omitempty"`
	Entities           []MessageEntity                 `json:"entities,omitempty"`
	ReplyMarkup        ReplyMarkup                     `json:"reply_markup,omitempty"`
}

// EditMessageReplyMarkupParams configures EditMessageReplyMarkup.
type EditMessageReplyMarkupParams struct {
	ChatID             any                             `json:"chat_id,omitempty"`
	MessageID          int                             `json:"message_id,omitempty"`
	InlineMessageID    string                          `json:"inline_message_id,omitempty"`
	RawInlineMessageID tg.InputBotInlineMessageIDClass `json:"-"`
	ReplyMarkup        ReplyMarkup                     `json:"reply_markup,omitempty"`
}

// DeleteMessageParams configures DeleteMessage.
type DeleteMessageParams struct {
	ChatID    any `json:"chat_id"`
	MessageID int `json:"message_id"`
}

// DeleteMessagesParams configures DeleteMessages.
type DeleteMessagesParams struct {
	ChatID     any   `json:"chat_id"`
	MessageIDs []int `json:"message_ids"`
}

// AnswerCallbackQueryParams configures AnswerCallbackQuery.
type AnswerCallbackQueryParams struct {
	CallbackQueryID string `json:"callback_query_id"`
	Text            string `json:"text,omitempty"`
	ShowAlert       bool   `json:"show_alert,omitempty"`
	URL             string `json:"url,omitempty"`
}
