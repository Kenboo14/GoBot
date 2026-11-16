package gobot

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"sync"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/tg"

	"github.com/Kenboo14/GoBot/engine"
	"github.com/Kenboo14/GoBot/parser"
	"github.com/Kenboo14/GoBot/peers"
	"github.com/Kenboo14/GoBot/types"
)

// Re-export Bot API compatible types, constants, and helpers so GotdBot
// gateway can import GoBot as a drop-in replacement for internal/botgotd.
type (
	ParseMode                    = types.ParseMode
	User                         = types.User
	Chat                         = types.Chat
	MessageEntity                = types.MessageEntity
	Contact                      = types.Contact
	PhotoSize                    = types.PhotoSize
	Document                     = types.Document
	Video                        = types.Video
	Audio                        = types.Audio
	Voice                        = types.Voice
	Message                      = types.Message
	MaybeInaccessibleMessage     = types.MaybeInaccessibleMessage
	CallbackQuery                = types.CallbackQuery
	Update                       = types.Update
	InlineQuery                  = types.InlineQuery
	InlineQueryResult            = types.InlineQueryResult
	InputMessageContent          = types.InputMessageContent
	InputTextMessageContent      = types.InputTextMessageContent
	InlineQueryResultArticle     = types.InlineQueryResultArticle
	AnswerInlineQueryParams      = types.AnswerInlineQueryParams
	InlineKeyboardButton         = types.InlineKeyboardButton
	InlineKeyboardMarkup         = types.InlineKeyboardMarkup
	KeyboardButton               = types.KeyboardButton
	ReplyKeyboardMarkup          = types.ReplyKeyboardMarkup
	ReplyKeyboardRemove          = types.ReplyKeyboardRemove
	ReplyParameters              = types.ReplyParameters
	ReplyMarkup                  = types.ReplyMarkup
	MessageId                    = types.MessageId
	SendMessageParams            = types.SendMessageParams
	CopyMessageParams            = types.CopyMessageParams
	CopyMessagesParams           = types.CopyMessagesParams
	EditMessageTextParams        = types.EditMessageTextParams
	EditMessageReplyMarkupParams = types.EditMessageReplyMarkupParams
	DeleteMessageParams          = types.DeleteMessageParams
	DeleteMessagesParams         = types.DeleteMessagesParams
	AnswerCallbackQueryParams    = types.AnswerCallbackQueryParams
	PeerStorage                  = peers.PeerStorage
)

const (
	ParseModeHTML       = types.ParseModeHTML
	ParseModeMarkdown   = types.ParseModeMarkdown
	ParseModeMarkdownV2 = types.ParseModeMarkdownV2
)

// NewPeerStorage creates a bounded, sharded peer storage backed optionally by SQLite.
func NewPeerStorage(sqliteDB *sql.DB) *PeerStorage {
	return peers.NewPeerStorage(sqliteDB)
}

// Re-export parser helpers used directly by GotdBot controllers.
var (
	FormatRichHTML = parser.FormatRichHTML
	CleanHTML      = parser.CleanHTML
	IsRichContent  = parser.IsRichContent
	ParseHTML      = parser.ParseHTML
)

type HandlerType int

const (
	HandlerTypeMessageText HandlerType = iota
	HandlerTypeCallbackQueryData
	HandlerTypeInlineQuery
)

type MatchType int

const (
	MatchTypeExact MatchType = iota
	MatchTypePrefix
)

// HandlerFunc is the callback signature used by Bot.RegisterHandler.
type HandlerFunc func(ctx context.Context, b *Bot, update *Update)

type registeredHandler struct {
	hType   HandlerType
	pattern string
	mType   MatchType
	handler HandlerFunc
}

type registeredMatchFunc struct {
	match   func(u *Update) bool
	handler HandlerFunc
}

const defaultDispatchWorkers = 64

// Bot is a drop-in compatibility wrapper matching GotdBot's gateway/internal/botgotd.Bot
// while using bounded dispatch concurrency and GoBot's shared peer cache.
type Bot struct {
	Client     *telegram.Client
	EngineConn *engine.Conn
	API        *tg.Client
	Sender     *message.Sender
	Peers      *PeerStorage
	Token      string
	ServerURL  string
	Dispatcher tg.UpdateDispatcher

	mu          sync.RWMutex
	handlers    []registeredHandler
	matchFuncs  []registeredMatchFunc
	httpClient  *http.Client
	dispatchSem chan struct{}
}

// NewBot constructs a Bot backed by a *telegram.Client.
func NewBot(client *telegram.Client, token string, peersStorage *PeerStorage) *Bot {
	var api *tg.Client
	var sender *message.Sender
	if client != nil {
		api = tg.NewClient(client)
		sender = message.NewSender(api)
	}
	return newBotWithAPI(client, nil, api, sender, token, peersStorage)
}

// NewEngineBot constructs a Bot backed by GoBot's lightweight *engine.Conn.
func NewEngineBot(conn *engine.Conn, token string, peersStorage *PeerStorage) *Bot {
	var api *tg.Client
	var sender *message.Sender
	if conn != nil {
		api = tg.NewClient(conn)
		sender = message.NewSender(api)
	}
	return newBotWithAPI(nil, conn, api, sender, token, peersStorage)
}

// NewTestBot constructs a Bot targeting a custom HTTP Bot API serverURL.
func NewTestBot(serverURL, token string) *Bot {
	b := newBotWithAPI(nil, nil, nil, nil, token, nil)
	b.ServerURL = serverURL
	return b
}

func newBotWithAPI(
	client *telegram.Client,
	conn *engine.Conn,
	api *tg.Client,
	sender *message.Sender,
	token string,
	peersStorage *PeerStorage,
) *Bot {
	return &Bot{
		Client:      client,
		EngineConn:  conn,
		API:         api,
		Sender:      sender,
		Peers:       peersStorage,
		Token:       token,
		Dispatcher:  tg.NewUpdateDispatcher(),
		httpClient:  sharedHTTPClient(),
		dispatchSem: make(chan struct{}, defaultDispatchWorkers),
	}
}

func (b *Bot) endpoint() apiEndpoint {
	return apiEndpoint{
		api:        b.API,
		peers:      b.Peers,
		token:      b.Token,
		serverURL:  b.ServerURL,
		httpClient: b.httpClient,
	}
}

// RegisterHandler registers a pattern handler for messages, callbacks, or inline queries.
func (b *Bot) RegisterHandler(hType HandlerType, pattern string, mType MatchType, handler HandlerFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, registeredHandler{
		hType:   hType,
		pattern: pattern,
		mType:   mType,
		handler: handler,
	})
}

// RegisterHandlerMatchFunc registers a custom update predicate handler.
func (b *Bot) RegisterHandlerMatchFunc(match func(u *Update) bool, handler HandlerFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.matchFuncs = append(b.matchFuncs, registeredMatchFunc{
		match:   match,
		handler: handler,
	})
}

// SetupUpdateHandlers wires b.Dispatcher to cache peer entities and call DispatchUpdate.
func (b *Bot) SetupUpdateHandlers() {
	b.Dispatcher.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
		return b.handleTGMessage(ctx, e, u.Message)
	})
	b.Dispatcher.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
		return b.handleTGMessage(ctx, e, u.Message)
	})
	b.Dispatcher.OnBotCallbackQuery(func(ctx context.Context, e tg.Entities, u *tg.UpdateBotCallbackQuery) error {
		if b.Peers != nil {
			b.Peers.CacheEntities(e)
		}
		cb := callbackFromBotQuery(u, e)
		b.DispatchUpdate(ctx, &Update{ID: int(u.QueryID), CallbackQuery: cb})
		return nil
	})
	b.Dispatcher.OnInlineBotCallbackQuery(func(ctx context.Context, e tg.Entities, u *tg.UpdateInlineBotCallbackQuery) error {
		if b.Peers != nil {
			b.Peers.CacheEntities(e)
		}
		cb := callbackFromInlineQuery(u, e)
		b.DispatchUpdate(ctx, &Update{ID: int(u.QueryID), CallbackQuery: cb})
		return nil
	})
	b.Dispatcher.OnBotInlineQuery(func(ctx context.Context, e tg.Entities, u *tg.UpdateBotInlineQuery) error {
		if b.Peers != nil {
			b.Peers.CacheEntities(e)
		}
		iq := inlineQueryFromTG(u, e)
		b.DispatchUpdate(ctx, &Update{ID: int(u.QueryID), InlineQuery: iq})
		return nil
	})
}

func (b *Bot) handleTGMessage(ctx context.Context, e tg.Entities, raw tg.MessageClass) error {
	msg, ok := raw.(*tg.Message)
	if !ok {
		return nil
	}
	if b.Peers != nil {
		b.Peers.CacheEntities(e)
	}
	if upd := types.ConvertMessageToUpdate(msg, e); upd != nil {
		b.DispatchUpdate(ctx, upd)
	}
	return nil
}

// DispatchUpdate dispatches u to the first matching handler using bounded
// concurrency.
func (b *Bot) DispatchUpdate(ctx context.Context, u *Update) {
	if u == nil {
		return
	}
	b.mu.RLock()
	h := b.matchHandlerLocked(u)
	b.mu.RUnlock()
	if h == nil {
		return
	}

	if b.dispatchSem != nil {
		b.dispatchSem <- struct{}{}
		go func() {
			defer func() { <-b.dispatchSem }()
			h(ctx, b, u)
		}()
		return
	}
	go h(ctx, b, u)
}

func (b *Bot) matchHandlerLocked(u *Update) HandlerFunc {
	for _, mf := range b.matchFuncs {
		if mf.match(u) {
			return mf.handler
		}
	}
	if u.CallbackQuery != nil {
		data := u.CallbackQuery.Data
		for _, rh := range b.handlers {
			if rh.hType != HandlerTypeCallbackQueryData {
				continue
			}
			if matchString(data, rh.pattern, rh.mType) {
				return rh.handler
			}
		}
	}
	if u.InlineQuery != nil {
		for _, rh := range b.handlers {
			if rh.hType == HandlerTypeInlineQuery {
				return rh.handler
			}
		}
	}
	if u.Message != nil {
		text := u.Message.Text
		for _, rh := range b.handlers {
			if rh.hType != HandlerTypeMessageText {
				continue
			}
			if rh.pattern == "" || (text != "" && matchString(text, rh.pattern, rh.mType)) {
				return rh.handler
			}
		}
	}
	return nil
}

func matchString(s, pattern string, mType MatchType) bool {
	if mType == MatchTypeExact {
		return s == pattern
	}
	if mType == MatchTypePrefix {
		return strings.HasPrefix(s, pattern)
	}
	return false
}

// SendMessage sends a message via MTProto or Bot API HTTP fallback.
func (b *Bot) SendMessage(ctx context.Context, params *SendMessageParams) (*Message, error) {
	return b.endpoint().sendMessage(ctx, params)
}

// SendRichMessage sends a rich HTML message via Bot API HTTP.
func (b *Bot) SendRichMessage(ctx context.Context, chatID int64, htmlContent string, markup any) (int, error) {
	return b.endpoint().sendRichMessage(ctx, chatID, htmlContent, markup)
}

// CopyMessage copies a single message via Bot API HTTP.
func (b *Bot) CopyMessage(ctx context.Context, params *CopyMessageParams) (*MessageId, error) {
	return b.endpoint().copyMessage(ctx, params)
}

// CopyMessages copies multiple messages via Bot API HTTP.
func (b *Bot) CopyMessages(ctx context.Context, params *CopyMessagesParams) ([]MessageId, error) {
	return b.endpoint().copyMessages(ctx, params)
}

// EditMessageText edits a chat or inline message via MTProto or Bot API HTTP fallback.
func (b *Bot) EditMessageText(ctx context.Context, params *EditMessageTextParams) (*Message, error) {
	return b.endpoint().editMessageText(ctx, params)
}

// EditMessageReplyMarkup edits a message's reply markup via MTProto or Bot API HTTP fallback.
func (b *Bot) EditMessageReplyMarkup(ctx context.Context, params *EditMessageReplyMarkupParams) (*Message, error) {
	return b.endpoint().editMessageReplyMarkup(ctx, params)
}

// DeleteMessage deletes a single message via MTProto or Bot API HTTP fallback.
func (b *Bot) DeleteMessage(ctx context.Context, params *DeleteMessageParams) (bool, error) {
	return b.endpoint().deleteMessage(ctx, params)
}

// DeleteMessages deletes multiple messages via MTProto or Bot API HTTP fallback.
func (b *Bot) DeleteMessages(ctx context.Context, params *DeleteMessagesParams) (bool, error) {
	return b.endpoint().deleteMessages(ctx, params)
}

// AnswerCallbackQuery answers a callback query via MTProto or Bot API HTTP fallback.
func (b *Bot) AnswerCallbackQuery(ctx context.Context, params *AnswerCallbackQueryParams) (bool, error) {
	return b.endpoint().answerCallbackQuery(ctx, params)
}

// AnswerInlineQuery answers an inline query via Bot API HTTP.
func (b *Bot) AnswerInlineQuery(ctx context.Context, params *AnswerInlineQueryParams) (bool, error) {
	return b.endpoint().answerInlineQuery(ctx, params)
}
