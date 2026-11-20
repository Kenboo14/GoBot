package gobot

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/telegram/message/rich"
	"github.com/gotd/td/tg"

	"github.com/Kenboo14/GoBot/keyboard"
	"github.com/Kenboo14/GoBot/parser"
	"github.com/Kenboo14/GoBot/peers"
	"github.com/Kenboo14/GoBot/types"
)

type apiEndpoint struct {
	api        *tg.Client
	peers      *peers.Cache
	token      string
	serverURL  string
	httpClient *http.Client
}

func (c *Client) endpoint() apiEndpoint {
	return apiEndpoint{
		api:        c.api,
		peers:      c.peers,
		token:      c.token,
		serverURL:  c.serverURL,
		httpClient: c.httpClient,
	}
}

func (e apiEndpoint) hasHTTP() bool {
	return e.token != "" || e.serverURL != ""
}

func (e apiEndpoint) apiURL(method string) string {
	base := e.serverURL
	if base == "" {
		base = "https://api.telegram.org"
	}
	return fmt.Sprintf("%s/bot%s/%s", strings.TrimRight(base, "/"), e.token, method)
}

func parseChatID(val any) int64 {
	switch v := val.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		id, _ := strconv.ParseInt(v, 10, 64)
		return id
	default:
		return 0
	}
}

func randomID() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:]))
}

func isNotModifiedError(err error) bool {
	if err == nil {
		return false
	}
	low := strings.ToLower(err.Error())
	return strings.Contains(low, "not_modified") || strings.Contains(low, "not modified")
}

func extractMsgIDFromUpdates(updates tg.UpdatesClass) int {
	if updates == nil {
		return 0
	}
	switch u := updates.(type) {
	case *tg.Updates:
		return extractFromUpdateSlice(u.Updates)
	case *tg.UpdatesCombined:
		return extractFromUpdateSlice(u.Updates)
	case *tg.UpdateShortSentMessage:
		return u.ID
	case *tg.UpdateShortMessage:
		return u.ID
	}
	return 0
}

func extractFromUpdateSlice(list []tg.UpdateClass) int {
	for _, upd := range list {
		switch m := upd.(type) {
		case *tg.UpdateNewMessage:
			if msg, ok := m.Message.(*tg.Message); ok {
				return msg.ID
			}
		case *tg.UpdateNewChannelMessage:
			if msg, ok := m.Message.(*tg.Message); ok {
				return msg.ID
			}
		}
	}
	return 0
}

// SendText is a shorthand for SendMessage with optional ParseMode.
func (c *Client) SendText(ctx context.Context, chatID int64, text string, parseMode ...types.ParseMode) (*types.Message, error) {
	p := &types.SendMessageParams{ChatID: chatID, Text: text}
	if len(parseMode) > 0 {
		p.ParseMode = parseMode[0]
	}
	return c.SendMessage(ctx, p)
}

// SendMessage sends a text message via MTProto, falling back to Bot API HTTP
// if configured.
func (c *Client) SendMessage(ctx context.Context, params *types.SendMessageParams) (*types.Message, error) {
	return c.endpoint().sendMessage(ctx, params)
}

// SendRichMessage sends a rich HTML message via Bot API HTTP.
func (c *Client) SendRichMessage(ctx context.Context, chatID int64, htmlContent string, markup any) (int, error) {
	return c.endpoint().sendRichMessage(ctx, chatID, htmlContent, markup)
}

// CopyMessage copies a single message via Bot API HTTP.
func (c *Client) CopyMessage(ctx context.Context, params *types.CopyMessageParams) (*types.MessageId, error) {
	return c.endpoint().copyMessage(ctx, params)
}

// CopyMessages copies multiple messages via Bot API HTTP.
func (c *Client) CopyMessages(ctx context.Context, params *types.CopyMessagesParams) ([]types.MessageId, error) {
	return c.endpoint().copyMessages(ctx, params)
}

// EditMessageText edits a chat or inline message via MTProto (or HTTP fallback).
func (c *Client) EditMessageText(ctx context.Context, params *types.EditMessageTextParams) (*types.Message, error) {
	return c.endpoint().editMessageText(ctx, params)
}

// EditMessageReplyMarkup edits a message's reply markup via MTProto (or HTTP fallback).
func (c *Client) EditMessageReplyMarkup(ctx context.Context, params *types.EditMessageReplyMarkupParams) (*types.Message, error) {
	return c.endpoint().editMessageReplyMarkup(ctx, params)
}

// DeleteMessage deletes a single message via MTProto (or HTTP fallback).
func (c *Client) DeleteMessage(ctx context.Context, params *types.DeleteMessageParams) (bool, error) {
	return c.endpoint().deleteMessage(ctx, params)
}

// DeleteMessages deletes multiple messages via MTProto (or HTTP fallback).
func (c *Client) DeleteMessages(ctx context.Context, params *types.DeleteMessagesParams) (bool, error) {
	return c.endpoint().deleteMessages(ctx, params)
}

// AnswerCallbackQuery answers a callback query via MTProto (or HTTP fallback).
func (c *Client) AnswerCallbackQuery(ctx context.Context, params *types.AnswerCallbackQueryParams) (bool, error) {
	return c.endpoint().answerCallbackQuery(ctx, params)
}

// AnswerInlineQuery answers an inline query via Bot API HTTP.
func (c *Client) AnswerInlineQuery(ctx context.Context, params *types.AnswerInlineQueryParams) (bool, error) {
	return c.endpoint().answerInlineQuery(ctx, params)
}

func (e apiEndpoint) sendMessage(ctx context.Context, params *types.SendMessageParams) (*types.Message, error) {
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}
	chatID := parseChatID(params.ChatID)

	var mtErr error
	if e.api != nil && e.peers != nil && chatID != 0 {
		if peer := e.peers.GetInputPeer(chatID); peer != nil {
			plainText, entities := parser.ParseHTML(params.Text)
			markup := keyboard.ConvertReplyMarkup(params.ReplyMarkup)
			req := &tg.MessagesSendMessageRequest{
				Peer:        peer,
				Message:     plainText,
				Entities:    entities,
				ReplyMarkup: markup,
				RandomID:    randomID(),
			}
			if params.MessageThreadID > 0 {
				req.SetReplyTo(&tg.InputReplyToMessage{
					ReplyToMsgID: params.MessageThreadID,
					TopMsgID:     params.MessageThreadID,
				})
			} else if params.ReplyParameters != nil && params.ReplyParameters.MessageID > 0 {
				req.SetReplyTo(&tg.InputReplyToMessage{
					ReplyToMsgID: params.ReplyParameters.MessageID,
				})
			}
			if parser.IsRichContent(params.Text) {
				req.SetRichMessage(rich.HTML(parser.FormatRichHTML(params.Text)))
			}
			updates, err := e.api.MessagesSendMessage(ctx, req)
			if err == nil {
				return &types.Message{
					ID:   extractMsgIDFromUpdates(updates),
					Chat: types.Chat{ID: chatID},
				}, nil
			}
			mtErr = err
			if !e.hasHTTP() {
				return nil, err
			}
			log.Printf("[gobot] MessagesSendMessage MTProto error: %v, falling back to HTTP", err)
		}
	}

	if !e.hasHTTP() {
		if mtErr != nil {
			return nil, mtErr
		}
		return nil, fmt.Errorf("gobot: no MTProto peer or HTTP token configured")
	}

	if parser.IsRichContent(params.Text) && chatID != 0 {
		msgID, err := e.sendRichMessage(ctx, chatID, parser.FormatRichHTML(params.Text), params.ReplyMarkup)
		if err == nil {
			return &types.Message{ID: msgID, Chat: types.Chat{ID: chatID}}, nil
		}
		log.Printf("[gobot] sendRichMessage fallback: %v", err)
	}

	body := map[string]any{
		"chat_id": params.ChatID,
		"text":    parser.CleanHTML(params.Text),
	}
	if params.MessageThreadID > 0 {
		body["message_thread_id"] = params.MessageThreadID
	}
	if params.ParseMode != "" {
		body["parse_mode"] = string(params.ParseMode)
	} else if strings.Contains(params.Text, "<") && strings.Contains(params.Text, ">") {
		body["parse_mode"] = "HTML"
	}
	if params.ReplyMarkup != nil {
		body["reply_markup"] = params.ReplyMarkup
	}
	if params.ReplyParameters != nil {
		body["reply_parameters"] = params.ReplyParameters
	}
	return e.postHTTPMessage(ctx, e.apiURL("sendMessage"), body)
}

func (e apiEndpoint) sendRichMessage(ctx context.Context, chatID int64, htmlContent string, markup any) (int, error) {
	body := map[string]any{
		"chat_id": chatID,
		"rich_message": map[string]any{
			"html": parser.FormatRichHTML(htmlContent),
		},
	}
	if markup != nil {
		body["reply_markup"] = markup
	}
	respBody, status, err := e.doHTTPJSON(ctx, e.apiURL("sendRichMessage"), body)
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("sendRichMessage status %d: %s", status, string(respBody))
	}
	var res struct {
		OK     bool `json:"ok"`
		Result struct {
			MessageID int `json:"message_id"`
		} `json:"result"`
	}
	_ = json.Unmarshal(respBody, &res)
	return res.Result.MessageID, nil
}

func (e apiEndpoint) copyMessage(ctx context.Context, params *types.CopyMessageParams) (*types.MessageId, error) {
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}
	body := map[string]any{
		"chat_id":      params.ChatID,
		"from_chat_id": params.FromChatID,
		"message_id":   params.MessageID,
	}
	if params.Caption != "" {
		body["caption"] = params.Caption
	}
	if params.ParseMode != "" {
		body["parse_mode"] = string(params.ParseMode)
	}
	if params.ReplyMarkup != nil {
		body["reply_markup"] = params.ReplyMarkup
	}
	respBody, status, err := e.doHTTPJSON(ctx, e.apiURL("copyMessage"), body)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("copyMessage status %d: %s", status, string(respBody))
	}
	var res struct {
		OK     bool            `json:"ok"`
		Result types.MessageId `json:"result"`
	}
	if err := json.Unmarshal(respBody, &res); err != nil {
		return nil, err
	}
	return &res.Result, nil
}

func (e apiEndpoint) copyMessages(ctx context.Context, params *types.CopyMessagesParams) ([]types.MessageId, error) {
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}
	body := map[string]any{
		"chat_id":      params.ChatID,
		"from_chat_id": params.FromChatID,
		"message_ids":  params.MessageIDs,
	}
	respBody, status, err := e.doHTTPJSON(ctx, e.apiURL("copyMessages"), body)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("copyMessages status %d: %s", status, string(respBody))
	}
	var res struct {
		OK     bool              `json:"ok"`
		Result []types.MessageId `json:"result"`
	}
	if err := json.Unmarshal(respBody, &res); err != nil {
		return nil, err
	}
	return res.Result, nil
}

func (e apiEndpoint) editMessageText(ctx context.Context, params *types.EditMessageTextParams) (*types.Message, error) {
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}

	var mtErr error
	if e.api != nil {
		plainText, entities := parser.ParseHTML(params.Text)
		markup := keyboard.ConvertReplyMarkup(params.ReplyMarkup)

		if params.RawInlineMessageID != nil {
			req := &tg.MessagesEditInlineBotMessageRequest{
				ID:          params.RawInlineMessageID,
				Message:     plainText,
				Entities:    entities,
				ReplyMarkup: markup,
			}
			if parser.IsRichContent(params.Text) {
				req.SetRichMessage(rich.HTML(parser.FormatRichHTML(params.Text)))
			}
			_, err := e.api.MessagesEditInlineBotMessage(ctx, req)
			if err == nil || isNotModifiedError(err) {
				return &types.Message{}, nil
			}
			mtErr = err
			if !e.hasHTTP() {
				return nil, err
			}
			log.Printf("[gobot] MessagesEditInlineBotMessage MTProto error: %v, falling back to HTTP", err)
		}

		chatID := parseChatID(params.ChatID)
		if chatID != 0 && params.MessageID != 0 && e.peers != nil {
			if peer := e.peers.GetInputPeer(chatID); peer != nil {
				req := &tg.MessagesEditMessageRequest{
					Peer:        peer,
					ID:          params.MessageID,
					Message:     plainText,
					Entities:    entities,
					ReplyMarkup: markup,
				}
				if parser.IsRichContent(params.Text) {
					req.SetRichMessage(rich.HTML(parser.FormatRichHTML(params.Text)))
				}
				_, err := e.api.MessagesEditMessage(ctx, req)
				if err == nil || isNotModifiedError(err) {
					return &types.Message{ID: params.MessageID, Chat: types.Chat{ID: chatID}}, nil
				}
				mtErr = err
				if !e.hasHTTP() {
					return nil, err
				}
				log.Printf("[gobot] MessagesEditMessage MTProto error: %v, falling back to HTTP", err)
			}
		}
	}

	if !e.hasHTTP() {
		if mtErr != nil {
			return nil, mtErr
		}
		return nil, fmt.Errorf("gobot: no MTProto peer or HTTP token configured")
	}

	body := map[string]any{
		"text": parser.CleanHTML(params.Text),
	}
	if params.InlineMessageID != "" {
		body["inline_message_id"] = params.InlineMessageID
	} else {
		body["chat_id"] = params.ChatID
		body["message_id"] = params.MessageID
	}
	if params.ParseMode != "" {
		body["parse_mode"] = string(params.ParseMode)
	} else if strings.Contains(params.Text, "<") && strings.Contains(params.Text, ">") {
		body["parse_mode"] = "HTML"
	}
	if params.ReplyMarkup != nil {
		body["reply_markup"] = params.ReplyMarkup
	}
	msg, err := e.postHTTPMessage(ctx, e.apiURL("editMessageText"), body)
	if isNotModifiedError(err) {
		return &types.Message{ID: params.MessageID, Chat: types.Chat{ID: parseChatID(params.ChatID)}}, nil
	}
	return msg, err
}

func (e apiEndpoint) editMessageReplyMarkup(ctx context.Context, params *types.EditMessageReplyMarkupParams) (*types.Message, error) {
	if params == nil {
		return nil, fmt.Errorf("params cannot be nil")
	}

	if e.api != nil {
		markup := keyboard.ConvertReplyMarkup(params.ReplyMarkup)
		if params.RawInlineMessageID != nil {
			if _, err := e.api.MessagesEditInlineBotMessage(ctx, &tg.MessagesEditInlineBotMessageRequest{
				ID:          params.RawInlineMessageID,
				ReplyMarkup: markup,
			}); err == nil {
				return &types.Message{}, nil
			}
		}
		chatID := parseChatID(params.ChatID)
		if chatID != 0 && params.MessageID != 0 && e.peers != nil {
			if peer := e.peers.GetInputPeer(chatID); peer != nil {
				if _, err := e.api.MessagesEditMessage(ctx, &tg.MessagesEditMessageRequest{
					Peer:        peer,
					ID:          params.MessageID,
					ReplyMarkup: markup,
				}); err == nil {
					return &types.Message{ID: params.MessageID, Chat: types.Chat{ID: chatID}}, nil
				}
			}
		}
	}

	body := map[string]any{}
	if params.InlineMessageID != "" {
		body["inline_message_id"] = params.InlineMessageID
	} else {
		body["chat_id"] = params.ChatID
		body["message_id"] = params.MessageID
	}
	if params.ReplyMarkup != nil {
		body["reply_markup"] = params.ReplyMarkup
	}
	return e.postHTTPMessage(ctx, e.apiURL("editMessageReplyMarkup"), body)
}

func (e apiEndpoint) deleteMessage(ctx context.Context, params *types.DeleteMessageParams) (bool, error) {
	if params == nil {
		return false, fmt.Errorf("params cannot be nil")
	}
	return e.deleteMessages(ctx, &types.DeleteMessagesParams{
		ChatID:     params.ChatID,
		MessageIDs: []int{params.MessageID},
	})
}

func (e apiEndpoint) deleteMessages(ctx context.Context, params *types.DeleteMessagesParams) (bool, error) {
	if params == nil || len(params.MessageIDs) == 0 {
		return false, fmt.Errorf("params or message_ids cannot be empty")
	}

	var mtErr error
	if e.api != nil && e.peers != nil {
		chatID := parseChatID(params.ChatID)
		peer := e.peers.GetInputPeer(chatID)
		if ch, ok := peer.(*tg.InputPeerChannel); ok {
			_, mtErr = e.api.ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{
				Channel: &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash},
				ID:      params.MessageIDs,
			})
		} else {
			_, mtErr = e.api.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{
				Revoke: true,
				ID:     params.MessageIDs,
			})
		}
		if mtErr == nil {
			return true, nil
		}
		if !e.hasHTTP() {
			return false, mtErr
		}
	}

	if !e.hasHTTP() {
		if mtErr != nil {
			return false, mtErr
		}
		return false, fmt.Errorf("gobot: no MTProto client or HTTP token configured")
	}

	body := map[string]any{
		"chat_id":     params.ChatID,
		"message_ids": params.MessageIDs,
	}
	return e.postHTTPBool(ctx, e.apiURL("deleteMessages"), body)
}

func (e apiEndpoint) answerCallbackQuery(ctx context.Context, params *types.AnswerCallbackQueryParams) (bool, error) {
	if params == nil {
		return false, fmt.Errorf("params cannot be nil")
	}

	if e.api != nil {
		if qID, err := strconv.ParseInt(params.CallbackQueryID, 10, 64); err == nil {
			req := &tg.MessagesSetBotCallbackAnswerRequest{
				QueryID: qID,
				Alert:   params.ShowAlert,
			}
			if params.Text != "" {
				req.SetMessage(params.Text)
			}
			if params.URL != "" {
				req.SetURL(params.URL)
			}
			if _, rpcErr := e.api.MessagesSetBotCallbackAnswer(ctx, req); rpcErr != nil {
				log.Printf("[gobot] MessagesSetBotCallbackAnswer error: %v", rpcErr)
				return false, rpcErr
			}
			return true, nil
		}
	}

	body := map[string]any{
		"callback_query_id": params.CallbackQueryID,
	}
	if params.Text != "" {
		body["text"] = params.Text
	}
	if params.ShowAlert {
		body["show_alert"] = true
	}
	if params.URL != "" {
		body["url"] = params.URL
	}
	return e.postHTTPBool(ctx, e.apiURL("answerCallbackQuery"), body)
}

func (e apiEndpoint) answerInlineQuery(ctx context.Context, params *types.AnswerInlineQueryParams) (bool, error) {
	if params == nil {
		return false, fmt.Errorf("params cannot be nil")
	}
	formattedResults := make([]any, 0, len(params.Results))
	for _, res := range params.Results {
		if art, ok := res.(*types.InlineQueryResultArticle); ok {
			resMap := map[string]any{
				"type":                  "article",
				"id":                    art.ID,
				"title":                 art.Title,
				"input_message_content": art.InputMessageContent,
			}
			if art.Description != "" {
				resMap["description"] = art.Description
			}
			if art.ReplyMarkup != nil {
				resMap["reply_markup"] = art.ReplyMarkup
			}
			formattedResults = append(formattedResults, resMap)
		} else {
			formattedResults = append(formattedResults, res)
		}
	}
	body := map[string]any{
		"inline_query_id": params.InlineQueryID,
		"results":         formattedResults,
		"cache_time":      params.CacheTime,
		"is_personal":     params.IsPersonal,
	}
	return e.postHTTPBool(ctx, e.apiURL("answerInlineQuery"), body)
}

func (e apiEndpoint) doHTTPJSON(ctx context.Context, url string, body map[string]any) ([]byte, int, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, 0, err
	}
	client := e.httpClient
	if client == nil {
		client = sharedHTTPClient()
	}
	var resp *http.Response
	for attempt := 1; attempt <= 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		if attempt == 2 || ctx.Err() != nil {
			return nil, 0, err
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return respBody, resp.StatusCode, nil
}

func (e apiEndpoint) postHTTPMessage(ctx context.Context, url string, body map[string]any) (*types.Message, error) {
	respBody, status, err := e.doHTTPJSON(ctx, url, body)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("telegram API status %d: %s", status, string(respBody))
	}
	var res struct {
		OK     bool          `json:"ok"`
		Result types.Message `json:"result"`
	}
	if err := json.Unmarshal(respBody, &res); err != nil {
		return nil, err
	}
	return &res.Result, nil
}

func (e apiEndpoint) postHTTPBool(ctx context.Context, url string, body map[string]any) (bool, error) {
	respBody, status, err := e.doHTTPJSON(ctx, url, body)
	if err != nil {
		return false, err
	}
	if status != http.StatusOK {
		return false, fmt.Errorf("telegram API status %d: %s", status, string(respBody))
	}
	return true, nil
}
