package gobot

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/Kenboo14/GoBot/engine"
	"github.com/Kenboo14/GoBot/filters"
	"github.com/Kenboo14/GoBot/peers"
	"github.com/Kenboo14/GoBot/types"
)

// ClientOptions configures a GoBot Client.
type ClientOptions struct {
	// Pool shares scheduler and peer cache across sessions. If nil, NewClient
	// creates an internal Pool owned by the Client and closed when Run returns.
	Pool *Pool

	AppID          int
	APIHash        string
	DC             int
	DCList         dcs.List
	Resolver       dcs.Resolver
	PublicKeys     []engine.PublicKey
	SessionStorage session.Storage
	Device         engine.DeviceConfig
	Layer          int
	NoUpdates      bool

	// Optional Bot API HTTP fallback configuration.
	BotToken  string
	ServerURL string
}

// MessageHandler handles incoming or outgoing messages matching a Filter.
type MessageHandler func(ctx context.Context, c *Client, m *types.Message) error

// CallbackHandler handles callback queries matching a CallbackFilter.
type CallbackHandler func(ctx context.Context, c *Client, cq *types.CallbackQuery) error

// InlineHandler handles inline queries.
type InlineHandler func(ctx context.Context, c *Client, iq *types.InlineQuery) error

// RawUpdateHandler handles raw MTProto updates and associated entities.
type RawUpdateHandler func(ctx context.Context, e tg.Entities, u tg.UpdateClass) error

type messageEntry struct {
	filter  filters.Filter
	handler MessageHandler
}

type callbackEntry struct {
	filter  filters.CallbackFilter
	handler CallbackHandler
}

// Client is a high-level Telegram MTProto / Bot API client.
type Client struct {
	pool     *Pool
	ownsPool bool
	conn     *engine.Conn
	tgClient *telegram.Client
	api      *tg.Client
	peers    *peers.Cache

	token      string
	serverURL  string
	httpClient *http.Client
	selfID     atomic.Int64

	mu             sync.RWMutex
	msgHandlers    []messageEntry
	cbHandlers     []callbackEntry
	inlineHandlers []InlineHandler
	rawHandlers    []RawUpdateHandler
}

// NewClient constructs a lightweight GoBot Client backed by engine.Conn.
func NewClient(opts ClientOptions) (*Client, error) {
	pool := opts.Pool
	ownsPool := false
	if pool == nil {
		pool = NewPool(PoolOptions{})
		ownsPool = true
	} else {
		if pool.Scheduler == nil {
			pool.Scheduler = engine.NewScheduler(engine.SchedulerOptions{})
		}
		if pool.Peers == nil {
			pool.Peers = peers.NewCache(peers.Options{})
		}
	}

	c := &Client{
		pool:      pool,
		ownsPool:  ownsPool,
		peers:     pool.Peers,
		token:     opts.BotToken,
		serverURL: opts.ServerURL,
	}
	if opts.BotToken != "" || opts.ServerURL != "" {
		c.httpClient = sharedHTTPClient()
	}

	conn, err := engine.NewConn(engine.ConnOptions{
		Scheduler:      pool.Scheduler,
		AppID:          opts.AppID,
		DC:             opts.DC,
		DCList:         opts.DCList,
		Resolver:       opts.Resolver,
		PublicKeys:     opts.PublicKeys,
		SessionStorage: opts.SessionStorage,
		Device:         opts.Device,
		Layer:          opts.Layer,
		NoUpdates:      opts.NoUpdates,
		UpdateHandler:  c.handleUpdates,
	})
	if err != nil {
		if ownsPool {
			pool.Close()
		}
		return nil, err
	}
	c.conn = conn
	c.api = tg.NewClient(conn)
	return c, nil
}

// NewFromGotd wraps an existing *telegram.Client with a shared *peers.Cache.
func NewFromGotd(tgClient *telegram.Client, cache *peers.Cache) *Client {
	if cache == nil {
		cache = peers.NewCache(peers.Options{})
	}
	var api *tg.Client
	if tgClient != nil {
		api = tg.NewClient(tgClient)
	}
	return &Client{
		tgClient: tgClient,
		api:      api,
		peers:    cache,
	}
}

var (
	httpClientOnce sync.Once
	httpClientInst *http.Client
)

func sharedHTTPClient() *http.Client {
	httpClientOnce.Do(func() {
		httpClientInst = &http.Client{
			Timeout: 35 * time.Second,
			Transport: &http.Transport{
				ForceAttemptHTTP2:   true,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		}
	})
	return httpClientInst
}

// Conn returns the underlying engine.Conn, or nil if created via NewFromGotd.
func (c *Client) Conn() *engine.Conn { return c.conn }

// API returns the raw *tg.Client RPC wrapper.
func (c *Client) API() *tg.Client { return c.api }

// Peers returns the shared peer cache.
func (c *Client) Peers() *peers.Cache { return c.peers }

// UpdateHandler returns a telegram.UpdateHandler forwarding updates into this
// Client's registered handlers and peer cache (useful with NewFromGotd).
func (c *Client) UpdateHandler() telegram.UpdateHandler {
	return telegram.UpdateHandlerFunc(c.handleUpdates)
}

// OnMessage registers a message handler guarded by filter f (nil matches all).
func (c *Client) OnMessage(f filters.Filter, h MessageHandler) {
	if h == nil {
		return
	}
	if f == nil {
		f = filters.All
	}
	c.mu.Lock()
	c.msgHandlers = append(c.msgHandlers, messageEntry{filter: f, handler: h})
	c.mu.Unlock()
}

// OnCallbackQuery registers a callback query handler guarded by f (nil matches all).
func (c *Client) OnCallbackQuery(f filters.CallbackFilter, h CallbackHandler) {
	if h == nil {
		return
	}
	c.mu.Lock()
	c.cbHandlers = append(c.cbHandlers, callbackEntry{filter: f, handler: h})
	c.mu.Unlock()
}

// OnInlineQuery registers an inline query handler.
func (c *Client) OnInlineQuery(h InlineHandler) {
	if h == nil {
		return
	}
	c.mu.Lock()
	c.inlineHandlers = append(c.inlineHandlers, h)
	c.mu.Unlock()
}

// OnRawUpdate registers a raw MTProto update handler.
func (c *Client) OnRawUpdate(h RawUpdateHandler) {
	if h == nil {
		return
	}
	c.mu.Lock()
	c.rawHandlers = append(c.rawHandlers, h)
	c.mu.Unlock()
}

// Run starts the client and keeps the connection running until ctx is canceled
// or the transport closes.
//
// In engine mode, onReady should return after startup actions (e.g. Self or
// initial sync); the connection stays alive until ctx is canceled.
func (c *Client) Run(ctx context.Context, onReady func(ctx context.Context) error) error {
	if c.conn != nil {
		if c.ownsPool {
			defer c.pool.Close()
		}
		return c.conn.Run(ctx, onReady)
	}
	if c.tgClient != nil {
		return c.tgClient.Run(ctx, func(ctx context.Context) error {
			if onReady != nil {
				if err := onReady(ctx); err != nil {
					return err
				}
			}
			<-ctx.Done()
			return ctx.Err()
		})
	}
	return errors.New("gobot: client has neither engine.Conn nor telegram.Client")
}

// Self fetches and caches the current authorized user.
func (c *Client) Self(ctx context.Context) (*tg.User, error) {
	if c.conn != nil {
		u, err := c.conn.Self(ctx)
		if err == nil && u != nil {
			c.selfID.Store(u.ID)
		}
		return u, err
	}
	if c.tgClient != nil {
		u, err := c.tgClient.Self(ctx)
		if err == nil && u != nil {
			c.selfID.Store(u.ID)
		}
		return u, err
	}
	return nil, errors.New("gobot: no MTProto client available")
}

// Ping measures round-trip connectivity with an MTProto ping.
func (c *Client) Ping(ctx context.Context) error {
	if c.conn != nil {
		return c.conn.Ping(ctx)
	}
	if c.tgClient != nil {
		return c.tgClient.Ping(ctx)
	}
	return errors.New("gobot: no MTProto client available")
}

// ResolvePeer resolves a tg.PeerClass using update entities, the shared cache,
// and RPC fallbacks.
func (c *Client) ResolvePeer(ctx context.Context, entities tg.Entities, p tg.PeerClass, msgID int) (tg.InputPeerClass, error) {
	return c.peers.ResolvePeer(ctx, c.api, c.selfID.Load(), entities, p, msgID)
}

func (c *Client) handleUpdates(ctx context.Context, u tg.UpdatesClass) error {
	if u == nil {
		return nil
	}

	var (
		updates []tg.UpdateClass
		users   []tg.UserClass
		chats   []tg.ChatClass
	)
	switch upd := u.(type) {
	case *tg.Updates:
		updates, users, chats = upd.Updates, upd.Users, upd.Chats
	case *tg.UpdatesCombined:
		updates, users, chats = upd.Updates, upd.Users, upd.Chats
	case *tg.UpdateShort:
		updates = []tg.UpdateClass{upd.Update}
	default:
		return nil
	}

	if c.peers != nil && (len(users) > 0 || len(chats) > 0) {
		c.peers.ApplyUsersChats(users, chats)
	}

	c.mu.RLock()
	hasHandlers := len(c.msgHandlers) > 0 || len(c.cbHandlers) > 0 || len(c.inlineHandlers) > 0 || len(c.rawHandlers) > 0
	c.mu.RUnlock()
	if !hasHandlers || len(updates) == 0 {
		return nil
	}

	entities := buildEntities(users, chats)
	for _, item := range updates {
		if item == nil {
			continue
		}
		c.dispatchSingleUpdate(ctx, entities, item)
	}
	return nil
}

func buildEntities(users []tg.UserClass, chats []tg.ChatClass) tg.Entities {
	var e tg.Entities
	if len(users) > 0 {
		e.Users = make(map[int64]*tg.User, len(users))
		for _, item := range users {
			if u, ok := item.(*tg.User); ok && u != nil {
				e.Users[u.ID] = u
			}
		}
	}
	if len(chats) > 0 {
		for _, item := range chats {
			switch ch := item.(type) {
			case *tg.Chat:
				if ch != nil {
					if e.Chats == nil {
						e.Chats = make(map[int64]*tg.Chat)
					}
					e.Chats[ch.ID] = ch
				}
			case *tg.Channel:
				if ch != nil {
					if e.Channels == nil {
						e.Channels = make(map[int64]*tg.Channel)
					}
					e.Channels[ch.ID] = ch
				}
			}
		}
	}
	return e
}

func (c *Client) dispatchSingleUpdate(ctx context.Context, entities tg.Entities, item tg.UpdateClass) {
	c.mu.RLock()
	raw := c.rawHandlers
	msgs := c.msgHandlers
	cbs := c.cbHandlers
	inlines := c.inlineHandlers
	c.mu.RUnlock()

	for _, h := range raw {
		_ = h(ctx, entities, item)
	}

	switch upd := item.(type) {
	case *tg.UpdateNewMessage:
		if len(msgs) == 0 {
			return
		}
		if rawMsg, ok := upd.Message.(*tg.Message); ok {
			m := types.ConvertMessage(rawMsg, entities)
			c.runMessageHandlers(ctx, msgs, m)
		}
	case *tg.UpdateNewChannelMessage:
		if len(msgs) == 0 {
			return
		}
		if rawMsg, ok := upd.Message.(*tg.Message); ok {
			m := types.ConvertMessage(rawMsg, entities)
			c.runMessageHandlers(ctx, msgs, m)
		}
	case *tg.UpdateBotCallbackQuery:
		if len(cbs) == 0 {
			return
		}
		cq := callbackFromBotQuery(upd, entities)
		c.runCallbackHandlers(ctx, cbs, cq)
	case *tg.UpdateInlineBotCallbackQuery:
		if len(cbs) == 0 {
			return
		}
		cq := callbackFromInlineQuery(upd, entities)
		c.runCallbackHandlers(ctx, cbs, cq)
	case *tg.UpdateBotInlineQuery:
		if len(inlines) == 0 {
			return
		}
		iq := inlineQueryFromTG(upd, entities)
		for _, h := range inlines {
			_ = h(ctx, c, iq)
		}
	}
}

func (c *Client) runMessageHandlers(ctx context.Context, list []messageEntry, m *types.Message) {
	if m == nil {
		return
	}
	for _, entry := range list {
		if entry.filter(m) {
			_ = entry.handler(ctx, c, m)
		}
	}
}

func (c *Client) runCallbackHandlers(ctx context.Context, list []callbackEntry, cq *types.CallbackQuery) {
	if cq == nil {
		return
	}
	for _, entry := range list {
		if entry.filter == nil || entry.filter(cq) {
			_ = entry.handler(ctx, c, cq)
		}
	}
}

func userFromEntities(id int64, e tg.Entities) types.User {
	u := types.User{ID: id}
	if found, ok := e.Users[id]; ok && found != nil {
		u.FirstName = found.FirstName
		u.LastName = found.LastName
		u.Username = found.Username
		u.IsBot = found.Bot
	}
	return u
}

func peerToChatID(p tg.PeerClass) int64 {
	switch v := p.(type) {
	case *tg.PeerUser:
		return v.UserID
	case *tg.PeerChat:
		return -v.ChatID
	case *tg.PeerChannel:
		return -1000000000000 - v.ChannelID
	default:
		return 0
	}
}

func callbackFromBotQuery(u *tg.UpdateBotCallbackQuery, e tg.Entities) *types.CallbackQuery {
	from := userFromEntities(u.UserID, e)
	chatID := peerToChatID(u.Peer)
	return &types.CallbackQuery{
		ID:   strconv.FormatInt(u.QueryID, 10),
		Data: string(u.Data),
		From: from,
		Message: &types.MaybeInaccessibleMessage{
			Message: &types.Message{
				ID:   u.MsgID,
				Chat: types.Chat{ID: chatID},
				From: &from,
			},
		},
	}
}

func callbackFromInlineQuery(u *tg.UpdateInlineBotCallbackQuery, e tg.Entities) *types.CallbackQuery {
	var inlineMsgID string
	if u.MsgID != nil {
		buf := &bin.Buffer{}
		if err := u.MsgID.Encode(buf); err == nil {
			inlineMsgID = base64.RawURLEncoding.EncodeToString(buf.Raw())
		}
	}
	return &types.CallbackQuery{
		ID:                 strconv.FormatInt(u.QueryID, 10),
		Data:               string(u.Data),
		InlineMessageID:    inlineMsgID,
		RawInlineMessageID: u.MsgID,
		From:               userFromEntities(u.UserID, e),
	}
}

func inlineQueryFromTG(u *tg.UpdateBotInlineQuery, e tg.Entities) *types.InlineQuery {
	return &types.InlineQuery{
		ID:     strconv.FormatInt(u.QueryID, 10),
		Query:  u.Query,
		Offset: u.Offset,
		From:   userFromEntities(u.UserID, e),
	}
}
