package profiles

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
)

func init() {
	register(Profile{
		Name:        "gotd-default",
		Description: "Plain gotd telegram.Client with a no-op counting update handler (lower bound for gotd).",
		Setup:       setupGotdDefault,
	})
	register(Profile{
		Name: "gotd-ubot",
		Description: "Mirrors GotdBot ubot session setup: tg.UpdateDispatcher, per-session peers.Manager " +
			"(InmemoryStorage/Cache) with UpdateHook, RPC middleware, OnConnectionState bookkeeping, " +
			"contacts+dialogs peer sync on ready, global access-hash sync.Maps.",
		Setup: setupGotdUbot,
	})
}

func baseOptions(env *Env) telegram.Options {
	return telegram.Options{
		PublicKeys:     []telegram.PublicKey{env.PublicKey},
		DCList:         env.DCList,
		Resolver:       env.Resolver,
		SessionStorage: &session.StorageMemory{},
	}
}

func setupGotdDefault(env *Env) Session {
	return func(ctx context.Context, env *Env, idx int, ready func()) error {
		opts := baseOptions(env)
		opts.UpdateHandler = telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
			env.Updates.Add(1)
			return nil
		})
		client := telegram.NewClient(env.APIID, env.APIHash, opts)
		return client.Run(ctx, func(ctx context.Context) error {
			if _, err := client.Self(ctx); err != nil {
				return err
			}
			ready()
			<-ctx.Done()
			return ctx.Err()
		})
	}
}

// ubotShared mirrors process-wide state held by GotdBot's userbot.Service.
type ubotShared struct {
	mu          sync.Mutex
	lastActive  map[int]time.Time
	lastUpdate  map[int]time.Time
	states      map[int]telegram.ConnectionState
	userHashes  sync.Map
	chanHashes  sync.Map
	peerSyncSem chan struct{}
}

type passMiddleware struct{}

func (passMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		return next.Invoke(ctx, input, output)
	}
}

func setupGotdUbot(env *Env) Session {
	sh := &ubotShared{
		lastActive:  map[int]time.Time{},
		lastUpdate:  map[int]time.Time{},
		states:      map[int]telegram.ConnectionState{},
		peerSyncSem: make(chan struct{}, 4),
	}
	cache := func(e tg.Entities) {
		for _, u := range e.Users {
			if u != nil && !u.Min && u.ID != 0 && u.AccessHash != 0 {
				sh.userHashes.Store(u.ID, u.AccessHash)
			}
		}
		for _, c := range e.Channels {
			if c != nil && !c.Min && c.ID != 0 && c.AccessHash != 0 {
				sh.chanHashes.Store(c.ID, c.AccessHash)
			}
		}
	}

	return func(ctx context.Context, env *Env, idx int, ready func()) error {
		var client *telegram.Client
		dispatcher := tg.NewUpdateDispatcher()
		onMsg := func(ctx context.Context, e tg.Entities, m tg.MessageClass) error {
			sh.mu.Lock()
			sh.lastActive[idx] = time.Now()
			sh.mu.Unlock()
			cache(e)
			if msg, ok := m.(*tg.Message); ok && msg.Out && strings.HasPrefix(msg.Message, ".") {
				_ = msg // command path not exercised by the benchmark
			}
			env.Updates.Add(1)
			return nil
		}
		dispatcher.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
			return onMsg(ctx, e, u.Message)
		})
		dispatcher.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
			return onMsg(ctx, e, u.Message)
		})

		var h telegram.UpdateHandler
		base := telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
			sh.mu.Lock()
			sh.lastUpdate[idx] = time.Now()
			sh.mu.Unlock()
			return dispatcher.Handle(ctx, u)
		})

		opts := baseOptions(env)
		opts.UpdateHandler = telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
			if h != nil {
				return h.Handle(ctx, u)
			}
			return base.Handle(ctx, u)
		})
		opts.Middlewares = []telegram.Middleware{passMiddleware{}}

		var pm *peers.Manager
		syncPeers := func(ctx context.Context) {
			select {
			case <-ctx.Done():
				return
			case sh.peerSyncSem <- struct{}{}:
				defer func() { <-sh.peerSyncSem }()
			}
			sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if c, err := client.API().ContactsGetContacts(sctx, 0); err == nil {
				if cc, ok := c.(*tg.ContactsContacts); ok {
					_ = pm.Apply(sctx, cc.Users, nil)
					for _, u := range cc.Users {
						if user, ok := u.(*tg.User); ok && user.AccessHash != 0 {
							sh.userHashes.Store(user.ID, user.AccessHash)
						}
					}
				}
			}
			if d, err := client.API().MessagesGetDialogs(sctx, &tg.MessagesGetDialogsRequest{
				Limit: 100, OffsetPeer: &tg.InputPeerEmpty{},
			}); err == nil {
				if mod, ok := d.AsModified(); ok {
					_ = pm.Apply(sctx, mod.GetUsers(), mod.GetChats())
					for _, u := range mod.GetUsers() {
						if user, ok := u.(*tg.User); ok && user.AccessHash != 0 {
							sh.userHashes.Store(user.ID, user.AccessHash)
						}
					}
					for _, c := range mod.GetChats() {
						if ch, ok := c.(*tg.Channel); ok && ch.AccessHash != 0 {
							sh.chanHashes.Store(ch.ID, ch.AccessHash)
						}
					}
				}
			}
		}

		var (
			runMu     sync.Mutex
			runCtx    context.Context
			readyOnce sync.Once
		)
		opts.OnConnectionState = func(st telegram.ConnectionState) {
			sh.mu.Lock()
			sh.states[idx] = st
			sh.mu.Unlock()
			runMu.Lock()
			rc := runCtx
			runMu.Unlock()
			if st == telegram.ConnectionStateReady && rc != nil {
				go syncPeers(rc) // re-sync after reconnect, like ubot
			}
		}

		client = telegram.NewClient(env.APIID, env.APIHash, opts)
		pm = peers.Options{
			Storage: &peers.InmemoryStorage{},
			Cache:   &peers.InmemoryCache{},
		}.Build(client.API())
		h = pm.UpdateHook(base)

		return client.Run(ctx, func(ctx context.Context) error {
			runMu.Lock()
			runCtx = ctx
			runMu.Unlock()
			_ = pm.Init(ctx)
			if _, err := client.Self(ctx); err != nil {
				return err
			}
			go syncPeers(ctx) // initial Ready fired before runCtx was set
			readyOnce.Do(ready)
			<-ctx.Done()
			return ctx.Err()
		})
	}
}
