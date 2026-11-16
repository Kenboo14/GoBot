package profiles

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"

	"github.com/Kenboo14/GoBot/peers"
)

func init() {
	register(Profile{
		Name: "gotd-optimized",
		Description: "Standard gotd telegram.Client replacing per-session peers.Manager " +
			"with GoBot's shared sharded LRU peers.Cache.",
		Setup: setupGotdOptimized,
	})
}

func setupGotdOptimized(env *Env) Session {
	cache := peers.NewCache(peers.Options{})
	var mu sync.Mutex
	lastActive := map[int]time.Time{}
	lastUpdate := map[int]time.Time{}
	states := map[int]telegram.ConnectionState{}
	peerSyncSem := make(chan struct{}, 4)

	return func(ctx context.Context, env *Env, idx int, ready func()) error {
		var client *telegram.Client
		dispatcher := tg.NewUpdateDispatcher()
		onMsg := func(ctx context.Context, e tg.Entities, m tg.MessageClass) error {
			mu.Lock()
			lastActive[idx] = time.Now()
			mu.Unlock()
			cache.CacheEntities(e)
			if msg, ok := m.(*tg.Message); ok && msg.Out && strings.HasPrefix(msg.Message, ".") {
				_ = msg
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

		opts := baseOptions(env)
		opts.UpdateHandler = telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
			mu.Lock()
			lastUpdate[idx] = time.Now()
			mu.Unlock()
			return dispatcher.Handle(ctx, u)
		})
		opts.Middlewares = []telegram.Middleware{passMiddleware{}}

		syncPeers := func(ctx context.Context) {
			select {
			case <-ctx.Done():
				return
			case peerSyncSem <- struct{}{}:
				defer func() { <-peerSyncSem }()
			}
			sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if c, err := client.API().ContactsGetContacts(sctx, 0); err == nil {
				if cc, ok := c.(*tg.ContactsContacts); ok {
					cache.ApplyUsersChats(cc.Users, nil)
				}
			}
			if d, err := client.API().MessagesGetDialogs(sctx, &tg.MessagesGetDialogsRequest{
				Limit: 100, OffsetPeer: &tg.InputPeerEmpty{},
			}); err == nil {
				if mod, ok := d.AsModified(); ok {
					cache.ApplyUsersChats(mod.GetUsers(), mod.GetChats())
				}
			}
		}

		var (
			runMu     sync.Mutex
			runCtx    context.Context
			readyOnce sync.Once
		)
		opts.OnConnectionState = func(st telegram.ConnectionState) {
			mu.Lock()
			states[idx] = st
			mu.Unlock()
			runMu.Lock()
			rc := runCtx
			runMu.Unlock()
			if st == telegram.ConnectionStateReady && rc != nil {
				go syncPeers(rc)
			}
		}

		client = telegram.NewClient(env.APIID, env.APIHash, opts)
		return client.Run(ctx, func(ctx context.Context) error {
			runMu.Lock()
			runCtx = ctx
			runMu.Unlock()
			if _, err := client.Self(ctx); err != nil {
				return err
			}
			go syncPeers(ctx)
			readyOnce.Do(ready)
			<-ctx.Done()
			return ctx.Err()
		})
	}
}

// ensure bin.Encoder import isn't shadowed in passMiddleware reference.
var _ = bin.Buffer{}
