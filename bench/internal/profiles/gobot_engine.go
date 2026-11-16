package profiles

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/tg"

	gobot "github.com/Kenboo14/GoBot"
	"github.com/Kenboo14/GoBot/engine"
	"github.com/Kenboo14/GoBot/filters"
	"github.com/Kenboo14/GoBot/types"
)

func init() {
	register(Profile{
		Name: "gobot-engine",
		Description: "GoBot lightweight multi-session engine (1 goroutine/session) " +
			"with shared Scheduler, worker pool, and sharded LRU peers.Cache.",
		Setup: setupGobotEngine,
	})
}

func setupGobotEngine(env *Env) Session {
	var (
		poolOnce sync.Once
		pool     *gobot.Pool
	)
	getPool := func() *gobot.Pool {
		poolOnce.Do(func() {
			pool = gobot.NewPool(gobot.PoolOptions{})
		})
		return pool
	}
	var mu sync.Mutex
	lastActive := map[int]time.Time{}
	peerSyncSem := make(chan struct{}, 4)
	var active atomic.Int64

	cmdCheck := filters.Me.And(filters.Prefix("."))

	return func(ctx context.Context, env *Env, idx int, ready func()) error {
		p := getPool()
		active.Add(1)
		defer func() {
			if active.Add(-1) == 0 && ctx.Err() != nil {
				p.Close()
			}
		}()

		client, err := p.NewClient(gobot.ClientOptions{
			AppID:          env.APIID,
			APIHash:        env.APIHash,
			DCList:         env.DCList,
			Resolver:       env.Resolver,
			PublicKeys:     []engine.PublicKey{env.PublicKey},
			SessionStorage: &session.StorageMemory{},
		})
		if err != nil {
			return err
		}

		client.OnMessage(filters.All, func(ctx context.Context, c *gobot.Client, m *types.Message) error {
			mu.Lock()
			lastActive[idx] = time.Now()
			mu.Unlock()
			if cmdCheck(m) {
				_ = m
			}
			env.Updates.Add(1)
			return nil
		})

		return client.Run(ctx, func(ctx context.Context) error {
			if _, err := client.Self(ctx); err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case peerSyncSem <- struct{}{}:
				sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				if c, err := client.API().ContactsGetContacts(sctx, 0); err == nil {
					if cc, ok := c.(*tg.ContactsContacts); ok {
						p.Peers.ApplyUsersChats(cc.Users, nil)
					}
				}
				if d, err := client.API().MessagesGetDialogs(sctx, &tg.MessagesGetDialogsRequest{
					Limit: 100, OffsetPeer: &tg.InputPeerEmpty{},
				}); err == nil {
					if mod, ok := d.AsModified(); ok {
						p.Peers.ApplyUsersChats(mod.GetUsers(), mod.GetChats())
					}
				}
				cancel()
				<-peerSyncSem
			}

			ready()
			return nil
		})
	}
}
