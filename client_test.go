package gobot_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	gobot "github.com/Kenboo14/GoBot"
	"github.com/Kenboo14/GoBot/engine"
	"github.com/Kenboo14/GoBot/filters"
	"github.com/Kenboo14/GoBot/internal/fakedc"
	"github.com/Kenboo14/GoBot/keyboard"
	"github.com/Kenboo14/GoBot/types"
)

func TestClient_PoolOnMessageAndResolve(t *testing.T) {
	srv, err := fakedc.New(fakedc.Options{Contacts: 5, Dialogs: 5})
	if err != nil {
		t.Fatal(err)
	}
	dcCtx, stopDC := context.WithCancel(context.Background())
	defer stopDC()
	infoCh := make(chan fakedc.Info, 1)
	go func() {
		_ = srv.Serve(dcCtx, "127.0.0.1:0", func(info fakedc.Info) {
			infoCh <- info
		})
	}()
	info := <-infoCh
	pk, err := info.PublicKey()
	if err != nil {
		t.Fatal(err)
	}

	pool := gobot.NewPool(gobot.PoolOptions{
		Scheduler: engine.SchedulerOptions{Workers: 4},
	})
	defer pool.Close()

	client, err := pool.NewClient(gobot.ClientOptions{
		AppID:          1,
		APIHash:        "test",
		DC:             info.DC,
		DCList:         dcs.List{Options: []tg.DCOption{{ID: info.DC, IPAddress: info.Addr, Port: info.Port, Static: true}}},
		Resolver:       dcs.Plain(dcs.PlainOptions{Protocol: transport.Intermediate}),
		PublicKeys:     []engine.PublicKey{pk},
		SessionStorage: &session.StorageMemory{},
	})
	if err != nil {
		t.Fatal(err)
	}

	msgCh := make(chan *types.Message, 4)
	client.OnMessage(filters.Incoming.And(filters.Private).And(filters.Text), func(ctx context.Context, c *gobot.Client, m *types.Message) error {
		msgCh <- m
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	readyCh := make(chan struct{})
	runErr := make(chan error, 1)
	go func() {
		runErr <- client.Run(ctx, func(ctx context.Context) error {
			u, err := client.Self(ctx)
			if err != nil {
				return err
			}
			if u.ID == 0 {
				t.Errorf("expected non-zero self ID")
			}
			close(readyCh)
			return nil
		})
	}()

	select {
	case <-readyCh:
	case err := <-runErr:
		t.Fatalf("client exited early: %v", err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for client ready")
	}

	srv.SetRate(10)
	var got *types.Message
	select {
	case got = <-msgCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for filtered message")
	}
	srv.SetRate(0)

	if got.From == nil || got.From.ID != 900000001 {
		t.Fatalf("unexpected sender: %+v", got.From)
	}
	// Verify incoming update entities populated the shared peer cache.
	if h, ok := pool.Peers.UserHash(900000001); !ok || h != 42 {
		t.Fatalf("expected sender hash 42 in shared cache, got %d (ok=%v)", h, ok)
	}

	cancel()
	<-runErr
}

func TestBotCompat_HandlersAndHTTPFallback(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/botTOK/sendMessage", "/botTOK/editMessageText":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":     true,
				"result": map[string]any{"message_id": 77, "chat": map[string]any{"id": 123}},
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": true})
		}
	}))
	defer srv.Close()

	bot := gobot.NewTestBot(srv.URL, "TOK")
	done := make(chan string, 2)
	bot.RegisterHandler(gobot.HandlerTypeMessageText, "/start", gobot.MatchTypePrefix, func(ctx context.Context, b *gobot.Bot, u *gobot.Update) {
		_, err := b.SendMessage(ctx, &gobot.SendMessageParams{
			ChatID:      u.Message.Chat.ID,
			Text:        "<b>Hi</b>",
			ReplyMarkup: keyboard.Inline(keyboard.Row(keyboard.Callback("OK", "ok"))),
		})
		if err != nil {
			t.Error(err)
		}
		done <- "msg"
	})
	bot.RegisterHandler(gobot.HandlerTypeCallbackQueryData, "ok", gobot.MatchTypeExact, func(ctx context.Context, b *gobot.Bot, u *gobot.Update) {
		if _, err := b.AnswerCallbackQuery(ctx, &gobot.AnswerCallbackQueryParams{
			CallbackQueryID: u.CallbackQuery.ID,
			Text:            "done",
		}); err != nil {
			t.Error(err)
		}
		if _, err := b.EditMessageText(ctx, &gobot.EditMessageTextParams{
			ChatID:    u.CallbackQuery.Message.Message.Chat.ID,
			MessageID: u.CallbackQuery.Message.Message.ID,
			Text:      "Edited",
		}); err != nil {
			t.Error(err)
		}
		if _, err := b.DeleteMessage(ctx, &gobot.DeleteMessageParams{
			ChatID:    u.CallbackQuery.Message.Message.Chat.ID,
			MessageID: u.CallbackQuery.Message.Message.ID,
		}); err != nil {
			t.Error(err)
		}
		done <- "cb"
	})

	ctx := context.Background()
	bot.DispatchUpdate(ctx, &gobot.Update{
		ID:      1,
		Message: &gobot.Message{ID: 1, Chat: gobot.Chat{ID: 123}, Text: "/start"},
	})
	bot.DispatchUpdate(ctx, &gobot.Update{
		ID: 2,
		CallbackQuery: &gobot.CallbackQuery{
			ID:   "cb1",
			Data: "ok",
			Message: &gobot.MaybeInaccessibleMessage{
				Message: &gobot.Message{ID: 77, Chat: gobot.Chat{ID: 123}},
			},
		},
	})

	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for DispatchUpdate handlers")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 4 {
		t.Fatalf("expected 4 HTTP API calls, got %v", calls)
	}
}

type countMiddleware struct {
	calls *atomic.Int64
}

func (m countMiddleware) Handle(next tg.Invoker) gobot.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		m.calls.Add(1)
		return next.Invoke(ctx, input, output)
	}
}

func TestNewTelegramClient_MiddlewareStateNoUpdatesAuthSender(t *testing.T) {
	srv, err := fakedc.New(fakedc.Options{Contacts: 2, Dialogs: 2})
	if err != nil {
		t.Fatal(err)
	}
	dcCtx, stopDC := context.WithCancel(context.Background())
	defer stopDC()
	infoCh := make(chan fakedc.Info, 1)
	go func() {
		_ = srv.Serve(dcCtx, "127.0.0.1:0", func(info fakedc.Info) {
			infoCh <- info
		})
	}()
	info := <-infoCh
	pk, err := info.PublicKey()
	if err != nil {
		t.Fatal(err)
	}

	pool := gobot.NewPool(gobot.PoolOptions{
		Scheduler: engine.SchedulerOptions{Workers: 4},
	})
	defer pool.Close()

	var mwCalls atomic.Int64
	var mu sync.Mutex
	var states []gobot.ConnectionState

	client := gobot.NewTelegramClient(1, "hash", gobot.TelegramOptions{
		Pool:           pool,
		DC:             info.DC,
		DCList:         dcs.List{Options: []tg.DCOption{{ID: info.DC, IPAddress: info.Addr, Port: info.Port, Static: true}}},
		Resolver:       dcs.Plain(dcs.PlainOptions{Protocol: transport.Intermediate}),
		PublicKeys:     []engine.PublicKey{pk},
		SessionStorage: &session.StorageMemory{},
		NoUpdates:      true,
		Middlewares:    []gobot.Middleware{countMiddleware{calls: &mwCalls}},
		OnConnectionState: func(st gobot.ConnectionState) {
			mu.Lock()
			states = append(states, st)
			mu.Unlock()
		},
	})

	if client.Auth() == nil {
		t.Fatal("expected non-nil Auth()")
	}
	if client.Sender() == nil {
		t.Fatal("expected non-nil Sender()")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// NoUpdates: true should exit Run with nil as soon as onReady returns nil.
	err = client.Run(ctx, func(ctx context.Context) error {
		u, err := client.Self(ctx)
		if err != nil {
			return err
		}
		if u.ID == 0 {
			t.Errorf("expected non-zero user ID")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected clean nil return for NoUpdates one-shot Run, got: %v", err)
	}
	if mwCalls.Load() == 0 {
		t.Fatal("expected middleware to be invoked on RPC")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(states) < 2 || states[0] != gobot.ConnectionStateConnecting || states[1] != gobot.ConnectionStateReady {
		t.Fatalf("expected [Connecting, Ready], got %v", states)
	}
}
