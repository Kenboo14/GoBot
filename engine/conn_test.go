package engine_test

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	"github.com/Kenboo14/GoBot/engine"
	"github.com/Kenboo14/GoBot/internal/fakedc"
)

func startTestDC(t *testing.T) (*fakedc.Server, fakedc.Info, context.CancelFunc) {
	t.Helper()
	srv, err := fakedc.New(fakedc.Options{Contacts: 10, Dialogs: 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	infoCh := make(chan fakedc.Info, 1)
	go func() {
		_ = srv.Serve(ctx, "127.0.0.1:0", func(info fakedc.Info) {
			infoCh <- info
		})
	}()
	info := <-infoCh
	return srv, info, cancel
}

func TestConn_E2E_HandshakeRPCUpdatesAndReconnect(t *testing.T) {
	srv, info, stopDC := startTestDC(t)
	defer stopDC()

	pk, err := info.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	sched := engine.NewScheduler(engine.SchedulerOptions{
		Workers:      4,
		AckInterval:  100 * time.Millisecond,
		AckBatchSize: 2,
		PingInterval: 30 * time.Second,
	})
	defer sched.Close()

	store := &session.StorageMemory{}
	var gotUpdates atomic.Int64
	updateCh := make(chan struct{}, 16)

	opts := engine.ConnOptions{
		Scheduler:      sched,
		AppID:          1,
		DC:             info.DC,
		DCList:         dcs.List{Options: []tg.DCOption{{ID: info.DC, IPAddress: info.Addr, Port: info.Port, Static: true}}},
		Resolver:       dcs.Plain(dcs.PlainOptions{Protocol: transport.Intermediate}),
		PublicKeys:     []engine.PublicKey{pk},
		SessionStorage: store,
		UpdateHandler: func(ctx context.Context, u tg.UpdatesClass) error {
			gotUpdates.Add(1)
			select {
			case updateCh <- struct{}{}:
			default:
			}
			return nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := engine.NewConn(opts)
	if err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	readyCh := make(chan struct{})
	go func() {
		runErr <- conn.Run(ctx, func(ctx context.Context) error {
			self, err := conn.Self(ctx)
			if err != nil {
				return err
			}
			if self.ID == 0 {
				t.Errorf("expected non-zero self ID")
			}
			api := tg.NewClient(conn)
			if _, err := api.ContactsGetContacts(ctx, 0); err != nil {
				return err
			}
			if _, err := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
				Limit:      10,
				OffsetPeer: &tg.InputPeerEmpty{},
			}); err != nil {
				return err
			}
			if err := conn.Ping(ctx); err != nil {
				return err
			}
			close(readyCh)
			return nil
		})
	}()

	select {
	case <-readyCh:
	case err := <-runErr:
		t.Fatalf("conn exited before ready: %v", err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for ready")
	}

	srv.SetRate(10)
	select {
	case <-updateCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for incoming update from fakedc")
	}
	srv.SetRate(0)

	cancel()
	if err := <-runErr; err != nil && err != context.Canceled {
		t.Fatalf("unexpected Run error on cancel: %v", err)
	}

	// Verify persisted session restores without re-exchanging auth key.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	conn2, err := engine.NewConn(opts)
	if err != nil {
		t.Fatal(err)
	}
	ready2 := make(chan struct{})
	runErr2 := make(chan error, 1)
	go func() {
		runErr2 <- conn2.Run(ctx2, func(ctx context.Context) error {
			if _, err := conn2.Self(ctx); err != nil {
				return err
			}
			close(ready2)
			return nil
		})
	}()
	select {
	case <-ready2:
	case err := <-runErr2:
		t.Fatalf("restored conn exited early: %v", err)
	case <-ctx2.Done():
		t.Fatal("timeout waiting for restored session")
	}
	cancel2()
	<-runErr2
}

func countClientGoroutines() int {
	buf := make([]byte, 4<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, block := range strings.Split(string(buf), "\n\n") {
		if strings.TrimSpace(block) == "" {
			continue
		}
		// startTestDC runs tgtest.Server in-process, which spawns 1 server-side
		// goroutine per accepted TCP connection. Exclude server-side frames.
		if strings.Contains(block, "github.com/gotd/td/tgtest.") {
			continue
		}
		n++
	}
	return n
}

func TestConn_FiftySessions_OneGoroutinePerSession(t *testing.T) {
	_, info, stopDC := startTestDC(t)
	defer stopDC()

	pk, err := info.PublicKey()
	if err != nil {
		t.Fatal(err)
	}

	sched := engine.NewScheduler(engine.SchedulerOptions{
		Workers:      4,
		PingInterval: 60 * time.Second,
	})
	baseTotal := runtime.NumGoroutine()
	baseClient := countClientGoroutines()

	const n = 50
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var ready atomic.Int64

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := engine.NewConn(engine.ConnOptions{
				Scheduler:      sched,
				AppID:          1,
				DC:             info.DC,
				DCList:         dcs.List{Options: []tg.DCOption{{ID: info.DC, IPAddress: info.Addr, Port: info.Port, Static: true}}},
				Resolver:       dcs.Plain(dcs.PlainOptions{Protocol: transport.Intermediate}),
				PublicKeys:     []engine.PublicKey{pk},
				SessionStorage: &session.StorageMemory{},
			})
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Run(ctx, func(ctx context.Context) error {
				if _, err := conn.Self(ctx); err != nil {
					return err
				}
				ready.Add(1)
				return nil
			})
		}()
	}

	deadline := time.Now().Add(20 * time.Second)
	for ready.Load() < n && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if ready.Load() != n {
		cancel()
		wg.Wait()
		sched.Close()
		t.Fatalf("only %d/%d sessions became ready", ready.Load(), n)
	}

	// Allow short-lived bootstrap goroutines to finish exiting.
	time.Sleep(150 * time.Millisecond)
	delta := float64(countClientGoroutines()-baseClient) / float64(n)
	t.Logf("client goroutines per session with n=%d: %.2f (client delta=%d)", n, delta, countClientGoroutines()-baseClient)
	if delta > 1.1 {
		cancel()
		wg.Wait()
		sched.Close()
		t.Fatalf("expected <= 1.1 client goroutines per session, got %.2f", delta)
	}

	cancel()
	wg.Wait()
	sched.Close()
	stopDC()

	time.Sleep(100 * time.Millisecond)
	if leaked := runtime.NumGoroutine() - baseTotal; leaked > 0 {
		t.Fatalf("expected 0 leaked goroutines after stop, got %d", leaked)
	}
}
