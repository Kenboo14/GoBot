// Package fakedc implements a local fake Telegram datacenter used to load-test
// MTProto clients without touching real Telegram infrastructure.
//
// It is built on github.com/gotd/td/tgtest and is meant to run in a separate
// process from the client under test, so the client's resource usage is not
// polluted by server-side goroutines and buffers.
package fakedc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/tgtest/services"
	"github.com/gotd/td/tgtest/services/config"
	"github.com/gotd/td/transport"
)

// Info is written to disk so the load generator can find and trust the DC.
type Info struct {
	DC           int    `json:"dc"`
	Addr         string `json:"addr"`
	Port         int    `json:"port"`
	PublicKeyPEM string `json:"public_key_pem"`
	Control      string `json:"control"`
}

// WriteFile writes info as JSON.
func (i Info) WriteFile(path string) error {
	b, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// ReadInfo reads info written by WriteFile.
func ReadInfo(path string) (Info, error) {
	var i Info
	b, err := os.ReadFile(path)
	if err != nil {
		return i, err
	}
	return i, json.Unmarshal(b, &i)
}

// PublicKey parses PublicKeyPEM.
func (i Info) PublicKey() (exchange.PublicKey, error) {
	blk, _ := pem.Decode([]byte(i.PublicKeyPEM))
	if blk == nil {
		return exchange.PublicKey{}, os.ErrInvalid
	}
	k, err := x509.ParsePKCS1PublicKey(blk.Bytes)
	if err != nil {
		return exchange.PublicKey{}, err
	}
	return exchange.PublicKey{RSA: k}, nil
}

// Options configures the fake DC.
type Options struct {
	DC       int
	Contacts int // users returned by contacts.getContacts
	Dialogs  int // users+channels returned by messages.getDialogs
}

// Server is a fake DC.
type Server struct {
	opts Options
	key  *rsa.PrivateKey
	srv  *tgtest.Server
	cfg  tg.Config

	mu       sync.Mutex
	sessions map[int64]tgtest.Session
	order    []int64
	nextUser atomic.Int64
	selfIDs  sync.Map // session id -> user id

	contacts *tg.ContactsContacts
	dialogs  *tg.MessagesDialogs
	sender   *tg.User

	rate     atomic.Uint64 // updates per session per second, *1000
	sent     atomic.Uint64
	sendErrs atomic.Uint64
	msgID    atomic.Int64
}

// New creates a fake DC. RSA key generation takes ~1s.
func New(opts Options) (*Server, error) {
	if opts.DC == 0 {
		opts.DC = 2
	}
	key, err := rsa.GenerateKey(rand.Reader, crypto.RSAKeyBits)
	if err != nil {
		return nil, err
	}
	s := &Server{
		opts:     opts,
		key:      key,
		sessions: map[int64]tgtest.Session{},
		sender:   &tg.User{ID: 900000001, AccessHash: 42, FirstName: "Sender", Username: "sender"},
	}
	s.nextUser.Store(1_000_000)
	s.buildFixtures()

	d := tgtest.NewDispatcher()
	config.NewService(&s.cfg, &tg.CDNConfig{}).Register(d)
	d.HandleFunc(tg.UsersGetUsersRequestTypeID, s.handleGetUsers)
	d.HandleFunc(tg.ContactsGetContactsRequestTypeID, func(srv *tgtest.Server, req *tgtest.Request) error {
		return srv.SendResult(req, s.contacts)
	})
	d.HandleFunc(tg.MessagesGetDialogsRequestTypeID, func(srv *tgtest.Server, req *tgtest.Request) error {
		return srv.SendResult(req, s.dialogs)
	})
	d.HandleFunc(tg.UpdatesGetStateRequestTypeID, func(srv *tgtest.Server, req *tgtest.Request) error {
		return srv.SendResult(req, &tg.UpdatesState{Pts: 1, Qts: 0, Date: int(time.Now().Unix()), Seq: 0})
	})
	d.Fallback(tgtest.HandlerFunc(services.NotImplemented))

	track := tgtest.HandlerFunc(func(srv *tgtest.Server, req *tgtest.Request) error {
		s.track(req.Session)
		return d.OnMessage(srv, req)
	})
	s.srv = tgtest.NewServer(exchange.PrivateKey{RSA: key}, tgtest.UnpackInvoke(track), tgtest.ServerOptions{
		DC:           opts.DC,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 30 * time.Second,
	})
	return s, nil
}

func (s *Server) buildFixtures() {
	users := make([]tg.UserClass, 0, s.opts.Contacts)
	contacts := make([]tg.Contact, 0, s.opts.Contacts)
	for i := 0; i < s.opts.Contacts; i++ {
		id := int64(500_000_000 + i)
		users = append(users, &tg.User{ID: id, AccessHash: id * 7, FirstName: "Contact", Username: "contact_user"})
		contacts = append(contacts, tg.Contact{UserID: id})
	}
	s.contacts = &tg.ContactsContacts{Contacts: contacts, SavedCount: len(contacts), Users: users}

	half := s.opts.Dialogs / 2
	dUsers := make([]tg.UserClass, 0, half)
	dChats := make([]tg.ChatClass, 0, s.opts.Dialogs-half)
	for i := 0; i < half; i++ {
		id := int64(600_000_000 + i)
		dUsers = append(dUsers, &tg.User{ID: id, AccessHash: id * 3, FirstName: "Dialog", LastName: "User"})
	}
	for i := 0; i < s.opts.Dialogs-half; i++ {
		id := int64(700_000_000 + i)
		ch := &tg.Channel{ID: id, AccessHash: id * 5, Title: "Some group title", Megagroup: true, Date: 1, Photo: &tg.ChatPhotoEmpty{}}
		dChats = append(dChats, ch)
	}
	s.dialogs = &tg.MessagesDialogs{Users: dUsers, Chats: dChats}
}

func (s *Server) track(k tgtest.Session) {
	s.mu.Lock()
	if _, ok := s.sessions[k.ID]; !ok {
		s.sessions[k.ID] = k
		s.order = append(s.order, k.ID)
	}
	s.mu.Unlock()
}

func (s *Server) handleGetUsers(srv *tgtest.Server, req *tgtest.Request) error {
	var r tg.UsersGetUsersRequest
	if err := r.Decode(req.Buf); err != nil {
		return err
	}
	v, _ := s.selfIDs.LoadOrStore(req.Session.ID, s.nextUser.Add(1))
	id := v.(int64)
	out := make([]bin.Encoder, 0, len(r.ID))
	for _, in := range r.ID {
		switch in.(type) {
		case *tg.InputUserSelf:
			out = append(out, &tg.User{ID: id, AccessHash: id, Self: true, FirstName: "Bench", Username: "bench"})
		default:
			out = append(out, s.sender)
		}
	}
	return srv.SendVector(req, out...)
}

// Serve listens on addr and serves until ctx is done. ready is called once
// the listener is bound.
func (s *Server) Serve(ctx context.Context, addr string, ready func(Info)) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	tcp := l.Addr().(*net.TCPAddr)
	s.cfg.DCOptions = []tg.DCOption{{ID: s.opts.DC, IPAddress: tcp.IP.String(), Port: tcp.Port, Static: true}}
	s.cfg.ThisDC = s.opts.DC

	go s.loadLoop(ctx)

	if ready != nil {
		pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&s.key.PublicKey)})
		ready(Info{DC: s.opts.DC, Addr: tcp.IP.String(), Port: tcp.Port, PublicKeyPEM: string(pemKey)})
	}
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()
	return s.srv.Serve(ctx, transport.ListenCodec(nil, l))
}

// SetRate sets the update rate in updates per session per second.
func (s *Server) SetRate(perSessionPerSec float64) {
	if perSessionPerSec < 0 {
		perSessionPerSec = 0
	}
	s.rate.Store(uint64(perSessionPerSec * 1000))
}

// Reset stops active load generation and clears tracked sessions and counters.
func (s *Server) Reset() {
	s.rate.Store(0)
	s.mu.Lock()
	s.sessions = map[int64]tgtest.Session{}
	s.order = nil
	s.mu.Unlock()
	s.selfIDs.Clear()
	s.sent.Store(0)
	s.sendErrs.Store(0)
}

// Stats reports server counters.
type Stats struct {
	Sessions int     `json:"sessions"`
	Sent     uint64  `json:"sent"`
	SendErrs uint64  `json:"send_errors"`
	Rate     float64 `json:"rate"`
}

// Stats returns counters.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	n := len(s.order)
	s.mu.Unlock()
	return Stats{Sessions: n, Sent: s.sent.Load(), SendErrs: s.sendErrs.Load(), Rate: float64(s.rate.Load()) / 1000}
}

// loadLoop pushes UpdateNewMessage to every known session at the configured
// rate using a bounded worker pool.
func (s *Server) loadLoop(ctx context.Context) {
	const tick = 20 * time.Millisecond
	jobs := make(chan tgtest.Session, 1024)
	for i := 0; i < 32; i++ {
		go func() {
			for k := range jobs {
				s.sendUpdate(ctx, k)
			}
		}()
	}
	defer close(jobs)

	t := time.NewTicker(tick)
	defer t.Stop()
	var budget float64
	cursor := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rate := float64(s.rate.Load()) / 1000
		if rate == 0 {
			budget = 0
			continue
		}
		s.mu.Lock()
		ids := s.order
		s.mu.Unlock()
		if len(ids) == 0 {
			continue
		}
		budget += rate * float64(len(ids)) * tick.Seconds()
		for budget >= 1 {
			budget--
			if cursor >= len(ids) {
				cursor = 0
			}
			s.mu.Lock()
			k, ok := s.sessions[ids[cursor]]
			s.mu.Unlock()
			cursor++
			if !ok {
				continue
			}
			select {
			case jobs <- k:
			case <-ctx.Done():
				return
			default:
				// Server-side backpressure: drop rather than queue unboundedly.
				s.sendErrs.Add(1)
			}
		}
	}
}

func (s *Server) sendUpdate(ctx context.Context, k tgtest.Session) {
	id := int(s.msgID.Add(1))
	now := int(time.Now().Unix())
	msg := &tg.Message{
		ID:      id,
		PeerID:  &tg.PeerUser{UserID: s.sender.ID},
		Message: "hello from fakedc, this is a regular incoming message",
		Date:    now,
	}
	msg.SetFromID(&tg.PeerUser{UserID: s.sender.ID})
	upd := &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateNewMessage{Message: msg, Pts: id, PtsCount: 1}},
		Users:   []tg.UserClass{s.sender},
		Date:    now,
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.srv.Send(sctx, k, proto.MessageFromServer, upd); err != nil {
		s.sendErrs.Add(1)
		return
	}
	s.sent.Add(1)
}
