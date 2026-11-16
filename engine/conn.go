package engine

import (
	"context"
	"crypto/rand"
	_ "embed"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/mtproto/salts"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/gotd/td/transport"
)

//go:embed _data/public_keys.pem
var defaultPublicKeysPEM []byte

var defaultKeysOnce struct {
	once sync.Once
	keys []exchange.PublicKey
}

func vendoredPublicKeys() []exchange.PublicKey {
	defaultKeysOnce.once.Do(func() {
		rsaKeys, err := crypto.ParseRSAPublicKeys(defaultPublicKeysPEM)
		if err != nil {
			panic(fmt.Errorf("engine: parse vendored public keys: %w", err))
		}
		keys := make([]exchange.PublicKey, 0, len(rsaKeys))
		for _, k := range rsaKeys {
			keys = append(keys, exchange.PublicKey{RSA: k})
		}
		defaultKeysOnce.keys = keys
	})
	return defaultKeysOnce.keys
}

var (
	// ErrConnClosed is returned when an RPC is invoked on a closed connection.
	ErrConnClosed = errors.New("engine: connection closed")
	// ErrPingTimeout is returned when a keepalive ping times out.
	ErrPingTimeout = errors.New("engine: keepalive ping timeout")
)

// PublicKey is an RSA public key used for MTProto DH exchange.
type PublicKey = exchange.PublicKey

// DeviceConfig describes client metadata sent during initConnection.
type DeviceConfig = telegram.DeviceConfig

// UpdateHandler processes decoded updates from Telegram.
type UpdateHandler func(ctx context.Context, u tg.UpdatesClass) error

// ConnOptions configures a lightweight Conn.
type ConnOptions struct {
	// Scheduler coordinates background maintenance and update dispatch. Required.
	Scheduler *Scheduler
	// AppID is the Telegram API ID.
	AppID int
	// DC is the primary datacenter ID (defaults to 2).
	DC int
	// DCList lists DC addresses (defaults to dcs.Prod()).
	DCList dcs.List
	// Resolver dials MTProto transports (defaults to dcs.DefaultResolver()).
	Resolver dcs.Resolver
	// PublicKeys lists trusted Telegram RSA public keys (defaults to bundled production keys).
	PublicKeys []exchange.PublicKey
	// SessionStorage persists auth keys and config (defaults to in-memory).
	SessionStorage session.Storage
	// UpdateHandler handles incoming updates on the scheduler's worker pool.
	UpdateHandler UpdateHandler
	// Device specifies device metadata for initConnection.
	Device telegram.DeviceConfig
	// Layer specifies the TL schema layer (defaults to tg.Layer).
	Layer int
	// NoUpdates wraps requests in invokeWithoutUpdates when true.
	NoUpdates bool
	// CompressThreshold gzip-compresses outgoing payloads larger than this
	// threshold in bytes. Negative disables; 0 defaults to 1024.
	CompressThreshold int
	// DialTimeout limits initial TCP dialing (defaults to 35s).
	DialTimeout time.Duration
	// ExchangeTimeout limits DH key exchange steps (defaults to exchange.DefaultTimeout).
	ExchangeTimeout time.Duration
	// RequestTimeout limits an individual RPC Invoke when ctx has no deadline
	// (defaults to 30s).
	RequestTimeout time.Duration
}

func (o *ConnOptions) setDefaults() {
	if o.DC == 0 {
		o.DC = 2
	}
	if o.DCList.Zero() {
		o.DCList = dcs.Prod()
	}
	if o.Resolver == nil {
		o.Resolver = dcs.DefaultResolver()
	}
	if len(o.PublicKeys) == 0 {
		o.PublicKeys = vendoredPublicKeys()
	}
	if o.Layer == 0 {
		o.Layer = tg.Layer
	}
	o.Device.SetDefaults()
	if o.CompressThreshold == 0 {
		o.CompressThreshold = 1024
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 35 * time.Second
	}
	if o.ExchangeTimeout <= 0 {
		o.ExchangeTimeout = exchange.DefaultTimeout
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 30 * time.Second
	}
}

type rpcCall struct {
	mu       sync.Mutex
	finished bool
	out      bin.Decoder
	ch       chan error
}

type badMessageError struct {
	Code    int
	NewSalt int64
}

func (e *badMessageError) Error() string {
	return fmt.Sprintf("bad msg notification code %d", e.Code)
}

const (
	codeMsgIDTooLow         = 16
	codeMsgIDTooHigh        = 17
	codeIncorrectServerSalt = 48

	maxMsgPast   = 300 * time.Second
	maxMsgFuture = 30 * time.Second
)

// Conn is an ultra-lightweight MTProto connection implementing bin.Invoker so
// tg.NewClient(conn) can be used directly.
type Conn struct {
	id         uint64
	sched      *Scheduler
	opts       ConnOptions
	storage    *session.Loader
	onUpdate   UpdateHandler
	runCtx     context.Context
	stopCh     chan struct{}
	closeOnce  sync.Once
	closed     atomic.Bool
	closeCause atomic.Value

	conn transport.Conn

	writeMu     sync.Mutex
	sentContent int32

	sessionMu sync.RWMutex
	authKey   crypto.AuthKey
	sessionID int64
	salt      int64
	dc        int

	inited atomic.Bool
	salts  salts.Salts

	seenMsgIDs [64]int64 // readLoop-only replay protection

	pendingMu sync.Mutex
	pending   map[int64]*rpcCall

	ackMu       sync.Mutex
	acks        []int64
	lastAckNano atomic.Int64
	ackQueued   atomic.Bool

	pingInFlight     atomic.Bool
	pingDeadlineNano atomic.Int64
	nextPingNano     atomic.Int64
	nextSaltNano     atomic.Int64

	pingMu      sync.Mutex
	pingWaiters map[int64]chan struct{}

	updMu      sync.Mutex
	updQueue   []tg.UpdatesClass
	updRunning bool
	updClosed  bool
}

// NewConn creates a new unstarted Conn bound to opts.Scheduler.
func NewConn(opts ConnOptions) (*Conn, error) {
	if opts.Scheduler == nil {
		return nil, errors.New("engine: Scheduler is required")
	}
	opts.setDefaults()
	var loader *session.Loader
	if opts.SessionStorage != nil {
		loader = &session.Loader{Storage: opts.SessionStorage}
	}
	return &Conn{
		id:       opts.Scheduler.nextID.Add(1),
		sched:    opts.Scheduler,
		opts:     opts,
		storage:  loader,
		onUpdate: opts.UpdateHandler,
		dc:       opts.DC,
		stopCh:   make(chan struct{}),
	}, nil
}

// Run establishes the MTProto session, runs initConnection and onReady, and
// drives the socket read loop directly on the calling goroutine until ctx is
// canceled or the transport disconnects.
//
// Unlike telegram.Client.Run, onReady should return after performing startup
// initialization; the connection stays alive until ctx is canceled or closed.
func (c *Conn) Run(ctx context.Context, onReady func(ctx context.Context) error) error {
	if err := c.restoreSession(ctx); err != nil {
		return err
	}
	if err := c.dialAndHandshake(ctx); err != nil {
		return err
	}

	runCtx, runCancel := context.WithCancelCause(ctx)
	c.runCtx = runCtx

	stopAfter := context.AfterFunc(runCtx, func() {
		c.closeTransport(runCtx.Err())
	})
	defer func() {
		stopAfter()
		c.updMu.Lock()
		c.updClosed = true
		c.updQueue = nil
		c.updMu.Unlock()
		c.sched.unregister(c)
		c.closeTransport(context.Canceled)
		c.failAllPending(ErrConnClosed)
		runCancel(context.Canceled)
	}()

	c.sched.register(c)

	initDone := make(chan struct{})
	go func() {
		defer close(initDone)
		if err := c.bootstrap(runCtx, onReady); err != nil {
			runCancel(err)
		}
	}()

	err := c.readLoop(runCtx)
	runCancel(err)
	<-initDone

	if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	if c.closeCause.Load() != nil {
		if ce, ok := c.closeCause.Load().(error); ok && ce != nil {
			return ce
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (c *Conn) closeTransport(cause error) {
	c.closeOnce.Do(func() {
		if cause != nil {
			c.closeCause.Store(cause)
		}
		c.closed.Store(true)
		close(c.stopCh)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

func (c *Conn) failAndClose(cause error) {
	c.closeTransport(cause)
}

func (c *Conn) restoreSession(ctx context.Context) error {
	if c.storage == nil {
		return nil
	}
	data, err := c.storage.Load(ctx)
	if errors.Is(err, session.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("engine: load session: %w", err)
	}
	var key crypto.AuthKey
	copy(key.Value[:], data.AuthKey)
	copy(key.ID[:], data.AuthKeyID)
	if !key.Zero() && key.Value.ID() != key.ID {
		return errors.New("engine: corrupted auth key")
	}

	c.sessionMu.Lock()
	c.authKey = key
	c.salt = data.Salt
	if data.DC != 0 {
		c.dc = data.DC
	}
	c.sessionMu.Unlock()
	return nil
}

func (c *Conn) saveSession(ctx context.Context, cfg tg.Config) {
	if c.storage == nil {
		return
	}
	data, err := c.storage.Load(ctx)
	if err != nil {
		data = &session.Data{}
	}
	_, _, key, salt, dc := c.snapshotSession()
	data.Config = session.ConfigFromTG(cfg)
	data.AuthKey = key.Value[:]
	data.AuthKeyID = key.ID[:]
	if cfg.ThisDC != 0 {
		data.DC = cfg.ThisDC
	} else {
		data.DC = dc
	}
	data.Salt = salt
	_ = c.storage.Save(ctx, data)
}

func (c *Conn) dialAndHandshake(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, c.opts.DialTimeout)
	defer cancel()

	tConn, err := c.opts.Resolver.Primary(dialCtx, c.dc, c.opts.DCList)
	if err != nil {
		return fmt.Errorf("engine: dial dc %d: %w", c.dc, err)
	}
	c.conn = tConn

	_, sid, key, _, _ := c.snapshotSession()
	if key.Zero() {
		if err := c.createAuthKey(ctx); err != nil {
			_ = tConn.Close()
			return fmt.Errorf("engine: auth key exchange: %w", err)
		}
		return nil
	}
	if sid == 0 {
		newSID, err := crypto.RandInt64(c.sched.rand)
		if err != nil {
			_ = tConn.Close()
			return err
		}
		c.sessionMu.Lock()
		c.sessionID = newSID
		c.sessionMu.Unlock()
	}
	return nil
}

func (c *Conn) createAuthKey(ctx context.Context) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	exCtx, cancel := context.WithTimeout(ctx, c.opts.ExchangeTimeout)
	defer cancel()

	ex := exchange.NewExchanger(c.conn, c.dc).
		WithTimeout(c.opts.ExchangeTimeout).
		WithRand(c.sched.rand)
	res, err := ex.Client(c.opts.PublicKeys).Run(exCtx)
	if err != nil {
		return err
	}

	c.sessionMu.Lock()
	c.authKey = res.AuthKey
	c.sessionID = res.SessionID
	c.salt = res.ServerSalt
	c.sentContent = 0
	c.sessionMu.Unlock()
	return nil
}

func (c *Conn) bootstrap(ctx context.Context, onReady func(ctx context.Context) error) error {
	cfg, err := c.initConnection(ctx)
	if err != nil {
		return err
	}
	c.saveSession(ctx, cfg)
	// Request initial future salts asynchronously via scheduler worker if desired.
	c.nextSaltNano.Store(time.Now().UnixNano() + c.sched.opts.SaltFetchInterval.Nanoseconds())
	c.requestSalts()

	if onReady != nil {
		return onReady(ctx)
	}
	return nil
}

func (c *Conn) initConnection(ctx context.Context) (tg.Config, error) {
	req := c.wrapInit(&tg.HelpGetConfigRequest{})
	var cfg tg.Config
	for attempt := 0; attempt < 3; attempt++ {
		err := c.invokeOnce(ctx, req, &cfg)
		if err == nil {
			c.inited.Store(true)
			return cfg, nil
		}
		if tgerr.Is(err, tgerr.ErrFloodWait) {
			select {
			case <-ctx.Done():
				return cfg, ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
		var bad *badMessageError
		if errors.As(err, &bad) {
			continue
		}
		return cfg, fmt.Errorf("engine: initConnection: %w", err)
	}
	return cfg, errors.New("engine: initConnection retries exhausted")
}

func (c *Conn) wrapInit(query bin.Object) bin.Object {
	if c.opts.NoUpdates {
		query = &tg.InvokeWithoutUpdatesRequest{Query: query}
	}
	q := &tg.InitConnectionRequest{
		APIID:          c.opts.AppID,
		DeviceModel:    c.opts.Device.DeviceModel,
		SystemVersion:  c.opts.Device.SystemVersion,
		AppVersion:     c.opts.Device.AppVersion,
		SystemLangCode: c.opts.Device.SystemLangCode,
		LangPack:       c.opts.Device.LangPack,
		LangCode:       c.opts.Device.LangCode,
		Proxy:          c.opts.Device.Proxy,
		Params:         c.opts.Device.Params,
		Query:          query,
	}
	req := bin.Object(&tg.InvokeWithLayerRequest{
		Layer: c.opts.Layer,
		Query: q,
	})
	if c.opts.NoUpdates {
		req = &tg.InvokeWithoutUpdatesRequest{Query: req}
	}
	return req
}

// Invoke sends an MTProto RPC request and decodes the response into output.
func (c *Conn) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.opts.RequestTimeout)
		defer cancel()
	}

	payload := input
	if !c.inited.Load() {
		if obj, ok := input.(bin.Object); ok {
			payload = c.wrapInit(obj)
		}
	} else if c.opts.NoUpdates {
		if obj, ok := input.(bin.Object); ok {
			payload = &tg.InvokeWithoutUpdatesRequest{Query: obj}
		}
	}

	for attempt := 0; attempt < 3; attempt++ {
		err := c.invokeOnce(ctx, payload, output)
		if err == nil {
			return nil
		}
		var bad *badMessageError
		if errors.As(err, &bad) {
			if bad.Code == codeIncorrectServerSalt && bad.NewSalt != 0 {
				c.storeSalt(bad.NewSalt)
				c.salts.Reset()
				continue
			}
			if bad.Code == codeMsgIDTooLow || bad.Code == codeMsgIDTooHigh {
				continue
			}
		}
		return err
	}
	return c.invokeOnce(ctx, payload, output)
}

func (c *Conn) invokeOnce(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	call := &rpcCall{
		out: output,
		ch:  make(chan error, 1),
	}

	msgID, err := c.writeAndRegister(ctx, input, call)
	if err != nil {
		return err
	}

	select {
	case err := <-call.ch:
		return err
	case <-ctx.Done():
		c.removePending(msgID)
		call.mu.Lock()
		if call.finished {
			call.mu.Unlock()
			return <-call.ch
		}
		call.finished = true
		call.mu.Unlock()
		return ctx.Err()
	case <-c.stopCh:
		c.removePending(msgID)
		call.mu.Lock()
		if call.finished {
			call.mu.Unlock()
			return <-call.ch
		}
		call.finished = true
		call.mu.Unlock()
		return ErrConnClosed
	}
}

func (c *Conn) removePending(msgID int64) {
	c.pendingMu.Lock()
	delete(c.pending, msgID)
	c.pendingMu.Unlock()
}

func (c *Conn) failAllPending(err error) {
	c.pendingMu.Lock()
	pending := c.pending
	c.pending = nil
	c.pendingMu.Unlock()

	for _, call := range pending {
		call.mu.Lock()
		if !call.finished {
			call.finished = true
			call.ch <- err
		}
		call.mu.Unlock()
	}
}

func (c *Conn) writeAndRegister(ctx context.Context, input bin.Encoder, call *rpcCall) (int64, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed.Load() {
		return 0, ErrConnClosed
	}

	msgID := c.sched.msgIDGen.New(proto.MessageFromClient)
	seqNo := c.sentContent * 2
	if call != nil {
		seqNo++
		c.sentContent++

		c.pendingMu.Lock()
		if c.pending == nil {
			c.pending = make(map[int64]*rpcCall, 4)
		}
		c.pending[msgID] = call
		c.pendingMu.Unlock()
	}

	b := c.sched.bufPool.Get()
	defer c.sched.bufPool.Put(b)

	if err := c.encodeEncrypted(msgID, seqNo, input, b); err != nil {
		if call != nil {
			c.removePending(msgID)
		}
		return 0, err
	}

	if err := c.conn.Send(ctx, b); err != nil {
		if call != nil {
			c.removePending(msgID)
		}
		return 0, err
	}
	return msgID, nil
}

func (c *Conn) writeServiceMessage(ctx context.Context, input bin.Encoder) error {
	_, err := c.writeAndRegister(ctx, input, nil)
	return err
}

func (c *Conn) encodeEncrypted(id int64, seq int32, payload bin.Encoder, out *bin.Buffer) error {
	c.updateSalt()
	_, sid, key, salt, _ := c.snapshotSession()

	var d crypto.EncryptedMessageData
	if c.opts.CompressThreshold <= 0 {
		d = crypto.EncryptedMessageData{
			SessionID: sid,
			Salt:      salt,
			MessageID: id,
			SeqNo:     seq,
			Message:   payload,
		}
	} else {
		pBuf := c.sched.bufPool.Get()
		defer c.sched.bufPool.Put(pBuf)
		if err := payload.Encode(pBuf); err != nil {
			return err
		}
		if pBuf.Len() > c.opts.CompressThreshold {
			d = crypto.EncryptedMessageData{
				SessionID: sid,
				Salt:      salt,
				MessageID: id,
				SeqNo:     seq,
				Message:   proto.GZIP{Data: pBuf.Raw()},
			}
		} else {
			d = crypto.EncryptedMessageData{
				SessionID:              sid,
				Salt:                   salt,
				MessageID:              id,
				SeqNo:                  seq,
				MessageDataLen:         int32(pBuf.Len()),
				MessageDataWithPadding: pBuf.Buf,
			}
		}
	}
	return c.sched.cipher.Encrypt(key, d, out)
}

func (c *Conn) snapshotSession() (dc int, sid int64, key crypto.AuthKey, salt int64, curDC int) {
	c.sessionMu.RLock()
	dc = c.dc
	sid = c.sessionID
	key = c.authKey
	salt = c.salt
	c.sessionMu.RUnlock()
	return dc, sid, key, salt, dc
}

func (c *Conn) storeSalt(salt int64) {
	c.sessionMu.Lock()
	c.salt = salt
	c.sessionMu.Unlock()
}

func (c *Conn) updateSalt() {
	salt, ok := c.salts.Get(time.Now().Add(5 * time.Minute))
	if ok {
		c.storeSalt(salt)
	}
}

// Ping sends an mt.PingRequest and waits for mt.Pong.
func (c *Conn) Ping(ctx context.Context) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	pingID, err := crypto.RandInt64(rand.Reader)
	if err != nil {
		return err
	}
	ch := make(chan struct{})
	c.pingMu.Lock()
	if c.pingWaiters == nil {
		c.pingWaiters = make(map[int64]chan struct{}, 1)
	}
	c.pingWaiters[pingID] = ch
	c.pingMu.Unlock()
	defer func() {
		c.pingMu.Lock()
		delete(c.pingWaiters, pingID)
		c.pingMu.Unlock()
	}()

	if err := c.writeServiceMessage(ctx, &mt.PingRequest{PingID: pingID}); err != nil {
		return err
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.stopCh:
		return ErrConnClosed
	}
}

// Self returns the currently authorized *tg.User.
func (c *Conn) Self(ctx context.Context) (*tg.User, error) {
	api := tg.NewClient(c)
	users, err := api.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
	if err != nil {
		return nil, err
	}
	u, ok := tg.UserClassArray(users).FirstAsNotEmpty()
	if !ok {
		return nil, fmt.Errorf("engine: empty self response: %v", users)
	}
	return u, nil
}

func (c *Conn) sendKeepalivePing() {
	pingID, err := crypto.RandInt64(c.sched.rand)
	if err != nil {
		c.pingInFlight.Store(false)
		return
	}
	delay := int((c.sched.opts.PingInterval + c.sched.opts.PingTimeout).Seconds())
	wCtx, cancel := context.WithTimeout(c.runCtx, min(c.sched.opts.PingTimeout, 10*time.Second))
	defer cancel()

	if err := c.writeServiceMessage(wCtx, &mt.PingDelayDisconnectRequest{
		PingID:          pingID,
		DisconnectDelay: delay,
	}); err != nil {
		c.pingInFlight.Store(false)
		if !errors.Is(err, ErrConnClosed) && c.runCtx.Err() == nil {
			c.failAndClose(err)
		}
	}
}

func (c *Conn) requestSalts() {
	wCtx, cancel := context.WithTimeout(c.runCtx, 10*time.Second)
	defer cancel()
	_ = c.writeServiceMessage(wCtx, &mt.GetFutureSaltsRequest{Num: 4})
}

func (c *Conn) hasPendingAcks() bool {
	c.ackMu.Lock()
	has := len(c.acks) > 0
	c.ackMu.Unlock()
	return has
}

func (c *Conn) queueAck(msgID int64) {
	c.ackMu.Lock()
	if len(c.acks) == 0 {
		c.lastAckNano.Store(time.Now().UnixNano())
	}
	c.acks = append(c.acks, msgID)
	n := len(c.acks)
	c.ackMu.Unlock()

	if n >= c.sched.opts.AckBatchSize && c.ackQueued.CompareAndSwap(false, true) {
		if !c.sched.submit(schedTask{conn: c, kind: taskAck}) {
			c.ackQueued.Store(false)
		}
	}
}

func (c *Conn) flushAcks() {
	c.ackQueued.Store(false)
	c.ackMu.Lock()
	if len(c.acks) == 0 {
		c.ackMu.Unlock()
		return
	}
	ids := c.acks
	c.acks = nil
	c.ackMu.Unlock()

	c.lastAckNano.Store(time.Now().UnixNano())
	wCtx, cancel := context.WithTimeout(c.runCtx, 10*time.Second)
	defer cancel()
	_ = c.writeServiceMessage(wCtx, &mt.MsgsAck{MsgIDs: ids})
}

func (c *Conn) readLoop(ctx context.Context) error {
	readBuf := bin.Buffer{Buf: make([]byte, 0, 256)}
	for {
		readBuf.Reset()
		if err := c.conn.Recv(ctx, &readBuf); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var netErr *net.OpError
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			var protoErr *codec.ProtocolErr
			if errors.As(err, &protoErr) && protoErr.Code == codec.CodeAuthKeyNotFound {
				if err := c.createAuthKey(ctx); err != nil {
					return fmt.Errorf("engine: recreate auth key: %w", err)
				}
				continue
			}
			return fmt.Errorf("engine: read: %w", err)
		}

		if err := c.consumePacket(ctx, &readBuf); err != nil {
			return err
		}
		// Keep idle read buffers tiny across thousands of sessions.
		if cap(readBuf.Buf) > 4096 {
			readBuf.Buf = make([]byte, 0, 256)
		}
	}
}

func (c *Conn) consumePacket(ctx context.Context, buf *bin.Buffer) error {
	_, sid, key, _, _ := c.snapshotSession()
	msg, err := c.sched.cipher.DecryptFromBuffer(key, buf)
	if err != nil {
		return fmt.Errorf("engine: decrypt: %w", err)
	}
	if msg.SessionID != sid {
		return nil // stale session packet
	}
	if !c.validIncomingMsgID(time.Now(), msg.MessageID) {
		return nil
	}
	if !c.consumeMsgID(msg.MessageID) {
		return nil
	}

	_ = c.handleMessage(msg.MessageID, &bin.Buffer{Buf: msg.Data()})

	if (msg.SeqNo & 0x01) != 0 {
		c.queueAck(msg.MessageID)
	}
	return nil
}

func (c *Conn) validIncomingMsgID(now time.Time, rawID int64) bool {
	id := proto.MessageID(rawID)
	switch id.Type() {
	case proto.MessageFromServer, proto.MessageServerResponse:
	default:
		return false
	}
	created := id.Time()
	if created.Before(now) && now.Sub(created) > maxMsgPast {
		return false
	}
	if created.Sub(now) > maxMsgFuture {
		return false
	}
	return true
}

func (c *Conn) consumeMsgID(newID int64) bool {
	if newID <= 0 {
		return false
	}
	minIdx := 0
	minID := c.seenMsgIDs[0]
	for i, id := range c.seenMsgIDs {
		if id == newID {
			return false
		}
		if id < minID {
			minIdx = i
			minID = id
		}
	}
	if newID < minID {
		return false
	}
	c.seenMsgIDs[minIdx] = newID
	return true
}

func (c *Conn) handleMessage(msgID int64, b *bin.Buffer) error {
	id, err := b.PeekID()
	if err != nil {
		return err
	}
	switch id {
	case mt.NewSessionCreatedTypeID:
		var s mt.NewSessionCreated
		if err := s.Decode(b); err != nil {
			return err
		}
		c.storeSalt(s.ServerSalt)
		return nil

	case mt.BadMsgNotificationTypeID:
		var bad mt.BadMsgNotification
		if err := bad.Decode(b); err != nil {
			return err
		}
		c.notifyRPCError(bad.BadMsgID, &badMessageError{Code: bad.ErrorCode})
		return nil

	case mt.BadServerSaltTypeID:
		var bad mt.BadServerSalt
		if err := bad.Decode(b); err != nil {
			return err
		}
		c.storeSalt(bad.NewServerSalt)
		c.salts.Reset()
		c.notifyRPCError(bad.BadMsgID, &badMessageError{Code: bad.ErrorCode, NewSalt: bad.NewServerSalt})
		return nil

	case mt.FutureSaltsTypeID:
		var res mt.FutureSalts
		if err := res.Decode(b); err != nil {
			return err
		}
		c.salts.Store(res.Salts)
		return nil

	case proto.MessageContainerTypeID:
		var container proto.MessageContainer
		if err := container.Decode(b); err != nil {
			return err
		}
		for _, m := range container.Messages {
			_ = c.handleMessage(msgID, &bin.Buffer{Buf: m.Body})
		}
		return nil

	case proto.ResultTypeID:
		return c.handleResult(b)

	case mt.PongTypeID:
		return c.handlePong(b)

	case mt.MsgsAckTypeID:
		var ack mt.MsgsAck
		return ack.Decode(b)

	case proto.GZIPTypeID:
		var gz proto.GZIP
		if err := gz.Decode(b); err != nil {
			return err
		}
		return c.handleMessage(msgID, &bin.Buffer{Buf: gz.Data})

	case mt.MsgDetailedInfoTypeID, mt.MsgNewDetailedInfoTypeID:
		return nil

	default:
		if c.onUpdate == nil {
			return nil
		}
		updates, err := tg.DecodeUpdates(b)
		if err != nil {
			return err
		}
		c.enqueueUpdate(normalizeUpdates(updates))
		return nil
	}
}

func (c *Conn) handleResult(b *bin.Buffer) error {
	var res proto.Result
	if err := res.Decode(b); err != nil {
		return err
	}
	b.ResetTo(res.Result)

	id, err := b.PeekID()
	if err != nil {
		return err
	}
	if id == proto.GZIPTypeID {
		var gz proto.GZIP
		if err := gz.Decode(b); err != nil {
			return err
		}
		b = &bin.Buffer{Buf: gz.Data}
		if id, err = b.PeekID(); err != nil {
			return err
		}
	}

	if id == mt.RPCErrorTypeID {
		var rpcErr mt.RPCError
		if err := rpcErr.Decode(b); err != nil {
			return err
		}
		c.notifyRPCError(res.RequestMessageID, tgerr.New(rpcErr.ErrorCode, rpcErr.ErrorMessage))
		return nil
	}
	if id == mt.PongTypeID {
		return c.handlePong(b)
	}
	c.notifyRPCResult(res.RequestMessageID, b)
	return nil
}

func (c *Conn) handlePong(b *bin.Buffer) error {
	var pong mt.Pong
	if err := pong.Decode(b); err != nil {
		return err
	}
	c.pingInFlight.Store(false)

	c.pingMu.Lock()
	ch, ok := c.pingWaiters[pong.PingID]
	if ok {
		delete(c.pingWaiters, pong.PingID)
	}
	c.pingMu.Unlock()
	if ok {
		close(ch)
	}
	return nil
}

func (c *Conn) notifyRPCResult(msgID int64, b *bin.Buffer) {
	c.pendingMu.Lock()
	call, ok := c.pending[msgID]
	if ok {
		delete(c.pending, msgID)
	}
	c.pendingMu.Unlock()
	if !ok {
		return
	}

	call.mu.Lock()
	if call.finished {
		call.mu.Unlock()
		return
	}
	err := call.out.Decode(b)
	call.finished = true
	call.mu.Unlock()
	call.ch <- err
}

func (c *Conn) notifyRPCError(msgID int64, rpcErr error) {
	c.pendingMu.Lock()
	call, ok := c.pending[msgID]
	if ok {
		delete(c.pending, msgID)
	}
	c.pendingMu.Unlock()
	if !ok {
		return
	}

	call.mu.Lock()
	if call.finished {
		call.mu.Unlock()
		return
	}
	call.finished = true
	call.mu.Unlock()
	call.ch <- rpcErr
}
