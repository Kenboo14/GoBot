// Package engine implements a lightweight, multi-session MTProto runtime that
// replaces per-session ping/ack/salt/close/update loops with one shared
// scheduler and bounded worker pool, reducing per-session overhead to 1
// goroutine.
package engine

import (
	"crypto/rand"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
)

// SchedulerOptions configures a shared multi-session Scheduler.
type SchedulerOptions struct {
	// Workers is the number of worker goroutines shared across all sessions
	// for update dispatch and keepalive maintenance. Defaults to max(16, 2*GOMAXPROCS).
	Workers int
	// AckInterval is the maximum delay before flushing queued message acks.
	// Defaults to 15s.
	AckInterval time.Duration
	// AckBatchSize triggers an early ack flush once this many acks accumulate.
	// Defaults to 20.
	AckBatchSize int
	// PingInterval is how often keepalive ping_delay_disconnect is sent per
	// session (staggered across sessions). Defaults to 60s.
	PingInterval time.Duration
	// PingTimeout closes a session if a keepalive ping is not answered within
	// this duration. Defaults to 15s.
	PingTimeout time.Duration
	// SaltFetchInterval is how often future salts are refreshed. Defaults to 1h.
	SaltFetchInterval time.Duration
}

func (o *SchedulerOptions) setDefaults() {
	if o.Workers <= 0 {
		o.Workers = max(16, 2*runtime.GOMAXPROCS(0))
	}
	if o.AckInterval <= 0 {
		o.AckInterval = 15 * time.Second
	}
	if o.AckBatchSize <= 0 {
		o.AckBatchSize = 20
	}
	if o.PingInterval <= 0 {
		o.PingInterval = 60 * time.Second
	}
	if o.PingTimeout <= 0 {
		o.PingTimeout = 15 * time.Second
	}
	if o.SaltFetchInterval <= 0 {
		o.SaltFetchInterval = time.Hour
	}
}

type taskKind uint8

const (
	taskUpdates taskKind = iota
	taskAck
	taskPing
	taskSalt
)

type schedTask struct {
	conn *Conn
	kind taskKind
}

// Scheduler coordinates keepalive pings, ack batching, salt refreshes, and
// ordered per-session update delivery across many Conn instances using a single
// timer loop and a fixed-size worker pool.
type Scheduler struct {
	opts     SchedulerOptions
	cipher   crypto.Cipher
	rand     io.Reader
	msgIDGen *proto.MessageIDGen
	bufPool  *bin.Pool
	nextID   atomic.Uint64

	mu    sync.RWMutex
	conns map[*Conn]struct{}

	taskCh   chan schedTask
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewScheduler starts a Scheduler and its background worker pool. Call Close to
// stop all scheduler goroutines cleanly.
func NewScheduler(opts SchedulerOptions) *Scheduler {
	opts.setDefaults()
	s := &Scheduler{
		opts:     opts,
		cipher:   crypto.NewClientCipher(rand.Reader),
		rand:     rand.Reader,
		msgIDGen: proto.NewMessageIDGen(time.Now),
		bufPool:  bin.NewPool(0),
		conns:    make(map[*Conn]struct{}),
		taskCh:   make(chan schedTask, max(4096, opts.Workers*128)),
		stopCh:   make(chan struct{}),
	}
	s.wg.Add(1 + opts.Workers)
	go s.tickLoop()
	for i := 0; i < opts.Workers; i++ {
		go s.worker()
	}
	return s
}

// Close shuts down the scheduler ticker and waits for all worker goroutines to exit.
func (s *Scheduler) Close() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.wg.Wait()
	})
}

func (s *Scheduler) register(c *Conn) {
	now := time.Now().UnixNano()
	stagger := (time.Duration(c.id%60) * time.Second) % s.opts.PingInterval
	c.nextPingNano.Store(now + s.opts.PingInterval.Nanoseconds() + stagger.Nanoseconds())
	c.nextSaltNano.Store(now + s.opts.SaltFetchInterval.Nanoseconds())
	c.lastAckNano.Store(now)

	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Scheduler) unregister(c *Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

func (s *Scheduler) submit(t schedTask) bool {
	select {
	case s.taskCh <- t:
		return true
	case <-s.stopCh:
		return false
	default:
		return false
	}
}

func (s *Scheduler) submitBlocking(c *Conn, kind taskKind) {
	select {
	case s.taskCh <- schedTask{conn: c, kind: kind}:
	case <-c.stopCh:
	case <-s.stopCh:
	}
}

func (s *Scheduler) tickLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var snapshot []*Conn
	for {
		select {
		case <-s.stopCh:
			return
		case t := <-ticker.C:
			now := t.UnixNano()
			snapshot = s.snapshotConns(snapshot[:0])
			s.checkConns(snapshot, now)
		}
	}
}

func (s *Scheduler) snapshotConns(dst []*Conn) []*Conn {
	s.mu.RLock()
	for c := range s.conns {
		dst = append(dst, c)
	}
	s.mu.RUnlock()
	return dst
}

func (s *Scheduler) checkConns(conns []*Conn, now int64) {
	ackInterval := s.opts.AckInterval.Nanoseconds()
	pingInterval := s.opts.PingInterval.Nanoseconds()
	pingTimeout := s.opts.PingTimeout.Nanoseconds()
	saltInterval := s.opts.SaltFetchInterval.Nanoseconds()

	for _, c := range conns {
		if c.closed.Load() {
			continue
		}
		// 1. Keepalive ping deadline & schedule check.
		if c.pingInFlight.Load() {
			if now > c.pingDeadlineNano.Load() {
				c.failAndClose(ErrPingTimeout)
				continue
			}
		} else if now >= c.nextPingNano.Load() {
			if c.pingInFlight.CompareAndSwap(false, true) {
				c.pingDeadlineNano.Store(now + pingTimeout)
				c.nextPingNano.Store(now + pingInterval)
				if !s.submit(schedTask{conn: c, kind: taskPing}) {
					c.pingInFlight.Store(false)
				}
			}
		}

		// 2. Periodic ack flush check.
		if c.hasPendingAcks() && now-c.lastAckNano.Load() >= ackInterval {
			if c.ackQueued.CompareAndSwap(false, true) {
				if !s.submit(schedTask{conn: c, kind: taskAck}) {
					c.ackQueued.Store(false)
				}
			}
		}

		// 3. Periodic salt refresh check.
		if now >= c.nextSaltNano.Load() {
			c.nextSaltNano.Store(now + saltInterval)
			s.submit(schedTask{conn: c, kind: taskSalt})
		}
	}
}

func (s *Scheduler) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		case task := <-s.taskCh:
			if task.conn == nil || task.conn.closed.Load() {
				if task.conn != nil && task.kind == taskAck {
					task.conn.ackQueued.Store(false)
				}
				continue
			}
			switch task.kind {
			case taskUpdates:
				task.conn.drainUpdates()
			case taskAck:
				task.conn.flushAcks()
			case taskPing:
				task.conn.sendKeepalivePing()
			case taskSalt:
				task.conn.requestSalts()
			}
		}
	}
}
