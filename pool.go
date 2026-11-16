// Package gobot provides a high-level, Pyrogram-inspired MTProto and Bot API
// library backed by a shared multi-session engine and bounded peer cache.
package gobot

import (
	"sync"

	"github.com/Kenboo14/GoBot/engine"
	"github.com/Kenboo14/GoBot/peers"
)

// PoolOptions configures a shared Pool of GoBot clients.
type PoolOptions struct {
	Scheduler engine.SchedulerOptions
	Peers     peers.Options
}

// Pool holds shared runtime resources (scheduler, worker pool, and sharded
// bounded peer cache) used across many Client instances.
type Pool struct {
	Scheduler *engine.Scheduler
	Peers     *peers.Cache

	closeOnce sync.Once
}

// NewPool creates a Pool with a started engine.Scheduler and bounded peers.Cache.
func NewPool(opts PoolOptions) *Pool {
	return &Pool{
		Scheduler: engine.NewScheduler(opts.Scheduler),
		Peers:     peers.NewCache(opts.Peers),
	}
}

// NewClient creates a new Client attached to this Pool.
func (p *Pool) NewClient(opts ClientOptions) (*Client, error) {
	opts.Pool = p
	return NewClient(opts)
}

// Close shuts down the pool's scheduler and waits for background workers to exit.
func (p *Pool) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		if p.Scheduler != nil {
			p.Scheduler.Close()
		}
	})
}
