// Package profiles contains client configurations under benchmark.
package profiles

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/telegram/dcs"
)

// Env is shared by all sessions in a run.
type Env struct {
	APIID     int
	APIHash   string
	PublicKey exchange.PublicKey
	DCList    dcs.List
	Resolver  dcs.Resolver

	// Updates counts updates delivered to user handlers across all sessions.
	Updates *atomic.Uint64
}

// Session runs one Telegram session until ctx is done. ready must be called
// exactly once when the session is authorized and usable.
type Session func(ctx context.Context, env *Env, idx int, ready func()) error

// Profile is a named client configuration.
type Profile struct {
	Name        string
	Description string
	// Setup is called once before sessions start; it returns the session func.
	Setup func(env *Env) Session
}

var registry = map[string]Profile{}

func register(p Profile) { registry[p.Name] = p }

// Get returns a profile by name.
func Get(name string) (Profile, error) {
	p, ok := registry[name]
	if !ok {
		return Profile{}, fmt.Errorf("unknown profile %q (available: %v)", name, Names())
	}
	return p, nil
}

// Names lists registered profiles.
func Names() []string {
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
