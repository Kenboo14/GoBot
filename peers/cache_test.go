package peers

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"
)

func TestCache_DeduplicationAndInvalidIDs(t *testing.T) {
	ps := NewPeerStorage(nil)

	ps.SaveUser(1001, 12345)
	hash, ok := ps.UserHash(1001)
	if !ok || hash != 12345 {
		t.Fatalf("expected hash 12345, got %d (ok=%v)", hash, ok)
	}

	// Repeated save with same access hash
	ps.SaveUser(1001, 12345)
	hash, _ = ps.UserHash(1001)
	if hash != 12345 {
		t.Fatalf("expected hash 12345, got %d", hash)
	}

	// Update with new access hash
	ps.SaveUser(1001, 99999)
	hash, _ = ps.UserHash(1001)
	if hash != 99999 {
		t.Fatalf("expected updated hash 99999, got %d", hash)
	}

	// Channel deduplication
	ps.SaveChannel(2001, 54321)
	cHash, ok := ps.ChannelHash(2001)
	if !ok || cHash != 54321 {
		t.Fatalf("expected chat hash 54321, got %d (ok=%v)", cHash, ok)
	}
	ps.SaveChannel(2001, 54321)
	cHash, _ = ps.ChannelHash(2001)
	if cHash != 54321 {
		t.Fatalf("expected chat hash 54321, got %d", cHash)
	}

	// Invalid IDs
	ps.SaveUser(0, 111)
	ps.SaveUser(-5, 222)
	ps.SaveChannel(0, 333)
	ps.SaveChannel(-10, 444)
	uCnt, cCnt, _ := ps.Counts()
	if uCnt != 1 || cCnt != 1 {
		t.Errorf("invalid IDs should not be saved: users=%d, channels=%d", uCnt, cCnt)
	}
}

func TestCache_LRUEviction(t *testing.T) {
	// Per-shard capacity = 2 when MaxUsers = 32 (32 / 16 = 2)
	c := NewCache(Options{MaxUsers: 32, MaxChannels: 16, MaxChats: 16})

	// Find 3 distinct user IDs that hash to shard 0.
	var ids []int64
	for id := int64(1); len(ids) < 3; id++ {
		if shardFor(id) == 0 {
			ids = append(ids, id)
		}
	}

	c.SaveUser(ids[0], 10)
	c.SaveUser(ids[1], 20)
	// Touch ids[0] via UserHash so ids[1] becomes LRU.
	if h, ok := c.UserHash(ids[0]); !ok || h != 10 {
		t.Fatalf("expected ids[0] hash 10, got %d (ok=%v)", h, ok)
	}
	// Inserting ids[2] should evict ids[1].
	c.SaveUser(ids[2], 30)

	if _, ok := c.UserHash(ids[1]); ok {
		t.Fatalf("expected LRU entry ids[1] (%d) to be evicted", ids[1])
	}
	if h, ok := c.UserHash(ids[0]); !ok || h != 10 {
		t.Fatalf("expected ids[0] to stay cached, got %d (ok=%v)", h, ok)
	}
	if h, ok := c.UserHash(ids[2]); !ok || h != 30 {
		t.Fatalf("expected ids[2] to stay cached, got %d (ok=%v)", h, ok)
	}
}

func TestCache_GetInputPeer(t *testing.T) {
	c := NewCache(Options{})
	c.SaveUser(42, 777)
	c.SaveChannel(999, 888)

	if p := c.GetInputPeer(0); p != nil {
		t.Fatalf("expected nil for 0, got %#v", p)
	}
	u, ok := c.GetInputPeer(42).(*tg.InputPeerUser)
	if !ok || u.UserID != 42 || u.AccessHash != 777 {
		t.Fatalf("unexpected user peer: %#v", u)
	}
	ch, ok := c.GetInputPeer(-1000000000999).(*tg.InputPeerChannel)
	if !ok || ch.ChannelID != 999 || ch.AccessHash != 888 {
		t.Fatalf("unexpected channel peer: %#v", ch)
	}
	grp, ok := c.GetInputPeer(-12345).(*tg.InputPeerChat)
	if !ok || grp.ChatID != 12345 {
		t.Fatalf("unexpected chat peer: %#v", grp)
	}
}

func TestCache_ResolvePeer(t *testing.T) {
	c := NewCache(Options{})
	ctx := context.Background()

	// Self
	p, err := c.ResolvePeer(ctx, nil, 100, tg.Entities{}, &tg.PeerUser{UserID: 100}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*tg.InputPeerSelf); !ok {
		t.Fatalf("expected InputPeerSelf, got %T", p)
	}

	// From Entities (skips Min users, caches non-Min)
	entities := tg.Entities{
		Users: map[int64]*tg.User{
			200: {ID: 200, AccessHash: 2222},
			201: {ID: 201, AccessHash: 9999, Min: true},
		},
		Channels: map[int64]*tg.Channel{
			300: {ID: 300, AccessHash: 3333},
		},
	}
	p, err = c.ResolvePeer(ctx, nil, 100, entities, &tg.PeerUser{UserID: 200}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if up, ok := p.(*tg.InputPeerUser); !ok || up.UserID != 200 || up.AccessHash != 2222 {
		t.Fatalf("unexpected resolved user: %#v", p)
	}
	// Min user should not be resolved without RPC fallback
	if _, err := c.ResolvePeer(ctx, nil, 100, entities, &tg.PeerUser{UserID: 201}, 0); err == nil {
		t.Fatal("expected error for min user without RPC fallback")
	}

	// Now cached even with empty entities
	p, err = c.ResolvePeer(ctx, nil, 100, tg.Entities{}, &tg.PeerUser{UserID: 200}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if up, ok := p.(*tg.InputPeerUser); !ok || up.AccessHash != 2222 {
		t.Fatalf("expected cached user peer, got %#v", p)
	}

	// Channel from cache
	p, err = c.ResolvePeer(ctx, nil, 100, tg.Entities{}, &tg.PeerChannel{ChannelID: 300}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cp, ok := p.(*tg.InputPeerChannel); !ok || cp.ChannelID != 300 || cp.AccessHash != 3333 {
		t.Fatalf("unexpected channel peer: %#v", p)
	}

	// Basic chat
	p, err = c.ResolvePeer(ctx, nil, 100, tg.Entities{}, &tg.PeerChat{ChatID: 55}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if gp, ok := p.(*tg.InputPeerChat); !ok || gp.ChatID != 55 {
		t.Fatalf("unexpected group peer: %#v", p)
	}
}
