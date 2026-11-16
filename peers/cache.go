// Package peers provides a sharded, bounded LRU peer access-hash cache and
// MTProto peer resolver designed to be shared across thousands of sessions.
package peers

import (
	"database/sql"
	"log"
	"sync"

	"github.com/gotd/td/tg"
)

const (
	numShards          = 16
	defaultMaxUsers    = 65536
	defaultMaxChannels = 32768
	defaultMaxChats    = 16384
)

// Options configures a Cache.
type Options struct {
	// MaxUsers is the maximum number of user access hashes retained in memory
	// across all shards. Defaults to 65536.
	MaxUsers int
	// MaxChannels is the maximum number of channel access hashes retained in
	// memory across all shards. Defaults to 32768.
	MaxChannels int
	// MaxChats is the maximum number of basic chat IDs retained in memory
	// across all shards. Defaults to 16384.
	MaxChats int
	// DB is an optional SQLite handle used to persist user and channel hashes in
	// the bot_peers table.
	DB *sql.DB
}

// Cache is a thread-safe, 16-way sharded compact LRU cache mapping Telegram peer
// IDs to access hashes.
type Cache struct {
	shards   [numShards]shard
	db       *sql.DB
	writeSem chan struct{}
}

// PeerStorage is an alias for Cache kept for drop-in compatibility with
// gateway/internal/botgotd.
type PeerStorage = Cache

type shard struct {
	mu       sync.Mutex
	users    lruTable
	channels lruTable
	chats    lruTable
}

type lruNode struct {
	id   int64
	hash int64
	prev int32
	next int32
}

// lruTable is an index-based intrusive LRU using a flat, pointer-free slice so
// the Go GC never scans cached peer entries.
type lruTable struct {
	cap   int
	head  int32
	tail  int32
	index map[int64]int32
	nodes []lruNode
}

func (t *lruTable) init(capacity int) {
	t.cap = capacity
	t.head = -1
	t.tail = -1
}

func (t *lruTable) get(id int64) (int64, bool) {
	if t.index == nil {
		return 0, false
	}
	idx, ok := t.index[id]
	if !ok {
		return 0, false
	}
	t.moveToFront(idx)
	return t.nodes[idx].hash, true
}

func (t *lruTable) put(id, hash int64) (changed bool) {
	if t.cap <= 0 {
		return false
	}
	if t.index == nil {
		initCap := t.cap
		if initCap > 16 {
			initCap = 16
		}
		t.index = make(map[int64]int32, initCap)
	}
	if idx, ok := t.index[id]; ok {
		t.moveToFront(idx)
		if t.nodes[idx].hash == hash {
			return false
		}
		t.nodes[idx].hash = hash
		return true
	}
	if len(t.nodes) < t.cap {
		idx := int32(len(t.nodes))
		t.nodes = append(t.nodes, lruNode{
			id:   id,
			hash: hash,
			prev: -1,
			next: t.head,
		})
		if t.head != -1 {
			t.nodes[t.head].prev = idx
		}
		t.head = idx
		if t.tail == -1 {
			t.tail = idx
		}
		t.index[id] = idx
		return true
	}
	// Evict least-recently-used tail node and reuse its slot in place.
	idx := t.tail
	delete(t.index, t.nodes[idx].id)
	t.nodes[idx].id = id
	t.nodes[idx].hash = hash
	t.index[id] = idx
	t.moveToFront(idx)
	return true
}

func (t *lruTable) len() int {
	return len(t.index)
}

func (t *lruTable) moveToFront(idx int32) {
	if idx == t.head {
		return
	}
	p := t.nodes[idx].prev
	n := t.nodes[idx].next
	if p != -1 {
		t.nodes[p].next = n
	}
	if n != -1 {
		t.nodes[n].prev = p
	} else {
		t.tail = p
	}
	t.nodes[idx].prev = -1
	t.nodes[idx].next = t.head
	if t.head != -1 {
		t.nodes[t.head].prev = idx
	}
	t.head = idx
	if t.tail == -1 {
		t.tail = idx
	}
}

func shardCap(total int) int {
	if total <= 0 {
		return 0
	}
	c := (total + numShards - 1) / numShards
	if c < 1 {
		return 1
	}
	return c
}

func shardFor(id int64) uint64 {
	// Fibonacci hashing distributes sequential IDs evenly across shards.
	return ((uint64(id) * 0x9e3779b97f4a7c15) >> 60) & (numShards - 1)
}

// NewCache creates a new bounded, sharded peer cache.
func NewCache(opts Options) *Cache {
	if opts.MaxUsers == 0 {
		opts.MaxUsers = defaultMaxUsers
	}
	if opts.MaxChannels == 0 {
		opts.MaxChannels = defaultMaxChannels
	}
	if opts.MaxChats == 0 {
		opts.MaxChats = defaultMaxChats
	}
	uCap := shardCap(opts.MaxUsers)
	cCap := shardCap(opts.MaxChannels)
	gCap := shardCap(opts.MaxChats)

	c := &Cache{
		db:       opts.DB,
		writeSem: make(chan struct{}, 4),
	}
	for i := range c.shards {
		c.shards[i].users.init(uCap)
		c.shards[i].channels.init(cCap)
		c.shards[i].chats.init(gCap)
	}
	c.initDB()
	return c
}

// NewPeerStorage creates a Cache backed by an optional SQLite database.
func NewPeerStorage(sqliteDB *sql.DB) *PeerStorage {
	return NewCache(Options{DB: sqliteDB})
}

func (c *Cache) initDB() {
	if c.db == nil {
		return
	}
	query := `
	CREATE TABLE IF NOT EXISTS bot_peers (
		peer_id INTEGER PRIMARY KEY,
		peer_type TEXT NOT NULL,
		access_hash INTEGER NOT NULL,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	if _, err := c.db.Exec(query); err != nil {
		log.Printf("[gobot/peers] init peers table error: %v", err)
		return
	}
	rows, err := c.db.Query("SELECT peer_id, peer_type, access_hash FROM bot_peers")
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id, hash int64
		var pType string
		if err := rows.Scan(&id, &pType, &hash); err == nil {
			sh := &c.shards[shardFor(id)]
			sh.mu.Lock()
			if pType == "channel" {
				sh.channels.put(id, hash)
			} else {
				sh.users.put(id, hash)
			}
			sh.mu.Unlock()
		}
	}
}

// SaveUser stores a user access hash if userID > 0, deduplicating unchanged values.
func (c *Cache) SaveUser(userID, accessHash int64) {
	if userID <= 0 {
		return
	}
	sh := &c.shards[shardFor(userID)]
	sh.mu.Lock()
	changed := sh.users.put(userID, accessHash)
	sh.mu.Unlock()

	if changed && c.db != nil {
		c.persistAsync(userID, "user", accessHash)
	}
}

// SaveChannel stores a channel access hash if channelID > 0, deduplicating unchanged values.
func (c *Cache) SaveChannel(channelID, accessHash int64) {
	if channelID <= 0 {
		return
	}
	sh := &c.shards[shardFor(channelID)]
	sh.mu.Lock()
	changed := sh.channels.put(channelID, accessHash)
	sh.mu.Unlock()

	if changed && c.db != nil {
		c.persistAsync(channelID, "channel", accessHash)
	}
}

// SaveChat records a basic chat ID (positive chat ID without - prefix).
func (c *Cache) SaveChat(chatID int64) {
	if chatID <= 0 {
		return
	}
	sh := &c.shards[shardFor(chatID)]
	sh.mu.Lock()
	sh.chats.put(chatID, 0)
	sh.mu.Unlock()
}

func (c *Cache) persistAsync(peerID int64, peerType string, accessHash int64) {
	go func() {
		if c.writeSem != nil {
			c.writeSem <- struct{}{}
			defer func() { <-c.writeSem }()
		}
		query := `INSERT INTO bot_peers (peer_id, peer_type, access_hash) VALUES (?, ?, ?)
		ON CONFLICT(peer_id) DO UPDATE SET access_hash = excluded.access_hash, updated_at = CURRENT_TIMESTAMP;`
		if _, err := c.db.Exec(query, peerID, peerType, accessHash); err != nil {
			log.Printf("[gobot/peers] peer db exec %s error: %v", peerType, err)
		}
	}()
}

// UserHash returns the cached access hash for userID, refreshing its LRU position.
func (c *Cache) UserHash(userID int64) (int64, bool) {
	if userID <= 0 {
		return 0, false
	}
	sh := &c.shards[shardFor(userID)]
	sh.mu.Lock()
	h, ok := sh.users.get(userID)
	sh.mu.Unlock()
	return h, ok
}

// ChannelHash returns the cached access hash for channelID, refreshing its LRU position.
func (c *Cache) ChannelHash(channelID int64) (int64, bool) {
	if channelID <= 0 {
		return 0, false
	}
	sh := &c.shards[shardFor(channelID)]
	sh.mu.Lock()
	h, ok := sh.channels.get(channelID)
	sh.mu.Unlock()
	return h, ok
}

// Counts returns the current number of cached users, channels, and basic chats.
func (c *Cache) Counts() (users, channels, chats int) {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		users += sh.users.len()
		channels += sh.channels.len()
		chats += sh.chats.len()
		sh.mu.Unlock()
	}
	return users, channels, chats
}

// CacheEntities extracts and stores non-min user and channel access hashes from e.
func (c *Cache) CacheEntities(e tg.Entities) {
	for _, u := range e.Users {
		if u != nil && !u.Min && u.ID != 0 && u.AccessHash != 0 {
			c.SaveUser(u.ID, u.AccessHash)
		}
	}
	for _, ch := range e.Channels {
		if ch != nil && !ch.Min && ch.ID != 0 && ch.AccessHash != 0 {
			c.SaveChannel(ch.ID, ch.AccessHash)
		}
	}
	for _, ch := range e.Chats {
		if ch != nil && ch.ID != 0 {
			c.SaveChat(ch.ID)
		}
	}
}

// ApplyUsersChats extracts and stores non-min user and channel access hashes from slices.
func (c *Cache) ApplyUsersChats(users []tg.UserClass, chats []tg.ChatClass) {
	for _, item := range users {
		if u, ok := item.(*tg.User); ok && u != nil && !u.Min && u.ID != 0 && u.AccessHash != 0 {
			c.SaveUser(u.ID, u.AccessHash)
		}
	}
	for _, item := range chats {
		switch ch := item.(type) {
		case *tg.Channel:
			if ch != nil && !ch.Min && ch.ID != 0 && ch.AccessHash != 0 {
				c.SaveChannel(ch.ID, ch.AccessHash)
			}
		case *tg.Chat:
			if ch != nil && ch.ID != 0 {
				c.SaveChat(ch.ID)
			}
		}
	}
}

// GetInputPeer builds a tg.InputPeerClass from a Bot API chat ID using cached hashes.
func (c *Cache) GetInputPeer(chatID int64) tg.InputPeerClass {
	if chatID == 0 {
		return nil
	}
	if chatID > 0 {
		if hash, ok := c.UserHash(chatID); ok {
			return &tg.InputPeerUser{UserID: chatID, AccessHash: hash}
		}
		return &tg.InputPeerUser{UserID: chatID, AccessHash: 0}
	}
	realID := -chatID
	if realID > 1000000000000 {
		realID -= 1000000000000
	}
	if hash, ok := c.ChannelHash(realID); ok {
		return &tg.InputPeerChannel{ChannelID: realID, AccessHash: hash}
	}
	return &tg.InputPeerChat{ChatID: realID}
}
