package cache

import (
	"github.com/tsiemens/kvstore/shared/api"
	//"github.com/tsiemens/kvstore/shared/log"
	"net"
	"time"
)

const garbageCollectionInterval = time.Millisecond * 2000
const maxCacheLife = time.Millisecond * 5000

type CacheEntry struct {
	Time  time.Time
	Reply api.Message
}

type Cache struct {
	M map[[16]byte]*CacheEntry
}

func New() *Cache {
	c := &Cache{map[[16]byte]*CacheEntry{}}
	go c.garbageCollectionLoop()
	return c
}

/* Returns if the message was already handled and cached,
 * and a (possibly nil) reply message */
func (cache *Cache) StoreAndGetReply(msg api.Message) (bool, api.Message) {
	entry, ok := cache.M[msg.UID()]
	if !ok {
		cache.M[msg.UID()] = &CacheEntry{
			Time: time.Now(),
		}
		return false, nil
	} else {
		return true, entry.Reply
	}
}

/* Sends the message as a reply. This sets the message as a reply to
 * the incoming message with the same UID
 * cache may be nil
 */
func (cache *Cache) SendReply(conn *net.UDPConn, msg api.Message, addr net.Addr) (int, error) {
	if cache != nil && msg.Command() == api.RespOk {
		if entry, ok := cache.M[msg.UID()]; ok {
			entry.Reply = msg
		}
	}
	return conn.WriteTo(msg.Bytes(), addr)
}

func (cache *Cache) garbageCollectionLoop() {
	for {
		cache.Clean()
		time.Sleep(garbageCollectionInterval)
	}
}

func (cache *Cache) Clean() {
	now := time.Now()
	for uid, entry := range cache.M {
		if entry.Time.Add(maxCacheLife).Before(now) {
			delete(cache.M, uid)
		}
	}
}
