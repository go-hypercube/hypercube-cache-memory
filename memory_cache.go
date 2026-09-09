package memorycache

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/go-hypercube/go-hypercube/cache"
)

type entry struct {
	value     string
	expiresAt time.Time // zero = no expiration
}

func (e entry) expired() bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}

type MemoryCache struct {
	mutex sync.RWMutex
	items map[string]entry
}

func New() *MemoryCache {
	c := &MemoryCache{items: make(map[string]entry)}
	go c.runJanitor(time.Minute)
	return c
}

func (c *MemoryCache) runJanitor(interval time.Duration) {
	for range time.Tick(interval) {
		c.mutex.Lock()
		for key, e := range c.items {
			if e.expired() {
				delete(c.items, key)
			}
		}
		c.mutex.Unlock()
	}
}

func (c *MemoryCache) Get(ctx context.Context, key string) (string, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	e, found := c.items[key]
	if !found || e.expired() {
		return "", cache.ErrNotFound
	}
	return e.value, nil
}

func (c *MemoryCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	if ttl < 0 {
		return cache.ErrInvalidTTL
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	c.mutex.Lock()
	c.items[key] = entry{value: value, expiresAt: expiresAt}
	c.mutex.Unlock()
	return nil
}

func (c *MemoryCache) Delete(ctx context.Context, key string) error {
	c.mutex.Lock()
	delete(c.items, key)
	c.mutex.Unlock()
	return nil
}

func (c *MemoryCache) Has(ctx context.Context, key string) (bool, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	e, found := c.items[key]
	return found && !e.expired(), nil
}

func (c *MemoryCache) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	e, found := c.items[key]

	var current int64

	if found {
		if e.expired() {
			// Treat expired keys as missing.
			found = false
		} else {
			parsed, err := strconv.ParseInt(e.value, 10, 64)
			if err != nil {
				return 0, err
			}
			current = parsed
		}
	}

	current += delta

	var expiresAt time.Time
	if found {
		expiresAt = e.expiresAt
	}

	c.items[key] = entry{
		value:     strconv.FormatInt(current, 10),
		expiresAt: expiresAt,
	}

	return current, nil
}

func (c *MemoryCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if ttl < 0 {
		return cache.ErrInvalidTTL
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	e, found := c.items[key]
	if !found || e.expired() {
		return cache.ErrNotFound
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	e.expiresAt = expiresAt
	c.items[key] = e
	return nil
}
