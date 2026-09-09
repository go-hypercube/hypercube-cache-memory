package memorycache

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-hypercube/go-hypercube/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryCache_Get(t *testing.T) {
	ctx := context.Background()
	c := New()

	t.Run("returns not found for missing key", func(t *testing.T) {
		value, err := c.Get(ctx, "missing")

		assert.Empty(t, value)
		assert.ErrorIs(t, err, cache.ErrNotFound)
	})

	t.Run("returns stored value", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "name", "nidal", 0))

		value, err := c.Get(ctx, "name")

		require.NoError(t, err)
		assert.Equal(t, "nidal", value)
	})

	t.Run("returns not found after expiration", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "temporary", "value", 30*time.Millisecond))

		value, err := c.Get(ctx, "temporary")
		require.NoError(t, err)
		assert.Equal(t, "value", value)

		time.Sleep(50 * time.Millisecond)

		value, err = c.Get(ctx, "temporary")
		assert.Empty(t, value)
		assert.ErrorIs(t, err, cache.ErrNotFound)
	})
}

func TestMemoryCache_Set(t *testing.T) {
	ctx := context.Background()
	c := New()

	t.Run("stores value without expiration", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 0))

		value, err := c.Get(ctx, "key")

		require.NoError(t, err)
		assert.Equal(t, "value", value)
	})

	t.Run("stores value with expiration", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 30*time.Millisecond))

		value, err := c.Get(ctx, "key")
		require.NoError(t, err)
		assert.Equal(t, "value", value)

		time.Sleep(50 * time.Millisecond)

		_, err = c.Get(ctx, "key")
		assert.ErrorIs(t, err, cache.ErrNotFound)
	})

	t.Run("rejects negative ttl", func(t *testing.T) {
		err := c.Set(ctx, "key", "value", -time.Second)

		assert.ErrorIs(t, err, cache.ErrInvalidTTL)
	})

	t.Run("zero ttl clears existing expiration", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "old", 30*time.Millisecond))
		require.NoError(t, c.Set(ctx, "key", "new", 0))

		time.Sleep(50 * time.Millisecond)

		value, err := c.Get(ctx, "key")

		require.NoError(t, err)
		assert.Equal(t, "new", value)
	})

	t.Run("overwrites existing value and ttl", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "first", 30*time.Millisecond))
		require.NoError(t, c.Set(ctx, "key", "second", 0))

		time.Sleep(50 * time.Millisecond)

		value, err := c.Get(ctx, "key")

		require.NoError(t, err)
		assert.Equal(t, "second", value)
	})
}

func TestMemoryCache_Delete(t *testing.T) {
	ctx := context.Background()
	c := New()

	t.Run("deletes existing key", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 0))
		require.NoError(t, c.Delete(ctx, "key"))

		_, err := c.Get(ctx, "key")

		assert.ErrorIs(t, err, cache.ErrNotFound)
	})

	t.Run("does not error when key is missing", func(t *testing.T) {
		err := c.Delete(ctx, "missing")

		assert.NoError(t, err)
	})

	t.Run("does not affect other keys", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "one", "1", 0))
		require.NoError(t, c.Set(ctx, "two", "2", 0))

		require.NoError(t, c.Delete(ctx, "one"))

		value, err := c.Get(ctx, "two")
		require.NoError(t, err)
		assert.Equal(t, "2", value)

		_, err = c.Get(ctx, "one")
		assert.ErrorIs(t, err, cache.ErrNotFound)
	})
}

func TestMemoryCache_Has(t *testing.T) {
	ctx := context.Background()
	c := New()

	t.Run("returns false for missing key", func(t *testing.T) {
		exists, err := c.Has(ctx, "missing")

		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("returns true for existing key", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 0))

		exists, err := c.Has(ctx, "key")

		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("returns false for expired key", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 30*time.Millisecond))

		exists, err := c.Has(ctx, "key")
		require.NoError(t, err)
		assert.True(t, exists)

		time.Sleep(50 * time.Millisecond)

		exists, err = c.Has(ctx, "key")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("never returns not found", func(t *testing.T) {
		_, err := c.Has(ctx, "missing")

		assert.NoError(t, err)
		assert.False(t, errors.Is(err, cache.ErrNotFound))
	})
}

func TestMemoryCache_Increment(t *testing.T) {
	ctx := context.Background()
	c := New()

	t.Run("creates missing key from zero", func(t *testing.T) {
		result, err := c.Increment(ctx, "counter", 5)

		require.NoError(t, err)
		assert.Equal(t, int64(5), result)

		value, err := c.Get(ctx, "counter")
		require.NoError(t, err)
		assert.Equal(t, "5", value)
	})

	t.Run("increments positive value", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "counter", "10", 0))

		result, err := c.Increment(ctx, "counter", 5)

		require.NoError(t, err)
		assert.Equal(t, int64(15), result)
	})

	t.Run("increments by negative delta", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "counter", "10", 0))

		result, err := c.Increment(ctx, "counter", -3)

		require.NoError(t, err)
		assert.Equal(t, int64(7), result)
	})

	t.Run("supports zero delta", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "counter", "10", 0))

		result, err := c.Increment(ctx, "counter", 0)

		require.NoError(t, err)
		assert.Equal(t, int64(10), result)
	})

	t.Run("returns error for non integer value", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "counter", "hello", 0))

		_, err := c.Increment(ctx, "counter", 1)

		assert.Error(t, err)
	})

	t.Run("expired key behaves as missing", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "counter", "100", 30*time.Millisecond))

		time.Sleep(50 * time.Millisecond)

		result, err := c.Increment(ctx, "counter", 5)

		require.NoError(t, err)
		assert.Equal(t, int64(5), result)

		value, err := c.Get(ctx, "counter")
		require.NoError(t, err)
		assert.Equal(t, "5", value)
	})

	t.Run("preserves ttl", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "counter", "10", 100*time.Millisecond))

		result, err := c.Increment(ctx, "counter", 5)

		require.NoError(t, err)
		assert.Equal(t, int64(15), result)

		time.Sleep(120 * time.Millisecond)

		_, err = c.Get(ctx, "counter")
		assert.ErrorIs(t, err, cache.ErrNotFound)
	})
}

func TestMemoryCache_Expire(t *testing.T) {
	ctx := context.Background()
	c := New()

	t.Run("rejects negative ttl", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 0))

		err := c.Expire(ctx, "key", -time.Second)

		assert.ErrorIs(t, err, cache.ErrInvalidTTL)
	})

	t.Run("returns not found for missing key", func(t *testing.T) {
		err := c.Expire(ctx, "missing", time.Second)

		assert.ErrorIs(t, err, cache.ErrNotFound)
	})

	t.Run("sets expiration", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 0))
		require.NoError(t, c.Expire(ctx, "key", 30*time.Millisecond))

		value, err := c.Get(ctx, "key")
		require.NoError(t, err)
		assert.Equal(t, "value", value)

		time.Sleep(50 * time.Millisecond)

		_, err = c.Get(ctx, "key")
		assert.ErrorIs(t, err, cache.ErrNotFound)
	})

	t.Run("zero ttl removes expiration", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 30*time.Millisecond))
		require.NoError(t, c.Expire(ctx, "key", 0))

		time.Sleep(50 * time.Millisecond)

		value, err := c.Get(ctx, "key")

		require.NoError(t, err)
		assert.Equal(t, "value", value)
	})

	t.Run("can replace existing ttl", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 50*time.Millisecond))
		require.NoError(t, c.Expire(ctx, "key", 200*time.Millisecond))

		time.Sleep(80 * time.Millisecond)

		value, err := c.Get(ctx, "key")

		require.NoError(t, err)
		assert.Equal(t, "value", value)
	})

	t.Run("expired key is treated as missing", func(t *testing.T) {
		require.NoError(t, c.Set(ctx, "key", "value", 30*time.Millisecond))

		time.Sleep(50 * time.Millisecond)

		err := c.Expire(ctx, "key", time.Second)

		assert.ErrorIs(t, err, cache.ErrNotFound)
	})
}

func TestMemoryCache_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	c := New()

	const (
		goroutines     = 100
		incrementsEach = 100
	)

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			for range incrementsEach {
				_, err := c.Increment(ctx, "counter", 1)
				require.NoError(t, err)
			}
		}()
	}

	wg.Wait()

	value, err := c.Get(ctx, "counter")
	require.NoError(t, err)
	assert.Equal(t, int64(goroutines*incrementsEach), func() int64 {
		result, err := strconv.ParseInt(value, 10, 64)
		require.NoError(t, err)
		return result
	}())
}
