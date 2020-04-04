package redisutil

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/assert"
)

type rediser struct {
	cli *redis.Client
}

func (r *rediser) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	// do something with ctx
	return r.cli.Set(key, value, expiration)
}

func (r *rediser) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	// do something with ctx
	return r.cli.SetNX(key, value, expiration)
}

func (r *rediser) GetSet(ctx context.Context, key string, value interface{}) *redis.StringCmd {
	// do something with ctx
	return r.cli.GetSet(key, value)
}

func (r *rediser) Get(ctx context.Context, key string) *redis.StringCmd {
	// do something with ctx
	return r.cli.Get(key)
}

func (r *rediser) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	// do something with ctx
	return r.cli.Del(keys...)
}

func dialCli() *Client {
	redCli := redis.NewClient(&redis.Options{
		Addr:       "localhost:6379",
		DB:         0,
		MaxRetries: 3,
		PoolSize:   8,
	})

	return &Client{
		Redis:   &rediser{cli: redCli},
		Timeout: time.Second,
	}
}

// case 1: th1 get lock success and hold 2s, th2 timeout
func TestClient_GetLock(t *testing.T) {
	cli := dialCli()
	key := "test_get_lock"
	ctx := context.Background()
	lock, err := cli.GetLock(ctx, key, 2*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, key, lock.Key)
	assert.True(t, lock.value > time.Now().UnixNano())
	assert.True(t, lock.value <= time.Now().Add(2*time.Second).UnixNano())

	// SetNX return false
	ok, err := cli.Redis.SetNX(ctx, key, time.Now().UnixNano(), 0).Result()
	assert.NoError(t, err)
	assert.False(t, ok)

	// loop to timeout
	lock2, err := cli.GetLock(ctx, key, time.Second)
	assert.Error(t, err)
	assert.True(t, err == TimeOutErr)
	assert.Nil(t, lock2)
	assert.True(t, time.Now().UnixNano() < lock.value)

	err = cli.Free(ctx, lock)
	assert.NoError(t, err)
}

// case 2:
//   1. th1 get lock success and hold 600ms
//   2. th2 loop to fetch
//   3. th1 lock expired
//   4. th2 get lock and rewrite expiration
func TestClient_GetLock_Expiration(t *testing.T) {
	cli := dialCli()
	key := "test_get_lock_expiration"
	ctx := context.Background()
	lock, err := cli.GetLock(ctx, key, 600*time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, key, lock.Key)
	assert.True(t, lock.value > time.Now().UnixNano())
	assert.True(t, lock.value <= time.Now().Add(600*time.Millisecond).UnixNano())

	// loop 600ms and get lock
	lock2, err := cli.GetLock(ctx, key, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, key, lock2.Key)
	assert.True(t, lock2.value > time.Now().UnixNano())
	assert.True(t, lock2.value <= time.Now().Add(time.Second).UnixNano())
	// now it's has expired
	assert.True(t, time.Now().UnixNano() > lock.value)

	assert.NoError(t, cli.Free(ctx, lock))
	val, err := cli.Redis.Get(ctx, key).Int64()
	assert.NoError(t, err)
	assert.True(t, val > 0)

	assert.NoError(t, cli.Free(ctx, lock2))
	val, err = cli.Redis.Get(ctx, key).Int64()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "redis: nil"))
}
