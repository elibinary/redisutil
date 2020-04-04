package redisutil

import (
	"context"
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

func TestClient_GetLock(t *testing.T) {
	cli := dialCli()
	key := "test_lock_get_lock"
	ctx := context.Background()
	lock, err := cli.GetLock(ctx, key, 2*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, key, lock.Key)
	assert.True(t, lock.value > 0)

	lock2, err := cli.GetLock(ctx, key, time.Second)
	assert.Error(t, err)
	assert.True(t, err == TimeOutErr)
	assert.Nil(t, lock2)

	cli.Free(ctx, lock)
}

// TODO other cases
