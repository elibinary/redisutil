package redisutil

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/sirupsen/logrus"
)

const stepDuration = 100 * time.Millisecond

var (
	TimeOutErr = errors.New("timeout on lock")
)

type operator interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	GetSet(ctx context.Context, key string, value interface{}) *redis.StringCmd

	Get(ctx context.Context, key string) *redis.StringCmd

	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

type Client struct {
	Redis operator

	Timeout time.Duration
}

type Lock struct {
	Key string

	value int64
}

func (cli *Client) GetLock(ctx context.Context, key string, expiration time.Duration) (*Lock, error) {
	if cli.Timeout == 0 {
		val, ok, err := cli.lockup(ctx, key, expiration)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, TimeOutErr
		}

		return &Lock{
			Key:   key,
			value: val,
		}, nil
	}

	start := time.Now()
	i := start
	for ; i.Before(start.Add(cli.Timeout)); i = time.Now() {
		logrus.Infof("For, exp: %v", expiration)
		val, ok, err := cli.lockup(ctx, key, expiration)
		if err != nil {
			return nil, err
		}

		if ok {
			return &Lock{
				Key:   key,
				value: val,
			}, nil
		}

		time.Sleep(stepDuration)
	}

	return nil, TimeOutErr
}

func (cli *Client) Free(ctx context.Context, l *Lock) error {
	// only DEL it if it hasn't expired
	if l.value > time.Now().UnixNano() {
		return cli.Redis.Del(ctx, l.Key).Err()
	}

	return nil
}

// 1. SETNX key, expiration time
// 2. if true, success and return
// 3. if false, lock is being held
// 4. now check to see if it's expired
// 5. GET key and check expiration time
// 6. if it's expired, GETSET new value
// 7. check result for GET and GETSET
func (cli *Client) lockup(ctx context.Context, key string, expiration time.Duration) (int64, bool, error) {
	exp := cli.generateExpiration(expiration)
	ok, err := cli.Redis.SetNX(ctx, key, exp, 0).Result()
	if err != nil {
		return 0, false, err
	}

	if ok {
		// success
		return exp, true, nil
	}

	// lock is being held
	val, err := cli.Redis.Get(ctx, key).Int64()
	if err != nil {
		logrus.Errorf("redis.get error: %v", err)
		return 0, false, err
	}

	if val < time.Now().UnixNano() {
		exp = cli.generateExpiration(expiration)
		oldVal, err := cli.Redis.GetSet(ctx, key, exp).Int64()
		if err != nil {
			logrus.Errorf("redis.getset error: %v", err)
			return 0, false, err
		}

		if oldVal < time.Now().UnixNano() {
			// success
			return exp, true, nil
		}
	}

	return 0, false, nil
}

func (cli *Client) generateExpiration(expiration time.Duration) int64 {
	if expiration < time.Millisecond {
		return time.Now().Add(time.Second).UnixNano()
	}

	return time.Now().Add(expiration).UnixNano()
}
