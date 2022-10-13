package globalkey

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hnhuaxi/platform/logger"
	"golang.org/x/exp/constraints"
)

type CounterKey[T any] interface {
	Inc() (T, error)
	IncBy(c T) (T, error)
	Dec() (T, error)
	DecBy(c T) (T, error)
	Load() (T, bool)
	Store(T) bool
	Remove() bool
	Subscribe() chan T
}

type KeyOption struct {
	Expires      time.Duration
	Suffer       string
	Publish      bool
	PublishTopic string
	PatternFunc  func() string
	Log          *logger.Logger
}

type KeyOptionFunc func(opt *KeyOption)

func OptExpires(dt time.Duration) KeyOptionFunc {
	return func(opt *KeyOption) {
		opt.Expires = dt
	}
}

func OptSuffer(suffer string) KeyOptionFunc {
	return func(opt *KeyOption) {
		opt.Suffer = suffer
	}
}

func OptPublish(name ...string) KeyOptionFunc {
	return func(opt *KeyOption) {
		opt.Publish = true
		if len(name) > 0 {
			opt.PublishTopic = name[0]
		}
	}
}

func OptLogger(logger *logger.Logger) KeyOptionFunc {
	return func(opt *KeyOption) {
		opt.Log = logger
	}
}

type counterKey[T constraints.Integer] struct {
	key      string
	rediscli *redis.Client
	Option   KeyOption
}

func NewCounterKey[T constraints.Integer](key string, rediscli *redis.Client, ops ...KeyOptionFunc) CounterKey[T] {
	var opts KeyOption
	for _, op := range ops {
		op(&opts)
	}

	return &counterKey[T]{
		key:      key,
		rediscli: rediscli,
		Option:   opts,
	}
}

func (key *counterKey[T]) Inc() (T, error) {
	var (
		ctx  = context.Background()
		opts = key.Option
		z    T
		incr *redis.IntCmd
		err  error
	)

	_, err = key.rediscli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		incr = pipe.Incr(ctx, key.key)
		if opts.Expires > 0 {
			pipe.Expire(ctx, key.key, opts.Expires)
		}
		return nil
	})

	if err != nil {
		return z, err
	}

	if opts.Publish {
		key.publish((T)(incr.Val()))
	}

	return (T)(incr.Val()), nil
}

func (key *counterKey[T]) IncBy(c T) (T, error) {
	var (
		ctx  = context.Background()
		opts = key.Option
		z    T
		err  error
		incr *redis.IntCmd
	)

	_, err = key.rediscli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		incr = pipe.IncrBy(ctx, key.key, int64(c))
		if opts.Expires > 0 {
			pipe.Expire(ctx, key.key, opts.Expires)
		}
		return nil
	})
	if err != nil {
		return z, err
	}

	if opts.Publish {
		key.publish((T)(incr.Val()))
	}
	return (T)(incr.Val()), nil
}

func (key *counterKey[T]) debug(f string, args ...interface{}) {
	if key.Option.Log != nil {
		log := key.Option.Log.Sugar()
		log.Debugf(f, args...)
	}
}

func (key *counterKey[T]) Dec() (T, error) {
	var (
		ctx  = context.Background()
		opts = key.Option
		z    T
		decr *redis.IntCmd
		err  error
	)

	_, err = key.rediscli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		decr = pipe.Decr(ctx, key.key)
		if opts.Expires > 0 {
			pipe.Expire(ctx, key.key, opts.Expires)
		}
		return nil
	})
	if err != nil {
		return z, err
	}

	if opts.Publish {
		key.publish((T)(decr.Val()))
	}
	return (T)(decr.Val()), nil
}

func (key *counterKey[T]) DecBy(c T) (T, error) {
	var (
		ctx  = context.Background()
		opts = key.Option
		z    T
		err  error
		decr *redis.IntCmd
	)

	_, err = key.rediscli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		decr = pipe.DecrBy(ctx, key.key, int64(c))
		if opts.Expires > 0 {
			pipe.Expire(ctx, key.key, opts.Expires)
		}
		return nil
	})
	if err != nil {
		return z, err
	}
	if opts.Publish {
		key.publish((T)(decr.Val()))
	}
	return (T)(decr.Val()), nil
}

func (key *counterKey[T]) Load() (T, bool) {
	var (
		ctx  = context.Background()
		opts = key.Option
		z    T
	)

	v, err := key.rediscli.GetEx(ctx, key.key, opts.Expires).Int64()
	if err != nil {
		return z, false
	}

	return (T)(v), true
}

func (key *counterKey[T]) Store(value T) bool {
	var (
		ctx  = context.Background()
		opts = key.Option
	)

	err := key.rediscli.Set(ctx, key.key, value, opts.Expires).Err()
	if opts.Publish {
		key.publish(value)
	}
	return err == nil
}

func (key *counterKey[T]) Remove() bool {
	var (
		ctx = context.Background()
	)

	_, err := key.rediscli.Del(ctx, key.key).Result()
	return err == nil
}

func (key *counterKey[T]) Subscribe() chan T {
	var (
		ctx   = context.Background()
		ch    = make(chan T)
		topic = key.key
	)

	if len(key.Option.PublishTopic) > 0 {
		topic = key.Option.PublishTopic
	}
	pubsub := key.rediscli.Subscribe(ctx, topic)
	go func() {
		var value T
		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				return
			}

			if err = json.Unmarshal([]byte(msg.Payload), &value); err != nil {
				return
			}

			ch <- value
		}
	}()

	return ch
}

func (key *counterKey[T]) publish(val T) error {
	var (
		ctx   = context.Background()
		topic = key.key
	)

	if len(key.Option.PublishTopic) > 0 {
		topic = key.Option.PublishTopic
	}

	b, err := json.Marshal(val)
	if err != nil {
		return err
	}

	key.debug("publish %v to key %s", val, topic)
	_, err = key.rediscli.Publish(ctx, topic, b).Result()
	return err
}
