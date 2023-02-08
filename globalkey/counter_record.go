package globalkey

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hnhuaxi/platform/logger"
	"golang.org/x/exp/constraints"
)

type CounterRecord[K any, T constraints.Integer] interface {
	Inc(k K) (T, error)
	IncBy(k K, c T) (T, error)
	Dec(k K) (T, error)
	DecBy(k K, c T) (T, error)
	Load(k K) (T, bool)
	Store(k K, value T) bool
	Remove(k K) bool
	Subscribe(k K) chan T
	SubscribeAll() chan Sub[T]
}

type Sub[T any] struct {
	Topic string
	Value T
}

type PatternFunc func(prefix string, key any) string

type SetOption struct {
	Expires      time.Duration
	Publish      bool
	PublishTopic string
	PatternFunc  PatternFunc
	Log          *logger.Logger
}

type SetOptionFunc func(*SetOption)

type counterRecord[K any, T constraints.Integer] struct {
	Prefix   string
	rediscli *redis.Client
	Option   SetOption
}

func OptSetExpires(dt time.Duration) SetOptionFunc {
	return func(opt *SetOption) {
		opt.Expires = dt
	}
}

func OptSetPattern(fn func(prefix string, key any) string) SetOptionFunc {
	return func(opt *SetOption) {
		opt.PatternFunc = fn
	}
}

func OptSetPublish(name ...string) SetOptionFunc {
	return func(opt *SetOption) {
		opt.Publish = true
		if len(name) > 0 {
			opt.PublishTopic = name[0]
		}
	}
}

func OptSetLogger(logger *logger.Logger) SetOptionFunc {
	return func(opt *SetOption) {
		opt.Log = logger
	}
}

var DefaultPattern = func(prefix string, k any) string {
	return fmt.Sprintf("%s:%v", prefix, k)
}

func NewCounterRecord[K any, T constraints.Integer](prefix string, rediscli *redis.Client, ops ...SetOptionFunc) CounterRecord[K, T] {
	var opts SetOption
	for _, op := range ops {
		op(&opts)
	}

	return &counterRecord[K, T]{
		Prefix:   prefix,
		rediscli: rediscli,
		Option:   opts,
	}
}

func (set *counterRecord[K, T]) getKey(k K) CounterKey[T] {
	var (
		pattern = DefaultPattern
		ops     []KeyOptionFunc
	)

	if set.Option.PatternFunc != nil {
		pattern = set.Option.PatternFunc
	}

	if set.Option.Log != nil {
		ops = append(ops, OptLogger(set.Option.Log))
	}

	if set.Option.Publish {
		ops = append(ops, OptPublish(set.Option.PublishTopic))
	}

	if set.Option.Expires > 0 {
		ops = append(ops, OptExpires(set.Option.Expires))
	}

	return NewCounterKey[T](pattern(set.Prefix, k), set.rediscli, ops...)
}

func (set *counterRecord[K, T]) all() CounterKey[T] {
	var (
		pattern = DefaultPattern
		ops     []KeyOptionFunc
	)

	if set.Option.PatternFunc != nil {
		pattern = set.Option.PatternFunc
	}

	return NewCounterKey[T](pattern(set.Prefix, "*"), set.rediscli, ops...)
}

func (set *counterRecord[K, T]) Inc(k K) (T, error) {
	return set.getKey(k).Inc()
}

func (set *counterRecord[K, T]) IncBy(k K, c T) (T, error) {
	return set.getKey(k).IncBy(c)
}

func (set *counterRecord[K, T]) Dec(k K) (T, error) {
	return set.getKey(k).Dec()
}

func (set *counterRecord[K, T]) DecBy(k K, c T) (T, error) {
	return set.getKey(k).DecBy(c)
}

func (set *counterRecord[K, T]) Load(k K) (T, bool) {
	return set.getKey(k).Load()
}

func (set *counterRecord[K, T]) Store(k K, value T) bool {
	return set.getKey(k).Store(value)
}

func (set *counterRecord[K, T]) Remove(k K) bool {
	return set.getKey(k).Remove()
}

func (set *counterRecord[K, T]) Subscribe(k K) chan T {
	return set.getKey(k).Subscribe()
}

func (set *counterRecord[K, T]) SubscribeAll() chan Sub[T] {
	var (
		pattern = DefaultPattern
		ctx     = context.Background()
		ch      = make(chan Sub[T])
	)

	if set.Option.PatternFunc != nil {
		pattern = set.Option.PatternFunc
	}

	pubsub := set.rediscli.PSubscribe(ctx, pattern(set.Prefix, "*"))
	go func() {
		for {
			var sub Sub[T]
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				return
			}

			sub.Topic = msg.Channel
			if err = json.Unmarshal([]byte(msg.Payload), &sub.Value); err != nil {
				return
			}

			ch <- sub
		}
	}()

	return ch
}
